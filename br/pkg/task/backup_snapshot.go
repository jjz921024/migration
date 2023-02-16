package task

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strconv"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/migration/br/pkg/glue"
	"github.com/tikv/migration/br/pkg/storage"
	"github.com/tikv/migration/br/pkg/summary"
	"github.com/tikv/migration/br/pkg/utils"
	"go.uber.org/zap"
)

const (
	backupFileName = "data_"
	defaultPrefix  = "all"

	flagSnapshotTs = "snapshotTs"
	flagFileSize = "fileSize"
	flagPrefix   = "prefix"
)

type TxnKvConfig struct {
	Config
	CompressionConfig

	Prefix      string `json:"prefix" toml:"prefix"`
	SnapshotTs  int64 `json:"snapshotTs" toml:"snapshotTs"`
	FileSize    int    `json:"fileSize" toml:"fileSize"`
}

// DefineSnapshotBackupFlags 定义snapshot子命令相关的选项
func DefineSnapshotBackupFlags(command *cobra.Command) {
	command.Flags().String(flagPrefix, defaultPrefix, "backup data which special prefix")
	command.Flags().String(flagSnapshotTs, "", "snapshot timestamp. Default value is current ts.\n"+
		"support TSO or datetime, format: '2018-05-11 01:42:23.000'")
	command.Flags().Int(flagFileSize, 100*1024*1024, "the size of each backup file")

	cobra.MarkFlagRequired(command.Flags(), flagBackupTS)
}

// ParseBackupConfigFromFlags 解析命令行上的参数, 并赋值给cfg
func (cfg *TxnKvConfig) ParseBackupConfigFromFlags(flags *pflag.FlagSet) error {
	var err error

	ts, err := flags.GetString(flagSnapshotTs)
	if err != nil {
		return errors.Trace(err)
	}
	if cfg.SnapshotTs, err = ParseTSString(ts); err != nil {
		return errors.Trace(err)
	}

	cfg.Prefix, err = flags.GetString(flagPrefix)
	if err != nil {
		return errors.Trace(err)
	}

	cfg.FileSize, err = flags.GetInt(flagFileSize)
	if err != nil {
		return errors.Trace(err)
	}

	compressionCfg, err := ParseCompressionFlags(flags)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.CompressionConfig = *compressionCfg
	level, err := flags.GetInt32(flagCompressionLevel)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.CompressionLevel = level

	if err = cfg.Config.ParseFromFlags(flags); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// RunBackupTxn 执行事务kv备份
func RunBackupTxn(c context.Context, g glue.Glue, cmdName string, cfg *TxnKvConfig) error {
	cfg.adjust()

	defer summary.Summary(cmdName)
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	// 创建tikv连接
	cli, err := txnkv.NewClient(cfg.PD)
	if err != nil {
		return errors.Trace(err)
	}

	snapshotTso := oracle.ComposeTS(cfg.SnapshotTs, 0)
	txn, err := cli.Begin(tikv.WithStartTS(snapshotTso))
	if err != nil {
		return errors.Trace(err)
	}

	// 过滤指定前缀的key
	startKey, endKey := []byte(""), []byte("")
	if cfg.Prefix != defaultPrefix {
		startKey = []byte(cfg.Prefix)
		endKey = utils.IncreatBytes(startKey)
	}

	snapshot := txn.GetSnapshot()
	iter, err := snapshot.Iter(startKey, endKey)
	if err != nil {
		return errors.Trace(err)
	}

	// 解析后端存储配置
	// eg: file://backup-dir/
	u, err := storage.ParseBackend(cfg.Storage, &cfg.BackendOptions)
	if err != nil {
		return errors.Trace(err)
	}

	// 创建存储客户端
	s, err := createStorage(ctx, u, cfg)
	if err != nil {
		return errors.Trace(err)
	}

	log.Info("start backup txn kv",
		zap.String("storage", cfg.Storage),
		zap.String("prefix", cfg.Prefix),
		zap.Int64("snapshotTs", cfg.SnapshotTs),
		zap.Int("size", cfg.FileSize),
		zap.String("compression", cfg.CompressionConfig.CompressionType.String()),
	)

	bCnt, fIdx := 0, 0
	var writer storage.ExternalFileWriter
	defer func() {
		if writer != nil {
			writer.Close(ctx)
		}
	}()

	var buf bytes.Buffer
	for iter.Valid() {
		if writer == nil || bCnt >= cfg.FileSize {
			if writer != nil {
				if err = writer.Close(ctx); err != nil {
					log.Warn("close file err", zap.Error(err))
					return err
				}
			}

			path := mkdirBackupPath(u.GetLocal().Path, cli.GetClusterID(), cfg.Prefix, cfg.SnapshotTs, fIdx)
			writer, err = s.Create(ctx, path)
			if err != nil {
				log.Warn("create writer error", zap.String("path", path))
				return err
			}

			bCnt = 0
			fIdx++
			log.Info("output dump file", zap.String("path", path))
		}

		buf.Write(iter.Key())
		buf.WriteByte('\t')
		buf.Write(iter.Value())
		buf.WriteByte('\n')

		i, err := writer.Write(ctx, buf.Bytes())
		if err != nil {
			log.Warn("write file err", zap.Error(err))
			return err
		}

		bCnt += i
		buf.Reset()
		iter.Next()
	}

	// Set task summary to success status.
	summary.SetSuccessStatus(true)
	return nil
}

// 在指定backup目录下创建backup子目录
// 目录名: <clusterID>_<prefix>_<tso>
func mkdirBackupPath(p string, clusterID uint64, prefix string, ts int64, idx int) string {
	_, err := os.Stat(p)
	if err != nil {
		_ = os.MkdirAll(p, 644)
	}

	subPath := strconv.FormatUint(clusterID, 10) + "_" + prefix + "_" + strconv.FormatInt(ts, 10)
	join := filepath.Join(subPath, backupFileName + strconv.Itoa(idx))
	_ = os.Mkdir(join, 644)

	return join
}

func createStorage(ctx context.Context, backend *backuppb.StorageBackend, cfg *TxnKvConfig) (storage.ExternalStorage, error) {
	var err error
	opts := &storage.ExternalStorageOptions{
		NoCredentials:   cfg.NoCreds,
		SendCredentials: cfg.SendCreds,
	}
	s, err := storage.New(ctx, backend, opts)
	if err != nil {
		return nil, errors.Trace(err)
	}

	storage.WithCompression(s, storage.CompressType(cfg.CompressionConfig.CompressionType))
	return s, nil
}

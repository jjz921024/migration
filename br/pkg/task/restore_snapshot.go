package task

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/migration/br/pkg/glue"
	"github.com/tikv/migration/br/pkg/storage"
	"github.com/tikv/migration/br/pkg/summary"
	"go.uber.org/zap"
)

type RestoreTxnKvConfig struct {
	Config
}

// DefineSnapshotRestoreFlags 定义restore snapshot子命令相关的选项
func DefineSnapshotRestoreFlags(command *cobra.Command) {
	command.Flags().String(flagCompressionType, "none",
		"restore file compression algorithm, value can be one of 'none|gzip|lz4|zstd|snappy'")
}

func (cfg *RestoreTxnKvConfig) ParseRestoreConfigFromFlags(flags *pflag.FlagSet) error {
	var err error
	cfg.CompressType, err = ParseCompressFlag(flags)
	if err != nil {
		return errors.Trace(err)
	}

	if err = cfg.Config.ParseFromFlags(flags); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func RunRestoreSnapshot(c context.Context, g glue.Glue, cmdName string, cfg *RestoreTxnKvConfig) (err error) {
	cfg.adjust()

	defer summary.Summary(cmdName)
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	backend, err := storage.ParseBackend(cfg.Storage, &cfg.BackendOptions)
	if err != nil {
		return errors.Trace(err)
	}

	s, err := createStorage(ctx, backend, cfg.CompressType)
	if err != nil {
		return errors.Trace(err)
	}

	// 创建tikv连接
	cli, err := txnkv.NewClient(cfg.PD)
	if err != nil {
		return errors.Trace(err)
	}
	defer cli.Close()

	log.Info("start restore txn kv",
		zap.String("storage", cfg.Storage),
		zap.String("compression", cfg.CompressType.Name()),
	)

	filepath.WalkDir(backend.GetLocal().Path, func(path string, d fs.DirEntry, _ error) error {
		// 只遍历给定路径下的文件
		if d.IsDir() || !strings.HasPrefix(d.Name(), backupFileName) {
			log.Info("ignore", zap.String("entry", d.Name()))
			return nil
		}

		log.Info("start restore file", zap.String("file", d.Name()))

		fd, err := s.Open(ctx, d.Name())
		if err != nil {
			log.Warn("fail to open file", zap.String("file", d.Name()), zap.Error(err))
			return errors.Trace(err)
		}
		defer fd.Close()

		txn, err := cli.Begin()
		if err != nil {
			return errors.Trace(err)
		}

		r := bufio.NewReader(fd)
		for {
			// TODO: prefix
			line, _, err := r.ReadLine()
			if err == io.EOF {
				log.Info("finish restore file", zap.String("file", d.Name()))
				break
			}
			if err != nil {
				log.Warn("read file err", zap.String("file", d.Name()), zap.Error(err))
				return errors.Trace(err)
			}

			decodedKey, decodedVal, err := decodeLine(line)
			if err != nil {
				return errors.Trace(err)
			}

			err = txn.Set(decodedKey, decodedVal)
			if err != nil {
				log.Warn("prewrite txn err", zap.Error(err))
				return errors.Trace(err)
			}
		}

		err = txn.Commit(ctx)
		if err != nil {
			log.Warn("commit txn err", zap.Error(err))
			return errors.Trace(err)
		}
		return nil
	})

	return nil
}

func decodeLine(line []byte) ([]byte, []byte, error) {
	s := bytes.SplitN(line, []byte{'|'}, 2)
	if len(s) != 2 {
		log.Warn("illeage format", zap.ByteString("data", line))
		return nil, nil, errors.New("illeage format")
	}

	decodedKey := make([]byte, base64.StdEncoding.DecodedLen(len(s[0])))
	_, err := base64.StdEncoding.Decode(decodedKey, s[0])
	if err != nil {
		log.Warn("decode key data fail", zap.ByteString("data", s[0]))
		return nil, nil, errors.New("decode key data fail")
	}

	decodedVal := make([]byte, base64.StdEncoding.DecodedLen(len(s[1])))
	_, err = base64.StdEncoding.Decode(decodedVal, s[1])
	if err != nil {
		log.Warn("decode value data fail", zap.ByteString("data", s[1]))
		return nil, nil, errors.New("decode value data fail")
	}

	return decodedKey, decodedVal, nil
}

package task

import (
	"context"

	"github.com/fsouza/fake-gcs-server/internal/backend"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/migration/br/pkg/glue"
	"github.com/tikv/migration/br/pkg/summary"
	"go.uber.org/zap"
)

type RestoreTxnKvConfig struct {
	Config
}

// DefineSnapshotRestoreFlags 定义restore snapshot子命令相关的选项
func DefineSnapshotRestoreFlags(command *cobra.Command) {
	// nothing
}

func (cfg *RestoreTxnKvConfig) ParseRestoreConfigFromFlags(flags *pflag.FlagSet) error {
	if err := cfg.Config.ParseFromFlags(flags); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func RunRestoreSnapshot(c context.Context, g glue.Glue, cmdName string, cfg *RestoreTxnKvConfig) (err error) {
	cfg.adjust()

	defer summary.Summary(cmdName)
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	// 创建tikv连接
	cli, err := txnkv.NewClient(cfg.PD)
	if err != nil {
		return errors.Trace(err)
	}

	backend, err := storage.ParseBackend(cfg.Storage, &cfg.BackendOptions)
	if err != nil {
		return errors.Trace(err)
	}



	log.Info("start restore txn kv",
		zap.String("storage", cfg.Storage),
	)





	return nil
}
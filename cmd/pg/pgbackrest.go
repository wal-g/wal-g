package pg

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const pgbackrestCommandDescription = "Interact with pgbackrest backups (beta)"

var pgbackrestCmd = &cobra.Command{
	Use:   "pgbackrest",
	Short: pgbackrestCommandDescription,
}

func init() {
	Cmd.AddCommand(pgbackrestCmd)
}

func configurePgbackrestSettings(ctx context.Context) (folder storage.Folder, stanza string) {
	st, err := internal.ConfigureStorage(ctx)
	tracelog.ErrorLogger.FatalOnError(err)
	stanza, _ = conf.GetSetting(conf.PgBackRestStanza)
	return st.RootFolder(), stanza
}

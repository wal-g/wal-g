package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const pgbackrestCommandDescription = "Interact with pgbackrest backups"

var pgbackrestCmd = &cobra.Command{
	Use:   "pgbackrest",
	Short: pgbackrestCommandDescription,
}

func init() {
	Cmd.AddCommand(pgbackrestCmd)
}

func configurePgbackrestSettings() (folder storage.Folder, stanza string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)
	stanza, _ = internal.GetSetting(internal.PgBackRestStanza)
	return folder, stanza
}

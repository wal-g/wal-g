package sqlserver

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

var confirmed = false

// deleteCmd represents the delete command
var deleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Clears old backups and transaction journals",
}

var deleteBeforeCmd = &cobra.Command{
	Use:     internal.DeleteBeforeUsageExample,
	Example: internal.DeleteBeforeExamples,
	Args:    internal.DeleteBeforeArgsValidator,
	Run:     runDeleteBefore,
}

var deleteRetainCmd = &cobra.Command{
	Use:       internal.DeleteRetainUsageExample,
	Example:   internal.DeleteRetainExamples,
	ValidArgs: internal.StringModifiers,
	Args:      internal.DeleteRetainArgsValidator,
	Run:       runDeleteRetain,
}

var deleteEverythingCmd = &cobra.Command{
	Use:       internal.DeleteEverythingUsageExample,
	Example:   internal.DeleteEverythingExamples,
	ValidArgs: internal.StringModifiersDeleteEverything,
	Args:      internal.DeleteEverythingArgsValidator,
	Run:       runDeleteEverything,
}

func runDeleteEverything(cmd *cobra.Command, args []string) {
	deleteHandler, err := newSQLServerDeleteHandler()
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler.DeleteEverything(confirmed)
}

func runDeleteBefore(cmd *cobra.Command, args []string) {
	deleteHandler, err := newSQLServerDeleteHandler()
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler.HandleDeleteBefore(args, confirmed)
}

func runDeleteRetain(cmd *cobra.Command, args []string) {
	deleteHandler, err := newSQLServerDeleteHandler()
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler.HandleDeleteRetain(args, confirmed)
}

func init() {
	cmd.AddCommand(deleteCmd)
	deleteCmd.AddCommand(deleteBeforeCmd, deleteRetainCmd, deleteEverythingCmd)
	deleteCmd.PersistentFlags().BoolVar(&confirmed, internal.ConfirmFlag, false, "Confirms backup deletion")
}

func newSQLServerDeleteHandler() (*internal.DeleteHandler, error) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	backups, err := internal.GetBackupSentinelObjects(folder)
	if err != nil {
		return nil, err
	}

	backupObjects := make([]internal.BackupObject, 0, len(backups))
	for _, object := range backups {
		backupObjects = append(backupObjects, internal.NewDefaultBackupObject(object))
	}

	return internal.NewDeleteHandler(folder, backupObjects, makeLessFunc()), nil
}

func makeLessFunc() func(object1, object2 storage.Object) bool {
	return func(object1, object2 storage.Object) bool {
		time1, ok1 := utility.TryFetchTimeRFC3999(object1.GetName())
		time2, ok2 := utility.TryFetchTimeRFC3999(object2.GetName())
		if !ok1 || !ok2 {
			return object2.GetLastModified().After(object1.GetLastModified())
		}
		return time1 < time2
	}
}

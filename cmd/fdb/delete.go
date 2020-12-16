package fdb

import (
	"time"

	"github.com/spf13/cobra"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

var confirmed = false

// deleteCmd represents the delete command
var deleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Clears old backups and oplog",
}

var deleteBeforeCmd = &cobra.Command{
	Use:     "before backup_name|timestamp", // TODO : improve description
	Example: internal.DeleteBeforeExamples,
	Args:    internal.DeleteBeforeArgsValidator,
	Run:     runDeleteBefore,
}

var deleteRetainCmd = &cobra.Command{
	Use:       "retain backup_count [--after backup_name|timestamp]", // TODO : improve description
	Example:   internal.DeleteRetainExamples,
	ValidArgs: internal.StringModifiers,
	Args:      internal.DeleteRetainArgsValidator,
	Run: func(cmd *cobra.Command, args []string) {
		afterValue, _ := cmd.Flags().GetString("after")
		if afterValue == "" {
			runDeleteRetain(cmd, args)
		} else {
			runDeleteRetainAfter(cmd, append(args, afterValue))
		}
	},
}

var deleteEverythingCmd = &cobra.Command{
	Use:       internal.DeleteEverythingUsageExample,
	Example:   internal.DeleteEverythingExamples,
	ValidArgs: internal.StringModifiersDeleteEverything,
	Args:      internal.DeleteEverythingArgsValidator,
	Run:       runDeleteEverything,
}

func runDeleteEverything(cmd *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler, err := newFdbDeleteHandler(folder)
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler.DeleteEverything(confirmed)
}

func runDeleteBefore(cmd *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler, err := newFdbDeleteHandler(folder)
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler.HandleDeleteBefore(args, confirmed)
}

func runDeleteRetain(cmd *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler, err := newFdbDeleteHandler(folder)
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler.HandleDeleteRetain(args, confirmed)
}

func runDeleteRetainAfter(cmd *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler, err := newFdbDeleteHandler(folder)
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler.HandleDeleteRetainAfter(args, confirmed)
}

func init() {
	cmd.AddCommand(deleteCmd)
	deleteRetainCmd.Flags().StringP("after", "a", "", "Set the time after which retain backups")
	deleteCmd.AddCommand(deleteBeforeCmd, deleteRetainCmd, deleteEverythingCmd)
	deleteCmd.PersistentFlags().BoolVar(&confirmed, internal.ConfirmFlag, false, "Confirms backup deletion")
}

func newFdbDeleteHandler(folder storage.Folder) (*internal.DeleteHandler, error) {
	backups, err := internal.GetBackupSentinelObjects(folder)
	if err != nil {
		return nil, err
	}

	backupObjects := make([]internal.BackupObject, 0, len(backups))
	for _, object := range backups {
		backupObjects = append(backupObjects, FdbBackupObject{object})
	}

	isPermanent := func(object storage.Object) bool { return true }
	return internal.NewDeleteHandler(folder, backupObjects, makeLessFunc(), isPermanent), nil
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

type FdbBackupObject struct {
	storage.Object
}

func (o FdbBackupObject) IsFullBackup() bool {
	return true
}

func (o FdbBackupObject) GetBackupTime() time.Time {
	return o.Object.GetLastModified()
}

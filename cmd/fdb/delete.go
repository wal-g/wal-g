package fdb

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
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
			runDeleteRetain(cmd.Context(), args)
		} else {
			runDeleteRetainAfter(cmd.Context(), append(args, afterValue))
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
	st, err := internal.ConfigureStorage(cmd.Context())
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler, err := newFdbDeleteHandler(cmd.Context(), st.RootFolder())
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler.DeleteEverything(cmd.Context(), confirmed)
}

func runDeleteBefore(cmd *cobra.Command, args []string) {
	st, err := internal.ConfigureStorage(cmd.Context())
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler, err := newFdbDeleteHandler(cmd.Context(), st.RootFolder())
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler.HandleDeleteBefore(cmd.Context(), args, confirmed)
}

func runDeleteRetain(ctx context.Context, args []string) {
	st, err := internal.ConfigureStorage(ctx)
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler, err := newFdbDeleteHandler(ctx, st.RootFolder())
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler.HandleDeleteRetain(ctx, args, confirmed)
}

func runDeleteRetainAfter(ctx context.Context, args []string) {
	st, err := internal.ConfigureStorage(ctx)
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler, err := newFdbDeleteHandler(ctx, st.RootFolder())
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler.HandleDeleteRetainAfter(ctx, args, confirmed)
}

func init() {
	cmd.AddCommand(deleteCmd)
	deleteRetainCmd.Flags().StringP("after", "a", "", "Set the time after which retain backups")
	deleteCmd.AddCommand(deleteBeforeCmd, deleteRetainCmd, deleteEverythingCmd)
	deleteCmd.PersistentFlags().BoolVar(&confirmed, internal.ConfirmFlag, false, "Confirms backup deletion")
}

func newFdbDeleteHandler(ctx context.Context, folder storage.Folder) (*internal.DeleteHandler, error) {
	backups, err := internal.GetBackupSentinelObjects(ctx, folder)
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

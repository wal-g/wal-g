package mongo

import (
	"regexp"

	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/internal/tracelog"
)

var patternTimeRFC3339 = "[0-9]{8}T[0-9]{6}Z"
var regexpTimeRFC3339 = regexp.MustCompile(patternTimeRFC3339)
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
	Use:       "retain backup_count", // TODO : improve description
	Example:   internal.DeleteRetainExamples,
	ValidArgs: internal.StringModifiers,
	Args:      internal.DeleteRetainArgsValidator,
	Run:       runDeleteRetain,
}

func runDeleteBefore(cmd *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}

	internal.HandleDeleteBefore(folder, args, confirmed, isFullBackup, GetLessFunc(folder))
}

func runDeleteRetain(cmd *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}

	internal.HandleDeleteRetain(folder, args, confirmed, isFullBackup, GetLessFunc(folder))
}

func isFullBackup(object storage.Object) bool {
	return true
}

func init() {
	MongoCmd.AddCommand(deleteCmd)
	deleteCmd.AddCommand(deleteBeforeCmd, deleteRetainCmd)
	deleteCmd.PersistentFlags().BoolVar(&confirmed, internal.ConfirmFlag, false, "Confirms backup deletion")
}

func GetLessFunc(folder storage.Folder) func(object1, object2 storage.Object) bool {
	return func(object1, object2 storage.Object) bool {
		time1, ok1 := tryFetchTimeRFC3999(object1)
		time2, ok2 := tryFetchTimeRFC3999(object2)
		if !ok1 || !ok2 {
			return object2.GetLastModified().After(object1.GetLastModified())
		}
		return time1 < time2
	}
}

func tryFetchTimeRFC3999(object storage.Object) (string, bool) {
	times := regexpTimeRFC3339.FindAllString(object.GetName(), 1)
	if len(times) > 0 {
		return times[0], true
	}
	return "", false
}

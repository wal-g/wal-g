package pg

import (
	"fmt"
	"regexp"
	"time"

	"github.com/spf13/cobra"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

var confirmed = false
var patternBackupName = fmt.Sprintf("base_%[1]s(_D_%[1]s)?", internal.PatternTimelineAndLogSegNo)
var regexpBackupName = regexp.MustCompile(patternBackupName)

// deleteCmd represents the delete command
var deleteCmd = &cobra.Command{
	Use:   "delete",
	Short: internal.DeleteShortDescription, // TODO : improve description
}

var deleteBeforeCmd = &cobra.Command{
	Use:     internal.DeleteBeforeUsageExample, // TODO : improve description
	Example: internal.DeleteBeforeExamples,
	Args:    internal.DeleteBeforeArgsValidator,
	Run:     runDeleteBefore,
}

var deleteRetainCmd = &cobra.Command{
	Use:       internal.DeleteRetainUsageExample, // TODO : improve description
	Example:   internal.DeleteRetainExamples,
	ValidArgs: internal.StringModifiers,
	Run:       runDeleteRetain,
}

var deleteEverythingCmd = &cobra.Command{
	Use:       internal.DeleteEverythingUsageExample, // TODO : improve description
	Example:   internal.DeleteEverythingExamples,
	ValidArgs: internal.StringModifiersDeleteEverything,
	Args:      internal.DeleteEverythingArgsValidator,
	Run:       runDeleteEverything,
}

func runDeleteBefore(cmd *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)
	isFullBackup := func(object storage.Object) bool {
		return postgresIsFullBackup(folder, object)
	}

	backups, err := internal.GetBackupSentinelObjects(folder)
	tracelog.ErrorLogger.FatalOnError(err)

	lessFunc := postgresTimelineAndSegmentNoLess
	backupTimeFunc := getBackupTime
	internal.HandleDeleteBefore(folder, backups, args, confirmed, isFullBackup, lessFunc, backupTimeFunc)
}

func runDeleteRetain(cmd *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)
	isFullBackup := func(object storage.Object) bool {
		return postgresIsFullBackup(folder, object)
	}

	backups, err := internal.GetBackupSentinelObjects(folder)
	tracelog.ErrorLogger.FatalOnError(err)

	lessFunc := postgresTimelineAndSegmentNoLess
	internal.HandleDeleteRetain(folder, backups, args, confirmed, isFullBackup, lessFunc)
}

func runDeleteEverything(cmd *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)
	internal.DeleteEverything(folder, confirmed, args)
}

func init() {
	Cmd.AddCommand(deleteCmd)

	deleteCmd.AddCommand(deleteRetainCmd, deleteBeforeCmd, deleteEverythingCmd)
	deleteCmd.PersistentFlags().BoolVar(&confirmed, internal.ConfirmFlag, false, "Confirms backup deletion")
}

// TODO: create postgres part and move it there, if it will be needed
func postgresTimelineAndSegmentNoLess(object1 storage.Object, object2 storage.Object) bool {
	tl1, segNo1, ok := internal.TryFetchTimelineAndLogSegNo(object1.GetName())
	if !ok {
		return false
	}
	tl2, segNo2, ok := internal.TryFetchTimelineAndLogSegNo(object2.GetName())
	if !ok {
		return false
	}
	return tl1 < tl2 || tl1 == tl2 && segNo1 < segNo2
}

func postgresIsFullBackup(folder storage.Folder, object storage.Object) bool {
	backup := internal.NewBackup(folder.GetSubFolder(utility.BaseBackupPath), fetchBackupName(object))
	sentinel, _ := backup.GetSentinel()
	return !sentinel.IsIncremental()
}

func fetchBackupName(object storage.Object) string {
	return regexpBackupName.FindString(object.GetName())
}

func getBackupTime(backupObject storage.Object) time.Time {
	return backupObject.GetLastModified()
}

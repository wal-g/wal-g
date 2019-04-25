package pg

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/internal/tracelog"
	"regexp"
)

var confirmed = false
var patternLSN = "[0-9A-F]{24}"
var patternBackupName = fmt.Sprintf("base_%[1]s(_D_%[1]s)?", patternLSN)
var regexpLSN = regexp.MustCompile(patternLSN)
var regexpBackupName = regexp.MustCompile(patternBackupName)
var maxCountOfLSN = 2

// DeleteCmd represents the delete command
var DeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: internal.DeleteShortDescription, // TODO : improve description
}

var deleteBeforeCmd = &cobra.Command{
	Use:     "before [FIND_FULL] backup_name|timestamp", // TODO : improve description
	Example: internal.DeleteBeforeExamples,
	Args:    internal.DeleteBeforeArgsValidator,
	Run:     runDeleteBefore,
}

var deleteRetainCmd = &cobra.Command{
	Use:       "retain [FULL|FIND_FULL] backup_count", // TODO : improve description
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
	isFullBackup := func(object storage.Object) bool {
		return postgresIsFullBackup(folder, object)
	}
	internal.HandleDeleteBefore(folder, args, confirmed, isFullBackup, postgresLess)
}

func runDeleteRetain(cmd *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}
	isFullBackup := func(object storage.Object) bool {
		return postgresIsFullBackup(folder, object)
	}
	internal.HandleDeleteRetain(folder, args, confirmed, isFullBackup, postgresLess)
}

func init() {
	PgCmd.AddCommand(DeleteCmd)

	DeleteCmd.AddCommand(deleteRetainCmd, deleteBeforeCmd)
	DeleteCmd.PersistentFlags().BoolVar(&confirmed, internal.ConfirmFlag, false, "Confirms backup deletion")
}

// TODO: create postgres part and move it there, if it will be needed
func postgresLess(object1 storage.Object, object2 storage.Object) bool {
	lsn1, ok := tryFetchLSN(object1)
	if !ok {
		return false
	}
	lsn2, ok := tryFetchLSN(object2)
	if !ok {
		return false
	}
	return lsn1 < lsn2
}

func postgresIsFullBackup(folder storage.Folder, object storage.Object) bool {
	backup := internal.NewBackup(folder.GetSubFolder(internal.BaseBackupPath), fetchBackupName(object))
	sentinel, _ := backup.FetchSentinel()
	return !sentinel.IsIncremental()
}

func tryFetchLSN(object storage.Object) (string, bool) {
	found_lsn := regexpLSN.FindAllString(object.GetName(), maxCountOfLSN)
	if len(found_lsn) > 0 {
		return regexpLSN.FindAllString(object.GetName(), maxCountOfLSN)[0], true
	}
	return "", false
}

func fetchBackupName(object storage.Object) string {
	return regexpBackupName.FindString(object.GetName())
}

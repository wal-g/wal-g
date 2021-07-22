package mysql

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mysql"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

var confirmed = false

// deleteCmd represents the delete command
var deleteCmd = &cobra.Command{
	Use:   "delete", //for example "delete mysql before time"
	Short: "Clears old backups and binlogs",
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
	Args:      internal.DeleteRetainArgsValidator,
	Run:       runDeleteRetain,
}

var deleteEverythingCmd = &cobra.Command{
	Use:       internal.DeleteEverythingUsageExample, // TODO : improve description
	Example:   internal.DeleteEverythingExamples,
	ValidArgs: internal.StringModifiersDeleteEverything,
	Args:      internal.DeleteEverythingArgsValidator,
	Run:       runDeleteEverything,
}

const (
	DeleteTargetUsageExample = "target"
	DeleteTargetExamples     = ""
)

var deleteTargetCmd = &cobra.Command{
	Use:     DeleteTargetUsageExample, // TODO : improve description
	Example: DeleteTargetExamples,
	Args:    cobra.ExactArgs(1),
	Run:     runDeleteTarget,
}

type DeleteHandler struct {
	*internal.DeleteHandler
	permanentObjects map[string]bool
}

func runDeleteEverything(cmd *cobra.Command, args []string) {
	deleteHandler, err := NewMySQLDeleteHandler()
	tracelog.ErrorLogger.FatalOnError(err)
	deleteHandler.HandleDeleteEverything(args, deleteHandler.permanentObjects, confirmed)
}

func runDeleteTarget(cmd *cobra.Command, args []string) {
	deleteHandler, err := NewMySQLDeleteHandler()
	tracelog.ErrorLogger.FatalOnError(err)

	bname := args[0]                                             // backup name
	backupSelector, err := internal.NewBackupNameSelector(bname) //todo: add selection by userdata
	tracelog.ErrorLogger.PrintOnError(err)

	deleteHandler.HandleDeleteTarget(backupSelector, confirmed, false)
}

func runDeleteBefore(cmd *cobra.Command, args []string) {
	deleteHandler, err := NewMySQLDeleteHandler()
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler.HandleDeleteBefore(args, confirmed)
}

func runDeleteRetain(cmd *cobra.Command, args []string) {
	deleteHandler, err := NewMySQLDeleteHandler()
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler.HandleDeleteRetain(args, confirmed)
}

func init() {
	cmd.AddCommand(deleteCmd)
	deleteCmd.AddCommand(deleteBeforeCmd, deleteRetainCmd, deleteEverythingCmd, deleteTargetCmd)
	deleteCmd.PersistentFlags().BoolVar(&confirmed, internal.ConfirmFlag, false, "Confirms backup deletion")
}

func makeLessFunc(folder storage.Folder) func(object1, object2 storage.Object) bool {
	return func(object1, object2 storage.Object) bool {
		time1, ok := utility.TryFetchTimeRFC3999(object1.GetName())
		if !ok {
			time1 = object1.GetLastModified().Format(utility.BackupTimeFormat)
		}
		time2, ok := utility.TryFetchTimeRFC3999(object2.GetName())
		if !ok {
			time2 = object2.GetLastModified().Format(utility.BackupTimeFormat)
		}
		return time1 < time2
	}
}

func permanentObjects(folder storage.Folder) map[string]bool {
	tracelog.InfoLogger.Println("retrieving permanent objects")
	backupTimes, err := internal.GetBackups(folder.GetSubFolder(utility.BaseBackupPath))
	if err != nil {
		return map[string]bool{}
	}

	permanentBackups := map[string]bool{}
	for _, backupTime := range backupTimes {
		meta, err := mysql.NewGenericMetaFetcher().Fetch(
			backupTime.BackupName, folder.GetSubFolder(utility.BaseBackupPath))
		if err != nil {
			tracelog.ErrorLogger.Printf("failed to fetch backup meta for backup %s with error %s, ignoring...",
				backupTime.BackupName, err.Error())
			continue
		}
		if meta.IsPermanent {
			permanentBackups[backupTime.BackupName] = true
		}
	}
	return permanentBackups
}

func IsPermanent(objectName string, permanentBackups map[string]bool) bool {
	if objectName[:len(utility.BaseBackupPath)] == utility.BaseBackupPath {
		backup := objectName[len(utility.BaseBackupPath) : len(utility.BaseBackupPath)+23]
		return permanentBackups[backup]
	}
	// impermanent backup or binlogs
	return false
}

func NewMySQLDeleteHandler() (*DeleteHandler, error) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	backups, err := internal.GetBackupSentinelObjects(folder)
	if err != nil {
		return nil, err
	}

	backupObjects := make([]internal.BackupObject, 0, len(backups))
	for _, object := range backups {
		b := internal.NewDefaultBackupObject(object)
		backupObjects = append(backupObjects, b)
	}

	permanentBackups := permanentObjects(folder)

	return &DeleteHandler{
		DeleteHandler: internal.NewDeleteHandler(folder, backupObjects, makeLessFunc(folder),
			internal.IsPermanentFunc(func(object storage.Object) bool {
				return IsPermanent(object.GetName(), permanentBackups)
			}),
		),
		permanentObjects: permanentBackups,
	}, nil
}

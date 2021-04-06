package mysql

import (
	"path"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mysql"
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

	if p := deleteHandler.permanentObjects; len(p) > 0 {
		tracelog.InfoLogger.Fatalf("found permanent objects %s\n", strings.Join(func() []string {
			ret := make([]string, 0)

			for e := range p {
				ret = append(ret, e)
			}

			return ret
		}(), ","))
	}

	deleteHandler.DeleteEverything(confirmed)
}
func (h *DeleteHandler) deleteTarget(bobj internal.BackupObject, confirmed bool) error {
	return storage.DeleteObjectsWhere(h.Folder.GetSubFolder(utility.BaseBackupPath),
		confirmed,
		func(object storage.Object) bool {
		return strings.HasPrefix(object.GetName(), strings.TrimSuffix(bobj.GetName(), utility.SentinelSuffix))
	})
}

func runDeleteTarget(cmd *cobra.Command, args []string) {
	deleteHandler, err := NewMySQLDeleteHandler()
	tracelog.ErrorLogger.FatalOnError(err)
	bname := args[0] // backup name

	for e := range deleteHandler.permanentObjects {
		if e == bname {
			tracelog.InfoLogger.Fatalf("unable to delete permanent backup %s\n", bname)
		}
	}
	if bobj, err := deleteHandler.FindTargetByName(bname); err != nil {
		tracelog.ErrorLogger.FatalfOnError("unable to delete target backup", err)
	} else if err := deleteHandler.deleteTarget(bobj, confirmed); err != nil {
		tracelog.ErrorLogger.FatalfOnError("unable to delete target backup", err)
	}
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
			return binlogLess(folder, object1, object2)
		}
		time2, ok := utility.TryFetchTimeRFC3999(object2.GetName())
		if !ok {
			return binlogLess(folder, object1, object2)
		}
		return time1 < time2
	}
}

func binlogLess(folder storage.Folder, object1, object2 storage.Object) bool {
	binlogName1, ok := tryFetchBinlogName(folder, object1)
	if !ok {
		return false
	}
	binlogName2, ok := tryFetchBinlogName(folder, object2)
	if !ok {
		return false
	}
	return binlogName1 < binlogName2
}

func tryFetchBinlogName(folder storage.Folder, object storage.Object) (string, bool) {
	name := object.GetName()
	if strings.HasPrefix(name, mysql.BinlogPath) {
		_, name = path.Split(name)
		return name, true
	}
	name = strings.Replace(name, utility.SentinelSuffix, "", 1)
	baseBackupFolder := folder.GetSubFolder(utility.BaseBackupPath)
	backup := internal.NewBackup(baseBackupFolder, name)
	var sentinel mysql.StreamSentinelDto
	err := internal.FetchStreamSentinel(backup, &sentinel)
	if err != nil {
		tracelog.InfoLogger.Println("Fail to fetch stream sentinel " + name)
		return "", false
	}
	return sentinel.BinLogStart, true
}

func permanentObjects(folder storage.Folder) map[string]bool {
	tracelog.InfoLogger.Println("retrieving permanent objects")
	backupTimes, err := internal.GetBackups(folder)
	if err != nil {
		return map[string]bool{}
	}

	permanentBackups := map[string]bool{}
	for _, backupTime := range backupTimes {
		backup, err := internal.GetBackupByName(backupTime.BackupName, utility.BaseBackupPath, folder)
		if err != nil {
			tracelog.ErrorLogger.Printf("failed to get backup by name with error %s, ignoring...", err.Error())
			continue
		}
		meta, err := backup.FetchMeta()
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
		b := mysql.BackupObject{Object: object}
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

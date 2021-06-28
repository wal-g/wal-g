package rocksdb

// import (
// 	"strconv"
// 	"strings"
// 	"time"

// 	"github.com/spf13/cobra"
// 	"github.com/wal-g/tracelog"
// 	"github.com/wal-g/wal-g/internal"
// 	"github.com/wal-g/wal-g/utility"
// 	"google.golang.org/api/storage/v1"
// 	// "github.com/wal-g/wal-g/utility"
// )

// const (
// 	retainAfterFlag = "retain-after"
// 	retainCountFlag = "retain-count"
// 	confirmFlag     = "confirm"
// )

// var (
// 	retainAfter string
// 	retainCount uint
// 	confirmed   bool
// )

// // deleteCmd represents the delete command
// var deleteCmd = &cobra.Command{
// 	Use:   "delete",
// 	Short: "Delete old backups",
// 	Run:   runDelete,
// }

// func runDelete(cmd *cobra.Command, args []string) {
// 	folder, err := internal.ConfigureFolder()
// 	tracelog.ErrorLogger.FatalOnError(err)

// 	objects, err := internal.GetBackupSentinelObjects(folder)
// 	tracelog.ErrorLogger.FatalOnError(err)

// 	backups := make([]internal.BackupObject, 0, len(objects))
// 	for _, object := range objects {
// 		backups = append(backups, internal.NewDefaultBackupObject(object))
// 	}

// 	folder = folder.GetSubFolder(utility.BaseBackupPath)
// 	deleteHandler := internal.NewDeleteHandler(folder, backups, lessFunc)
// 	if cmd.Flags().Changed(retainAfterFlag) {

// 	}
// }

// func lessFunc(object1 storage.Object, object2 storage.Object) bool {
// 	return (strings.Compare(object1.Name, object2.Name) > 0)
// }

// func init() {
// 	cmd.AddCommand(deleteCmd)
// 	deleteCmd.Flags().BoolVar(&confirmed, confirmFlag, false, "Confirms backup deletion")
// 	deleteCmd.Flags().StringVar(&retainAfter, retainAfterFlag, "", "Keep backups newer")
// 	deleteCmd.Flags().UintVar(&retainCount, retainCountFlag, 0, "Keep minimum count")
// }

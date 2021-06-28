package rocksdb

// import (
// 	"time"

// 	"github.com/wal-g/storages/storage"
// 	"github.com/wal-g/wal-g/internal"
// )

// type RocksdbBackupObject struct {
// 	storage.Object
// 	BackupName   string
// 	creationTime time.Time
// }

// func (o RocksdbBackupObject) GetBackupName() string {
// 	return o.BackupName
// }

// func (o RocksdbBackupObject) GetBackupTime() time.Time {
// 	return o.creationTime
// }

// func (o RocksdbBackupObject) IsFullBackup() bool {
// 	return true
// }

// func (o RocksdbBackupObject) GetBaseBackupName() string {
// 	return o.BackupName
// }

// func (o RocksdbBackupObject) GetIncrementFromName() string {
// 	return ""
// }

// func NewDeleteHandler(folder storage.Folder, backups []internal.BackupTime, options ...internal.DeleteHandlerOption) (*internal.DeleteHandler, error) {

// 	return internal.NewDeleteHandler()
// }

// func crateBackupObjectsFromBackups(backups []internal.BackupTime) []internal.BackupObject {
// 	result := make([]internal.BackupObject, 0, len(backups))
// 	for _, backup := range backups {
// 		result = append(result, internal.NewDefaultBackupObject())
// 	}
// }

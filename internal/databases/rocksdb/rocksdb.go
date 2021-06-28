package rocksdb

// #cgo CFLAGS: -I../../../submodules/rocksdb/include
// #cgo LDFLAGS: -L../../../submodules/rocksdb -lrocksdb -lpthread -lrt -ldl -lgflags -lbz2 -lm -lstdc++
// #include "rocksdb/c.h"
import "C"
import (
	"errors"
)

type DatabaseOptions struct {
	DbPath  string
	WalPath string
}

type BackupInfo struct {
	Id         int    `json:"Id"`
	RawSize    uint64 `json:"RawSize"`
	BackupSize uint64 `json:"BackupSize"`
	Timestamp  int64  `json:"Timestamp"`
	BackupName string `json:"BackupName"`
}

type RestoreOptions struct {
	BackupName         string
	CreateDbIfNotExist bool
}

type BackupEngine *C.rocksdb_backup_engine_t

func NewDatabaseOptions(dbPath string, walDirectory string) DatabaseOptions {
	return DatabaseOptions{dbPath, walDirectory}
}

func NewRestoreOptions(backupName string, createDbIfNotExist bool) RestoreOptions {
	return RestoreOptions{backupName, createDbIfNotExist}
}

func OpenDatabase(dbOptions DatabaseOptions) (db *C.rocksdb_t, err error) {
	dbPathC := C.CString(dbOptions.DbPath)
	options := C.rocksdb_options_create()
	C.rocksdb_options_set_wal_dir(options, C.CString(dbOptions.WalPath))
	var cErr *C.char = nil
	db = C.rocksdb_open(options, dbPathC, &cErr)
	C.rocksdb_options_destroy(options)
	if cErr != nil {
		err = errors.New(C.GoString(cErr))
	}
	return
}

func OpenBackupEngine(backupEngineDirectory string, createIfNotExists bool) (be *C.rocksdb_backup_engine_t, err error) {
	bePathC := C.CString(backupEngineDirectory)
	options := C.rocksdb_options_create()
	if createIfNotExists {
		C.rocksdb_options_set_create_if_missing(options, 1)
	}
	var cErr *C.char = nil

	be = C.rocksdb_backup_engine_open(options, bePathC, &cErr)
	C.rocksdb_options_destroy(options)
	if cErr != nil {
		err = errors.New(C.GoString(cErr))
	}
	return
}

func (db *C.rocksdb_t) CloseDb() {
	C.rocksdb_close(db)
}

func (be *C.rocksdb_backup_engine_t) CloseBackupEngine() {
	C.rocksdb_backup_engine_close(be)
}

func (be *C.rocksdb_backup_engine_t) CreateBackup(db *C.rocksdb_t) (backupInfo BackupInfo, err error) {
	var cErr *C.char = nil
	C.rocksdb_backup_engine_create_new_backup(be, db, &cErr)
	if cErr != nil {
		err = errors.New(C.GoString(cErr))
		return
	}

	info := be.GetBackupEngineInfo()
	latestBackupIndex := info.getBackupsCount() - 1

	return BackupInfo{info.getBackupId(latestBackupIndex), 0, 0, info.getBackupTimestamp(latestBackupIndex), ""}, nil
}

func (be *C.rocksdb_backup_engine_t) RestoreBackup(dbOptions DatabaseOptions, backupId int) error {
	restoreOptions := C.rocksdb_restore_options_create()
	dbPathC := C.CString(dbOptions.DbPath)
	walDirC := C.CString(dbOptions.WalPath)
	var errC *C.char = nil
	C.rocksdb_backup_engine_restore_db_from_backup(be, dbPathC, walDirC, restoreOptions, C.uint(uint(backupId)), &errC)
	C.rocksdb_restore_options_destroy(restoreOptions)
	if errC != nil {
		return errors.New(C.GoString(errC))
	}

	return nil
}

func (be *C.rocksdb_backup_engine_t) GetBackupEngineInfo() *C.rocksdb_backup_engine_info_t {
	return C.rocksdb_backup_engine_get_backup_info(be)
}

func (info *C.rocksdb_backup_engine_info_t) getBackupId(index int) int {
	return int(C.rocksdb_backup_engine_info_backup_id(info, C.int(index)))
}

func (info *C.rocksdb_backup_engine_info_t) getBackupsCount() int {
	return int(C.rocksdb_backup_engine_info_count(info))
}

func (info *C.rocksdb_backup_engine_info_t) getBackupSize(index int) uint64 {
	return uint64(C.rocksdb_backup_engine_info_size(info, C.int(index)))
}

func (info *C.rocksdb_backup_engine_info_t) getBackupTimestamp(index int) int64 {
	return int64(C.rocksdb_backup_engine_info_timestamp(info, C.int(index)))
}

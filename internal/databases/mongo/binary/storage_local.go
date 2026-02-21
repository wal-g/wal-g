package binary

import (
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"regexp"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	conf "github.com/wal-g/wal-g/internal/config"
)

const (
	mongoFsLock = "mongod.lock"
)

type LocalStorage struct {
	MongodDBPath string
	whitelist    *regexp.Regexp
}

func CreateLocalStorage(mongodDBPath string) *LocalStorage {
	return &LocalStorage{
		MongodDBPath: mongodDBPath,
		whitelist:    CreateWhiteList(),
	}
}

func (localStorage *LocalStorage) EnsureMongodFsLockFileIsEmpty() error {
	mongoFsLockFilePath := path.Join(localStorage.MongodDBPath, mongoFsLock)

	slog.Info(fmt.Sprintf("Check mongod has been shutdown (file '%v' should be empty)", mongoFsLockFilePath))

	lockFileStat, err := os.Stat(mongoFsLockFilePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			tracelog.WarningLogger.Printf("Mongod lock file '%v' not exists. "+
				"May it remove by previous restore process or manually?", mongoFsLockFilePath)
			return nil
		}
		return errors.Wrapf(err, "check for lock file %s", mongoFsLockFilePath)
	}

	if lockFileStat.Size() != 0 {
		return fmt.Errorf("mongod lock file %s, so it can be run or incorrectly turned off", mongoFsLock)
	}
	return nil
}

func (localStorage *LocalStorage) CleanupMongodDBPath() error {
	slog.Info(fmt.Sprintf("Cleanup data in dbPath '%v'", localStorage.MongodDBPath))

	openedDBPath, err := os.Open(localStorage.MongodDBPath)
	if err != nil {
		return errors.Wrap(err, "open dir")
	}
	defer func() { _ = openedDBPath.Close() }()

	names, err := openedDBPath.Readdirnames(-1)
	if err != nil {
		return errors.Wrap(err, "read file names")
	}
	if len(names) == 0 {
		slog.Warn(fmt.Sprintf("dbPath '%v' is empty already", localStorage.MongodDBPath))
		return nil
	}
	for _, name := range names {
		fullPath := filepath.Join(localStorage.MongodDBPath, name)
		if localStorage.whitelist != nil && localStorage.whitelist.MatchString(name) {
			slog.Info(fmt.Sprintf("skip remove %s", fullPath))
			continue
		}

		err = os.RemoveAll(fullPath)
		if err != nil {
			return errors.Wrapf(err, "unable to remove '%s' in '%s'", name, localStorage.MongodDBPath)
		}
		slog.Info(fmt.Sprintf("remove %s", filepath.Join(localStorage.MongodDBPath, name)))
	}
	return nil
}

func (localStorage *LocalStorage) EnsureEmptyDBPath() error {
	openedPath, err := os.Open(localStorage.MongodDBPath)
	if err != nil {
		return errors.Wrap(err, "open dir")
	}
	defer func() { _ = openedPath.Close() }()

	names, err := openedPath.Readdirnames(-1)
	if err == io.EOF {
		return nil
	}

	if err != nil {
		return errors.Wrap(err, "readdirnames dir")
	}

	for _, name := range names {
		if localStorage.whitelist == nil || !localStorage.whitelist.MatchString(name) {
			return fmt.Errorf("directory '%v' is not empty", localStorage.MongodDBPath)
		}
	}

	return nil
}

func CreateWhiteList() *regexp.Regexp {
	val, ok := conf.GetSetting(conf.MongoDBDeletionProtectionWhitelist)
	re, err := regexp.Compile(val)

	if !ok || err != nil {
		return regexp.MustCompile(`^lost\+found$`)
	}

	return re
}

func (localStorage *LocalStorage) CleanUpExcessFilesOnPartiallyBackup(filter map[string]struct{}) error {
	slog.Info(fmt.Sprintf("Cleanup excess files after partially backup in dbPath '%v'", localStorage.MongodDBPath))

	openedDBPath, err := os.Open(localStorage.MongodDBPath)
	if err != nil {
		return errors.Wrap(err, "open dir")
	}
	defer func() { _ = openedDBPath.Close() }()

	err = filepath.Walk(localStorage.MongodDBPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(localStorage.MongodDBPath, path)
		if err != nil {
			return err
		}
		if _, ok := filter["/"+rel]; !ok && !info.IsDir() {
			abs, err := filepath.Abs(path)
			if err != nil {
				return errors.Wrapf(err, "get abs path to '%s'", path)
			}
			err = os.Remove(abs)
			if err != nil {
				return errors.Wrapf(err, "unable to remove '%s'", abs)
			}
			slog.Info(fmt.Sprintf("remove %s", abs))
		}
		return nil
	})

	return err
}

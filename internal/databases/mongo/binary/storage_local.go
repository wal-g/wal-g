package binary

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/checksum"
	"github.com/wal-g/wal-g/utility"
)

const (
	mongoFsLock = "mongod.lock"
)

type LocalStorage struct {
	MongodDBPath string
}

func CreateLocalStorage(mongodDBPath string) *LocalStorage {
	return &LocalStorage{
		MongodDBPath: mongodDBPath,
	}
}

func (localStorage *LocalStorage) EnsureMongodFsLockFileIsEmpty() error {
	mongoFsLockFilePath := path.Join(localStorage.MongodDBPath, mongoFsLock)

	tracelog.InfoLogger.Printf("Check mongod has been shutdown (file '%v' should be empty)", mongoFsLockFilePath)

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
	tracelog.InfoLogger.Printf("Cleanup data in dbPath '%v'", localStorage.MongodDBPath)

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
		tracelog.WarningLogger.Printf("dbPath '%v' is empty already", localStorage.MongodDBPath)
		return nil
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(localStorage.MongodDBPath, name))
		if err != nil {
			return errors.Wrapf(err, "unable to remove '%s' in '%s'", name, localStorage.MongodDBPath)
		}
		tracelog.InfoLogger.Printf("remove %s", filepath.Join(localStorage.MongodDBPath, name))
	}
	return nil
}

func (localStorage *LocalStorage) EnsureEmptyDBPath() error {
	return localStorage.EnsureEmptyDirectory(localStorage.MongodDBPath)
}

func (localStorage *LocalStorage) EnsureEmptyDirectory(directoryPath string) error {
	openedPath, err := os.Open(directoryPath)
	if err != nil {
		return errors.Wrap(err, "open dir")
	}
	defer func() { _ = openedPath.Close() }()

	_, err = openedPath.Readdirnames(1)
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "readdirnames dir")
	}
	return fmt.Errorf("directory '%v' is not empty", directoryPath)
}

func (localStorage *LocalStorage) CreateDirectories(directoryMetas []*BackupDirectoryMeta) error {
	// todo: sort directories to avoid check on existence
	for _, directoryMeta := range directoryMetas {
		directoryPath := filepath.Join(localStorage.MongodDBPath, directoryMeta.Path)
		_, err := os.Stat(directoryPath)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return err
			}
			// directory not exists
			err = os.MkdirAll(directoryPath, directoryMeta.FileMode)
		} else {
			// directory already exists
			err = os.Chmod(directoryPath, directoryMeta.FileMode)
		}
		if err != nil {
			return err
		}
		userName := "mongodb"  // todo: get from sentinel!!!
		groupName := "mongodb" // todo: get from sentinel!!!
		err = chown(directoryPath, userName, groupName)
		if err != nil {
			return err
		}
	}

	return nil
}

func (localStorage *LocalStorage) IsMongodData(absoluteMongodDataFilePath string) bool {
	return strings.HasPrefix(absoluteMongodDataFilePath, localStorage.MongodDBPath+"/")
}

func (localStorage *LocalStorage) GetRelativeMongodPath(absoluteMongodDataPath string) (string, error) {
	if !strings.HasPrefix(absoluteMongodDataPath, localStorage.MongodDBPath+"/") {
		return "", fmt.Errorf("file %v is not from mongod data path %v", absoluteMongodDataPath,
			localStorage.MongodDBPath)
	}
	return strings.TrimPrefix(absoluteMongodDataPath, localStorage.MongodDBPath+"/"), nil
}

func (localStorage *LocalStorage) GetAbsolutePath(relativeMongodDataPath string) string {
	return filepath.Join(localStorage.MongodDBPath, relativeMongodDataPath)
}

func (localStorage *LocalStorage) CreateReader(backupFileMeta *BackupFileMeta) (io.ReadCloser, error) {
	absoluteFilePath := localStorage.GetAbsolutePath(backupFileMeta.Path)
	return os.Open(absoluteFilePath)
}

func (localStorage *LocalStorage) SaveStreamToMongodFile(inputStream io.Reader, backupFileMeta *BackupFileMeta) error {
	absolutePath := localStorage.GetAbsolutePath(backupFileMeta.Path)
	fileWriter, err := os.Create(absolutePath)
	if err != nil {
		return errors.Wrapf(err, "create destination file <%s>", absolutePath)
	}
	defer utility.LoggedClose(fileWriter, fmt.Sprintf("close backup file writer %v", absolutePath))

	checksumCalculator := checksum.CreateCalculator() // todo: find checksum calculator by name
	if checksumCalculator.Algorithm() != backupFileMeta.Checksum.Algorithm {
		return fmt.Errorf("different checksum algorithms file: %v != backup: %v", checksumCalculator.Algorithm(),
			backupFileMeta.Checksum.Algorithm)
	}
	writerWithChecksum := checksum.CreateWriterWithChecksum(fileWriter, checksumCalculator)

	_, err = utility.FastCopy(&utility.EmptyWriteIgnorer{Writer: writerWithChecksum}, inputStream)
	if err != nil {
		return fmt.Errorf("failed to decompress and decrypt file <%v>: %w", absolutePath, err)
	}

	checksumData := checksumCalculator.Checksum()
	if checksumData != backupFileMeta.Checksum.Data {
		return fmt.Errorf("different checksum data file %v != backup %v", checksumData, backupFileMeta.Checksum.Data)
	}

	return localStorage.ApplyFileOwnerAndPermissions(backupFileMeta)
}

func (localStorage *LocalStorage) ApplyFileOwnerAndPermissions(backupFileMeta *BackupFileMeta) (err error) {
	absolutePath := localStorage.GetAbsolutePath(backupFileMeta.Path)

	err = os.Chmod(absolutePath, backupFileMeta.FileMode)
	if err != nil {
		return errors.Wrapf(err, "change permissions for file <%s>", absolutePath)
	}
	userName := "mongodb"  // todo: get from sentinel!!!
	groupName := "mongodb" // todo: get from sentinel!!!
	return chown(absolutePath, userName, groupName)
}

func (localStorage *LocalStorage) FixFileOwnerOfMongodData() error {
	userName := "mongodb"  // todo: get from sentinel!!!
	groupName := "mongodb" // todo: get from sentinel!!!
	// todo: check if some files own non mongodb user
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	command := exec.CommandContext(ctx, "chown", "-R", userName+"."+groupName, localStorage.MongodDBPath)
	err := command.Start()
	if err != nil {
		return err
	}
	return command.Wait()
}

func chown(path, userName, groupName string) error {
	osUser, err := user.Lookup(userName)
	if err != nil {
		return errors.Wrapf(err, "find mongo user %v", userName)
	}
	osGroup, err := user.LookupGroup(groupName)
	if err != nil {
		return errors.Wrapf(err, "find mongo group %v", groupName)
	}
	uid, err := strconv.Atoi(osUser.Uid)
	if err != nil {
		return errors.Wrapf(err, "parse uid %v", osUser.Gid)
	}
	gid, err := strconv.Atoi(osGroup.Gid)
	if err != nil {
		return errors.Wrapf(err, "parse gid %v", osGroup.Gid)
	}
	err = os.Chown(path, uid, gid)
	if err != nil {
		return errors.Wrapf(err, "chown %v:%v (%v:%v) for file %v", userName, groupName, uid, gid, path)
	}
	return nil
}

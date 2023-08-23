package cache

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/wal-g/wal-g/pkg/storages/storage"
	"golang.org/x/sys/unix"
)

// StatusFile is stored on a disk and therefore shared between all WAL-G processes and commands.
var StatusFile string

func init() {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		panic(fmt.Errorf("failed to get current user's home dir: %w", err))
	}

	StatusFile = filepath.Join(homeDir, ".walg_storage_status_cache")
}

type storageStatuses map[key]status

type key string

func formatKey(name string, hash storage.Hash) key {
	return key(fmt.Sprintf("%s#%d", name, hash))
}

type status struct {
	Alive   bool      `json:"alive"`
	Checked time.Time `json:"checked"`
}

func updateFileContent(oldContent storageStatuses, checkResult map[key]bool) (newContent storageStatuses) {
	newContent = make(storageStatuses, len(oldContent))
	for key, status := range oldContent {
		newContent[key] = status
	}

	checkTime := time.Now()
	for key, alive := range checkResult {
		newContent[key] = status{
			Alive:   alive,
			Checked: checkTime,
		}
	}

	return newContent
}

func readFile() (storageStatuses, error) {
	file, err := os.OpenFile(StatusFile, os.O_RDONLY, 0666)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open cache file: %w", err)
	}
	defer func() { _ = file.Close() }()

	err = lockFile(file, false)
	if err != nil {
		return nil, fmt.Errorf("acquire shared lock for the cache file: %w", err)
	}

	bytes, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("read cache file: %w", err)
	}

	var content storageStatuses
	err = json.Unmarshal(bytes, &content)
	if err != nil {
		return nil, fmt.Errorf("unmarshal cache file content: %w", err)
	}

	return content, nil
}

func writeFile(content storageStatuses) error {
	bytes, err := json.Marshal(content)
	if err != nil {
		return fmt.Errorf("marshal cache file content: %w", err)
	}

	file, err := os.OpenFile(StatusFile, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("open cache file: %w", err)
	}
	defer func() { _ = file.Close() }()

	err = lockFile(file, true)
	if err != nil {
		return fmt.Errorf("acquire exclusive lock for the cache file: %w", err)
	}

	err = file.Truncate(int64(len(bytes)))
	if err != nil {
		return fmt.Errorf("truncate cache file: %w", err)
	}

	_, err = file.Write(bytes)
	if err != nil {
		return fmt.Errorf("write cache file: %w", err)
	}

	return nil
}

func lockFile(file *os.File, exclusive bool) (err error) {
	how := unix.LOCK_SH
	if exclusive {
		how = unix.LOCK_EX
	}

	for {
		err = unix.Flock(int(file.Fd()), how)
		// When calling syscalls directly, we need to retry EINTR errors. They mean the call was interrupted by a signal.
		if err != unix.EINTR {
			break
		}
	}
	return err
}

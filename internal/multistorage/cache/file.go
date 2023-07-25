package cache

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

// StatusFile is stored on a disk and therefore shared between all WAL-G processes and commands.
const StatusFile = ".walg_storage_status_cache"

type storageStatuses map[string]status

type status struct {
	Alive   bool      `json:"alive"`
	Checked time.Time `json:"checked"`
}

func updateFileContent(oldContent storageStatuses, checkResult map[string]bool) (newContent storageStatuses) {
	newContent = make(storageStatuses, len(oldContent))
	for storage, status := range oldContent {
		newContent[storage] = status
	}

	checkTime := time.Now()
	for s, alive := range checkResult {
		newContent[s] = status{
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

	file, err := os.OpenFile(StatusFile, os.O_RDWR, 0666)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open cache file: %w", err)
	}
	defer func() { _ = file.Close() }()

	err = lockFile(file, true)
	if err != nil {
		return fmt.Errorf("acquire exclusive lock for the cache file: %w", err)
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

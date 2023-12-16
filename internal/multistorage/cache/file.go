package cache

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// HomeStatusFile is the default file for storing cache on disk that is shared between all WAL-G processes and commands.
var HomeStatusFile = func() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("can't get user home dir: %w", err)
	}

	return filepath.Join(homeDir, ".walg_storage_status_cache"), nil
}

type storageStatuses map[Key]status

type Key struct {
	Name string
	Hash string
}

func ParseKey(str string) Key {
	delim := strings.LastIndex(str, "#")
	return Key{str[:delim], str[delim+1:]}
}

func (k Key) String() string {
	return fmt.Sprintf("%s#%s", k.Name, k.Hash)
}

type status struct {
	Alive   bool      `json:"alive"`
	Checked time.Time `json:"checked"`
}

func updateFileContent(oldContent storageStatuses, checkResult map[Key]bool) (newContent storageStatuses) {
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

func readFile(path string) (storageStatuses, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0666)
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

	var validJSONContent map[string]status
	err = json.Unmarshal(bytes, &validJSONContent)
	if err != nil {
		return nil, fmt.Errorf("unmarshal cache file content: %w", err)
	}
	content := make(storageStatuses, len(validJSONContent))
	for str, stat := range validJSONContent {
		content[ParseKey(str)] = stat
	}

	return content, nil
}

func writeFile(path string, content storageStatuses) error {
	validJSONContent := make(map[string]status, len(content))
	for key, stat := range content {
		validJSONContent[key.String()] = stat
	}
	bytes, err := json.Marshal(validJSONContent)
	if err != nil {
		return fmt.Errorf("marshal cache file content: %w", err)
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
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

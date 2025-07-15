package cache

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"
)

type SharedFile struct {
	Path    string
	Updated time.Time
}

func NewSharedFile(path string) *SharedFile {
	info, err := os.Stat(path)
	var updated time.Time
	if err == nil {
		updated = info.ModTime()
	} else {
		// File does not exist or is not available
		// Set very low time, so any comparison with timeout will return True
		updated = time.Time{}
	}
	return &SharedFile{
		Path:    path,
		Updated: updated,
	}
}

func (sf *SharedFile) read() (storageStatuses, error) {
	file, err := os.OpenFile(sf.Path, os.O_RDONLY, 0666)
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

	var validJSONContent map[string]storageStatus
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

func (sf *SharedFile) write(content storageStatuses) error {
	validJSONContent := make(map[string]storageStatus, len(content))
	for key, stat := range content {
		validJSONContent[key.String()] = stat
	}
	bytes, err := json.Marshal(validJSONContent)
	if err != nil {
		return fmt.Errorf("marshal cache file content: %w", err)
	}

	file, err := os.OpenFile(sf.Path, os.O_RDWR|os.O_CREATE, 0666)
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

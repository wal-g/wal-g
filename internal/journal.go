package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const (
	JournalSize = "JournalSize"
)

func AddJournalSizeToPreviousBackup(
	root storage.Folder,
	journalPath string,
	sentinelPath string,
	sentinelName string,
	oldBackupTime time.Time,
	newBackupTime time.Time,
) error {
	if len(sentinelName) == 0 {
		return fmt.Errorf("sentinel name is empty")
	}

	folder := root.GetSubFolder(sentinelPath)
	sentinelReader, err := folder.ReadObject(sentinelName)
	if err != nil {
		return err
	}

	rawSentinel, err := io.ReadAll(sentinelReader)
	if err != nil {
		return err
	}

	var sentinel map[string]interface{}
	err = json.Unmarshal(rawSentinel, &sentinel)
	if err != nil {
		return err
	}

	journalSize, err := GetJournalSizeInSemiInterval(root, journalPath, oldBackupTime, newBackupTime)
	if err != nil {
		return err
	}

	sentinel[JournalSize] = journalSize
	sentinelJSON, err := json.Marshal(sentinel)
	if err != nil {
		return err
	}

	r := bytes.NewReader(sentinelJSON)
	err = folder.PutObject(sentinelName, r)
	if err != nil {
		return err
	}

	return nil
}

// (start;end]
func GetJournalSizeInSemiInterval(folder storage.Folder, journalPath string, start, end time.Time) (int64, error) {
	folder = folder.GetSubFolder(journalPath)
	journalFiles, _, err := folder.ListFolder()
	if err != nil {
		return 0, err
	}
	if len(journalFiles) == 0 {
		return 0, nil
	}

	sum := int64(0)
	for i := 0; i < len(journalFiles); i++ {
		jt := journalFiles[i].GetLastModified()
		if start.Before(jt) && (end.After(jt) || end.Equal(jt)) {
			sum += journalFiles[i].GetSize()
		}
	}
	tracelog.InfoLogger.Printf("Journal Sum: %d\n", sum)

	return sum, nil
}

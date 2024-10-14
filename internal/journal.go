package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

const (
	JournalSize = "JournalSize"
)

func AddJournalSizeToPreviousBackup(
	root storage.Folder,
	journalPath string,
	sentinelPath string,
	sentinelName string,
	journalExtractor func(sentinel map[string]interface{}) (firstBackupJournal, lastBackupJournal string),
	journalNameLess func(a, b string) bool,
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
	firstBackupJournal, lastBackupJournal := journalExtractor(sentinel)

	journalSize, err := GetJournalSizeInSemiInterval(
		root,
		journalPath,
		journalNameLess,
		firstBackupJournal,
		lastBackupJournal,
	)
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
func GetJournalSizeInSemiInterval(
	folder storage.Folder,
	journalPath string,
	journalCmpLess func(a, b string) bool,
	start, end string,
) (int64, error) {
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
		jt := utility.TrimFileExtension(journalFiles[i].GetName())

		isEqual := !journalCmpLess(jt, end) && !journalCmpLess(end, jt)

		if journalCmpLess(start, jt) && (journalCmpLess(jt, end) || isEqual) {
			sum += journalFiles[i].GetSize()
		}
	}
	tracelog.InfoLogger.Printf("Journal Sum: %d\n", sum)

	return sum, nil
}

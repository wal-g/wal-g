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
	BackupsInfo = "backups.json"
)

type BackupAndJournalInfo struct {
	JournalStart     string `json:"JournalStart"`
	JournalEnd       string `json:"JournalEnd"`
	JournalSize      int64  `json:"JournalSize"`
	CompressedSize   int64  `json:"CompressedSize"`
	UncompressedSize int64  `json:"UncompressedSize"`
	IsPermanent      bool   `json:"IsPermanent"`
}

func GetBackupInfo(folder storage.Folder, sentinelName string) (BackupAndJournalInfo, error) {
	backupsInfo, err := GetBackupsInfo(folder)
	if err != nil {
		return BackupAndJournalInfo{}, err
	}

	if _, ok := backupsInfo[sentinelName]; !ok {
		return BackupAndJournalInfo{}, fmt.Errorf("%s isn't contained in backups.json", sentinelName)
	}

	return backupsInfo[sentinelName], nil
}

func UploadBackupInfo(folder storage.Folder, sentinelName string, info BackupAndJournalInfo) error {
	backupsInfo, err := GetBackupsInfo(folder)
	if err != nil {
		return err
	}

	backupsInfo[sentinelName] = info

	err = UpdateBackupsInfo(folder, backupsInfo)
	if err != nil {
		return err
	}

	return nil
}

func GetBackupsInfo(folder storage.Folder) (map[string]BackupAndJournalInfo, error) {
	ok, err := folder.Exists(BackupsInfo)
	if !ok {
		tracelog.InfoLogger.Printf("can not find backups.json, creating it...")
		err := folder.PutObject(BackupsInfo, bytes.NewBuffer([]byte("{}")))
		if err != nil {
			return nil, err
		}
	}

	backupsInfo, err := readBackupsInfo(folder)
	if err != nil {
		return nil, err
	}

	return backupsInfo, nil
}

func readBackupsInfo(folder storage.Folder) (map[string]BackupAndJournalInfo, error) {
	backupsInfoReader, err := folder.ReadObject(BackupsInfo)
	if err != nil {
		return nil, err
	}

	backupsInfoBytes, err := io.ReadAll(backupsInfoReader)
	if err != nil {
		return nil, err
	}

	var backupsInfo map[string]BackupAndJournalInfo
	err = json.Unmarshal(backupsInfoBytes, &backupsInfo)
	if err != nil {
		return nil, err
	}

	return backupsInfo, nil
}

func UpdateBackupsInfo(folder storage.Folder, backupsInfo map[string]BackupAndJournalInfo) error {
	rawBackupsInfo, err := json.Marshal(backupsInfo)
	if err != nil {
		return err
	}

	err = folder.PutObject(BackupsInfo, bytes.NewBuffer(rawBackupsInfo))
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

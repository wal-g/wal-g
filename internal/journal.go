package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

const (
	JournalSize = "JournalSize"
	BackupsInfo = "backups.json"
)

type BackupInfo struct {
	JournalStart     string    `json:"JournalStart"`
	JournalEnd       string    `json:"JournalEnd"`
	JournalSize      int64     `json:"JournalSize"`
	CompressedSize   int64     `json:"CompressedSize"`
	UncompressedSize int64     `json:"UncompressedSize"`
	IsPermanent      bool      `json:"IsPermanent"`
	StopLocalTime    time.Time `json:"StopLocalTime"`
}

func GetBackupInfo(folder storage.Folder, sentinelName string) (BackupInfo, error) {
	backupsInfo, err := GetAllBackupsInfo(folder)
	if err != nil {
		return BackupInfo{}, err
	}

	if _, ok := backupsInfo[sentinelName]; !ok {
		return BackupInfo{}, fmt.Errorf("%s isn't contained in backups.json", sentinelName)
	}

	return backupsInfo[sentinelName], nil
}

func UploadBackupInfo(folder storage.Folder, sentinelName string, info BackupInfo) error {
	allBackupsInfo, err := GetAllBackupsInfo(folder)
	if err != nil {
		return err
	}

	allBackupsInfo[sentinelName] = info

	err = UpdateBackupsInfo(folder, allBackupsInfo)
	if err != nil {
		return err
	}

	return nil
}

func UpdatePreviousBackupInfoJournal(folder storage.Folder, journalPath string, newJournalEnd string) error {
	latestSentinelName, latestSentinel, err := GetLastNotPermanentBackupInfo(folder)
	if err != nil {
		return err
	}

	if len(latestSentinelName) == 0 {
		tracelog.WarningLogger.Printf("latest sentinel was not found, we can not evaluate journal size")
		return nil
	}

	journalSize, err := GetJournalSizeInSemiInterval(
		folder,
		journalPath,
		func(a, b string) bool {
			return a < b
		},
		latestSentinel.JournalEnd,
		newJournalEnd,
	)
	if err != nil {
		tracelog.ErrorLogger.Printf("can not evaluate journal sum for %s: %s", latestSentinelName, err)
		return err
	}
	tracelog.InfoLogger.Printf(
		"journal size for %s in the semi interval (%s; %s] is equal to %d",
		latestSentinelName,
		latestSentinel.JournalEnd,
		newJournalEnd,
		journalSize,
	)

	latestBackupInfo, err := GetBackupInfo(folder, latestSentinelName)
	if err != nil {
		tracelog.ErrorLogger.Printf("can not find journal sum in backups.json for %s: %s", latestSentinelName, err)
		return err
	}

	if latestBackupInfo.JournalSize != 0 {
		tracelog.WarningLogger.Printf(
			"previous backup info contains non-zero journal size '%d', its values will be updated to '%d'",
			latestBackupInfo.JournalSize,
			journalSize,
		)
	}

	err = UploadBackupInfo(folder, latestSentinelName, BackupInfo{
		JournalStart:     latestSentinel.JournalStart,
		JournalEnd:       latestSentinel.JournalEnd,
		JournalSize:      journalSize,
		CompressedSize:   latestSentinel.CompressedSize,
		UncompressedSize: latestSentinel.UncompressedSize,
		IsPermanent:      latestSentinel.IsPermanent,
		StopLocalTime:    latestSentinel.StopLocalTime,
	})
	if err != nil {
		tracelog.ErrorLogger.Printf("can not update journal info for %s: %s", latestSentinelName, err)
		return err
	}

	tracelog.ErrorLogger.Printf("journal info has been updated for %s", latestSentinelName)
	return nil
}

func GetLastNotPermanentBackupInfo(folder storage.Folder) (string, BackupInfo, error) {
	allBackupsInfo, err := GetAllBackupsInfo(folder)
	if err != nil {
		return "", BackupInfo{}, err
	}

	lastestBackupInfo := BackupInfo{}
	lastestBackupName := ""

	for k, v := range allBackupsInfo {
		if !v.IsPermanent && v.StopLocalTime.After(lastestBackupInfo.StopLocalTime) {
			lastestBackupName = k
			lastestBackupInfo = v
		}
	}

	return lastestBackupName, lastestBackupInfo, nil
}

func GetAllBackupsInfo(folder storage.Folder) (map[string]BackupInfo, error) {
	ok, err := folder.Exists(BackupsInfo)
	if err != nil {
		return nil, err
	}
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

func readBackupsInfo(folder storage.Folder) (map[string]BackupInfo, error) {
	backupsInfoReader, err := folder.ReadObject(BackupsInfo)
	if err != nil {
		return nil, err
	}

	backupsInfoBytes, err := io.ReadAll(backupsInfoReader)
	if err != nil {
		return nil, err
	}

	var backupsInfo map[string]BackupInfo
	err = json.Unmarshal(backupsInfoBytes, &backupsInfo)
	if err != nil {
		return nil, err
	}

	return backupsInfo, nil
}

func UpdateBackupsInfo(folder storage.Folder, backupsInfo map[string]BackupInfo) error {
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

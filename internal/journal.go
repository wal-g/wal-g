package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

const (
	JournalSize   = "JournalSize"
	JournalPrefix = "journal_"
)

type JournalInfo struct {
	JournalStart string `json:"JournalStart"`
	JournalEnd   string `json:"JournalEnd"`
	JournalSize  int64  `json:"JournalSize"`
}

func UploadBackupInfo(folder storage.Folder, metaName string, info JournalInfo) error {
	folder = folder.GetSubFolder(utility.BaseBackupPath)
	rawBackupsInfo, err := json.Marshal(info)
	if err != nil {
		return err
	}

	err = folder.PutObject(fmt.Sprintf("%s%s", JournalPrefix, metaName), bytes.NewBuffer(rawBackupsInfo))
	if err != nil {
		return err
	}

	return nil
}

func GetLastBackupInfo(folder storage.Folder) (string, JournalInfo, error) {
	objs, _, err := folder.GetSubFolder(utility.BaseBackupPath).ListFolder()
	if err != nil {
		return "", JournalInfo{}, nil
	}

	var lastBackupInfo storage.Object
	lastBackupName := ""
	for _, v := range objs {
		if strings.HasPrefix(v.GetName(), JournalPrefix) {
			if lastBackupInfo == nil || v.GetLastModified().After(lastBackupInfo.GetLastModified()) {
				lastBackupName = v.GetName()
				lastBackupInfo = v
			}
		}
	}

	backupInfo, err := GetBackupInfo(folder, lastBackupName)
	if err != nil {
		return "", JournalInfo{}, nil
	}

	return lastBackupName, backupInfo, nil
}

func GetBackupInfo(folder storage.Folder, metaName string) (JournalInfo, error) {
	folder = folder.GetSubFolder(utility.BaseBackupPath)
	backupInfoReader, err := folder.ReadObject(metaName)
	if err != nil {
		return JournalInfo{}, err
	}

	backupInfoRaw, err := io.ReadAll(backupInfoReader)
	if err != nil {
		return JournalInfo{}, err
	}

	backupInfo := JournalInfo{}
	err = json.Unmarshal(backupInfoRaw, &backupInfo)
	if err != nil {
		return JournalInfo{}, err
	}

	return backupInfo, nil
}

func UpdatePreviousBackupInfo(
	folder storage.Folder,
	journalPath string,
	journalCmpLess func(a, b string) bool,
	newJournalEnd string,
) error {
	lastSentinelName, lastSentinel, err := GetLastBackupInfo(folder)
	if err != nil {
		return err
	}
	lastSentinelName = strings.ReplaceAll(lastSentinelName, JournalPrefix, "")

	if len(lastSentinelName) == 0 {
		tracelog.WarningLogger.Printf("last sentinel was not found, we can not evaluate journal size")
		return nil
	}

	journalSize, err := GetJournalSizeInSemiInterval(
		folder,
		journalPath,
		journalCmpLess,
		lastSentinel.JournalEnd,
		newJournalEnd,
	)
	if err != nil {
		tracelog.ErrorLogger.Printf("can not evaluate journal sum for %s: %s", lastSentinelName, err)
		return err
	}
	tracelog.InfoLogger.Printf(
		"journal size for %s in the semi interval (%s; %s] is equal to %d",
		lastSentinelName,
		lastSentinel.JournalEnd,
		newJournalEnd,
		journalSize,
	)

	err = UploadBackupInfo(folder, lastSentinelName, JournalInfo{
		JournalStart: lastSentinel.JournalStart,
		JournalEnd:   lastSentinel.JournalEnd,
		JournalSize:  journalSize,
	})
	if err != nil {
		tracelog.ErrorLogger.Printf("can not update journal info for %s: %s", lastSentinelName, err)
		return err
	}

	tracelog.InfoLogger.Printf("journal info has been updated for %s", lastSentinelName)
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
			tracelog.InfoLogger.Printf("Found in range: %+v\n", jt)
			sum += journalFiles[i].GetSize()
		} else {
			tracelog.InfoLogger.Printf("Not in range: %+v\n", jt)
		}
	}
	tracelog.InfoLogger.Printf("Journal Sum: %d\n", sum)

	return sum, nil
}

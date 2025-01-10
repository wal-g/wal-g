package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"

	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

const (
	JournalSize     = "JournalSize"
	JournalPrefix   = "journal_"
	cantFindJournal = "can not find appropriate journal on S3"
)

var (
	DefaultLessCmp = func(a, b string) bool { return a < b }
)

// JournalInfo is a projection of the S3 journal info object.
// When a JournalInfo instance was changed, it should be synced with S3 using the upload/read method.
type JournalInfo struct {
	JournalDirectoryName      string                 `json:"-"`
	JournalNameLessComparator func(a, b string) bool `json:"-"`
	JournalName               string                 `json:"-"`
	PriorBackupEnd            string                 `json:"JournalStart"`
	CurrentBackupEnd          string                 `json:"JournalEnd"`
	SizeToNextBackup          int64                  `json:"JournalSize"`
}

// NewEmptyJournalInfo creates instance of JournalInfo without sync with S3
func NewEmptyJournalInfo(
	backupName string,
	currentBackupEnd, priorBackupEnd string,
	journalDir string,
	journalLessComparator func(a, b string) bool,
) JournalInfo {
	return JournalInfo{
		JournalName:               fmt.Sprintf("%s%s", JournalPrefix, backupName),
		JournalNameLessComparator: journalLessComparator,
		JournalDirectoryName:      journalDir,
		PriorBackupEnd:            currentBackupEnd,
		CurrentBackupEnd:          priorBackupEnd,
		SizeToNextBackup:          0,
	}
}

func NewJournalInfo(
	backupName string,
	folder storage.Folder,
	journalDir string,
	journalLessComparator func(a, b string) bool,
) (JournalInfo, error) {
	ji := JournalInfo{
		JournalName:               fmt.Sprintf("%s%s", JournalPrefix, backupName),
		JournalDirectoryName:      journalDir,
		JournalNameLessComparator: journalLessComparator,
	}

	err := ji.Read(folder)
	if err != nil {
		return JournalInfo{}, err
	}

	return ji, nil
}

// Read syncs JournalInfo by reading the file on S3
func (ji *JournalInfo) Read(folder storage.Folder) error {
	folder = folder.GetSubFolder(utility.BaseBackupPath)
	journalInfoReader, err := folder.ReadObject(ji.JournalName)
	if err != nil {
		return err
	}

	journalInfoRaw, err := io.ReadAll(journalInfoReader)
	if err != nil {
		return err
	}

	return json.Unmarshal(journalInfoRaw, ji)
}

// Upload syncs Journalinfo by uploading the structure as a file on S3
func (ji *JournalInfo) Upload(folder storage.Folder) error {
	folder = folder.GetSubFolder(utility.BaseBackupPath)
	rawJournalInfo, err := json.Marshal(ji)
	if err != nil {
		return err
	}

	return folder.PutObject(ji.JournalName, bytes.NewBuffer(rawJournalInfo))
}

// GetNextOlder retrieves the JournalInfo that is immediately older than the current one from S3, using the JournalNameLessComparator.
func (ji *JournalInfo) GetNextOlder(folder storage.Folder) (JournalInfo, error) {
	objs, _, err := folder.GetSubFolder(utility.BaseBackupPath).ListFolder()
	if err != nil {
		return JournalInfo{}, err
	}

	var olderJournalInfoObject storage.Object
	less := ji.JournalNameLessComparator
	for _, obj := range objs {
		isJournal := strings.HasPrefix(obj.GetName(), JournalPrefix)
		isNewerThenCurrentJournal := less(ji.JournalName, obj.GetName())
		isCurrentJournal := obj.GetName() == ji.JournalName
		if !isJournal || isNewerThenCurrentJournal || isCurrentJournal {
			continue
		}

		isNothingFound := olderJournalInfoObject == nil
		if isNothingFound {
			olderJournalInfoObject = obj
		}

		isYoungerThenFound := less(olderJournalInfoObject.GetName(), obj.GetName())
		if isYoungerThenFound {
			olderJournalInfoObject = obj
		}
	}
	if olderJournalInfoObject == nil {
		return JournalInfo{}, xerrors.New(cantFindJournal)
	}

	olderBackupName := strings.TrimPrefix(
		olderJournalInfoObject.GetName(),
		JournalPrefix,
	)
	olderJournalInfo, err := NewJournalInfo(
		olderBackupName,
		folder,
		ji.JournalDirectoryName,
		ji.JournalNameLessComparator,
	)
	if err != nil {
		return JournalInfo{}, err
	}
	return olderJournalInfo, err
}

// GetNextNewer retrieves the JournalInfo that is immediately newer than the current one from S3, using the JournalNameLessComparator.
func (ji *JournalInfo) GetNextNewer(folder storage.Folder) (JournalInfo, error) {
	objs, _, err := folder.GetSubFolder(utility.BaseBackupPath).ListFolder()
	if err != nil {
		return JournalInfo{}, err
	}

	var newerJournalInfoObject storage.Object
	less := ji.JournalNameLessComparator
	for _, obj := range objs {
		isJournal := strings.HasPrefix(obj.GetName(), JournalPrefix)
		isOlderThenCurrentJournal := less(obj.GetName(), ji.JournalName)
		isCurrentJournal := obj.GetName() == ji.JournalName
		if !isJournal || isOlderThenCurrentJournal || isCurrentJournal {
			continue
		}

		isNothingFound := newerJournalInfoObject == nil
		if isNothingFound {
			newerJournalInfoObject = obj
		}

		isOlderThenFound := less(obj.GetName(), newerJournalInfoObject.GetName())
		if isOlderThenFound {
			newerJournalInfoObject = obj
		}
	}
	if newerJournalInfoObject == nil {
		return JournalInfo{}, xerrors.New(cantFindJournal)
	}

	newerBackupName := strings.TrimPrefix(
		newerJournalInfoObject.GetName(),
		JournalPrefix,
	)
	newerJournalInfo, err := NewJournalInfo(
		newerBackupName,
		folder,
		ji.JournalDirectoryName,
		ji.JournalNameLessComparator,
	)
	if err != nil {
		return JournalInfo{}, err
	}
	return newerJournalInfo, err
}

// Delete deletes the current JournalInfo from S3,
// update the JournalStart of a newer JournalInfo with the JournalStart of the current one,
// updates the older one with the journal size of the deleted one
func (ji *JournalInfo) Delete(folder storage.Folder) error {
	err := folder.
		GetSubFolder(utility.BaseBackupPath).
		DeleteObjects([]string{ji.JournalName})
	if err != nil {
		return err
	}

	newerJi, err := ji.GetNextNewer(folder)
	if err != nil {
		if err.Error() != cantFindJournal {
			return err
		}

		// JournalSize is the sum in bytes of binlogs between two backups.
		// If the current backup was the newest one, the older one will be the newest then, and the JournalSize of it should be equal to zero.
		olderJi, err := ji.GetNextOlder(folder)
		if err != nil {
			if err.Error() != cantFindJournal {
				return err
			}
			return nil
		}

		olderJi.SizeToNextBackup = 0
		return olderJi.Upload(folder)
	}

	newerJi.PriorBackupEnd = ji.PriorBackupEnd
	err = newerJi.Upload(folder)
	if err != nil {
		return err
	}

	err = newerJi.UpdateIntervalSize(folder)
	if err != nil {
		return err
	}

	return nil
}

// GetLastJournalInfo receives the most recently created JournalInfo on S3
func GetLastJournalInfo(
	folder storage.Folder,
	journalDir string,
	journalLessComparator func(a, b string) bool,
) (JournalInfo, error) {
	objs, _, err := folder.GetSubFolder(utility.BaseBackupPath).ListFolder()
	if err != nil {
		return JournalInfo{}, err
	}
	if len(objs) == 0 {
		return JournalInfo{}, nil
	}

	var lastJournalInfo storage.Object
	for _, v := range objs {
		if strings.HasPrefix(v.GetName(), JournalPrefix) &&
			(lastJournalInfo == nil || journalLessComparator(lastJournalInfo.GetName(), v.GetName())) {
			lastJournalInfo = v
		}
	}

	if lastJournalInfo == nil {
		return JournalInfo{}, xerrors.New("there are no journals on the S3")
	}

	lastBackupName := strings.TrimPrefix(lastJournalInfo.GetName(), JournalPrefix)
	backupInfo, err := NewJournalInfo(
		lastBackupName,
		folder,
		journalDir,
		journalLessComparator,
	)
	if err != nil {
		return JournalInfo{}, err
	}

	return backupInfo, nil
}

// UpdateIntervalSize calculates the size of the JournalInfo in the semi-interval (PriorBackupEnd; CurrentBackupEnd]
// using journal files on JournalDirectoryName and save it for the previous JournalInfo
func (ji *JournalInfo) UpdateIntervalSize(folder storage.Folder) error {
	journalFiles, _, err := folder.GetSubFolder(ji.JournalDirectoryName).ListFolder()
	if err != nil {
		return err
	}
	if len(journalFiles) == 0 {
		return nil
	}

	sum := int64(0)
	cmp := ji.JournalNameLessComparator
	for i := 0; i < len(journalFiles); i++ {
		jf := utility.TrimFileExtension(journalFiles[i].GetName())

		isEqual := !cmp(jf, ji.CurrentBackupEnd) && !cmp(ji.CurrentBackupEnd, jf)

		if cmp(ji.PriorBackupEnd, jf) && (cmp(jf, ji.CurrentBackupEnd) || isEqual) {
			tracelog.DebugLogger.Printf("Taking into account: %s\n", jf)
			sum += journalFiles[i].GetSize()
		}
	}
	tracelog.InfoLogger.Printf("Journal Sum of %s: %d\n", ji.JournalName, sum)

	prevJi, err := ji.GetNextOlder(folder)
	if err != nil {
		// There can only be one backup on S3 or we can delete the oldest one
		if err.Error() == cantFindJournal {
			return nil
		}
		return err
	}

	prevJi.SizeToNextBackup = sum

	err = prevJi.Upload(folder)
	if err != nil {
		return err
	}

	return nil
}

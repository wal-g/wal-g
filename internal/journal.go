package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/wal-g/tracelog"
	"golang.org/x/xerrors"

	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

const (
	JournalPrefix           = "journal_"
	JournalTimeLayout       = "20060102T150405Z"
	cantFindJournal         = "can not find appropriate journal on S3"
	fullJournalPrefixLength = len(JournalPrefix) + len(StreamPrefix)
)

type direction = int

const (
	older direction = 0
	newer direction = 1
)

// JournalInfo is a projection of the S3 journal info object.
// When a JournalInfo instance was changed, it should be synced with S3 using the upload/read method.
type JournalInfo struct {
	JournalDirectoryName string    `json:"-"`
	JournalName          string    `json:"-"`
	PriorBackupEnd       time.Time `json:"PriorBackupEnd"`
	CurrentBackupEnd     time.Time `json:"CurrentBackupEnd"`
	SizeToNextBackup     int64     `json:"SizeToNextBackup"`
}

// NewEmptyJournalInfo creates instance of JournalInfo without sync with S3
func NewEmptyJournalInfo(
	backupName string,
	currentBackupEnd time.Time,
	priorBackupEnd time.Time,
	journalDir string,
) JournalInfo {
	return JournalInfo{
		JournalName:          fmt.Sprintf("%s%s", JournalPrefix, backupName),
		JournalDirectoryName: journalDir,
		PriorBackupEnd:       currentBackupEnd,
		CurrentBackupEnd:     priorBackupEnd,
		SizeToNextBackup:     0,
	}
}

// NewJournalInfo creates instance of JournalInfo and reads its content from S3
func NewJournalInfo(
	backupName string,
	folder storage.Folder,
	journalDir string,
) (JournalInfo, error) {
	ji := JournalInfo{
		JournalName:          fmt.Sprintf("%s%s", JournalPrefix, backupName),
		JournalDirectoryName: journalDir,
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

// Upload syncs JournalInfo by uploading the structure as a file on S3
func (ji *JournalInfo) Upload(folder storage.Folder) error {
	folder = folder.GetSubFolder(utility.BaseBackupPath)
	rawJournalInfo, err := json.Marshal(ji)
	if err != nil {
		return err
	}

	return folder.PutObject(ji.JournalName, bytes.NewBuffer(rawJournalInfo))
}

// GetNext retrieves the JournalInfo that is immediately older/newer than the current one from S3
func (ji *JournalInfo) GetNext(folder storage.Folder, direction direction) (JournalInfo, error) {
	objs, _, err := folder.GetSubFolder(utility.BaseBackupPath).ListFolder()
	if err != nil {
		return JournalInfo{}, err
	}
	currentJournalTimestamp := getJournalTimestamp(ji.JournalName)

	objs = filterJournalsInfoFiles(objs)
	switch direction {
	case older:
		objs = filterJournalsInfoOlderThen(objs, currentJournalTimestamp)
	case newer:
		objs = filterJournalsInfoNewerThen(objs, currentJournalTimestamp)
	}
	objs = sortJournalsInfo(objs)

	if len(objs) == 0 {
		return JournalInfo{}, xerrors.New(cantFindJournal)
	}

	var journalName string
	switch direction {
	case older:
		journalName = objs[len(objs)-1].GetName()
	case newer:
		journalName = objs[0].GetName()
	}

	backupName := strings.TrimPrefix(
		journalName,
		JournalPrefix,
	)
	newerJournalInfo, err := NewJournalInfo(
		backupName,
		folder,
		ji.JournalDirectoryName,
	)
	if err != nil {
		return JournalInfo{}, err
	}
	return newerJournalInfo, err
}

// Delete deletes the current JournalInfo from S3,
// updates the PriorBackupEnd of a newer JournalInfo with the PriorBackupEnd of the current one,
// updates the older one journal size.
func (ji *JournalInfo) Delete(folder storage.Folder) error {
	err := folder.
		GetSubFolder(utility.BaseBackupPath).
		DeleteObjects([]string{ji.JournalName})
	if err != nil {
		return err
	}

	newerJi, err := ji.GetNext(folder, newer)
	if err != nil {
		if err.Error() != cantFindJournal {
			return err
		}

		// SizeToNextBackup is the sum in bytes of binlogs between two backups.
		// If the current backup was the newest one, the older one will be the newest then,
		// and the SizeToNextBackup of it should be equal to zero.
		olderJi, err := ji.GetNext(folder, older)
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

// GetMostRecentJournalInfo receives the most recently created JournalInfo on S3
func GetMostRecentJournalInfo(
	folder storage.Folder,
	journalDir string,
) (JournalInfo, error) {
	objs, _, err := folder.GetSubFolder(utility.BaseBackupPath).ListFolder()
	if err != nil {
		return JournalInfo{}, err
	}
	if len(objs) == 0 {
		return JournalInfo{}, nil
	}

	objs = filterJournalsInfoFiles(objs)
	objs = sortJournalsInfo(objs)
	if len(objs) == 0 {
		return JournalInfo{}, xerrors.New("there are no journals on the S3")
	}

	theMostRecentJournalObject := objs[len(objs)-1]
	theMostRecentBackupName := strings.TrimPrefix(theMostRecentJournalObject.GetName(), JournalPrefix)
	backupInfo, err := NewJournalInfo(
		theMostRecentBackupName,
		folder,
		journalDir,
	)
	if err != nil {
		return JournalInfo{}, err
	}

	return backupInfo, nil
}

// UpdateIntervalSize calculates the size of the SizeToNextBackup in the semi-interval (PriorBackupEnd; CurrentBackupEnd]
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
	for _, journal := range journalFiles {
		timestamp := journal.GetLastModified()

		isInInterval := timestamp.After(ji.PriorBackupEnd) && timestamp.Before(ji.CurrentBackupEnd)
		isEqualToCurrentBackupEnd := timestamp.Equal(ji.CurrentBackupEnd)

		if isInInterval || isEqualToCurrentBackupEnd {
			tracelog.DebugLogger.Printf("Taking into account: %s (%s)", journal.GetName(), journal.GetLastModified())
			sum += journal.GetSize()
		}
	}

	prevJi, err := ji.GetNext(folder, older)
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

func getJournalTimestamp(journal string) time.Time {
	if journal == "" {
		return time.Time{}
	}

	timestampStr := journal[fullJournalPrefixLength:]
	timestamp, err := time.Parse(JournalTimeLayout, timestampStr)
	if err != nil {
		tracelog.WarningLogger.Printf("Error during parsing timestamp '%s': %s", journal, err)
	}

	return timestamp
}

func filterJournalsInfoFiles(objects []storage.Object) []storage.Object {
	newObjects := make([]storage.Object, 0, len(objects))
	for _, obj := range objects {
		if strings.HasPrefix(obj.GetName(), JournalPrefix) {
			newObjects = append(newObjects, obj)
		}
	}
	return newObjects
}

func filterJournalsInfoOlderThen(objects []storage.Object, timestamp time.Time) []storage.Object {
	newObjects := make([]storage.Object, 0, len(objects))
	for _, obj := range objects {
		objTimestamp := getJournalTimestamp(obj.GetName())
		if objTimestamp.Before(timestamp) {
			newObjects = append(newObjects, obj)
		}
	}
	return newObjects
}

func filterJournalsInfoNewerThen(objects []storage.Object, timestamp time.Time) []storage.Object {
	newObjects := make([]storage.Object, 0, len(objects))
	for _, obj := range objects {
		objTimestamp := getJournalTimestamp(obj.GetName())
		if objTimestamp.After(timestamp) {
			newObjects = append(newObjects, obj)
		}
	}
	return newObjects
}

func sortJournalsInfo(objects []storage.Object) []storage.Object {
	sort.Slice(objects, func(i, j int) bool {
		ti := getJournalTimestamp(objects[i].GetName())
		tj := getJournalTimestamp(objects[j].GetName())
		return ti.Before(tj)
	})
	return objects
}

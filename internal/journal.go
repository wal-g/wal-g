package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
	"golang.org/x/xerrors"
)

const (
	JournalPrefix     = "journal_"
	JournalTimeLayout = "20060102T150405Z"
	cantFindJournal   = "can not find appropriate journal on S3"
)

type direction = int

const (
	older direction = 0
	newer direction = 1
)

var JournalsNotFound = xerrors.New("there are no journals on the S3")

// JournalInfo is a projection of the S3 journal info object.
// When a JournalInfo instance was changed, it should be synced with S3 using the upload/read method.
type JournalInfo struct {
	JournalDirectoryName string `json:"-"`
	JournalName          string `json:"-"`
	// PriorBackupEnd is the completion time of the previous backup before this journal's backup.
	// Together with CurrentBackupEnd it defines the previous-to-current backup interval
	// (PriorBackupEnd; CurrentBackupEnd], which is used to recalculate the previous
	// journal's SizeToNextBackup when this backup is created or when a backup is deleted.
	PriorBackupEnd time.Time `json:"PriorBackupEnd"`
	// CurrentBackupEnd is the completion time of the backup this journal belongs to.
	// It is also used to order journal_<backup> objects chronologically.
	CurrentBackupEnd time.Time `json:"CurrentBackupEnd"`
	// SizeToNextBackup belongs to a different interval than PriorBackupEnd/CurrentBackupEnd:
	// it is the size, in bytes, of archived journal files needed to get from this backup
	// to the next one. It is zero for the newest backup.
	SizeToNextBackup int64 `json:"SizeToNextBackup"`
}

// NewEmptyJournalInfo creates instance of JournalInfo without sync with S3
func NewEmptyJournalInfo(
	backupName string,
	priorBackupEnd time.Time,
	currentBackupEnd time.Time,
	journalDir string,
) JournalInfo {
	return JournalInfo{
		JournalName:          fmt.Sprintf("%s%s", JournalPrefix, backupName),
		JournalDirectoryName: journalDir,
		PriorBackupEnd:       priorBackupEnd,
		CurrentBackupEnd:     currentBackupEnd,
		SizeToNextBackup:     0,
	}
}

// NewJournalInfo creates instance of JournalInfo and reads its content from S3
func NewJournalInfo(
	ctx context.Context,
	backupName string,
	folder storage.Folder,
	journalDir string,
) (JournalInfo, error) {
	ji := JournalInfo{
		JournalName:          fmt.Sprintf("%s%s", JournalPrefix, backupName),
		JournalDirectoryName: journalDir,
	}

	err := ji.Read(ctx, folder)
	if err != nil {
		return JournalInfo{}, err
	}

	return ji, nil
}

// Read syncs JournalInfo by reading the file on S3
func (ji *JournalInfo) Read(ctx context.Context, folder storage.Folder) error {
	folder = folder.GetSubFolder(utility.BaseBackupPath)
	journalInfoReader, err := folder.ReadObject(ctx, ji.JournalName)
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
func (ji *JournalInfo) Upload(ctx context.Context, folder storage.Folder) error {
	folder = folder.GetSubFolder(utility.BaseBackupPath)
	rawJournalInfo, err := json.Marshal(ji)
	if err != nil {
		return err
	}

	return folder.PutObject(ctx, ji.JournalName, bytes.NewBuffer(rawJournalInfo))
}

// GetNext retrieves the JournalInfo that is immediately older/newer than the current one from S3
func (ji *JournalInfo) GetNext(ctx context.Context, folder storage.Folder, direction direction) (JournalInfo, error) {
	objs, _, err := folder.GetSubFolder(utility.BaseBackupPath).ListFolder(ctx)
	if err != nil {
		return JournalInfo{}, err
	}

	journals, err := loadJournalsInfo(ctx, folder, filterJournalsInfoFiles(objs))
	if err != nil {
		return JournalInfo{}, err
	}

	switch direction {
	case older:
		journals = filterJournalsInfoOlderThen(journals, ji.CurrentBackupEnd)
	case newer:
		journals = filterJournalsInfoNewerThen(journals, ji.CurrentBackupEnd)
	}
	journals = sortJournalsInfo(journals)

	if len(journals) == 0 {
		return JournalInfo{}, xerrors.New(cantFindJournal)
	}

	var next JournalInfo
	switch direction {
	case older:
		next = journals[len(journals)-1]
	case newer:
		next = journals[0]
	}

	// JournalDirectoryName is not part of the stored object, it comes from the caller.
	next.JournalDirectoryName = ji.JournalDirectoryName

	return next, nil
}

// Previous is the journal of the backup preceding this one, ok == false when this is the oldest.
func (ji *JournalInfo) Previous(ctx context.Context, folder storage.Folder) (JournalInfo, bool, error) {
	prevJi, err := ji.GetNext(ctx, folder, older)
	if err != nil {
		if err.Error() == cantFindJournal {
			return JournalInfo{}, false, nil
		}
		return JournalInfo{}, false, err
	}

	return prevJi, true, nil
}

// Unlink deletes the journal object and merges the interval it covered into the newer journal,
// which is returned so that the caller can recalculate the size of the merged interval.
// ok == false when this was the newest journal and there is nothing to merge into.
func (ji *JournalInfo) Unlink(ctx context.Context, folder storage.Folder) (JournalInfo, bool, error) {
	err := folder.
		GetSubFolder(utility.BaseBackupPath).
		DeleteObjects(ctx, []storage.Object{storage.NewLocalObject(ji.JournalName, time.Time{}, 0)})
	if err != nil {
		return JournalInfo{}, false, err
	}

	newerJi, err := ji.GetNext(ctx, folder, newer)
	if err != nil {
		if err.Error() != cantFindJournal {
			return JournalInfo{}, false, err
		}

		// SizeToNextBackup is the sum in bytes of binlogs between two backups.
		// If the current backup was the newest one, the older one will be the newest then,
		// and the SizeToNextBackup of it should be equal to zero.
		olderJi, ok, err := ji.Previous(ctx, folder)
		if err != nil || !ok {
			return JournalInfo{}, false, err
		}

		olderJi.SizeToNextBackup = 0
		return JournalInfo{}, false, olderJi.Upload(ctx, folder)
	}

	newerJi.PriorBackupEnd = ji.PriorBackupEnd
	err = newerJi.Upload(ctx, folder)
	if err != nil {
		return JournalInfo{}, false, err
	}

	return newerJi, true, nil
}

// Delete unlinks the journal and recalculates the merged interval from JournalDirectoryName.
// A database that measures the interval differently calls Unlink itself instead.
func (ji *JournalInfo) Delete(ctx context.Context, folder storage.Folder) error {
	newerJi, ok, err := ji.Unlink(ctx, folder)
	if err != nil || !ok {
		return err
	}

	return newerJi.UpdateIntervalSize(ctx, folder, &JournalFiles{})
}

// DeleteJournalInfo removes the journal of the given backup and re-links the neighboring journals,
// so that the journal volume of the deleted interval is accounted for by the previous backup.
// A backup pushed without journal counting has no journal at all, which is not an error.
func DeleteJournalInfo(
	ctx context.Context,
	folder storage.Folder,
	backupName string,
	journalDir string,
	confirmed bool,
) {
	journalInfo, err := NewJournalInfo(ctx, backupName, folder, journalDir)
	if err != nil {
		tracelog.WarningLogger.Printf("Can't find the journal info: %s", err.Error())
		return
	}

	if !confirmed {
		tracelog.InfoLogger.Printf("Journal info to delete: %+v", journalInfo)
		return
	}

	if err := journalInfo.Delete(ctx, folder); err != nil {
		tracelog.ErrorLogger.Print(err)
		return
	}

	tracelog.InfoLogger.Printf("Deleted journal info: %+v", journalInfo)
}

// GetMostRecentJournalInfo receives the most recently created JournalInfo on S3
func GetMostRecentJournalInfo(
	ctx context.Context,
	folder storage.Folder,
	journalDir string,
) (JournalInfo, error) {
	objs, _, err := folder.GetSubFolder(utility.BaseBackupPath).ListFolder(ctx)
	if err != nil {
		return JournalInfo{}, err
	}
	if len(objs) == 0 {
		return JournalInfo{}, nil
	}

	objs = filterJournalsInfoFiles(objs)
	if len(objs) == 0 {
		return JournalInfo{}, JournalsNotFound
	}

	journals, err := loadJournalsInfo(ctx, folder, objs)
	if err != nil {
		return JournalInfo{}, err
	}
	journals = sortJournalsInfo(journals)

	backupInfo := journals[len(journals)-1]
	// JournalDirectoryName is not part of the stored object, it comes from the caller.
	backupInfo.JournalDirectoryName = journalDir

	return backupInfo, nil
}

// JournalFiles caches a journal directory listing, to be reused across one recalculation.
type JournalFiles struct {
	files       []storage.Object
	initialized bool
}

// UpdateIntervalSize calculates the size of the SizeToNextBackup in the semi-interval (PriorBackupEnd; CurrentBackupEnd]
// using journal files on JournalDirectoryName and save it for the previous JournalInfo
func (ji *JournalInfo) UpdateIntervalSize(ctx context.Context, folder storage.Folder, journalFilesObj *JournalFiles) error {
	if !journalFilesObj.initialized {
		// doing this 1 time for reusing it in next runs during single calculation
		journalFiles, _, err := folder.GetSubFolder(ji.JournalDirectoryName).ListFolder(ctx)
		if err != nil {
			return err
		}
		journalFilesObj.files = journalFiles
		journalFilesObj.initialized = true
	}

	journalFiles := journalFilesObj.files
	if len(journalFiles) == 0 {
		// Not measured rather than zero: overwriting SizeToNextBackup with a 0 would lose it.
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

	// There can only be one backup on S3 or we can delete the oldest one
	prevJi, ok, err := ji.Previous(ctx, folder)
	if err != nil || !ok {
		return err
	}
	prevJi.SizeToNextBackup = sum

	err = prevJi.Upload(ctx, folder)
	if err != nil {
		return err
	}

	return nil
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

// loadJournalsInfo reads the journal behind every object, so that they can be ordered by
// CurrentBackupEnd. Their names can not be used for that: backup naming conventions differ across
// databases (MySQL's "stream_<timestamp>" vs Postgres' "base_<timeline><segno>").
func loadJournalsInfo(ctx context.Context, folder storage.Folder, objects []storage.Object) ([]JournalInfo, error) {
	journals := make([]JournalInfo, 0, len(objects))
	for _, obj := range objects {
		backupName := strings.TrimPrefix(obj.GetName(), JournalPrefix)
		ji, err := NewJournalInfo(ctx, backupName, folder, "")
		if err != nil {
			return nil, err
		}
		journals = append(journals, ji)
	}
	return journals, nil
}

func filterJournalsInfoOlderThen(journals []JournalInfo, timestamp time.Time) []JournalInfo {
	newJournals := make([]JournalInfo, 0, len(journals))
	for _, ji := range journals {
		if ji.CurrentBackupEnd.Before(timestamp) {
			newJournals = append(newJournals, ji)
		}
	}
	return newJournals
}

func filterJournalsInfoNewerThen(journals []JournalInfo, timestamp time.Time) []JournalInfo {
	newJournals := make([]JournalInfo, 0, len(journals))
	for _, ji := range journals {
		if ji.CurrentBackupEnd.After(timestamp) {
			newJournals = append(newJournals, ji)
		}
	}
	return newJournals
}

func sortJournalsInfo(journals []JournalInfo) []JournalInfo {
	slices.SortFunc(journals, func(a, b JournalInfo) int {
		return a.CurrentBackupEnd.Compare(b.CurrentBackupEnd)
	})
	return journals
}

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

	candidates, err := loadJournalCandidates(ctx, folder, filterJournalsInfoFiles(objs))
	if err != nil {
		return JournalInfo{}, err
	}

	switch direction {
	case older:
		candidates = filterCandidatesOlderThen(candidates, ji.CurrentBackupEnd)
	case newer:
		candidates = filterCandidatesNewerThen(candidates, ji.CurrentBackupEnd)
	}
	sortCandidates(candidates)

	if len(candidates) == 0 {
		return JournalInfo{}, xerrors.New(cantFindJournal)
	}

	var journalName string
	switch direction {
	case older:
		journalName = candidates[len(candidates)-1].obj.GetName()
	case newer:
		journalName = candidates[0].obj.GetName()
	}

	backupName := strings.TrimPrefix(
		journalName,
		JournalPrefix,
	)
	newerJournalInfo, err := NewJournalInfo(
		ctx,
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
func (ji *JournalInfo) Delete(ctx context.Context, folder storage.Folder) error {
	err := folder.
		GetSubFolder(utility.BaseBackupPath).
		DeleteObjects(ctx, []storage.Object{storage.NewLocalObject(ji.JournalName, time.Time{}, 0)})
	if err != nil {
		return err
	}

	newerJi, err := ji.GetNext(ctx, folder, newer)
	if err != nil {
		if err.Error() != cantFindJournal {
			return err
		}

		// SizeToNextBackup is the sum in bytes of binlogs between two backups.
		// If the current backup was the newest one, the older one will be the newest then,
		// and the SizeToNextBackup of it should be equal to zero.
		olderJi, err := ji.GetNext(ctx, folder, older)
		if err != nil {
			if err.Error() != cantFindJournal {
				return err
			}
			return nil
		}

		olderJi.SizeToNextBackup = 0
		return olderJi.Upload(ctx, folder)
	}

	newerJi.PriorBackupEnd = ji.PriorBackupEnd
	err = newerJi.Upload(ctx, folder)
	if err != nil {
		return err
	}

	err = newerJi.UpdateIntervalSize(ctx, folder, &JournalFiles{})
	if err != nil {
		return err
	}

	return nil
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

	candidates, err := loadJournalCandidates(ctx, folder, objs)
	if err != nil {
		return JournalInfo{}, err
	}
	sortCandidates(candidates)

	theMostRecentJournalObject := candidates[len(candidates)-1].obj
	theMostRecentBackupName := strings.TrimPrefix(theMostRecentJournalObject.GetName(), JournalPrefix)
	backupInfo, err := NewJournalInfo(
		ctx,
		theMostRecentBackupName,
		folder,
		journalDir,
	)
	if err != nil {
		return JournalInfo{}, err
	}

	return backupInfo, nil
}

type JournalFiles struct {
	files       []storage.Object
	initialized bool
}

// UpdateIntervalSize calculates the size of the SizeToNextBackup in the semi-interval (PriorBackupEnd; CurrentBackupEnd]
// using journal files on JournalDirectoryName and save it for the previous JournalInfo
func (ji *JournalInfo) UpdateIntervalSize(ctx context.Context, folder storage.Folder, journalFilesObj *JournalFiles) error {
	var err error
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

	prevJi, err := ji.GetNext(ctx, folder, older)
	if err != nil {
		// There can only be one backup on S3 or we can delete the oldest one
		if err.Error() == cantFindJournal {
			return nil
		}
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

// journalCandidate pairs a journal's storage object with its CurrentBackupEnd, i.e. the
// completion time of the backup it belongs to.
type journalCandidate struct {
	obj       storage.Object
	timestamp time.Time
}

// loadJournalCandidates reads each journal's CurrentBackupEnd from its content instead of
// deriving a timestamp from the associated backup's name, since backup naming conventions
// differ across databases (e.g. MySQL's "stream_<timestamp>" vs Postgres' "base_<timeline><segno>").
func loadJournalCandidates(ctx context.Context, folder storage.Folder, objects []storage.Object) ([]journalCandidate, error) {
	candidates := make([]journalCandidate, 0, len(objects))
	for _, obj := range objects {
		backupName := strings.TrimPrefix(obj.GetName(), JournalPrefix)
		ji, err := NewJournalInfo(ctx, backupName, folder, "")
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, journalCandidate{obj: obj, timestamp: ji.CurrentBackupEnd})
	}
	return candidates, nil
}

func filterCandidatesOlderThen(candidates []journalCandidate, timestamp time.Time) []journalCandidate {
	newCandidates := make([]journalCandidate, 0, len(candidates))
	for _, c := range candidates {
		if c.timestamp.Before(timestamp) {
			newCandidates = append(newCandidates, c)
		}
	}
	return newCandidates
}

func filterCandidatesNewerThen(candidates []journalCandidate, timestamp time.Time) []journalCandidate {
	newCandidates := make([]journalCandidate, 0, len(candidates))
	for _, c := range candidates {
		if c.timestamp.After(timestamp) {
			newCandidates = append(newCandidates, c)
		}
	}
	return newCandidates
}

func sortCandidates(candidates []journalCandidate) {
	slices.SortFunc(candidates, func(a, b journalCandidate) int {
		return a.timestamp.Compare(b.timestamp)
	})
}

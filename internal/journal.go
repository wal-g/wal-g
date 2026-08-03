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
// updates the older one journal size. The size of the merged interval is recalculated from the
// journal files archived in ji.JournalDirectoryName, which is what a database keeping its journals
// in a directory of its own needs. See DeleteWith for the databases that do not.
func (ji *JournalInfo) Delete(ctx context.Context, folder storage.Folder) error {
	return ji.DeleteWith(ctx, folder, &JournalFiles{})
}

// DeleteWith is Delete with the size of the merged interval recalculated by calc.
func (ji *JournalInfo) DeleteWith(ctx context.Context, folder storage.Folder, calc IntervalSizeCalculator) error {
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

	err = newerJi.UpdateIntervalSize(ctx, folder, calc)
	if err != nil {
		return err
	}

	return nil
}

// DeleteJournalInfo removes the journal of the given backup and re-links the neighboring journals,
// so that the journal volume of the deleted interval is accounted for by the previous backup.
// A backup pushed without journal counting has no journal at all, which is not an error.
func DeleteJournalInfo(
	ctx context.Context,
	folder storage.Folder,
	backupName string,
	journalDir string,
	calc IntervalSizeCalculator,
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

	if err := journalInfo.DeleteWith(ctx, folder, calc); err != nil {
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

// IntervalSizeCalculator computes the volume of journal data accumulated in the semi-interval
// (ji.PriorBackupEnd; ji.CurrentBackupEnd], which is stored as prevJi.SizeToNextBackup.
type IntervalSizeCalculator interface {
	// Calculate returns ok == false when the size can not be determined. In that case the caller
	// must leave prevJi.SizeToNextBackup as it is instead of resetting it to zero, since a missing
	// measurement is indistinguishable from a genuine zero once it has been written to storage.
	Calculate(ctx context.Context, folder storage.Folder, ji, prevJi JournalInfo) (size int64, ok bool, err error)
}

type JournalFiles struct {
	files       []storage.Object
	initialized bool
}

func (c *JournalFiles) Calculate(
	ctx context.Context,
	folder storage.Folder,
	ji, _ JournalInfo,
) (int64, bool, error) {
	if !c.initialized {
		journalFiles, _, err := folder.GetSubFolder(ji.JournalDirectoryName).ListFolder(ctx)
		if err != nil {
			return 0, false, err
		}
		c.files = journalFiles
		c.initialized = true
	}

	if len(c.files) == 0 {
		return 0, false, nil
	}

	sum := int64(0)
	for _, journal := range c.files {
		timestamp := journal.GetLastModified()

		isInInterval := timestamp.After(ji.PriorBackupEnd) && timestamp.Before(ji.CurrentBackupEnd)
		isEqualToCurrentBackupEnd := timestamp.Equal(ji.CurrentBackupEnd)

		if isInInterval || isEqualToCurrentBackupEnd {
			tracelog.DebugLogger.Printf("Taking into account: %s (%s)", journal.GetName(), journal.GetLastModified())
			sum += journal.GetSize()
		}
	}

	return sum, true, nil
}

// UpdateIntervalSize calculates the size of the SizeToNextBackup in the semi-interval (PriorBackupEnd; CurrentBackupEnd]
// and saves it for the previous JournalInfo
func (ji *JournalInfo) UpdateIntervalSize(ctx context.Context, folder storage.Folder, calc IntervalSizeCalculator) error {
	prevJi, err := ji.GetNext(ctx, folder, older)
	if err != nil {
		// There can only be one backup on S3 or we can delete the oldest one
		if err.Error() == cantFindJournal {
			return nil
		}
		return err
	}

	sum, ok, err := calc.Calculate(ctx, folder, *ji, prevJi)
	if err != nil {
		return err
	}
	if !ok {
		tracelog.WarningLogger.Printf("Can not determine the journal size for %s, leaving SizeToNextBackup of %s intact",
			ji.JournalName, prevJi.JournalName)
		return nil
	}

	prevJi.SizeToNextBackup = sum

	return prevJi.Upload(ctx, folder)
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

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
	JournalStart              string                 `json:"JournalStart"`
	JournalEnd                string                 `json:"JournalEnd"`
	JournalSize               int64                  `json:"JournalSize"`
}

// NewEmptyJournalInfo creates instance of JournalInfo without sync with S3
func NewEmptyJournalInfo(
	backupName string,
	start, end string,
	journalDir string,
	journalLessComparator func(a, b string) bool,
) JournalInfo {
	return JournalInfo{
		JournalName:               fmt.Sprintf("%s%s", JournalPrefix, backupName),
		JournalNameLessComparator: journalLessComparator,
		JournalDirectoryName:      journalDir,
		JournalStart:              start,
		JournalEnd:                end,
		JournalSize:               0,
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
	backupInfoReader, err := folder.ReadObject(ji.JournalName)
	if err != nil {
		return err
	}

	backupInfoRaw, err := io.ReadAll(backupInfoReader)
	if err != nil {
		return err
	}

	err = json.Unmarshal(backupInfoRaw, ji)
	if err != nil {
		return err
	}

	return nil
}

// Upload syncs Journalinfo by uploading the structure as a file on S3
func (ji *JournalInfo) Upload(folder storage.Folder) error {
	folder = folder.GetSubFolder(utility.BaseBackupPath)

	rawBackupsInfo, err := json.Marshal(ji)
	if err != nil {
		return err
	}

	err = folder.PutObject(ji.JournalName, bytes.NewBuffer(rawBackupsInfo))
	if err != nil {
		return err
	}

	return nil
}

// GetPrevious gets the previous JournalInfo on S3 using JournalNameLessComparator
func (ji *JournalInfo) GetPrevious(folder storage.Folder) (JournalInfo, error) {
	objs, _, err := folder.GetSubFolder(utility.BaseBackupPath).ListFolder()
	if err != nil {
		return JournalInfo{}, err
	}

	var previousJournalObject storage.Object
	less := ji.JournalNameLessComparator
	for _, v := range objs {
		if strings.HasPrefix(v.GetName(), JournalPrefix) && less(v.GetName(), ji.JournalName) &&
			(previousJournalObject == nil || less(previousJournalObject.GetName(), v.GetName())) {
			previousJournalObject = v
		}
	}

	if previousJournalObject == nil {
		return JournalInfo{}, xerrors.New(cantFindJournal)
	}

	previousJournalInfo, err := NewJournalInfo(
		strings.TrimPrefix(
			previousJournalObject.GetName(),
			JournalPrefix,
		),
		folder,
		ji.JournalDirectoryName,
		ji.JournalNameLessComparator,
	)
	if err != nil {
		return JournalInfo{}, err
	}

	return previousJournalInfo, err
}

// GetNext gets the next JournalInfo on S3 using JournalNameLessComparator
func (ji *JournalInfo) GetNext(folder storage.Folder) (JournalInfo, error) {
	objs, _, err := folder.GetSubFolder(utility.BaseBackupPath).ListFolder()
	if err != nil {
		return JournalInfo{}, err
	}

	var nextJournalObject storage.Object
	less := ji.JournalNameLessComparator
	for _, v := range objs {
		if strings.HasPrefix(v.GetName(), JournalPrefix) && less(ji.JournalName, v.GetName()) &&
			(nextJournalObject == nil || less(v.GetName(), nextJournalObject.GetName())) {
			nextJournalObject = v
		}
	}

	if nextJournalObject == nil {
		return JournalInfo{}, xerrors.New(cantFindJournal)
	}

	previousJournalInfo, err := NewJournalInfo(
		strings.TrimPrefix(
			nextJournalObject.GetName(),
			JournalPrefix,
		),
		folder,
		ji.JournalDirectoryName,
		ji.JournalNameLessComparator,
	)
	if err != nil {
		return JournalInfo{}, err
	}

	return previousJournalInfo, err
}

// Delete deletes the current JournalInfo from S3 and updates the previous one with the journal size of the deleted one
func (ji *JournalInfo) Delete(folder storage.Folder) error {
	err := folder.
		GetSubFolder(utility.BaseBackupPath).
		DeleteObjects([]string{ji.JournalName})
	if err != nil {
		return err
	}

	nextJi, err := ji.GetNext(folder)
	if err != nil {
		// We could delete last backup or there could be just one backups on S3
		if err.Error() == cantFindJournal {
			return nil
		}
		return err
	}
	tracelog.InfoLogger.Printf("found the next journal %+v", nextJi)

	nextJi.JournalStart = ji.JournalStart
	err = nextJi.Upload(folder)
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("the next journal updated %+v", nextJi)

	err = nextJi.Calculate(folder)
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

// Calculate calculates the size of the JournalInfo in the semi-interval (JournalStart; JournalEnd] using journal files on JournalDirectoryName
// and save it for the previous JournalInfo
func (ji *JournalInfo) Calculate(folder storage.Folder) error {
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

		isEqual := !cmp(jf, ji.JournalEnd) && !cmp(ji.JournalEnd, jf)

		if cmp(ji.JournalStart, jf) && (cmp(jf, ji.JournalEnd) || isEqual) {
			tracelog.DebugLogger.Printf("Taking into accoutn: %s\n", jf)
			sum += journalFiles[i].GetSize()
		}
	}
	tracelog.InfoLogger.Printf("Journal Sum of %s: %d\n", ji.JournalName, sum)

	prevJi, err := ji.GetPrevious(folder)
	if err != nil {
		// We could delete the oldest backup or there could be just one backups on S3
		if err.Error() == cantFindJournal {
			return nil
		}
		return err
	}

	prevJi.JournalSize = sum

	err = prevJi.Upload(folder)
	if err != nil {
		return err
	}

	return nil
}

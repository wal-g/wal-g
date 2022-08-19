package postgres

import (
	"fmt"
	"regexp"
	"strconv"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

const PatternTimelineAndLogSegNo = "[0-9A-F]{24}"
const PatternLSN = "[0-9A-F]{8}"

var regexpTimelineAndLogSegNo = regexp.MustCompile(PatternTimelineAndLogSegNo)

const maxCountOfLSN = 2

type BytesPerWalSegmentError struct {
	error
}

func newBytesPerWalSegmentError() BytesPerWalSegmentError {
	return BytesPerWalSegmentError{
		errors.New(
			"bytes_per_wal_segment of the server does not match expected value," +
				" you may need to set WALG_PG_WAL_SIZE")}
}

func (err BytesPerWalSegmentError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type IncorrectLogSegNoError struct {
	error
}

func newIncorrectLogSegNoError(name string) IncorrectLogSegNoError {
	return IncorrectLogSegNoError{errors.Errorf("Incorrect logSegNoLo in WAL file name: %s", name)}
}

func (err IncorrectLogSegNoError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type IncorrectBackupNameError struct {
	error
}

func newIncorrectBackupNameError(name string) IncorrectBackupNameError {
	return IncorrectBackupNameError{errors.Errorf("Incorrect backup name: %s", name)}
}

func (err IncorrectBackupNameError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

const (
	sizeofInt32bits = sizeofInt32 * 8
	hexadecimal     = 16
)

var (
	// WalSegmentSize is the size of one WAL file
	WalSegmentSize        = uint64(16 * 1024 * 1024)
	xLogSegmentsPerXLogID = 0x100000000 / WalSegmentSize // xlog_internal.h line 101
	// .history file name regexp. For more details, see
	// https://doxygen.postgresql.org/backend_2access_2transam_2timeline_8c_source.html
	timelineHistoryFileRegexp *regexp.Regexp
)

func init() {
	timelineHistoryFileRegexp = regexp.MustCompile(`^([0-9a-fA-F]+)\.history(\.\w+)?$`)
}

const (
	walFileFormat        = "%08X%08X%08X" // xlog_internal.h line 155
	walHistoryFileFormat = "%08X.history"
)

func SetWalSize(sizeMb uint64) {
	WalSegmentSize = sizeMb * 1024 * 1024
	xLogSegmentsPerXLogID = 0x100000000 / WalSegmentSize
}

// getWalFilename formats WAL file name using PostgreSQL connection. Essentially reads timeline of the server.
func getWalFilename(lsn LSN, queryRunner *PgQueryRunner) (walFilename string, timeline uint32, err error) {
	timeline, err = queryRunner.readTimeline()
	if err != nil {
		return "", 0, err
	}

	walSegmentNo := newWalSegmentNo(lsn - 1)

	return walSegmentNo.getFilename(timeline), timeline, nil
}

func formatWALFileName(timeline uint32, logSegNo uint64) string {
	return fmt.Sprintf(walFileFormat, timeline, logSegNo/xLogSegmentsPerXLogID, logSegNo%xLogSegmentsPerXLogID)
}

// ParseWALFilename extracts numeric parts from WAL file name
func ParseWALFilename(name string) (timelineID uint32, logSegNo uint64, err error) {
	if len(name) != 24 {
		err = newNotWalFilenameError(name)
		return
	}
	timelineID64, err := strconv.ParseUint(name[0:8], 0x10, sizeofInt32bits)
	timelineID = uint32(timelineID64)
	if err != nil {
		return
	}
	logSegNoHi, err := strconv.ParseUint(name[8:16], 0x10, sizeofInt32bits)
	if err != nil {
		return
	}
	logSegNoLo, err := strconv.ParseUint(name[16:24], 0x10, sizeofInt32bits)
	if err != nil {
		return
	}
	if logSegNoLo >= xLogSegmentsPerXLogID {
		err = newIncorrectLogSegNoError(name)
		return
	}

	logSegNo = logSegNoHi*xLogSegmentsPerXLogID + logSegNoLo
	return
}

func TryFetchTimelineAndLogSegNo(objectName string) (uint32, uint64, bool) {
	foundLsn := regexpTimelineAndLogSegNo.FindAllString(objectName, maxCountOfLSN)
	if len(foundLsn) > 0 {
		timelineID, logSegNo, err := ParseWALFilename(foundLsn[0])

		if err == nil {
			return timelineID, logSegNo, true
		}
	}
	return 0, 0, false
}

func isWalFilename(filename string) bool {
	_, _, err := ParseWALFilename(filename)
	return err == nil
}

func ParseTimelineFromBackupName(backupName string) (uint32, error) {
	if len(backupName) == 0 {
		return 0, newIncorrectBackupNameError(backupName)
	}
	prefixLength := len(utility.BackupNamePrefix)
	return ParseTimelineFromString(backupName[prefixLength : prefixLength+8])
}

func ParseTimelineFromString(timelineString string) (uint32, error) {
	timelineID64, err := strconv.ParseUint(timelineString, hexadecimal, sizeofInt32bits)
	if err != nil {
		return 0, err
	}
	return uint32(timelineID64), nil
}

// GetNextWalFilename computes name of next WAL segment
func GetNextWalFilename(name string) (string, error) {
	timelineID, logSegNo, err := ParseWALFilename(name)
	if err != nil {
		return "", err
	}
	logSegNo++
	return formatWALFileName(timelineID, logSegNo), nil
}

func shouldPrefault(name string) (lsn LSN, shouldPrefault bool, timelineID uint32, err error) {
	timelineID, logSegNo, err := ParseWALFilename(name)
	if err != nil {
		return 0, false, 0, err
	}
	if logSegNo%WalFileInDelta != 0 {
		return 0, false, 0, nil
	}
	logSegNo += WalFileInDelta

	return LSN(logSegNo * WalSegmentSize), true, timelineID, nil
}

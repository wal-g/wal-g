package postgres

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strconv"

	"github.com/wal-g/wal-g/internal"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// regexp for .history file record. For more details, see
// https://doxygen.postgresql.org/backend_2access_2transam_2timeline_8c_source.html
var timelineHistoryRecordRegexp *regexp.Regexp

func init() {
	timelineHistoryRecordRegexp = regexp.MustCompile(`^(\d+)\t(.+)\t(.+)$`)
}

type HistoryFileNotFoundError struct {
	error
}

func newHistoryFileNotFoundError(historyFileName string) HistoryFileNotFoundError {
	return HistoryFileNotFoundError{errors.Errorf("History file '%s' does not exist.\n", historyFileName)}
}

func (err HistoryFileNotFoundError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// TimelineHistoryRecord represents entry in .history file
type TimelineHistoryRecord struct {
	timeline uint32
	lsn      LSN
	comment  string
}

func NewTimelineHistoryRecord(timeline uint32, lsn LSN, comment string) *TimelineHistoryRecord {
	return &TimelineHistoryRecord{timeline: timeline, lsn: lsn, comment: comment}
}

func newHistoryRecordFromString(row string) (*TimelineHistoryRecord, error) {
	matchResult := timelineHistoryRecordRegexp.FindStringSubmatch(row)
	if matchResult == nil || len(matchResult) < 4 {
		return nil, nil
	}
	timeline, err := strconv.ParseUint(matchResult[1], 10, sizeofInt32bits)
	if err != nil {
		return nil, err
	}
	lsn, err := ParseLSN(matchResult[2])
	if err != nil {
		return nil, err
	}
	comment := matchResult[3]
	return &TimelineHistoryRecord{timeline: uint32(timeline), lsn: lsn, comment: comment}, nil
}

// createTimelineSwitchMap creates a map to lookup the TimelineHistoryRecords of .history file
// by WalSegmentNo. So we can use this map to do a fast lookup
// and check if there was a timeline switch on the provided segment number.
func createTimelineSwitchMap(startTimeline uint32,
	walFolder storage.Folder) (map[WalSegmentNo]*TimelineHistoryRecord, error) {
	timeLineHistoryMap := make(map[WalSegmentNo]*TimelineHistoryRecord)
	historyRecords, err := GetTimeLineHistoryRecords(startTimeline, walFolder)
	if _, ok := err.(HistoryFileNotFoundError); ok {
		// return empty map if not found any history
		return timeLineHistoryMap, nil
	}
	if err != nil {
		return nil, err
	}
	// store records in a map for fast lookup by wal segment number
	for _, record := range historyRecords {
		walSegmentNo := newWalSegmentNo(record.lsn)
		timeLineHistoryMap[walSegmentNo] = record
	}
	return timeLineHistoryMap, nil
}

func GetTimeLineHistoryRecords(startTimeline uint32, walFolder storage.Folder) ([]*TimelineHistoryRecord, error) {
	historyReadCloser, err := getHistoryFileFromStorage(startTimeline, walFolder)
	if err != nil {
		return nil, err
	}
	historyRecords, err := parseHistoryFile(historyReadCloser)
	if err != nil {
		return nil, err
	}
	err = historyReadCloser.Close()
	if err != nil {
		return nil, err
	}
	return historyRecords, nil
}

func parseHistoryFile(historyReader io.Reader) ([]*TimelineHistoryRecord, error) {
	scanner := bufio.NewScanner(historyReader)
	historyRecords := make([]*TimelineHistoryRecord, 0)
	for scanner.Scan() {
		nextRow := scanner.Text()
		if nextRow == "" {
			// skip empty rows in .history file
			continue
		}
		record, err := newHistoryRecordFromString(nextRow)
		if err != nil {
			return nil, err
		}
		if record == nil {
			// skip any irrelevant rows (like comments)
			continue
		}
		historyRecords = append(historyRecords, record)
	}
	return historyRecords, nil
}

func getHistoryFileFromStorage(timeline uint32, walFolder storage.Folder) (io.ReadCloser, error) {
	historyFileName := fmt.Sprintf(walHistoryFileFormat, timeline)
	reader, err := internal.DownloadAndDecompressStorageFile(walFolder, historyFileName)
	if _, ok := err.(internal.ArchiveNonExistenceError); ok {
		return nil, newHistoryFileNotFoundError(historyFileName)
	}
	if err != nil {
		return nil, errors.Wrapf(err, "Error during .history file '%s' downloading.", historyFileName)
	}
	return reader, nil
}

package internal

import (
	"bufio"
	"fmt"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"io"
	"regexp"
	"strconv"
)

var walHistoryRecordRegexp *regexp.Regexp

func init() {
	walHistoryRecordRegexp = regexp.MustCompile("^(\\d+)\\t(.+)\\t(.+)$")
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
	lsn      uint64
	comment  string
}

func NewTimelineHistoryRecord(timeline uint32, lsn uint64, comment string) *TimelineHistoryRecord {
	return &TimelineHistoryRecord{timeline: timeline, lsn: lsn, comment: comment}
}

func newHistoryRecordFromString(row string) (*TimelineHistoryRecord, error) {
	matchResult := walHistoryRecordRegexp.FindStringSubmatch(row)
	if matchResult == nil || len(matchResult) < 4 {
		return nil, nil
	}
	timeline, err := strconv.ParseUint(matchResult[1], 10, sizeofInt32)
	if err != nil {
		return nil, err
	}
	lsn, err := pgx.ParseLSN(matchResult[2])
	if err != nil {
		return nil, err
	}
	comment := matchResult[3]
	return &TimelineHistoryRecord{timeline: uint32(timeline), lsn: lsn, comment: comment}, nil
}

// createTimelineSwitchMap creates a map to lookup the TimelineHistoryRecords of .history file
// by WalSegmentNo. So we can use this map to do a fast lookup
// and check if there was a timeline switch on the provided segment number.
func createTimelineSwitchMap(startTimeline uint32, walFolder storage.Folder) (map[WalSegmentNo]*TimelineHistoryRecord, error) {
	timeLineHistoryMap := make(map[WalSegmentNo]*TimelineHistoryRecord, 0)
	historyRecords, err := getTimeLineHistoryRecords(startTimeline, walFolder)
	if _, ok := err.(HistoryFileNotFoundError); ok {
		// return empty map if not found any history
		return timeLineHistoryMap, nil
	}
	// store records in a map for fast lookup by wal segment number
	for _, record := range historyRecords {
		walSegmentNo := newWalSegmentNo(record.lsn)
		timeLineHistoryMap[walSegmentNo] = record
	}
	return timeLineHistoryMap, nil
}

func getTimeLineHistoryRecords(startTimeline uint32, walFolder storage.Folder) ([]*TimelineHistoryRecord, error) {
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
			break
		}
		historyRecords = append(historyRecords, record)
	}
	return historyRecords, nil
}

func getHistoryFileFromStorage(timeline uint32, walFolder storage.Folder) (io.ReadCloser, error) {
	historyFileName := fmt.Sprintf(walHistoryFileFormat, timeline)
	reader, err := DownloadAndDecompressStorageFile(walFolder, historyFileName)
	if _, ok := err.(ArchiveNonExistenceError); ok {
		return nil, newHistoryFileNotFoundError(historyFileName)
	}
	if err != nil {
		return nil, errors.Wrapf(err, "Error during .history file '%s' downloading.", historyFileName)
	}
	return reader, nil
}

package mysql

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

const testFilenameSmall = "testdata/binlog_small_test"
const testFilenameBig = "testdata/binlog_big_test"

func TestReadWholeBinlog(t *testing.T) {
	startTs := MinTime
	endTs := MaxTime
	binlog, err := os.Open(testFilenameBig)
	if err != nil {
		t.Errorf("failed to open data example: %v", err)
	}
	defer binlog.Close()
	data1, err := ioutil.ReadAll(binlog)
	if err != nil {
		t.Errorf("failed to read data exapmple: %v", err)
	}
	_, err = binlog.Seek(0, 0)
	if err != nil {
		t.Errorf("failed to seek data exapmple: %v", err)
	}
	br := NewBinlogReader(binlog, startTs, endTs)
	data2, err := ioutil.ReadAll(br)
	if err != nil {
		t.Errorf("failed to read whole binlog through BinlogReader: %v", err)
	}
	if bytes.Compare(data1, data2) != 0 {
		t.Errorf("binlog differs from orriginal one")
	}
	if br.NeedAbort() {
		t.Errorf("binlog reader unexpected marked as needed abort")
	}
}

func TestReadBinlogAfterInterval(t *testing.T) {
	startTs := MinTime
	endTs := MinTime.Add(time.Second)
	binlog, err := os.Open(testFilenameBig)
	if err != nil {
		t.Errorf("failed to open data example: %v", err)
	}
	defer binlog.Close()
	br := NewBinlogReader(binlog, startTs, endTs)
	data, err := ioutil.ReadAll(br)
	if err != nil {
		t.Errorf("failed to read binlog through BinlogReader: %v", err)
	}
	if bytes.Compare(data, []byte{}) != 0 {
		t.Errorf("binlog should be empty due to interval, but got %v", data[:1000])
	}
	if !br.NeedAbort() {
		t.Errorf("binlog reader should be marked as needed abort")
	}
}

func TestReadBinlogBeforeInterval(t *testing.T) {
	startTs := MaxTime
	endTs := MaxTime.Add(-time.Second)
	binlog, err := os.Open(testFilenameBig)
	if err != nil {
		t.Errorf("failed to open data example: %v", err)
	}
	defer binlog.Close()
	br := NewBinlogReader(binlog, startTs, endTs)
	data, err := ioutil.ReadAll(br)
	if err != nil {
		t.Errorf("failed to read binlog through BinlogReader: %v", err)
	}
	if bytes.Compare(data, []byte{}) != 0 {
		t.Errorf("binlog should be empty due to interval, but got %v", data[:1000])
	}
	if br.NeedAbort() {
		t.Errorf("binlog reader should not be marked as needed abort")
	}
}

func TestReadPartOfBinlog(t *testing.T) {
	startTs := MinTime
	endTs := time.Unix(1565531903, 0) // corresponds to the middle of the binlog
	binlog, err := os.Open(testFilenameBig)
	if err != nil {
		t.Errorf("failed to open data example: %v", err)
	}
	defer binlog.Close()
	data1, err := ioutil.ReadAll(binlog)
	if err != nil {
		t.Errorf("failed to read data exapmple: %v", err)
	}
	_, err = binlog.Seek(0, 0)
	if err != nil {
		t.Errorf("failed to seek data exapmple: %v", err)
	}
	br := NewBinlogReader(binlog, startTs, endTs)
	data2, err := ioutil.ReadAll(br)
	if err != nil {
		t.Errorf("failed to read whole binlog through BinlogReader: %v", err)
	}
	if bytes.Compare(data1[:len(data2)], data2) != 0 {
		t.Errorf("binlog differs from orriginal one (at read prefix)")
	}
	if !br.NeedAbort() {
		t.Errorf("binlog reader should be marked as needed abort")
	}
}

// stupid reader that reads by small chunks
type antiBufReader struct {
	Reader io.Reader
	Limit  int
}

func (abr *antiBufReader) Read(buf []byte) (int, error) {
	limit := abr.Limit
	if len(buf) < limit {
		limit = len(buf)
	}
	return abr.Reader.Read(buf[:limit])
}

func TestReadWholeBinlogWithDifferentChunks(t *testing.T) {
	startTs := MinTime
	endTs := MaxTime
	binlog, err := os.Open(testFilenameBig)
	if err != nil {
		t.Errorf("failed to open data example: %v", err)
	}
	defer binlog.Close()
	data1, err := ioutil.ReadAll(binlog)
	if err != nil {
		t.Errorf("failed to read data exapmple: %v", err)
	}
	// read binlog through binlog reader with different input and output chunk combinations
	variants := []int{5, 13, 967, 11087}
	for _, underChunk := range variants {
		for _, overChunk := range variants {
			_, err = binlog.Seek(0, 0)
			if err != nil {
				t.Errorf("failed to seek data exapmple: %v", err)
			}
			ur := &antiBufReader{binlog, underChunk}
			br := NewBinlogReader(ur, startTs, endTs)
			or := &antiBufReader{br, overChunk}
			data2, err := ioutil.ReadAll(or)
			if err != nil {
				t.Errorf("failed to read whole binlog through BinlogReader: %v", err)
			}
			if bytes.Compare(data1, data2) != 0 {
				t.Errorf("binlog differs from orriginal one, underChunk=%d overChunk=%d", underChunk, overChunk)
			}
			if br.NeedAbort() {
				t.Errorf("binlog reader unexpected marked as needed abort")
			}
		}
	}
}

func TestGetBinlogStartTimestamp(t *testing.T) {
	var tests = []struct {
		name        string
		testLogPath string
		exp         time.Time
	}{
		{"Small instance", testFilenameSmall, time.Unix(int64(1566047760), 0)},
		{"Big real instance", testFilenameBig, time.Unix(int64(1565528401), 0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetBinlogStartTimestamp(tt.testLogPath)
			if err != nil {
				t.Errorf("parseFirstTimestampFromHeader(%s) error %v", tt.testLogPath, err)
			}
			if got != tt.exp {
				t.Errorf("parseFirstTimestampFromHeader(%s) got %v, want %v", tt.testLogPath, got, tt.exp)
			}
		})
	}
}

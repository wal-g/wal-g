package mysql

import (
	"testing"
	"time"
)

const testFilenameSmall string = "./testdata/binlog_small_test"
const testFilenameBig string = "./testdata/binlog_big_test"

func TestParseFirstTimestampFromHeader_ParseDataCorrect(t *testing.T) {
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
			got, err := parseFromBinlog(tt.testLogPath)

			if err != nil {
				t.Errorf("ParseFirstTimestampFromHeader(%s) error %v", tt.testLogPath, err)
			}

			if got != tt.exp {
				t.Errorf("ParseFirstTimestampFromHeader(%s) got %v, want %v", tt.testLogPath, got, tt.exp)
			}
		})
	}
}

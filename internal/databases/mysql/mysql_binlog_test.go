package mysql

import (
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
)

const testFilenameSmall = "testdata/binlog_small_test"
const testFilenameBig = "testdata/binlog_big_test"

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
			got, err := GetBinlogStartTimestamp(tt.testLogPath, mysql.MySQLFlavor)
			if err != nil {
				t.Errorf("parseFirstTimestampFromHeader(%s) error %v", tt.testLogPath, err)
			}
			if got != tt.exp {
				t.Errorf("parseFirstTimestampFromHeader(%s) got %v, want %v", tt.testLogPath, got, tt.exp)
			}
		})
	}
}

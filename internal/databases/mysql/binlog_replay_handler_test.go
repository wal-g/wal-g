package mysql

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShouldApplyStartPosition(t *testing.T) {
	tests := []struct {
		name           string
		backupFileName string
		backupPosition int64
		backupLastGTID string
		actualServerID uint32
		serverIDErr    error
		actualUUID     string
		uuidErr        error
		candidateName  string
		want           bool
	}{
		{
			name:           "no gtid recorded -- old filename-only behavior",
			backupFileName: "mysql-bin.000003", backupPosition: 385, backupLastGTID: "",
			candidateName: "mysql-bin.000003", want: true,
		},
		{
			name:           "matching filename and matching server id (mariadb)",
			backupFileName: "mysql-bin.000003", backupPosition: 385, backupLastGTID: "0-1-12",
			actualServerID: 1, candidateName: "mysql-bin.000003", want: true,
		},
		{
			name:           "matching filename but different server -- coincidental match after failover (mariadb)",
			backupFileName: "mysql-bin.000003", backupPosition: 385, backupLastGTID: "0-1-12",
			actualServerID: 2, candidateName: "mysql-bin.000003", want: false,
		},
		{
			name:           "different filename entirely",
			backupFileName: "mysql-bin.000003", backupPosition: 385, backupLastGTID: "0-1-12",
			actualServerID: 1, candidateName: "mysql-bin.000004", want: false,
		},
		{
			name:           "no position recorded",
			backupFileName: "mysql-bin.000003", backupPosition: 0, backupLastGTID: "0-1-12",
			actualServerID: 1, candidateName: "mysql-bin.000003", want: false,
		},
		{
			name:           "server id lookup fails -- don't trust the position (mariadb)",
			backupFileName: "mysql-bin.000003", backupPosition: 385, backupLastGTID: "0-1-12",
			serverIDErr: assert.AnError, candidateName: "mysql-bin.000003", want: false,
		},
		{
			name:           "matching filename and matching server uuid (mysql)",
			backupFileName: "mysql-bin.000003", backupPosition: 385,
			backupLastGTID: "3f6cd603-8d0b-11f1-8432-d00dc21b3e49:1-2647395",
			actualUUID:     "3f6cd603-8d0b-11f1-8432-d00dc21b3e49",
			candidateName:  "mysql-bin.000003", want: true,
		},
		{
			name:           "matching filename but different server uuid -- coincidental match after failover (mysql)",
			backupFileName: "mysql-bin.000003", backupPosition: 385,
			backupLastGTID: "3f6cd603-8d0b-11f1-8432-d00dc21b3e49:1-2647395",
			actualUUID:     "aaaaaaaa-8d0b-11f1-8432-d00dc21b3e49",
			candidateName:  "mysql-bin.000003", want: false,
		},
		{
			name:           "server uuid lookup fails -- don't trust the position (mysql)",
			backupFileName: "mysql-bin.000003", backupPosition: 385,
			backupLastGTID: "3f6cd603-8d0b-11f1-8432-d00dc21b3e49:1-2647395",
			uuidErr:        assert.AnError,
			candidateName:  "mysql-bin.000003", want: false,
		},
		{
			name:           "gtid recorded but not parseable in any known flavor -- don't trust the position",
			backupFileName: "mysql-bin.000003", backupPosition: 385, backupLastGTID: "not-a-gtid",
			candidateName: "mysql-bin.000003", want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rh := new(replayHandler)
			rh.backupBinlogPos.FileName = tt.backupFileName
			rh.backupBinlogPos.FilePosition = tt.backupPosition
			rh.backupBinlogPos.LastGTID = tt.backupLastGTID
			rh.getBinlogServerID = func(string) (uint32, error) {
				return tt.actualServerID, tt.serverIDErr
			}
			rh.getBinlogServerUUID = func(string) (string, error) {
				return tt.actualUUID, tt.uuidErr
			}

			got := rh.shouldApplyStartPosition(filepath.Join(t.TempDir(), tt.candidateName))
			assert.Equal(t, tt.want, got)
		})
	}
}

package mysql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func TestSkipCoveredBinlogs(t *testing.T) {
	base := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	at := func(minutes int) time.Time { return base.Add(time.Duration(minutes) * time.Minute) }
	binlog := func(name string, minutes int) storage.Object {
		return storage.NewLocalObject(name, at(minutes), 1)
	}
	// backup sentinel uploaded at minute 30, boundary file mysql-bin.000011
	sentinelTS := at(30)

	tests := []struct {
		name     string
		binlogs  []storage.Object
		startTS  time.Time
		boundary string
		want     time.Time
	}{
		{
			name: "skips files before the boundary uploaded before the backup",
			binlogs: []storage.Object{
				binlog("mysql-bin.000011.br", 40),
				binlog("mysql-bin.000009.br", 0),
				binlog("mysql-bin.000010.br", 20),
				binlog("mysql-bin.000012.br", 50),
			},
			startTS:  at(0),
			boundary: "mysql-bin.000011",
			want:     at(40),
		},
		{
			name: "keeps files uploaded after the backup, as after a failover",
			binlogs: []storage.Object{
				binlog("mysql-bin.000009.br", 0),
				binlog("mysql-bin.000005.br", 35),
				binlog("mysql-bin.000011.br", 40),
			},
			startTS:  at(0),
			boundary: "mysql-bin.000011",
			want:     at(35),
		},
		{
			name: "keeps everything from the first file that is not covered",
			binlogs: []storage.Object{
				binlog("mysql-bin.000009.br", 0),
				binlog("mysql-bin.000011.br", 10),
				binlog("mysql-bin.000010.br", 20),
			},
			startTS:  at(0),
			boundary: "mysql-bin.000011",
			want:     at(10),
		},
		{
			name: "keeps files of another sequence",
			binlogs: []storage.Object{
				binlog("other-bin.000003.br", 0),
				binlog("mysql-bin.000011.br", 40),
			},
			startTS:  at(0),
			boundary: "mysql-bin.000011",
			want:     at(0),
		},
		{
			name: "keeps a file uploaded at the same instant as the first kept one",
			binlogs: []storage.Object{
				binlog("mysql-bin.000009.br", 0),
				binlog("mysql-bin.000010.br", 20),
				binlog("mysql-bin.000011.br", 20),
			},
			startTS:  at(0),
			boundary: "mysql-bin.000011",
			want:     at(20),
		},
		{
			name: "ignores files uploaded before startTS",
			binlogs: []storage.Object{
				binlog("mysql-bin.000008.br", 0),
				binlog("mysql-bin.000009.br", 10),
				binlog("mysql-bin.000011.br", 40),
			},
			startTS:  at(10),
			boundary: "mysql-bin.000011",
			want:     at(40),
		},
		{
			name: "keeps startTS when only covered files are left",
			binlogs: []storage.Object{
				binlog("mysql-bin.000009.br", 0),
				binlog("mysql-bin.000010.br", 20),
			},
			startTS:  at(0),
			boundary: "mysql-bin.000011",
			want:     at(0),
		},
		{
			name: "keeps startTS without a recorded boundary",
			binlogs: []storage.Object{
				binlog("mysql-bin.000009.br", 0),
				binlog("mysql-bin.000011.br", 40),
			},
			startTS:  at(0),
			boundary: "",
			want:     at(0),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := skipCoveredBinlogs(tt.binlogs, tt.startTS, sentinelTS, tt.boundary)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSkipCoveredBinlogs_NoSentinel(t *testing.T) {
	binlogs := []storage.Object{
		storage.NewLocalObject("mysql-bin.000009.br", time.Unix(0, 0), 1),
		storage.NewLocalObject("mysql-bin.000011.br", time.Unix(60, 0), 1),
	}
	got := skipCoveredBinlogs(binlogs, time.Unix(0, 0), utility.MaxTime, "mysql-bin.000011")
	assert.Equal(t, time.Unix(0, 0), got)
}

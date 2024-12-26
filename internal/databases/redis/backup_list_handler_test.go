package redis_test

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/redis"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

func init() {
	internal.ConfigureSettings("")
	config.InitConfig()
	config.Configure()
}

func TestHandleDetailedBackupList(t *testing.T) {
	t.Run("print correct backup details in correct order", func(t *testing.T) {
		folder := testtools.MakeDefaultInMemoryStorageFolder()
		curTime := time.Unix(1690000000, 0)

		backups := []archive.Backup{
			archive.Backup{
				BackupName:      "b0",
				StartLocalTime:  curTime.Add(4 * time.Second).UTC(),
				FinishLocalTime: curTime.Add(5 * time.Second).UTC(),
				DataSize:        100000,
				BackupSize:      200000,
				Permanent:       true,
				UserData:        []string{"g", "h", "i"},
				Version:         "4.5.4",
				BackupType:      "rdb",
			},
			archive.Backup{
				BackupName:      "b1",
				StartLocalTime:  curTime.Add(0 * time.Second).UTC(),
				FinishLocalTime: curTime.Add(time.Second).UTC(),
				DataSize:        100000,
				BackupSize:      200000,
				Permanent:       true,
				UserData:        []string{"a", "b", "c"},
				Version:         "4.5.4",
				BackupType:      "rdb",
			},
			archive.Backup{
				BackupName:      "b2",
				StartLocalTime:  curTime.Add(2 * time.Second).UTC(),
				FinishLocalTime: curTime.Add(3 * time.Second).UTC(),
				DataSize:        100000,
				BackupSize:      200000,
				Permanent:       true,
				UserData:        []string{"d", "e", "f"},
				Version:         "4.5.4",
				BackupType:      "rdb",
			},
		}

		for _, b := range backups {
			serialized, _ := json.Marshal(b)

			assert.NoError(
				t,
				folder.PutObject(b.BackupName+utility.SentinelSuffix, bytes.NewReader(serialized)),
				"couldn't put sentinel in the folder",
			)
		}

		rescueStdout := os.Stdout
		r, w, _ := os.Pipe()
		os.Stdout = w
		defer func() { os.Stdout = rescueStdout }()

		redis.HandleDetailedBackupList(folder, true, true)

		_ = w.Close()
		got, _ := io.ReadAll(r)

		slices.SortFunc(backups, func(a, b archive.Backup) int {
			return int(a.FinishLocalTime.Sub(b.FinishLocalTime))
		})

		serializedBackups, _ := json.Marshal(backups)
		want := string(serializedBackups)

		assert.JSONEq(t, want, string(got))
	})

	t.Run("handle error with no backups", func(t *testing.T) {
		folder := memory.NewFolder("", memory.NewKVS())

		infoOutput := new(bytes.Buffer)
		rescueInfoOutput := tracelog.InfoLogger.Writer()
		tracelog.InfoLogger.SetOutput(infoOutput)
		defer func() { tracelog.InfoLogger.SetOutput(rescueInfoOutput) }()

		rescueStdout := os.Stdout
		r, w, _ := os.Pipe()
		os.Stdout = w
		defer func() { os.Stdout = rescueStdout }()

		redis.HandleDetailedBackupList(folder, true, false)

		_ = w.Close()
		captured, _ := io.ReadAll(r)

		assert.Empty(t, string(captured))
		assert.Contains(t, infoOutput.String(), "No backups found")
	})
}

package postgres

import (
	"bytes"
	"io"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/internal/multistorage/cache"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestHandleDetailedBackupList(t *testing.T) {
	curTime := time.Time{}
	curTimeFunc := func() time.Time {
		return curTime.UTC()
	}

	t.Run("print correct backup details in correct order", func(t *testing.T) {
		folder := memory.NewFolder("", memory.NewStorage(memory.WithCustomTime(curTimeFunc)))
		curTime = time.Unix(1690000000, 0)
		_ = folder.PutObject("base_111_backup_stop_sentinel.json", &bytes.Buffer{})
		_ = folder.PutObject("base_111/metadata.json", bytes.NewBufferString("{}"))
		curTime = curTime.Add(time.Second)
		_ = folder.PutObject("base_222_backup_stop_sentinel.json", &bytes.Buffer{})
		_ = folder.PutObject("base_222/metadata.json", bytes.NewBufferString("{}"))
		curTime = curTime.Add(time.Second)
		_ = folder.PutObject("base_333_backup_stop_sentinel.json", &bytes.Buffer{})
		_ = folder.PutObject("base_333/metadata.json", bytes.NewBufferString("{}"))

		rescueStdout := os.Stdout
		r, w, _ := os.Pipe()
		os.Stdout = w
		defer func() { os.Stdout = rescueStdout }()

		HandleDetailedBackupList(folder, true, true)

		_ = w.Close()
		captured, _ := io.ReadAll(r)

		want := `[
    {
        "backup_name": "base_111",
        "time": "2023-07-22T04:26:40Z",
        "wal_file_name": "ZZZZZZZZZZZZZZZZZZZZZZZZ",
        "storage_name": "default",
        "start_time": "0001-01-01T00:00:00Z",
        "finish_time": "0001-01-01T00:00:00Z",
        "date_fmt": "",
        "hostname": "",
        "data_dir": "",
        "pg_version": 0,
        "start_lsn": 0,
        "finish_lsn": 0,
        "is_permanent": false,
        "system_identifier": null,
        "uncompressed_size": 0,
        "compressed_size": 0
    },
    {
        "backup_name": "base_222",
        "time": "2023-07-22T04:26:41Z",
        "wal_file_name": "ZZZZZZZZZZZZZZZZZZZZZZZZ",
        "storage_name": "default",
        "start_time": "0001-01-01T00:00:00Z",
        "finish_time": "0001-01-01T00:00:00Z",
        "date_fmt": "",
        "hostname": "",
        "data_dir": "",
        "pg_version": 0,
        "start_lsn": 0,
        "finish_lsn": 0,
        "is_permanent": false,
        "system_identifier": null,
        "uncompressed_size": 0,
        "compressed_size": 0
    },
    {
        "backup_name": "base_333",
        "time": "2023-07-22T04:26:42Z",
        "wal_file_name": "ZZZZZZZZZZZZZZZZZZZZZZZZ",
        "storage_name": "default",
        "start_time": "0001-01-01T00:00:00Z",
        "finish_time": "0001-01-01T00:00:00Z",
        "date_fmt": "",
        "hostname": "",
        "data_dir": "",
        "pg_version": 0,
        "start_lsn": 0,
        "finish_lsn": 0,
        "is_permanent": false,
        "system_identifier": null,
        "uncompressed_size": 0,
        "compressed_size": 0
    }
]
`
		assert.Equal(t, want, string(captured))
	})

	t.Run("print backups from different storages", func(t *testing.T) {
		mockCtrl := gomock.NewController(t)
		t.Cleanup(mockCtrl.Finish)
		cacheMock := cache.NewMockStatusCache(mockCtrl)

		memStorages := []cache.NamedFolder{
			{
				Name:   "storage_1",
				Root:   "",
				Folder: memory.NewFolder("", memory.NewStorage(memory.WithCustomTime(curTimeFunc))),
			},
			{
				Name:   "storage_2",
				Root:   "",
				Folder: memory.NewFolder("", memory.NewStorage(memory.WithCustomTime(curTimeFunc))),
			},
		}
		cacheMock.EXPECT().AllAliveStorages().Return(memStorages, nil)
		cacheMock.EXPECT().SpecificStorage("storage_1").Return(&memStorages[0], nil)
		cacheMock.EXPECT().SpecificStorage("storage_2").Return(&memStorages[1], nil)

		multiFolder := multistorage.NewFolder(cacheMock).(storage.Folder)
		multiFolder = multistorage.SetPolicies(multiFolder, policies.UniteAllStorages)
		multiFolder, err := multistorage.UseAllAliveStorages(multiFolder)
		require.NoError(t, err)

		curTime = time.Unix(1690000000, 0)
		_ = memStorages[0].PutObject("base_111_backup_stop_sentinel.json", &bytes.Buffer{})
		_ = memStorages[0].PutObject("base_111/metadata.json", bytes.NewBufferString("{}"))
		curTime = curTime.Add(time.Second)
		_ = memStorages[1].PutObject("base_111_backup_stop_sentinel.json", &bytes.Buffer{})
		_ = memStorages[1].PutObject("base_111/metadata.json", bytes.NewBufferString("{}"))

		rescueStdout := os.Stdout
		r, w, _ := os.Pipe()
		os.Stdout = w
		defer func() { os.Stdout = rescueStdout }()

		HandleDetailedBackupList(multiFolder, true, true)

		_ = w.Close()
		captured, _ := io.ReadAll(r)

		want := `[
    {
        "backup_name": "base_111",
        "time": "2023-07-22T04:26:40Z",
        "wal_file_name": "ZZZZZZZZZZZZZZZZZZZZZZZZ",
        "storage_name": "storage_1",
        "start_time": "0001-01-01T00:00:00Z",
        "finish_time": "0001-01-01T00:00:00Z",
        "date_fmt": "",
        "hostname": "",
        "data_dir": "",
        "pg_version": 0,
        "start_lsn": 0,
        "finish_lsn": 0,
        "is_permanent": false,
        "system_identifier": null,
        "uncompressed_size": 0,
        "compressed_size": 0
    },
    {
        "backup_name": "base_111",
        "time": "2023-07-22T04:26:41Z",
        "wal_file_name": "ZZZZZZZZZZZZZZZZZZZZZZZZ",
        "storage_name": "storage_2",
        "start_time": "0001-01-01T00:00:00Z",
        "finish_time": "0001-01-01T00:00:00Z",
        "date_fmt": "",
        "hostname": "",
        "data_dir": "",
        "pg_version": 0,
        "start_lsn": 0,
        "finish_lsn": 0,
        "is_permanent": false,
        "system_identifier": null,
        "uncompressed_size": 0,
        "compressed_size": 0
    }
]
`
		assert.Equal(t, want, string(captured))
	})

	t.Run("handle error with no backups", func(t *testing.T) {
		folder := memory.NewFolder("", memory.NewStorage(memory.WithCustomTime(curTimeFunc)))

		infoOutput := new(bytes.Buffer)
		rescueInfoOutput := tracelog.InfoLogger.Writer()
		tracelog.InfoLogger.SetOutput(infoOutput)
		defer func() { tracelog.InfoLogger.SetOutput(rescueInfoOutput) }()

		rescueStdout := os.Stdout
		r, w, _ := os.Pipe()
		os.Stdout = w
		defer func() { os.Stdout = rescueStdout }()

		HandleDetailedBackupList(folder, true, false)

		_ = w.Close()
		captured, _ := io.ReadAll(r)

		assert.Empty(t, string(captured))
		assert.Contains(t, infoOutput.String(), "No backups found")
	})
}

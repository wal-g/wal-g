package mysql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/pkg/storages/memory"
)

func initDTOConfig(t *testing.T) {
	t.Helper()
	internal.ConfigureSettings("")
	conf.InitConfig()
}

func TestGenericMetaFetcher_Fetch_Success(t *testing.T) {
	initDTOConfig(t)

	kvs := memory.NewKVS()
	st := memory.NewStorage("memory://generic-meta-interactor-test", kvs)
	folder := st.RootFolder()

	backupName := "base_000000010000000000000001"
	backup, err := internal.NewBackup(folder, backupName)
	require.NoError(t, err)

	start := time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)
	stop := time.Date(2001, 1, 1, 1, 1, 1, 1, time.UTC)

	incFrom := "base_000000010000000000000000"
	incFull := "base_000000010000000000000000_full"
	incCount := 3

	sentinel := StreamSentinelDto{
		UncompressedSize:  7654321,
		CompressedSize:    1234567,
		Hostname:          "host1",
		StartLocalTime:    start,
		StopLocalTime:     stop,
		IsPermanent:       true,
		UserData:          "userdata",
		IsIncremental:     true,
		IncrementFrom:     &incFrom,
		IncrementFullName: &incFull,
		IncrementCount:    &incCount,
	}

	require.NoError(t, backup.UploadSentinel(sentinel))

	mf := NewGenericMetaFetcher()
	meta, err := mf.Fetch(backupName, folder)
	require.NoError(t, err)

	require.Equal(t, backupName, meta.BackupName)
	require.Equal(t, sentinel.UncompressedSize, meta.UncompressedSize)
	require.Equal(t, sentinel.CompressedSize, meta.CompressedSize)
	require.Equal(t, sentinel.Hostname, meta.Hostname)
	require.Equal(t, start, meta.StartTime)
	require.Equal(t, stop, meta.FinishTime)
	require.Equal(t, sentinel.IsPermanent, meta.IsPermanent)
	require.Equal(t, sentinel.UserData, meta.UserData)

	ok, inc, err := meta.IncrementDetails.Fetch()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, incFrom, inc.IncrementFrom)
	require.Equal(t, incFull, inc.IncrementFullName)
	require.Equal(t, incCount, inc.IncrementCount)
}

func TestGenericMetaFetcher_Fetch_NoSentinel_ReturnsError(t *testing.T) {
	initDTOConfig(t)

	kvs := memory.NewKVS()
	st := memory.NewStorage("memory://generic-meta-interactor-test", kvs)
	folder := st.RootFolder()

	mf := NewGenericMetaFetcher()
	_, err := mf.Fetch("nonexistent_backup", folder)
	require.Error(t, err)
}

func TestGenericMetaFetcher_Fetch_NonIncremental(t *testing.T) {
	initDTOConfig(t)

	kvs := memory.NewKVS()
	st := memory.NewStorage("memory://generic-meta-interactor-test", kvs)
	folder := st.RootFolder()

	backupName := "base_000000010000000000000002"
	backup, err := internal.NewBackup(folder, backupName)
	require.NoError(t, err)

	start := time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)
	stop := time.Date(2001, 1, 1, 1, 1, 1, 1, time.UTC)

	sentinel := StreamSentinelDto{
		UncompressedSize: 7654321,
		CompressedSize:   1234567,
		Hostname:         "host2",
		StartLocalTime:   start,
		StopLocalTime:    stop,
		IsPermanent:      false,
		UserData:         "userdata",
		IsIncremental:    false,
	}

	require.NoError(t, backup.UploadSentinel(sentinel))

	mf := NewGenericMetaFetcher()
	meta, err := mf.Fetch(backupName, folder)
	require.NoError(t, err)

	require.Equal(t, backupName, meta.BackupName)
	require.Equal(t, sentinel.UncompressedSize, meta.UncompressedSize)
	require.Equal(t, sentinel.CompressedSize, meta.CompressedSize)
	require.Equal(t, sentinel.Hostname, meta.Hostname)
	require.Equal(t, start, meta.StartTime)
	require.Equal(t, stop, meta.FinishTime)
	require.Equal(t, sentinel.IsPermanent, meta.IsPermanent)
	require.Equal(t, sentinel.UserData, meta.UserData)

	ok, inc, err := meta.IncrementDetails.Fetch()
	require.NoError(t, err)
	require.False(t, ok)
	require.Zero(t, inc)
}

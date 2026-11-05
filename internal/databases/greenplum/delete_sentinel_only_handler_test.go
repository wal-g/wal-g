package greenplum

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

const (
	sentinelOnlyFull1    = "backup_20240101T000000Z"
	sentinelOnlyDelta2   = "backup_20240102T000000Z_D_20240101T000000Z"
	sentinelOnlyDelta3   = "backup_20240103T000000Z_D_20240102T000000Z"
	sentinelOnlyFull4    = "backup_20240104T000000Z"
	sentinelOnlyFromFull = "backup_20240105T000000Z_D_20240101T000000Z"

	sentinelOnlySegmentSentinel = "segments_005/seg0/basebackups_005/base_000000010000000000000002" + utility.SentinelSuffix
	sentinelOnlyRestorePoint    = "basebackups_005/" + sentinelOnlyFull1 + RestorePointSuffix
)

var sentinelOnlyBackups = []string{
	sentinelOnlyFull1, sentinelOnlyDelta2, sentinelOnlyDelta3, sentinelOnlyFull4, sentinelOnlyFromFull,
}

// sentinelOnlyReadErrorFolder fails reading the object with the given full path, like S3 answering 500.
type sentinelOnlyReadErrorFolder struct {
	storage.Folder
	failing string
}

func (f sentinelOnlyReadErrorFolder) GetSubFolder(path string) storage.Folder {
	return sentinelOnlyReadErrorFolder{f.Folder.GetSubFolder(path), f.failing}
}

func (f sentinelOnlyReadErrorFolder) ReadObject(ctx context.Context, key string) (io.ReadCloser, error) {
	if f.GetPath()+key == f.failing {
		return nil, errors.New("GET 500")
	}
	return f.Folder.ReadObject(ctx, key)
}

func sentinelOnlyFixture(t *testing.T, permanent ...string) storage.Folder {
	t.Helper()
	root := memory.NewFolder("", memory.NewKVS())
	base := root.GetSubFolder(utility.BaseBackupPath)
	for _, name := range sentinelOnlyBackups {
		sentinel := BackupSentinelDto{}
		for _, p := range permanent {
			sentinel.IsPermanent = sentinel.IsPermanent || p == name
		}
		require.NoError(t, internal.UploadDto(t.Context(), base, sentinel, name+utility.SentinelSuffix))
	}
	for _, key := range []string{sentinelOnlySegmentSentinel, sentinelOnlyRestorePoint} {
		require.NoError(t, root.PutObject(t.Context(), key, strings.NewReader("{}")))
	}
	return root
}

func deleteSentinelsOnly(t *testing.T, folder storage.Folder, target string, args DeleteArgs) error {
	t.Helper()
	handler, err := NewDeleteSentinelOnlyHandler(t.Context(), folder, args)
	require.NoError(t, err)
	selector, err := internal.NewBackupNameSelector(target, true)
	require.NoError(t, err)
	return handler.deleteTargetSentinels(t.Context(), selector)
}

func requireClusterBackups(t *testing.T, root storage.Folder, expected ...string) {
	t.Helper()
	var actual []string
	for _, name := range sentinelOnlyBackups {
		exists, err := root.GetSubFolder(utility.BaseBackupPath).Exists(t.Context(), name+utility.SentinelSuffix)
		require.NoError(t, err)
		if exists {
			actual = append(actual, name)
		}
	}
	require.ElementsMatch(t, expected, actual)
	for _, key := range []string{sentinelOnlySegmentSentinel, sentinelOnlyRestorePoint} {
		exists, err := root.Exists(t.Context(), key)
		require.NoError(t, err)
		require.True(t, exists, "only cluster sentinels may be deleted: %s", key)
	}
}

func TestDeleteSentinelOnlyRemovesIncrements(t *testing.T) {
	root := sentinelOnlyFixture(t)
	require.NoError(t, deleteSentinelsOnly(t, root, sentinelOnlyDelta2, DeleteArgs{Confirmed: true}))
	requireClusterBackups(t, root, sentinelOnlyFull1, sentinelOnlyFull4, sentinelOnlyFromFull)
}

func TestDeleteSentinelOnlyFindFull(t *testing.T) {
	root := sentinelOnlyFixture(t)
	require.NoError(t, deleteSentinelsOnly(t, root, sentinelOnlyDelta3, DeleteArgs{Confirmed: true, FindFull: true}))
	requireClusterBackups(t, root, sentinelOnlyFull4)
}

func TestDeleteSentinelOnlyDryRun(t *testing.T) {
	root := sentinelOnlyFixture(t)
	require.NoError(t, deleteSentinelsOnly(t, root, sentinelOnlyFull1, DeleteArgs{}))
	requireClusterBackups(t, root, sentinelOnlyBackups...)
}

func TestDeleteSentinelOnlyToleratesUnreadableSentinels(t *testing.T) {
	root := sentinelOnlyFixture(t)
	base := root.GetSubFolder(utility.BaseBackupPath)
	require.NoError(t, base.PutObject(t.Context(), sentinelOnlyFull1+utility.SentinelSuffix, strings.NewReader("broken")))
	folder := sentinelOnlyReadErrorFolder{root, utility.BaseBackupPath + sentinelOnlyDelta2 + utility.SentinelSuffix}

	require.NoError(t, deleteSentinelsOnly(t, folder, sentinelOnlyFull1, DeleteArgs{Confirmed: true}))
	requireClusterBackups(t, root, sentinelOnlyFull4)
}

func TestDeleteSentinelOnlyMissingTarget(t *testing.T) {
	root := sentinelOnlyFixture(t)
	require.Error(t, deleteSentinelsOnly(t, root, "backup_20240109T000000Z", DeleteArgs{Confirmed: true}))
	requireClusterBackups(t, root, sentinelOnlyBackups...)
}

// DeleteTarget stops with Fatalf on a permanent backup, so the permanence check is tested directly.
func TestSentinelPermanence(t *testing.T) {
	// All three are permanent, but the sentinels of full1 and full4 cannot be read.
	root := sentinelOnlyFixture(t, sentinelOnlyDelta3, sentinelOnlyFull1, sentinelOnlyFull4)
	base := root.GetSubFolder(utility.BaseBackupPath)
	require.NoError(t, base.PutObject(t.Context(), sentinelOnlyFull1+utility.SentinelSuffix, strings.NewReader("broken")))
	folder := sentinelOnlyReadErrorFolder{base, utility.BaseBackupPath + sentinelOnlyFull4 + utility.SentinelSuffix}
	sentinel := func(name string) storage.Object {
		return storage.NewLocalObject(name+utility.SentinelSuffix, time.Time{}, 0)
	}

	permanence := &sentinelPermanence{ctx: t.Context(), folder: folder, checked: map[string]bool{}}
	require.True(t, permanence.isPermanent(sentinel(sentinelOnlyDelta3)))
	require.False(t, permanence.isPermanent(sentinel(sentinelOnlyDelta2)))
	require.False(t, permanence.isPermanent(sentinel(sentinelOnlyFull1)), "a malformed sentinel is considered impermanent")
	require.False(t, permanence.isPermanent(sentinel(sentinelOnlyFull4)), "an unreadable sentinel is considered impermanent")
}

func TestParseClusterBackupName(t *testing.T) {
	for name, expected := range map[string][2]string{
		sentinelOnlyFull1:  {"20240101T000000Z", ""},
		sentinelOnlyDelta2: {"20240102T000000Z", "20240101T000000Z"},
	} {
		backupTime, baseTime, ok := parseClusterBackupName(name)
		require.True(t, ok, name)
		require.Equal(t, expected, [2]string{backupTime, baseTime}, name)
	}
	for _, name := range []string{
		"base_000000010000000000000002",
		"backup_2024",
		"backup_20240101T000000Z_D_",
		"backup_20240101T000000Z_X_20231231T000000Z",
	} {
		_, _, ok := parseClusterBackupName(name)
		require.False(t, ok, name)
	}
}

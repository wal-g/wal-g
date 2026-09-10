package greenplum

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/greenplum/ao"
	"github.com/wal-g/wal-g/internal/databases/greenplum/pax"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

const (
	firstSegBackup  = "base_000000010000000000000001"
	secondSegBackup = "base_000000010000000000000002"
	thirdSegBackup  = "base_000000010000000000000003"
)

func TestReassignedAOSharedSize(t *testing.T) {
	t.Run("reassigns objects across a deleted middle block", func(t *testing.T) {
		folder := newSharedMetadataTestFolder()
		putSharedMetadataBackup(t, folder, firstSegBackup, ao.GetFilesMetadataPath(firstSegBackup),
			ao.FilesMetadataDTO{
				Files: ao.BackupFiles{
					"local/x": {StoragePath: "x_aoseg", EOF: 10},
				},
				UploadedSharedSize: 10,
			})
		// The second backup has already been deleted. The third one still references an object
		// uploaded by it, as well as an object of its own.
		putSharedMetadataBackup(t, folder, thirdSegBackup, ao.GetFilesMetadataPath(thirdSegBackup),
			ao.FilesMetadataDTO{
				Files: ao.BackupFiles{
					"local/x": {StoragePath: "x_aoseg", EOF: 10},
					"local/y": {StoragePath: "y_aoseg", EOF: 20},
					"local/z": {StoragePath: "z_aoseg", EOF: 30},
				},
				UploadedSharedSize: 30,
			})

		size, err := reassignedSharedSize(t.Context(), folder, thirdSegBackup, ao.FetchReferencedFiles)
		require.NoError(t, err)
		assert.Equal(t, int64(50), size)

		// Reassignment must not rewrite the segment files metadata.
		meta := fetchAOMetadata(t, folder, thirdSegBackup)
		assert.Equal(t, int64(30), meta.UploadedSharedSize)

		// Repeating the calculation must return the same exact total, not add it again.
		size, err = reassignedSharedSize(t.Context(), folder, thirdSegBackup, ao.FetchReferencedFiles)
		require.NoError(t, err)
		assert.Equal(t, int64(50), size)
	})

	t.Run("the oldest survivor owns all of its references", func(t *testing.T) {
		folder := newSharedMetadataTestFolder()
		putSharedMetadataBackup(t, folder, thirdSegBackup, ao.GetFilesMetadataPath(thirdSegBackup),
			ao.FilesMetadataDTO{
				Files: ao.BackupFiles{
					"local/x": {StoragePath: "x_aoseg", EOF: 10},
					"local/y": {StoragePath: "y_aoseg", EOF: 20},
				},
			})

		size, err := reassignedSharedSize(t.Context(), folder, thirdSegBackup, ao.FetchReferencedFiles)
		require.NoError(t, err)
		assert.Equal(t, int64(30), size)
	})
}

func TestReassignedPaxSharedSize(t *testing.T) {
	t.Run("uses the per-file PAX size and counts a StoragePath once", func(t *testing.T) {
		folder := newSharedMetadataTestFolder()
		putSharedMetadataBackup(t, folder, firstSegBackup, pax.GetFilesMetadataPath(firstSegBackup),
			pax.FilesMetadataDTO{
				Files: pax.BackupFiles{
					"local/x":       {StoragePath: "x_pax", Size: 10},
					"local/x-alias": {StoragePath: "x_pax", Size: 10},
				},
				UploadedSharedSize: 777,
			})
		putSharedMetadataBackup(t, folder, thirdSegBackup, pax.GetFilesMetadataPath(thirdSegBackup),
			pax.FilesMetadataDTO{
				Files: pax.BackupFiles{
					"local/x": {StoragePath: "x_pax", Size: 10},
					"local/y": {StoragePath: "y_pax", Size: 20},
				},
			})

		firstSize, err := reassignedSharedSize(t.Context(), folder, firstSegBackup, pax.FetchReferencedFiles)
		require.NoError(t, err)
		assert.Equal(t, int64(10), firstSize)
		thirdSize, err := reassignedSharedSize(t.Context(), folder, thirdSegBackup, pax.FetchReferencedFiles)
		require.NoError(t, err)
		assert.Equal(t, int64(20), thirdSize)
		assert.Equal(t, int64(777), fetchPaxMetadata(t, folder, firstSegBackup).UploadedSharedSize)
	})

	t.Run("does not guess across missing metadata", func(t *testing.T) {
		folder := newSharedMetadataTestFolder()
		putSharedMetadataBackup(t, folder, firstSegBackup, pax.GetFilesMetadataPath(firstSegBackup),
			pax.FilesMetadataDTO{
				Files:              pax.BackupFiles{"local/x": {StoragePath: "x_pax", Size: 10}},
				UploadedSharedSize: 10,
			})
		putSharedMetadataSentinel(t, folder, secondSegBackup)
		putSharedMetadataBackup(t, folder, thirdSegBackup, pax.GetFilesMetadataPath(thirdSegBackup),
			pax.FilesMetadataDTO{
				Files:              pax.BackupFiles{"local/y": {StoragePath: "y_pax", Size: 20}},
				UploadedSharedSize: 777,
			})

		_, err := reassignedSharedSize(t.Context(), folder, thirdSegBackup, pax.FetchReferencedFiles)
		assert.Error(t, err)
	})
}

func TestBackupsWithChangedPredecessor(t *testing.T) {
	backupTimes := func(names ...string) []internal.BackupTime {
		backups := make([]internal.BackupTime, 0, len(names))
		for _, name := range names {
			backups = append(backups, internal.BackupTime{BackupName: name})
		}
		return backups
	}

	t.Run("one survivor per deleted block is affected", func(t *testing.T) {
		before := backupTimes("a", "b", "c", "d", "e")
		after := backupTimes("a", "c", "e")
		assert.Equal(t, []string{"c", "e"}, backupsWithChangedPredecessor(before, after))
	})

	t.Run("the next full backup is affected when a full backup and all its increments are deleted", func(t *testing.T) {
		before := backupTimes(
			"full-1", "full-1-increment-1",
			"full-2", "full-2-increment-1", "full-2-increment-2",
			"full-3", "full-3-increment-1",
		)
		after := backupTimes(
			"full-1", "full-1-increment-1",
			"full-3", "full-3-increment-1",
		)

		assert.Equal(t, []string{"full-3"}, backupsWithChangedPredecessor(before, after))
	})

	t.Run("the oldest survivor is affected after deleting a prefix", func(t *testing.T) {
		before := backupTimes("a", "b", "c", "d")
		after := backupTimes("c", "d")
		assert.Equal(t, []string{"c"}, backupsWithChangedPredecessor(before, after))
	})

	t.Run("no survivor is affected after deleting a suffix", func(t *testing.T) {
		before := backupTimes("a", "b", "c", "d")
		after := backupTimes("a", "b")
		assert.Empty(t, backupsWithChangedPredecessor(before, after))
	})

	t.Run("nothing is affected when no backup was deleted", func(t *testing.T) {
		backups := backupTimes("a", "b", "c")
		assert.Empty(t, backupsWithChangedPredecessor(backups, backups))
	})
}

func newSharedMetadataTestFolder() storage.Folder {
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	kvs := memory.NewKVS(memory.WithCustomTime(func() time.Time {
		now = now.Add(time.Second)
		return now
	}))
	return memory.NewFolder("", kvs).GetSubFolder(utility.BaseBackupPath)
}

func putSharedMetadataSentinel(t *testing.T, folder storage.Folder, backupName string) {
	t.Helper()
	require.NoError(t, folder.PutObject(t.Context(), internal.SentinelNameFromBackup(backupName), strings.NewReader("{}")))
}

func putSharedMetadataBackup(t *testing.T, folder storage.Folder, backupName, metadataPath string, meta any) {
	t.Helper()
	putSharedMetadataSentinel(t, folder, backupName)
	require.NoError(t, internal.UploadDto(t.Context(), folder, meta, metadataPath))
}

func fetchAOMetadata(t *testing.T, folder storage.Folder, backupName string) ao.FilesMetadataDTO {
	t.Helper()
	var meta ao.FilesMetadataDTO
	require.NoError(t, internal.FetchDto(t.Context(), folder, &meta, ao.GetFilesMetadataPath(backupName)))
	return meta
}

func fetchPaxMetadata(t *testing.T, folder storage.Folder, backupName string) pax.FilesMetadataDTO {
	t.Helper()
	var meta pax.FilesMetadataDTO
	require.NoError(t, internal.FetchDto(t.Context(), folder, &meta, pax.GetFilesMetadataPath(backupName)))
	return meta
}

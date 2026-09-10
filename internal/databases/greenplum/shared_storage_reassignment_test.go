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

func TestReassignAOSharedStorage(t *testing.T) {
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

		retained, err := reassignAOSharedStorage(t.Context(), folder, true)
		require.NoError(t, err)
		assert.Equal(t, map[string]struct{}{"x_aoseg": {}, "y_aoseg": {}, "z_aoseg": {}}, retained)

		meta := fetchAOMetadata(t, folder, thirdSegBackup)
		assert.Equal(t, int64(50), meta.UploadedSharedSize)

		// Repeating cleanup must replace the value with the same exact total, not add it again.
		_, err = reassignAOSharedStorage(t.Context(), folder, true)
		require.NoError(t, err)
		assert.Equal(t, int64(50), fetchAOMetadata(t, folder, thirdSegBackup).UploadedSharedSize)
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

		_, err := reassignAOSharedStorage(t.Context(), folder, true)
		require.NoError(t, err)
		assert.Equal(t, int64(30), fetchAOMetadata(t, folder, thirdSegBackup).UploadedSharedSize)
	})

	t.Run("does not write during a dry run", func(t *testing.T) {
		folder := newSharedMetadataTestFolder()
		putSharedMetadataBackup(t, folder, firstSegBackup, ao.GetFilesMetadataPath(firstSegBackup),
			ao.FilesMetadataDTO{
				Files:              ao.BackupFiles{"local/x": {StoragePath: "x_aoseg", EOF: 10}},
				UploadedSharedSize: 777,
			})

		_, err := reassignAOSharedStorage(t.Context(), folder, false)
		require.NoError(t, err)
		assert.Equal(t, int64(777), fetchAOMetadata(t, folder, firstSegBackup).UploadedSharedSize)
	})
}

func TestReassignPaxSharedStorage(t *testing.T) {
	t.Run("uses the per-file PAX size and counts a StoragePath once", func(t *testing.T) {
		folder := newSharedMetadataTestFolder()
		putSharedMetadataBackup(t, folder, firstSegBackup, pax.GetFilesMetadataPath(firstSegBackup),
			pax.FilesMetadataDTO{
				Files: pax.BackupFiles{
					"local/x":       {StoragePath: "x_pax", Size: 10},
					"local/x-alias": {StoragePath: "x_pax", Size: 10},
				},
			})
		putSharedMetadataBackup(t, folder, thirdSegBackup, pax.GetFilesMetadataPath(thirdSegBackup),
			pax.FilesMetadataDTO{
				Files: pax.BackupFiles{
					"local/x": {StoragePath: "x_pax", Size: 10},
					"local/y": {StoragePath: "y_pax", Size: 20},
				},
			})

		_, err := reassignPaxSharedStorage(t.Context(), folder, true)
		require.NoError(t, err)
		assert.Equal(t, int64(10), fetchPaxMetadata(t, folder, firstSegBackup).UploadedSharedSize)
		assert.Equal(t, int64(20), fetchPaxMetadata(t, folder, thirdSegBackup).UploadedSharedSize)
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

		_, err := reassignPaxSharedStorage(t.Context(), folder, true)
		require.NoError(t, err)
		assert.Equal(t, int64(777), fetchPaxMetadata(t, folder, thirdSegBackup).UploadedSharedSize)
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

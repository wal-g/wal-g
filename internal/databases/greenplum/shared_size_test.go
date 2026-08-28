package greenplum_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/internal/databases/greenplum/ao"
	"github.com/wal-g/wal-g/internal/databases/greenplum/pax"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

func TestUploadSharedSizes(t *testing.T) {
	segBackups := map[int]string{
		-1: "base_000000010000000000000001",
		0:  "base_000000010000000000000002",
		1:  "base_000000010000000000000003",
	}

	t.Run("keeps the two shared storages in objects of their own", func(t *testing.T) {
		root := testtools.MakeDefaultInMemoryStorageFolder()
		putGpSentinel(t, root, gpBackupName, segBackups)
		putSegmentFilesMetadata(t, root, -1, segBackups[-1], 6, 4)
		putSegmentFilesMetadata(t, root, 0, segBackups[0], 12, 8)
		putSegmentFilesMetadata(t, root, 1, segBackups[1], 18, 12)

		require.NoError(t, greenplum.UploadSharedSizes(t.Context(), root, gpBackupName))

		aoSize, err := greenplum.FetchAOSharedSize(t.Context(), root, gpBackupName)
		require.NoError(t, err)
		assert.Equal(t, int64(36), aoSize)

		paxSize, err := greenplum.FetchPaxSharedSize(t.Context(), root, gpBackupName)
		require.NoError(t, err)
		assert.Equal(t, int64(24), paxSize, "the PAX volume must not be folded into the AO one")
	})

	t.Run("a backup that added nothing shared records a zero", func(t *testing.T) {
		root := testtools.MakeDefaultInMemoryStorageFolder()
		putGpSentinel(t, root, gpBackupName, segBackups)
		for contentID, segBackup := range segBackups {
			putSegmentFilesMetadata(t, root, contentID, segBackup, 0, 0)
		}

		require.NoError(t, greenplum.UploadSharedSizes(t.Context(), root, gpBackupName))

		aoSize, err := greenplum.FetchAOSharedSize(t.Context(), root, gpBackupName)
		require.NoError(t, err, "everything being deduplicated is a known zero, not a missing measurement")
		assert.Zero(t, aoSize)
	})

	t.Run("leaves the object unwritten when a single segment does not report", func(t *testing.T) {
		root := testtools.MakeDefaultInMemoryStorageFolder()
		putGpSentinel(t, root, gpBackupName, segBackups)
		putSegmentFilesMetadata(t, root, -1, segBackups[-1], 6, 4)
		putSegmentFilesMetadata(t, root, 0, segBackups[0], 12, 8)
		// Segment 1 has no files metadata at all, so neither sum can be trusted.

		require.NoError(t, greenplum.UploadSharedSizes(t.Context(), root, gpBackupName),
			"an unavailable volume must not fail the whole backup-push")

		_, err := greenplum.FetchAOSharedSize(t.Context(), root, gpBackupName)
		assert.Error(t, err, "a partial sum must not be recorded as if it were the real volume")
		_, err = greenplum.FetchPaxSharedSize(t.Context(), root, gpBackupName)
		assert.Error(t, err)
	})

	t.Run("one storage staying unavailable does not hold back the other", func(t *testing.T) {
		root := testtools.MakeDefaultInMemoryStorageFolder()
		putGpSentinel(t, root, gpBackupName, segBackups)
		for contentID, segBackup := range segBackups {
			putSegmentAOFilesMetadata(t, root, contentID, segBackup, 5)
		}
		// None of the segments wrote PAX metadata, as on a cluster without PAX relations.

		require.NoError(t, greenplum.UploadSharedSizes(t.Context(), root, gpBackupName))

		aoSize, err := greenplum.FetchAOSharedSize(t.Context(), root, gpBackupName)
		require.NoError(t, err)
		assert.Equal(t, int64(15), aoSize)

		_, err = greenplum.FetchPaxSharedSize(t.Context(), root, gpBackupName)
		assert.Error(t, err)
	})

	t.Run("reports the backup being gone instead of recording a zero", func(t *testing.T) {
		root := testtools.MakeDefaultInMemoryStorageFolder()

		assert.Error(t, greenplum.UploadSharedSizes(t.Context(), root, gpBackupName))
	})
}

// putSegmentFilesMetadata writes the two files metadata objects a segment backup-push leaves next
// to its backup, each reporting the volume it uploaded to its shared storage.
func putSegmentFilesMetadata(t *testing.T, root storage.Folder, contentID int, backupName string,
	aoSize, paxSize int64) {
	t.Helper()

	putSegmentAOFilesMetadata(t, root, contentID, backupName, aoSize)

	// The file list is there to keep the reader honest: it is what the size view must skip.
	folder := root.GetSubFolder(greenplum.FormatSegmentStoragePrefix(contentID))
	putDTO(t, folder, utility.BaseBackupPath+pax.GetFilesMetadataPath(backupName),
		pax.FilesMetadataDTO{
			Files:              pax.BackupFiles{"base/13/16385_pax/3": {StoragePath: "paxfiles/3", Kind: pax.FileKindData}},
			UploadedSharedSize: paxSize,
		})
}

func putSegmentAOFilesMetadata(t *testing.T, root storage.Folder, contentID int, backupName string, aoSize int64) {
	t.Helper()

	folder := root.GetSubFolder(greenplum.FormatSegmentStoragePrefix(contentID))
	putDTO(t, folder, utility.BaseBackupPath+ao.GetFilesMetadataPath(backupName),
		ao.FilesMetadataDTO{
			Files:              ao.BackupFiles{"1337.1": {StoragePath: "aosegments/1337.1", EOF: 4096}},
			UploadedSharedSize: aoSize,
		})
}

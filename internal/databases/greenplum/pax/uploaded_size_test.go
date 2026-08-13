package pax_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/crypto/openpgp"
	"github.com/wal-g/wal-g/internal/databases/greenplum/pax"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/testtools"
)

// storedSize is the total volume the uploader actually put into the shared PAX storage.
func storedSize(t *testing.T, folder storage.Folder) int64 {
	t.Helper()

	objects, _, err := folder.GetSubFolder(pax.StoragePath).ListFolder(t.Context())
	require.NoError(t, err)

	total := int64(0)
	for _, obj := range objects {
		total += obj.GetSize()
	}
	return total
}

// newCountingUploader mirrors newUploader, but hands back the folder as well so the counter can be
// compared against what really landed in storage.
func newCountingUploader(t *testing.T, baseFiles pax.BackupFiles) (*pax.StorageUploader, storage.Folder) {
	t.Helper()

	kvs := memory.NewKVS()
	folder := memory.NewFolder("in_memory/", kvs)
	crypter := openpgp.CrypterFromKeyPath(privateKeyFilePath, func() (string, bool) {
		return "", false
	})

	uploader := pax.NewStorageUploader(testtools.NewStoringMockUploader(kvs), baseFiles, crypter,
		&internal.RegularBundleFiles{}, deduplicationAgeLimit, newPaxFilesID)

	return uploader, folder
}

func paxTestFiles() map[string]testFile {
	return map[string]testFile{
		"base/13/16385_pax/3": {
			RelFileMetadata: pax.RelFileMetadata{RelNameMd5: "md5val", BlockID: 3, Kind: pax.FileKindData},
			FileKey:         pax.FileKey{SpcNode: 1009, DBNode: 13, RelFileNode: 16385, Filename: "3"},
		},
	}
}

func TestPaxUploadedSize(t *testing.T) {
	t.Run("counts what actually landed in storage", func(t *testing.T) {
		uploader, folder := newCountingUploader(t, pax.BackupFiles{})

		testDir, files := generateData(t, paxTestFiles())
		defer os.RemoveAll(testDir)

		for _, tf := range files {
			cfi := tf.ComposeFileInfo
			require.NoError(t, uploader.AddFile(t.Context(), &cfi, tf.RelFileMetadata, tf.FileKey))
		}

		uploaded, err := uploader.UploadedDataSize()
		require.NoError(t, err)
		assert.Positive(t, uploaded)
		// The files are encrypted on the way out, so this only holds if the counter sits after
		// encryption rather than measuring the files on disk.
		assert.Equal(t, storedSize(t, folder), uploaded)
	})

	t.Run("does not count deduplicated files", func(t *testing.T) {
		first, _ := newCountingUploader(t, pax.BackupFiles{})

		testDir, files := generateData(t, paxTestFiles())
		defer os.RemoveAll(testDir)

		for _, tf := range files {
			cfi := tf.ComposeFileInfo
			require.NoError(t, first.AddFile(t.Context(), &cfi, tf.RelFileMetadata, tf.FileKey))
		}
		firstMeta := first.GetFiles()
		firstUploaded, err := first.UploadedDataSize()
		require.NoError(t, err)
		require.Positive(t, firstUploaded)

		baseFiles := make(pax.BackupFiles)
		for name, desc := range firstMeta.Files {
			desc.InitialUploadTS = time.Now()
			baseFiles[name] = desc
		}

		second, secondFolder := newCountingUploader(t, baseFiles)
		for _, tf := range files {
			cfi := tf.ComposeFileInfo
			require.NoError(t, second.AddFile(t.Context(), &cfi, tf.RelFileMetadata, tf.FileKey))
		}

		secondMeta := second.GetFiles()
		for name, desc := range secondMeta.Files {
			require.True(t, desc.IsSkipped, "%s should have been deduplicated", name)
		}
		secondUploaded, err := second.UploadedDataSize()
		require.NoError(t, err)
		assert.Zero(t, secondUploaded)
		assert.Zero(t, storedSize(t, secondFolder))
	})
}

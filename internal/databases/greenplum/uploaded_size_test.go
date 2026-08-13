package greenplum_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/crypto/openpgp"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/internal/walparser"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/testtools"
)

// storedSize is the total volume the uploader actually put into the shared AO storage.
func storedSize(t *testing.T, folder storage.Folder) int64 {
	t.Helper()

	objects, _, err := folder.GetSubFolder(greenplum.AoStoragePath).ListFolder(t.Context())
	require.NoError(t, err)

	total := int64(0)
	for _, obj := range objects {
		total += obj.GetSize()
	}
	return total
}

// newCountingAoUploader mirrors the test helper in ao_storage_uploader_test.go, but hands back the
// folder as well so the counter can be compared against what really landed in storage.
func newCountingAoUploader(t *testing.T, baseFiles greenplum.BackupAOFiles) (*greenplum.AoStorageUploader, storage.Folder) {
	t.Helper()

	kvs := memory.NewKVS()
	folder := memory.NewFolder("in_memory/", kvs)
	crypter := openpgp.CrypterFromKeyPath(PrivateKeyFilePath, func() (string, bool) {
		return "", false
	})

	uploader := greenplum.NewAoStorageUploader(testtools.NewStoringMockUploader(kvs), baseFiles, crypter,
		&internal.RegularBundleFiles{}, true, deduplicationAgeLimit, NewAoSegFilesID)

	return uploader, folder
}

func aoTestFile(relNode walparser.Oid, blockNo uint32, modCount int64) TestFileInfo {
	return TestFileInfo{
		AoRelFileMetadata: greenplum.NewAoRelFileMetadata("md5summock", greenplum.AppendOptimized, 100, modCount),
		BlockLocation: walparser.BlockLocation{
			RelationFileNode: walparser.RelFileNode{SpcNode: 0, DBNode: 13, RelNode: relNode},
			BlockNo:          blockNo,
		},
	}
}

func TestAoUploadedSize(t *testing.T) {
	t.Run("counts what actually landed in storage", func(t *testing.T) {
		uploader, folder := newCountingAoUploader(t, greenplum.BackupAOFiles{})

		testFiles := map[string]TestFileInfo{
			"1337.1": aoTestFile(1337, 1, 3),
			"1337.2": aoTestFile(1337, 2, 4),
		}
		testDir, testFiles := generateData("uploaded_size_data", testFiles, t)
		defer os.RemoveAll(testDir)

		for _, testFile := range testFiles {
			cfi := testFile.ComposeFileInfo
			location := testFile.BlockLocation
			require.NoError(t, uploader.AddFile(t.Context(), &cfi, testFile.AoRelFileMetadata, &location))
		}

		uploaded, err := uploader.UploadedDataSize()
		require.NoError(t, err)
		assert.Positive(t, uploaded)
		// The files are encrypted on the way out, so this only holds if the counter sits after
		// encryption rather than measuring the files on disk.
		assert.Equal(t, storedSize(t, folder), uploaded)
	})

	t.Run("does not count deduplicated files", func(t *testing.T) {
		testFiles := map[string]TestFileInfo{"1337.1": aoTestFile(1337, 1, 3)}

		// First backup uploads the file and records what it cost.
		first, _ := newCountingAoUploader(t, greenplum.BackupAOFiles{})
		testDir, testFiles := generateData("uploaded_size_dedup_data", testFiles, t)
		defer os.RemoveAll(testDir)

		for _, testFile := range testFiles {
			cfi := testFile.ComposeFileInfo
			location := testFile.BlockLocation
			require.NoError(t, first.AddFile(t.Context(), &cfi, testFile.AoRelFileMetadata, &location))
		}
		firstMeta := first.GetFiles()
		firstUploaded, err := first.UploadedDataSize()
		require.NoError(t, err)
		require.Positive(t, firstUploaded)

		// The second backup finds the same file already in storage and skips it, so it added nothing.
		baseFiles := make(greenplum.BackupAOFiles)
		for name, desc := range firstMeta.Files {
			desc.InitialUploadTS = time.Now()
			baseFiles[name] = desc
		}

		second, secondFolder := newCountingAoUploader(t, baseFiles)
		for _, testFile := range testFiles {
			cfi := testFile.ComposeFileInfo
			location := testFile.BlockLocation
			require.NoError(t, second.AddFile(t.Context(), &cfi, testFile.AoRelFileMetadata, &location))
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

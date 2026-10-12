package ao_test

import (
	"archive/tar"
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/crypto/openpgp"
	"github.com/wal-g/wal-g/internal/databases/greenplum/ao"
	"github.com/wal-g/wal-g/internal/walparser"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

type TestFileInfo struct {
	internal.ComposeFileInfo
	ao.RelFileMetadata
	walparser.BlockLocation
}

type ExpectedResult struct {
	StoragePath   string
	IsSkipped     bool
	IsIncremented bool
	StorageType   ao.RelStorageType
	EOF           int64
	ModCount      int64
}

const deduplicationAgeLimit = 720 * time.Hour // 30 days
const NewAoSegFilesID = "test"
const PrivateKeyFilePath = "../../../../test/testdata/waleGpgKey"

func TestRegularAoUpload(t *testing.T) {
	baseFiles := make(ao.BackupFiles)
	bundleFiles := &internal.RegularBundleFiles{}
	testFiles := map[string]TestFileInfo{
		"1663.1": {
			RelFileMetadata: ao.NewRelFileMetadata("md5summock", ao.ColumnOriented, 100, 3),
			BlockLocation: walparser.BlockLocation{
				RelationFileNode: walparser.RelFileNode{
					SpcNode: 1009,
					DBNode:  13,
					RelNode: 1663,
				},
				BlockNo: 1,
			},
		},
		"1337.120": {
			RelFileMetadata: ao.NewRelFileMetadata("md5summock", ao.AppendOptimized, 60, 4),
			BlockLocation: walparser.BlockLocation{
				RelationFileNode: walparser.RelFileNode{
					SpcNode: 0,
					DBNode:  13,
					RelNode: 1337,
				},
				BlockNo: 120,
			},
		},
		"1337.60": {
			RelFileMetadata: ao.NewRelFileMetadata("md5summock", ao.AppendOptimized, 77, 5),
			BlockLocation: walparser.BlockLocation{
				RelationFileNode: walparser.RelFileNode{
					SpcNode: 0,
					DBNode:  13,
					RelNode: 1337,
				},
				BlockNo: 60,
			},
		},
	}
	expectedResults := map[string]ExpectedResult{
		"1337.60": {
			StoragePath:   "0_13_md5summock_1337_60_5_test_aoseg",
			IsSkipped:     false,
			IsIncremented: false,
			StorageType:   ao.AppendOptimized,
			EOF:           77,
			ModCount:      5,
		},
		"1663.1": {
			StoragePath:   "1009_13_md5summock_1663_1_3_test_aoseg",
			IsSkipped:     false,
			IsIncremented: false,
			StorageType:   ao.ColumnOriented,
			EOF:           100,
			ModCount:      3,
		},
		"1337.120": {
			StoragePath:   "0_13_md5summock_1337_120_4_test_aoseg",
			IsSkipped:     false,
			IsIncremented: false,
			StorageType:   ao.AppendOptimized,
			EOF:           60,
			ModCount:      4,
		},
	}
	runSingleTest(t, baseFiles, bundleFiles, testFiles, expectedResults, deduplicationAgeLimit, true)
}

func TestAoUpload_MaxAge(t *testing.T) {
	initialUploadTS := time.Now().Add(-(deduplicationAgeLimit + 1*time.Minute)) // file should be reuploaded
	baseFiles := ao.BackupFiles{
		"1663.1": {
			StoragePath:     "1009_13_md5summock_1663_1_4_test_aoseg",
			IsSkipped:       false,
			IsIncremented:   false,
			MTime:           initialUploadTS,
			StorageType:     ao.ColumnOriented,
			EOF:             70,
			ModCount:        4,
			Compressor:      "",
			FileMode:        420,
			InitialUploadTS: initialUploadTS,
		},
		"1337.120": {
			StoragePath:     "0_13_md5summock_1337_120_4_test_D_5_aoseg",
			IsSkipped:       false,
			IsIncremented:   true,
			MTime:           initialUploadTS,
			StorageType:     ao.AppendOptimized,
			EOF:             60,
			ModCount:        4,
			Compressor:      "",
			FileMode:        420,
			InitialUploadTS: initialUploadTS,
		},
	}
	bundleFiles := &internal.RegularBundleFiles{}
	testFiles := map[string]TestFileInfo{
		"1663.1": {
			RelFileMetadata: ao.NewRelFileMetadata("md5summock", ao.ColumnOriented, 70, 4),
			BlockLocation: walparser.BlockLocation{
				RelationFileNode: walparser.RelFileNode{
					SpcNode: 1009,
					DBNode:  13,
					RelNode: 1663,
				},
				BlockNo: 1,
			},
		},
		"1337.120": {
			RelFileMetadata: ao.NewRelFileMetadata("md5summock", ao.AppendOptimized, 70, 5),
			BlockLocation: walparser.BlockLocation{
				RelationFileNode: walparser.RelFileNode{
					SpcNode: 0,
					DBNode:  13,
					RelNode: 1337,
				},
				BlockNo: 120,
			},
		},
	}
	expectedResults := map[string]ExpectedResult{
		"1663.1": {
			StoragePath:   "1009_13_md5summock_1663_1_4_test_aoseg",
			IsSkipped:     false,
			IsIncremented: false,
			StorageType:   ao.ColumnOriented,
			EOF:           70,
			ModCount:      4,
		},
		"1337.120": {
			StoragePath:   "0_13_md5summock_1337_120_5_test_aoseg",
			IsSkipped:     false,
			IsIncremented: false,
			StorageType:   ao.AppendOptimized,
			EOF:           70,
			ModCount:      5,
		},
	}
	runSingleTest(t, baseFiles, bundleFiles, testFiles, expectedResults, deduplicationAgeLimit, true)
}

func TestIncrementalAoUpload(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "1337.120")
	require.NoError(t, os.WriteFile(path, []byte{1, 2, 3, 4, 5, 6, 7}, 0o600))
	baseFiles := ao.BackupFiles{
		"1337.120": {
			StoragePath:     "0_13_md5summock_1337_120_4_test_aoseg",
			IsSkipped:       false,
			IsIncremented:   false,
			MTime:           time.Now(),
			StorageType:     ao.AppendOptimized,
			EOF:             60,
			ModCount:        4,
			Compressor:      "",
			FileMode:        420,
			InitialUploadTS: time.Now(),
			Checksum:        "cdeda69d6c5295db2e37947983fcbc0f",
		},
	}
	bundleFiles := &internal.RegularBundleFiles{}
	testFiles := map[string]TestFileInfo{
		"1663.1": {
			RelFileMetadata: ao.NewRelFileMetadata("md5summock", ao.ColumnOriented, 100, 3),
			BlockLocation: walparser.BlockLocation{
				RelationFileNode: walparser.RelFileNode{
					SpcNode: 0,
					DBNode:  13,
					RelNode: 1663,
				},
				BlockNo: 1,
			},
		},
		"1337.120": {
			RelFileMetadata: ao.NewRelFileMetadata("md5summock", ao.AppendOptimized, 70, 5),
			BlockLocation: walparser.BlockLocation{
				RelationFileNode: walparser.RelFileNode{
					SpcNode: 0,
					DBNode:  13,
					RelNode: 1337,
				},
				BlockNo: 120,
			},
		},
		"1337.60": {
			RelFileMetadata: ao.NewRelFileMetadata("md5summock", ao.AppendOptimized, 77, 5),
			BlockLocation: walparser.BlockLocation{
				RelationFileNode: walparser.RelFileNode{
					SpcNode: 0,
					DBNode:  13,
					RelNode: 1337,
				},
				BlockNo: 60,
			},
		},
	}
	expectedResults := map[string]ExpectedResult{
		"1337.60": {
			StoragePath:   "0_13_md5summock_1337_60_5_test_aoseg",
			IsSkipped:     false,
			IsIncremented: false,
			StorageType:   ao.AppendOptimized,
			EOF:           77,
			ModCount:      5,
		},
		"1663.1": {
			StoragePath:   "0_13_md5summock_1663_1_3_test_aoseg",
			IsSkipped:     false,
			IsIncremented: false,
			StorageType:   ao.ColumnOriented,
			EOF:           100,
			ModCount:      3,
		},
		"1337.120": {
			StoragePath:   "0_13_md5summock_1337_120_4_test_D_5_aoseg",
			IsSkipped:     false,
			IsIncremented: true,
			StorageType:   ao.AppendOptimized,
			EOF:           70,
			ModCount:      5,
		},
	}
	runSingleTest(t, baseFiles, bundleFiles, testFiles, expectedResults, deduplicationAgeLimit, true)
}

func TestIncrementalAoUpload_EqualEOF_DifferentModCount(t *testing.T) {
	baseFiles := ao.BackupFiles{
		"1663.1": {
			StoragePath:     "1009_13_md5summock_1663_1_4_test_aoseg",
			IsSkipped:       false,
			IsIncremented:   false,
			MTime:           time.Now(),
			StorageType:     ao.ColumnOriented,
			EOF:             100,
			ModCount:        4,
			Compressor:      "",
			FileMode:        420,
			InitialUploadTS: time.Now(),
		},
	}
	bundleFiles := &internal.RegularBundleFiles{}
	testFiles := map[string]TestFileInfo{
		"1663.1": {
			RelFileMetadata: ao.NewRelFileMetadata("md5summock", ao.ColumnOriented, 100, 5),
			BlockLocation: walparser.BlockLocation{
				RelationFileNode: walparser.RelFileNode{
					SpcNode: 1009,
					DBNode:  13,
					RelNode: 1663,
				},
				BlockNo: 1,
			},
		},
	}
	expectedResults := map[string]ExpectedResult{
		"1663.1": {
			StoragePath:   "1009_13_md5summock_1663_1_5_test_aoseg",
			IsSkipped:     false,
			IsIncremented: false,
			StorageType:   ao.ColumnOriented,
			EOF:           100,
			ModCount:      5,
		},
	}
	runSingleTest(t, baseFiles, bundleFiles, testFiles, expectedResults, deduplicationAgeLimit, true)
}

func TestIncrementalAoUpload_DifferentEOF_EqualModCount(t *testing.T) {
	baseFiles := ao.BackupFiles{
		"1663.1": {
			StoragePath:     "1009_13_md5summock_1663_1_4_test_aoseg",
			IsSkipped:       false,
			IsIncremented:   false,
			MTime:           time.Now(),
			StorageType:     ao.ColumnOriented,
			EOF:             70,
			ModCount:        4,
			Compressor:      "",
			FileMode:        420,
			InitialUploadTS: time.Now(),
		},
	}
	bundleFiles := &internal.RegularBundleFiles{}
	testFiles := map[string]TestFileInfo{
		"1663.1": {
			RelFileMetadata: ao.NewRelFileMetadata("md5summock", ao.ColumnOriented, 100, 4),
			BlockLocation: walparser.BlockLocation{
				RelationFileNode: walparser.RelFileNode{
					SpcNode: 1009,
					DBNode:  13,
					RelNode: 1663,
				},
				BlockNo: 1,
			},
		},
	}
	expectedResults := map[string]ExpectedResult{
		"1663.1": {
			StoragePath:   "1009_13_md5summock_1663_1_4_test_aoseg",
			IsSkipped:     false,
			IsIncremented: false,
			StorageType:   ao.ColumnOriented,
			EOF:           100,
			ModCount:      4,
		},
	}
	runSingleTest(t, baseFiles, bundleFiles, testFiles, expectedResults, deduplicationAgeLimit, true)
}

func TestIncrementalAoUpload_FullAfterDelta(t *testing.T) {
	baseFiles := ao.BackupFiles{
		"1663.1": {
			StoragePath:     "1009_13_md5summock_1663_1_4_test_D_aoseg",
			IsSkipped:       false,
			IsIncremented:   true,
			MTime:           time.Now(),
			StorageType:     ao.ColumnOriented,
			EOF:             70,
			ModCount:        4,
			Compressor:      "",
			FileMode:        420,
			InitialUploadTS: time.Now(),
		},
	}
	bundleFiles := &internal.RegularBundleFiles{}
	testFiles := map[string]TestFileInfo{
		"1663.1": {
			RelFileMetadata: ao.NewRelFileMetadata("md5summock", ao.ColumnOriented, 70, 4),
			BlockLocation: walparser.BlockLocation{
				RelationFileNode: walparser.RelFileNode{
					SpcNode: 1009,
					DBNode:  13,
					RelNode: 1663,
				},
				BlockNo: 1,
			},
		},
	}
	expectedResults := map[string]ExpectedResult{
		"1663.1": {
			StoragePath:   "1009_13_md5summock_1663_1_4_test_aoseg",
			IsSkipped:     false,
			IsIncremented: false,
			StorageType:   ao.ColumnOriented,
			EOF:           70,
			ModCount:      4,
		},
	}
	runSingleTest(t, baseFiles, bundleFiles, testFiles, expectedResults, deduplicationAgeLimit, false)
}

func TestAoUpload_MTime(t *testing.T) {
	for _, isIncremental := range []bool{false, true} {
		for _, storageType := range []ao.RelStorageType{ao.AppendOptimized, ao.ColumnOriented} {
			for _, tc := range []struct {
				name      string
				baseMTime func(time.Time) time.Time
				wantSkip  bool
			}{
				{"unchanged", func(t time.Time) time.Time { return t }, true},
				{"same_instant_different_timezone", func(t time.Time) time.Time {
					return t.In(time.FixedZone("test", 3600))
				}, true},
				{"newer", func(t time.Time) time.Time { return t.Add(-time.Second) }, false},
				{"older", func(t time.Time) time.Time { return t.Add(time.Second) }, false},
				{"subsecond_change", func(t time.Time) time.Time { return t.Add(-time.Nanosecond) }, false},
				{"missing", func(time.Time) time.Time { return time.Time{} }, false},
			} {
				t.Run(fmt.Sprintf("incremental=%t/storage=%c/%s", isIncremental, storageType, tc.name), func(t *testing.T) {
					const name = "1663.1"
					const baseStoragePath = "previous_aoseg"
					oldData := []byte("old contents")
					data := oldData
					if !tc.wantSkip {
						// Simulate compaction reusing a segment without changing EOF or modcount.
						data = []byte("new contents")
					}
					filePath := filepath.Join(t.TempDir(), name)
					require.NoError(t, os.WriteFile(filePath, data, 0o600))
					info, err := os.Stat(filePath)
					require.NoError(t, err)
					header, err := tar.FileInfoHeader(info, "")
					require.NoError(t, err)
					header.Name = name
					cfi := internal.NewComposeFileInfo(filePath, info, true, false, header)

					hasher := xxh3.New128()
					_, err = hasher.Write(oldData)
					require.NoError(t, err)
					oldChecksum := hex.EncodeToString(hasher.Sum(nil))
					baseFiles := ao.BackupFiles{
						name: {
							StoragePath: baseStoragePath, MTime: tc.baseMTime(info.ModTime()),
							StorageType: storageType, EOF: int64(len(oldData)), ModCount: 4,
							InitialUploadTS: time.Now(), Checksum: oldChecksum,
						},
					}
					folder := testtools.MakeDefaultInMemoryStorageFolder()
					require.NoError(t, folder.PutObject(t.Context(), ao.StoragePath+"/"+baseStoragePath, bytes.NewReader(oldData)))
					bundleFiles := &internal.RegularBundleFiles{}
					uploader := ao.NewStorageUploader(internal.NewRegularUploader(nil, folder), baseFiles,
						nil, bundleFiles, isIncremental, deduplicationAgeLimit, NewAoSegFilesID)
					meta := ao.NewRelFileMetadata("md5summock", storageType, int64(len(data)), 4)
					location := walparser.NewBlockLocation(1009, 13, 1663, 1)
					require.NoError(t, uploader.AddFile(t.Context(), cfi, meta, location))

					got := uploader.GetFiles().Files[name]
					require.NotNil(t, got)
					assert.Equal(t, tc.wantSkip, got.IsSkipped)
					assert.False(t, got.IsIncremented)
					assert.True(t, got.MTime.Equal(info.ModTime()))
					assert.Equal(t, int64(len(data)), got.EOF)
					assert.Equal(t, int64(4), got.ModCount)
					if tc.wantSkip {
						assert.Equal(t, baseStoragePath, got.StoragePath)
						assert.Equal(t, oldChecksum, got.Checksum)
					} else {
						assert.NotEqual(t, baseStoragePath, got.StoragePath)
						hasher.Reset()
						_, err = hasher.Write(data)
						require.NoError(t, err)
						assert.Equal(t, hex.EncodeToString(hasher.Sum(nil)), got.Checksum)
					}
					reader, err := folder.ReadObject(t.Context(), ao.StoragePath+"/"+got.StoragePath)
					require.NoError(t, err)
					defer reader.Close()
					uploaded, err := io.ReadAll(reader)
					require.NoError(t, err)
					assert.Equal(t, data, uploaded)
				})
			}
		}
	}
}

func TestAoUpload_NotExistFile(t *testing.T) {
	name := "1663.1"
	baseFiles := ao.BackupFiles{}
	bundleFiles := &internal.RegularBundleFiles{}
	deduplicationAgeLimit := 720 * time.Hour
	uploader := newStorageUploader(baseFiles, bundleFiles, true, deduplicationAgeLimit)
	meta := ao.NewRelFileMetadata("md5summock", ao.ColumnOriented, 70, 4)
	location := walparser.BlockLocation{
		RelationFileNode: walparser.RelFileNode{
			SpcNode: 0,
			DBNode:  13,
			RelNode: 1337,
		},
		BlockNo: 60,
	}
	f, err := os.Create(name)
	if err != nil {
		t.Log(err)
	}

	fInfo, err := f.Stat()
	if err != nil {
		t.Log(err)
	}

	header, err := tar.FileInfoHeader(fInfo, f.Name())
	if err != nil {
		t.Log(err)
	}
	header.Name = name

	cfi := internal.NewComposeFileInfo(f.Name(), fInfo, false, false, header)

	assert.NoError(t, f.Close())
	assert.NoError(t, os.Remove(f.Name()))

	err = uploader.AddFile(t.Context(), cfi, meta, &location)
	assert.NoError(t, err)
	assert.Empty(t, uploader.GetFiles().Files)
}

func runSingleTest(t *testing.T, baseFiles ao.BackupFiles,
	bundleFiles *internal.RegularBundleFiles, testFiles map[string]TestFileInfo, expectedResults map[string]ExpectedResult,
	deduplicationAgeLimit time.Duration, isAploaderIncremental bool) {
	uploader := newStorageUploader(baseFiles, bundleFiles, isAploaderIncremental, deduplicationAgeLimit)
	testDir, testFiles := generateData("data", testFiles, t)
	defer os.RemoveAll(testDir)

	for _, testFile := range testFiles {
		cfi := testFile.ComposeFileInfo
		aoMeta := testFile.RelFileMetadata
		location := testFile.BlockLocation
		err := uploader.AddFile(t.Context(), &cfi, aoMeta, &location)
		assert.NoError(t, err)
	}

	filesMetaDto := uploader.GetFiles()
	assert.Equal(t, len(expectedResults), len(filesMetaDto.Files))

	bundleFilesMap := bundleFiles.GetUnderlyingMap()

	for name, resFile := range filesMetaDto.Files {
		assert.Contains(t, expectedResults, name)
		expFile := expectedResults[name]
		assert.Equal(t, expFile.StoragePath, resFile.StoragePath)
		assert.Equal(t, expFile.IsSkipped, resFile.IsSkipped)
		assert.Equal(t, expFile.IsIncremented, resFile.IsIncremented)
		assert.Equal(t, expFile.StorageType, resFile.StorageType)
		assert.Equal(t, expFile.EOF, resFile.EOF)
		assert.Equal(t, expFile.ModCount, resFile.ModCount)

		fileDescRaw, ok := bundleFilesMap.Load(name)
		assert.True(t, ok)
		fileDesc := fileDescRaw.(internal.BackupFileDescription)
		assert.Equal(t, expFile.IsSkipped, fileDesc.IsSkipped)
		assert.Equal(t, expFile.IsIncremented, fileDesc.IsIncremented)
	}
}

func newStorageUploader(
	baseFiles ao.BackupFiles, bundleFiles internal.BundleFiles, isIncremental bool,
	deduplicationAgeLimit time.Duration,
) *ao.StorageUploader {
	storage := memory.NewKVS()
	mockUploader := testtools.NewStoringMockUploader(storage)
	crypter := openpgp.CrypterFromKeyPath(PrivateKeyFilePath, func() (string, bool) {
		return "", false
	})
	return ao.NewStorageUploader(mockUploader, baseFiles, crypter, bundleFiles, isIncremental, deduplicationAgeLimit, NewAoSegFilesID)
}

func generateData(dirName string, testFiles map[string]TestFileInfo, t *testing.T) (string, map[string]TestFileInfo) {
	cwd, err := filepath.Abs("./")
	if err != nil {
		t.Log(err)
	}

	// Create temp directory.
	dir, err := os.MkdirTemp(cwd, dirName)
	if err != nil {
		t.Log(err)
	}
	fmt.Println(dir)

	sb := testtools.NewStrideByteReader(10)

	// Generates 100 byte files
	for name, tfi := range testFiles {
		lr := &io.LimitedReader{
			R: sb,
			N: int64(100),
		}
		f, err := os.Create(filepath.Join(dir, name))
		if err != nil {
			t.Log(err)
		}
		io.Copy(f, lr)

		fInfo, err := f.Stat()
		if err != nil {
			t.Log(err)
		}

		header, err := tar.FileInfoHeader(fInfo, f.Name())
		if err != nil {
			t.Log(err)
		}

		header.Name = name

		cfi := internal.NewComposeFileInfo(f.Name(), fInfo, false, false, header)
		tfi.ComposeFileInfo = *cfi
		testFiles[name] = tfi

		defer utility.LoggedClose(f, "")
	}

	return dir, testFiles
}

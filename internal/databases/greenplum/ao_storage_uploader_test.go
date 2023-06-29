package greenplum_test

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/crypto/openpgp"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/internal/walparser"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

type TestFileInfo struct {
	internal.ComposeFileInfo
	greenplum.AoRelFileMetadata
	walparser.BlockLocation
}

type ExpectedResult struct {
	StoragePath   string
	IsSkipped     bool
	IsIncremented bool
	StorageType   greenplum.RelStorageType
	EOF           int64
	ModCount      int64
}

const deduplicationAgeLimit = 720 * time.Hour // 30 days
const NewAoSegFilesID = "test"
const PrivateKeyFilePath = "../../../test/testdata/waleGpgKey"

func TestRegularAoUpload(t *testing.T) {
	baseFiles := make(greenplum.BackupAOFiles)
	bundleFiles := &internal.RegularBundleFiles{}
	testFiles := map[string]TestFileInfo{
		"1663.1": {
			AoRelFileMetadata: greenplum.NewAoRelFileMetadata("md5summock", greenplum.ColumnOriented, 100, 3),
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
			AoRelFileMetadata: greenplum.NewAoRelFileMetadata("md5summock", greenplum.AppendOptimized, 60, 4),
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
			AoRelFileMetadata: greenplum.NewAoRelFileMetadata("md5summock", greenplum.AppendOptimized, 77, 5),
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
			StorageType:   greenplum.AppendOptimized,
			EOF:           77,
			ModCount:      5,
		},
		"1663.1": {
			StoragePath:   "1009_13_md5summock_1663_1_3_test_aoseg",
			IsSkipped:     false,
			IsIncremented: false,
			StorageType:   greenplum.ColumnOriented,
			EOF:           100,
			ModCount:      3,
		},
		"1337.120": {
			StoragePath:   "0_13_md5summock_1337_120_4_test_aoseg",
			IsSkipped:     false,
			IsIncremented: false,
			StorageType:   greenplum.AppendOptimized,
			EOF:           60,
			ModCount:      4,
		},
	}
	runSingleTest(t, baseFiles, bundleFiles, testFiles, expectedResults, deduplicationAgeLimit)
}

func TestAoUpload_MaxAge(t *testing.T) {
	initialUploadTS := time.Now().Add(-(deduplicationAgeLimit + 1*time.Minute)) // file should be reuploaded
	baseFiles := greenplum.BackupAOFiles{
		"1663.1": {
			StoragePath:     "1009_13_md5summock_1663_1_4_test_aoseg",
			IsSkipped:       false,
			IsIncremented:   false,
			MTime:           initialUploadTS,
			StorageType:     greenplum.ColumnOriented,
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
			StorageType:     greenplum.AppendOptimized,
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
			AoRelFileMetadata: greenplum.NewAoRelFileMetadata("md5summock", greenplum.ColumnOriented, 70, 4),
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
			AoRelFileMetadata: greenplum.NewAoRelFileMetadata("md5summock", greenplum.AppendOptimized, 70, 5),
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
			StorageType:   greenplum.ColumnOriented,
			EOF:           70,
			ModCount:      4,
		},
		"1337.120": {
			StoragePath:   "0_13_md5summock_1337_120_5_test_aoseg",
			IsSkipped:     false,
			IsIncremented: false,
			StorageType:   greenplum.AppendOptimized,
			EOF:           70,
			ModCount:      5,
		},
	}
	runSingleTest(t, baseFiles, bundleFiles, testFiles, expectedResults, deduplicationAgeLimit)
}

func TestIncrementalAoUpload(t *testing.T) {
	baseFiles := greenplum.BackupAOFiles{
		"1337.120": {
			StoragePath:     "0_13_md5summock_1337_120_4_test_aoseg",
			IsSkipped:       false,
			IsIncremented:   false,
			MTime:           time.Now(),
			StorageType:     greenplum.AppendOptimized,
			EOF:             60,
			ModCount:        4,
			Compressor:      "",
			FileMode:        420,
			InitialUploadTS: time.Now(),
		},
	}
	bundleFiles := &internal.RegularBundleFiles{}
	testFiles := map[string]TestFileInfo{
		"1663.1": {
			AoRelFileMetadata: greenplum.NewAoRelFileMetadata("md5summock", greenplum.ColumnOriented, 100, 3),
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
			AoRelFileMetadata: greenplum.NewAoRelFileMetadata("md5summock", greenplum.AppendOptimized, 70, 5),
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
			AoRelFileMetadata: greenplum.NewAoRelFileMetadata("md5summock", greenplum.AppendOptimized, 77, 5),
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
			StorageType:   greenplum.AppendOptimized,
			EOF:           77,
			ModCount:      5,
		},
		"1663.1": {
			StoragePath:   "0_13_md5summock_1663_1_3_test_aoseg",
			IsSkipped:     false,
			IsIncremented: false,
			StorageType:   greenplum.ColumnOriented,
			EOF:           100,
			ModCount:      3,
		},
		"1337.120": {
			StoragePath:   "0_13_md5summock_1337_120_4_test_D_5_aoseg",
			IsSkipped:     false,
			IsIncremented: true,
			StorageType:   greenplum.AppendOptimized,
			EOF:           70,
			ModCount:      5,
		},
	}
	runSingleTest(t, baseFiles, bundleFiles, testFiles, expectedResults, deduplicationAgeLimit)
}

func TestIncrementalAoUpload_EqualEof_DifferentModCount(t *testing.T) {
	baseFiles := greenplum.BackupAOFiles{
		"1663.1": {
			StoragePath:     "1009_13_md5summock_1663_1_4_test_aoseg",
			IsSkipped:       false,
			IsIncremented:   false,
			MTime:           time.Now(),
			StorageType:     greenplum.ColumnOriented,
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
			AoRelFileMetadata: greenplum.NewAoRelFileMetadata("md5summock", greenplum.ColumnOriented, 100, 5),
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
			StorageType:   greenplum.ColumnOriented,
			EOF:           100,
			ModCount:      5,
		},
	}
	runSingleTest(t, baseFiles, bundleFiles, testFiles, expectedResults, deduplicationAgeLimit)
}

func TestIncrementalAoUpload_DifferentEof_EqualModCount(t *testing.T) {
	baseFiles := greenplum.BackupAOFiles{
		"1663.1": {
			StoragePath:     "1009_13_md5summock_1663_1_4_test_aoseg",
			IsSkipped:       false,
			IsIncremented:   false,
			MTime:           time.Now(),
			StorageType:     greenplum.ColumnOriented,
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
			AoRelFileMetadata: greenplum.NewAoRelFileMetadata("md5summock", greenplum.ColumnOriented, 100, 4),
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
			StorageType:   greenplum.ColumnOriented,
			EOF:           100,
			ModCount:      4,
		},
	}
	runSingleTest(t, baseFiles, bundleFiles, testFiles, expectedResults, deduplicationAgeLimit)
}

func TestAoUpload_SkippedFile(t *testing.T) {
	baseFiles := greenplum.BackupAOFiles{
		"1663.1": {
			StoragePath:     "1009_13_md5summock_1663_1_4_test_aoseg",
			IsSkipped:       false,
			IsIncremented:   false,
			MTime:           time.Now(),
			StorageType:     greenplum.ColumnOriented,
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
			AoRelFileMetadata: greenplum.NewAoRelFileMetadata("md5summock", greenplum.ColumnOriented, 70, 4),
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
			IsSkipped:     true,
			IsIncremented: false,
			StorageType:   greenplum.ColumnOriented,
			EOF:           70,
			ModCount:      4,
		},
	}
	runSingleTest(t, baseFiles, bundleFiles, testFiles, expectedResults, deduplicationAgeLimit)
}

func TestAoUpload_NotExistFile(t *testing.T) {
	name := "1663.1"
	baseFiles := greenplum.BackupAOFiles{}
	bundleFiles := &internal.RegularBundleFiles{}
	deduplicationAgeLimit := 720 * time.Hour
	uploader := newAoStorageUploader(baseFiles, bundleFiles, true, deduplicationAgeLimit)
	meta := greenplum.NewAoRelFileMetadata("md5summock", greenplum.ColumnOriented, 70, 4)
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

	err = os.Remove(f.Name())
	if err != nil {
		t.Log(err)
	}

	err = uploader.AddFile(cfi, meta, &location)
	assert.NoError(t, err)
}

func runSingleTest(t *testing.T, baseFiles greenplum.BackupAOFiles,
	bundleFiles *internal.RegularBundleFiles, testFiles map[string]TestFileInfo, expectedResults map[string]ExpectedResult,
	deduplicationAgeLimit time.Duration) {
	uploader := newAoStorageUploader(baseFiles, bundleFiles, true, deduplicationAgeLimit)
	testDir, testFiles := generateData("data", testFiles, t)
	defer os.RemoveAll(testDir)

	for _, testFile := range testFiles {
		cfi := testFile.ComposeFileInfo
		aoMeta := testFile.AoRelFileMetadata
		location := testFile.BlockLocation
		err := uploader.AddFile(&cfi, aoMeta, &location)
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

func newAoStorageUploader(
	baseFiles greenplum.BackupAOFiles, bundleFiles internal.BundleFiles, isIncremental bool,
	deduplicationAgeLimit time.Duration,
) *greenplum.AoStorageUploader {
	storage := memory.NewStorage()
	mockUploader := testtools.NewStoringMockUploader(storage)
	crypter := openpgp.CrypterFromKeyPath(PrivateKeyFilePath, func() (string, bool) {
		return "", false
	})
	return greenplum.NewAoStorageUploader(mockUploader, baseFiles, crypter, bundleFiles, isIncremental, deduplicationAgeLimit, NewAoSegFilesID)
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

package internal_test

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/testtools"
)

// This test has known race condition. We expect that background worker will
// upload 32 files. But we have no guarantees for this.
func TestBackgroundWALUpload(t *testing.T) {
	tests := []struct {
		name                 string
		numFilesOnDisk       int
		maxNumFilesUploaded  int
		maxParallelism       int
		wantNumFilesUploaded int
	}{
		{
			name:                 "parallelism=1",
			numFilesOnDisk:       100,
			maxNumFilesUploaded:  32,
			maxParallelism:       1,
			wantNumFilesUploaded: 32,
		},
		{
			name:                 "parallelism=10",
			numFilesOnDisk:       100,
			maxNumFilesUploaded:  32,
			maxParallelism:       10,
			wantNumFilesUploaded: 32,
		},
		{
			name:                 "parallelism=100 and fewer files on disk",
			numFilesOnDisk:       16,
			maxNumFilesUploaded:  32,
			maxParallelism:       100,
			wantNumFilesUploaded: 16,
		},
		{
			name:                 "parallelism=100 and extra files on disk",
			numFilesOnDisk:       100,
			maxNumFilesUploaded:  32,
			maxParallelism:       100,
			wantNumFilesUploaded: 32,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			internal.InitConfig()
			totalBgUploadedLimitStr, ok := internal.GetSetting(internal.TotalBgUploadedLimit)
			if !ok {
				t.Fatalf("failed to get setting for %s", internal.TotalBgUploadedLimit)
			}
			numBgUploadFiles, err := strconv.Atoi(totalBgUploadedLimitStr)
			if err != nil {
				t.Fatalf("failed to parse setting for %s: %v", internal.TotalBgUploadedLimit, err)
			}

			dir, a := setupArchiveStatus(t, "")
			// create more files than the TotalBgUploadedLimit
			for i := 0; i < tt.numFilesOnDisk; i++ {
				addTestDataFile(t, dir, i)
			}

			// Re-use generated data to test uploading WAL.
			tu := testtools.NewMockWalUploader(false, false)
			fakeASM := internal.NewFakeASM()
			tu.ArchiveStatusManager = fakeASM
			bu := internal.NewBgUploader(a, int32(tt.maxParallelism), int32(numBgUploadFiles), tu, false)

			// Look for new WALs while doing main upload
			bu.Start()
			time.Sleep(time.Second) // time to spin up new uploaders
			bu.Stop()

			walgDataDir := internal.GetDataFolderPath()

			for i := 0; i < tt.wantNumFilesUploaded; i++ {
				bname := testFilename(i)
				wasUploaded := fakeASM.IsWalAlreadyUploaded(bname)
				assert.True(t, wasUploaded, bname+" was not marked as uploaded")
			}

			cleanup(t, dir)
			cleanup(t, walgDataDir)
		})
	}
}

func setupArchiveStatus(t *testing.T, dir string) (string, string) {
	cwd, err := filepath.Abs("./")
	if err != nil {
		t.Log(err)
	}

	var testDir = dir
	if dir != "" {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			err := os.Mkdir(dir, 0700)
			if err != nil {
				t.Log(err)
			}
		}
	} else {
		// Create temp directory.
		tmpDir, err := ioutil.TempDir(cwd, "data")
		testDir = tmpDir
		if err != nil {
			t.Log(err)
		}
	}

	err = os.MkdirAll(filepath.Join(testDir, "archive_status"), 0700)
	if err != nil {
		t.Log(err)
	}

	a := filepath.Join(testDir, "A")
	file, err := os.Create(a)
	if err != nil {
		t.Log(err)
	}
	file.WriteString(strconv.Itoa(rand.Int()))
	file.WriteString(strconv.Itoa(rand.Int()))
	file.Close()

	return testDir, a
}

func addTestDataFile(t *testing.T, dir string, i int) {
	bname := testFilename(i)
	b := filepath.Join(dir, bname)

	if _, err := os.Stat(b); os.IsNotExist(err) {
		file, err := os.Create(b)
		if err != nil {
			t.Log(err)
		}
		file.Close()
	}

	br := filepath.Join(dir, "archive_status", bname+".ready")
	if _, err := os.Stat(br); os.IsNotExist(err) {
		file, err := os.Create(br)
		if err != nil {
			t.Log(err)
		}
		file.Close()
	}
}

func testFilename(i int) string {
	return fmt.Sprintf("B%010d", i)
}

func cleanup(t *testing.T, dir string) {
	err := os.RemoveAll(dir)
	if err != nil {
		t.Log("temporary data directory was not deleted ", err)
	}
}

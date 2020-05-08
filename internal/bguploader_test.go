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

// This test has known race condition. Since we cannot directly control
// BgUploader's internals, the test will wait for 1 second to allow BgUploader
// to run.
func TestBackgroundWALUpload(t *testing.T) {
	tests := []struct {
		name                 string
		maxParallelism       int
		numFilesOnDisk       int
		maxNumFilesUploaded  int
		wantNumFilesUploaded int // This is more of a minimum. BgUploader may actually upload more files.
	}{
		{
			name:                 "parallelism=0 no-op",
			maxParallelism:       0,
			numFilesOnDisk:       10,
			maxNumFilesUploaded:  32,
			wantNumFilesUploaded: 0,
		},
		{
			name:                 "parallelism=1 with a few files",
			maxParallelism:       1,
			numFilesOnDisk:       3,
			maxNumFilesUploaded:  32,
			wantNumFilesUploaded: 3,
		},
		{
			name:                 "parallelism=1 with lots of files",
			maxParallelism:       1,
			numFilesOnDisk:       100,
			maxNumFilesUploaded:  32,
			wantNumFilesUploaded: 32,
		},
		{
			name:                 "parallelism=10",
			maxParallelism:       10,
			numFilesOnDisk:       100,
			maxNumFilesUploaded:  32,
			wantNumFilesUploaded: 32,
		},
		{
			name:                 "parallelism=100 and fewer files on disk",
			maxParallelism:       100,
			numFilesOnDisk:       16,
			maxNumFilesUploaded:  32,
			wantNumFilesUploaded: 16,
		},
		{
			name:                 "parallelism=100 and extra files on disk",
			maxParallelism:       100,
			numFilesOnDisk:       100,
			maxNumFilesUploaded:  32,
			wantNumFilesUploaded: 32,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer cleanup(t, internal.GetDataFolderPath())

			// Use generated data to test uploading WAL.
			dir, a := setupArchiveStatus(t, "")
			for i := 0; i < tt.numFilesOnDisk; i++ {
				addTestDataFile(t, dir, i)
			}
			defer cleanup(t, dir)

			// Setup BgUploader
			tu := testtools.NewMockWalUploader(false, false)
			fakeASM := internal.NewFakeASM()
			tu.ArchiveStatusManager = fakeASM
			bu := internal.NewBgUploader(a, int32(tt.maxParallelism), int32(tt.maxNumFilesUploaded), tu, false)

			// Run BgUploader and wait 1 second before stopping
			bu.Start()
			// KLUDGE If maxParallelism=0, we expect to do no work. Therefore, do not wait.
			if tt.maxParallelism > 0 {
				time.Sleep(time.Second) // time to spin up new uploaders
			}
			bu.Stop()

			// Check that at least the expected number of files were created
			for i := 0; i < tt.wantNumFilesUploaded; i++ {
				bname := testFilename(i)
				wasUploaded := fakeASM.IsWalAlreadyUploaded(bname)
				assert.True(t, wasUploaded, bname+" was not marked as uploaded")
			}

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

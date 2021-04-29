package postgres_test

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/asm"
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
			numFilesOnDisk:       200,
			maxNumFilesUploaded:  32,
			wantNumFilesUploaded: 32,
		},
		{
			name:                 "parallelism=3 with lots of files",
			maxParallelism:       3,
			numFilesOnDisk:       200,
			maxNumFilesUploaded:  150,
			wantNumFilesUploaded: 150,
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

	viper.Set(internal.UploadWalMetadata, "NOMETADATA")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer testtools.Cleanup(t, internal.GetDataFolderPath())

			// Use generated data to test uploading WAL.
			dir, a := setupArchiveStatus(t, "")
			dirName := filepath.Join(dir, "pg_wal")
			for i := 0; i < tt.numFilesOnDisk; i++ {
				addTestDataFile(t, dirName, fmt.Sprint(i))
			}
			defer testtools.Cleanup(t, dir)

			// Setup BgUploader
			tu := testtools.NewMockWalUploader(false, false)
			fakeASM := asm.NewFakeASM()
			tu.ArchiveStatusManager = fakeASM
			bu := postgres.NewBgUploader(a, int32(tt.maxParallelism), int32(tt.maxNumFilesUploaded), tu, false)

			// Run BgUploader and wait 1 second before stopping
			bu.Start()
			// KLUDGE If maxParallelism=0, we expect to do no work. Therefore, do not wait.
			if tt.maxParallelism > 0 {
				time.Sleep(time.Second) // time to spin up new uploaders
			}
			bu.Stop()

			// Check that at least the expected number of files were created
			for i := 0; i < tt.wantNumFilesUploaded; i++ {
				bname := testFilename(fmt.Sprint(i))
				wasUploaded := fakeASM.WalAlreadyUploaded(bname)
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

	err = os.MkdirAll(filepath.Join(testDir, "pg_wal", "archive_status"), 0700)
	if err != nil {
		t.Log(err)
	}
	a := filepath.Join(testDir, "pg_wal", "A")
	file, err := os.Create(a)
	if err != nil {
		t.Log(err)
	}
	file.WriteString(strconv.Itoa(rand.Int()))
	file.WriteString(strconv.Itoa(rand.Int()))
	file.Close()

	return testDir, a
}

func addTestDataFile(t *testing.T, dir string, i string) {
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

func testFilename(i string) string {
	return fmt.Sprintf("%08d%08d%08s", 1, 1, i)
}

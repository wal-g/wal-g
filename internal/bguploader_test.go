package internal_test

import (
	"context"
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
		readDirLatency       time.Duration
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
			name:                 "parallelism=1 with a few files and slow ReadDir",
			maxParallelism:       1,
			numFilesOnDisk:       3,
			maxNumFilesUploaded:  32,
			readDirLatency:       time.Minute, // This basically disables ReadDir
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
			name:                 "parallelism=3 with lots of files and slow ReadDir",
			maxParallelism:       3,
			numFilesOnDisk:       200,
			maxNumFilesUploaded:  150,
			readDirLatency:       time.Minute, // This basically disables ReadDir
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
		{
			name:                 "parallelism=100 and extra files on disk and slow ReadDir",
			maxParallelism:       100,
			numFilesOnDisk:       100,
			maxNumFilesUploaded:  32,
			readDirLatency:       time.Minute, // This basically disables ReadDir
			wantNumFilesUploaded: 32,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer cleanup(t, internal.GetDataFolderPath())

			// Use generated data to test uploading WAL.
			dir, oldestWalFilepath := setupArchiveStatus(t, "")
			for i := 0; i < tt.numFilesOnDisk; i++ {
				addTestDataFile(t, dir, i+1)
			}
			defer cleanup(t, dir)

			// Setup BgUploader dependencies
			mockWalUploader := testtools.NewMockWalUploader(false, false)
			fakeASM := internal.NewFakeASM()
			mockWalUploader.ArchiveStatusManager = fakeASM

			fdrCtx, cancelFdrFunc := context.WithCancel(context.TODO())
			defer cancelFdrFunc()
			fakeArchiveStatusDataFolder := &stallableDataFolder{
				dirname:      filepath.Join(dir, "archive_status"),
				ctx:          fdrCtx,
				waitDuration: tt.readDirLatency,
			}

			// Setup BgUploader
			bu := internal.NewBgUploader(
				oldestWalFilepath,
				int32(tt.maxParallelism),
				int32(tt.maxNumFilesUploaded),
				mockWalUploader,
				false,
			)
			bu.ArchiveStatusFolder = fakeArchiveStatusDataFolder

			// Run BgUploader and wait 1 second before stopping
			bu.Start()
			// KLUDGE If maxParallelism=0, we expect to do no work. Therefore, do not wait.
			if tt.maxParallelism > 0 {
				time.Sleep(time.Second) // time to spin up new uploaders
			}
			bu.Stop()
			cancelFdrFunc()

			// Check that at least the expected number of files were created
			for i := 0; i < tt.wantNumFilesUploaded; i++ {
				walFilename := testFilename(i + 1)
				wasUploaded := fakeASM.IsWalAlreadyUploaded(walFilename)
				assert.True(t, wasUploaded, walFilename+" was not marked as uploaded")
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

	a := filepath.Join(testDir, testFilename(0))
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
	arbitraryTimeline := uint32(578)
	return internal.FormatWALFileName(arbitraryTimeline, uint64(i))
}

func cleanup(t *testing.T, dir string) {
	err := os.RemoveAll(dir)
	if err != nil {
		t.Log("temporary data directory was not deleted ", err)
	}
}

// stallableDataFolder implements a portion of internal.DataFolder and can be
// used to simulate a very slow ioutil.ReadDir operation. This can be used to
// test BgUploader behavior in adverse conditions.
type stallableDataFolder struct {
	internal.DataFolder
	dirname      string
	ctx          context.Context
	waitDuration time.Duration
}

func (s *stallableDataFolder) ListFilenames() ([]string, error) {
	select {
	case <-s.ctx.Done():
		return nil, fmt.Errorf(
			"fake dir reader was cancelled before wait completed: %v",
			s.ctx.Err(),
		)
	case <-time.After(s.waitDuration):
	}

	files, err := ioutil.ReadDir(s.dirname)
	if err != nil {
		return nil, err
	}
	filenames := []string{}
	for _, file := range files {
		filenames = append(filenames, file.Name())
	}
	return filenames, err
}

func (s *stallableDataFolder) FileExists(filename string) bool {
	filePath := filepath.Join(s.dirname, filename)
	_, err := os.Stat(filePath)
	return !os.IsNotExist(err)
}

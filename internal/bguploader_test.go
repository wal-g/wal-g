package internal_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/testtools"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// This test has known race condition
// We expect that background worker will upload 100 files.
// But we have no guaranties for this
func TestBackgroundWALUpload(t *testing.T) {
	dir, a := setupArchiveStatus(t, "")
	for i := 0; i < 100; i++ {
		addTestDataFile(t, dir, i)
	}

	// Re-use generated data to test uploading WAL.
	tu := testtools.NewMockUploader(false, false)
	bu := internal.NewBgUploader(a, 16, tu, false)
	// Look for new WALs while doing main upload
	bu.Start()
	time.Sleep(time.Second) // time to spin up new uploaders
	bu.Stop()

	for i := 0; i < 100; i++ {
		bname := "B" + strconv.Itoa(i)
		bd := filepath.Join(dir, "archive_status", bname+".done")
		_, err := os.Stat(bd)
		assert.Falsef(t, os.IsNotExist(err), bname+".done was not created")

		br := filepath.Join(dir, "archive_status", bname+".ready")
		_, err = os.Stat(br)
		assert.Truef(t, os.IsNotExist(err), bname+".ready was not deleted")
	}

	cleanup(t, dir)
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
	bname := "B" + strconv.Itoa(i)
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

func cleanup(t *testing.T, dir string) {
	err := os.RemoveAll(dir)
	if err != nil {
		t.Log("temporary data directory was not deleted ", err)
	}
}

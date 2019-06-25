package test

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
	archInfoFolder, _ := internal.NewDiskDataFolder(filepath.Join(dir, ".walg_archive_status"))
	archStatFolder, _ := internal.NewDiskDataFolder(filepath.Join(dir, "archive_status"))
	tu := testtools.NewMockUploader(false, false, internal.NewArchiveStatusManager(archInfoFolder, archStatFolder))
	bu := internal.NewBgUploader(a, 16, tu, false)
	// Look for new WALs while doing main upload
	bu.Start()
	time.Sleep(time.Second) // time to spin up new uploaders
	bu.Stop()

	for i := 0; i < 100; i++ {
		bname := "B" + strconv.Itoa(i)
		bd := filepath.Join(dir, ".walg_archive_status", bname)
		_, err := os.Stat(bd)
		assert.Falsef(t, os.IsNotExist(err), bname+"stat file was not created")
	}

	err := os.Remove(filepath.Join(dir, "archive_status", "B0.ready"))

	time.Sleep(time.Second)

	_, err = os.Stat(filepath.Join(dir, ".walg_archive_status", "B0"))

	assert.Truef(t, os.IsNotExist(err), "stat file was not deleted")

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
	_, _ = file.WriteString(strconv.Itoa(rand.Int()))
	_, _ = file.WriteString(strconv.Itoa(rand.Int()))
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

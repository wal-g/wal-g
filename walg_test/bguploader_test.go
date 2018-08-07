package walg_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g"
	"github.com/wal-g/wal-g/testtools"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// This test has known race condition
// We expect that background worker will upload 100 files.
// But we have no guaranties for this
// TODO : this test is really inconvenient for debugging, maybe its better to do its single-threaded version.
func TestBackgroundWALUpload(t *testing.T) {
	cwd, err := filepath.Abs("./")
	if err != nil {
		t.Log(err)
	}

	//Create temp directory.
	dir, err := ioutil.TempDir(cwd, "data")
	if err != nil {
		t.Log(err)
	}

	err = os.MkdirAll(filepath.Join(dir, "archive_status"), 0700)
	if err != nil {
		t.Log(err)
	}

	a := filepath.Join(dir, "A")
	file, err := os.Create(a)
	if err != nil {
		t.Log(err)
	}
	file.Close()

	for i := 0; i < 100; i++ {
		bname := "B" + strconv.Itoa(i)
		b := filepath.Join(dir, bname)
		file, err = os.Create(b)
		if err != nil {
			t.Log(err)
		}
		file.Close()

		br := filepath.Join(dir, "archive_status", bname+".ready")
		file, err = os.Create(br)
		if err != nil {
			t.Log(err)
		}
		file.Close()
	}

	// Re-use generated data to test uploading WAL.
	uploader := testtools.NewMockTarUploader(false, false)
	bgUploader := walg.BgUploader{}
	// Look for new WALs while doing main upload
	bgUploader.Start(a, 16, uploader, false)
	time.Sleep(time.Second) //time to spin up new uploaders
	bgUploader.Stop()

	for i := 0; i < 100; i++ {
		bname := "B" + strconv.Itoa(i)
		bd := filepath.Join(dir, "archive_status", bname+".done")
		_, err = os.Stat(bd)
		assert.Falsef(t, os.IsNotExist(err), bname+".done was not created")

		br := filepath.Join(dir, "archive_status", bname+".ready")
		_, err = os.Stat(br)
		assert.Truef(t, os.IsNotExist(err), bname+".ready was not deleted")
	}

	err = os.RemoveAll(dir)
	if err != nil {
		t.Log("temporary data directory was not deleted ", err)
	}
}

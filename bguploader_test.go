package walg_test

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/wal-g/wal-g"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
	"math/rand"
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
	tu := walg.NewLz4MockTarUploader()
	tu.UploaderApi = &mockS3Uploader{}
	pre := &walg.S3Prefix{
		Svc: &mockS3Client{
			err:      true,
			notFound: true,
		},
		Bucket: aws.String("mock bucket"),
		Server: aws.String("mock server"),
	}
	bu := walg.BgUploader{}
	// Look for new WALs while doing main upload
	bu.Start(a, 16, tu, pre, false)
	time.Sleep(time.Second) //time to spin up new uploaders
	bu.Stop()

	for i := 0; i < 100; i++ {
		bname := "B" + strconv.Itoa(i)
		bd := filepath.Join(dir, "archive_status", bname+".done")
		_, err := os.Stat(bd)
		if os.IsNotExist(err) {
			t.Error(bname + ".done was not created")
		}

		br := filepath.Join(dir, "archive_status", bname+".ready")
		_, err = os.Stat(br)
		if !os.IsNotExist(err) {
			t.Error(bname + ".ready was not deleted")
		}
	}

	cleanup(t, dir)
}

func TestBackgroundNoOverwriteWALUpload(t *testing.T) {
	var overwriteDir = "overwritetestdata"

	_, a := setupArchiveStatus(t, overwriteDir)

	// Re-use generated data to test uploading WAL.
	tu := walg.NewLz4MockTarUploader()
	tu.UploaderApi = &mockS3Uploader{}
	pre := &walg.S3Prefix{
		Svc: &mockS3Client{
			err:      false,
			notFound: false,
		},
		Bucket:              aws.String("mock bucket"),
		Server:              aws.String("mock server"),
		PreventWalOverwrite: true,
	}
	wasPanic := false
	defer func() {
		if r := recover(); r != nil {
			t.Log("Recovered ", r)
			wasPanic = true
		}
	}()
	walg.UploadWALFile(tu, a, pre, false, false)
	if !wasPanic {
		t.Errorf("WAL was overwritten")
	}

	file, err := os.OpenFile(a, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_EXCL, 0666)
	if err != nil {
		t.Error(err)
	}
	file.WriteString("Hello")
	file.Close()
	walg.UploadWALFile(tu, a, pre, false, false)
	// Should not panic
	walg.UploadWALFile(tu, a, pre, false, false)

	cleanup(t, overwriteDir)
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
		//Create temp directory.
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

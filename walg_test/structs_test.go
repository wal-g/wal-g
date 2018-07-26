package walg_test

import (
	"archive/tar"
	"testing"

	"github.com/wal-g/wal-g"
	"github.com/wal-g/wal-g/testtools"
	"os"
	"time"
)

// TODO : refactor this pile of shit

// Tests S3 get and set methods.
func TestS3TarBall(t *testing.T) {
	tarBallCounter := 0
	bundle := &walg.Bundle{
		TarSizeThreshold: int64(10),
	}

	bundle.TarBallMaker = &walg.S3TarBallMaker{
		ArchiveDirectory: "/usr/local",
		BackupName:       "test",
		TarUploader:      testtools.NewLz4MockTarUploader(),
	}

	bundle.NewTarBall(false)
	tarBallCounter += 1

	if bundle.TarBall == nil {
		t.Errorf("make: Did not successfully create a new tarball.")
	}

	tarBall := bundle.TarBall

	if tarBall.Size() != 0 {
		t.Errorf("make: Expected tarball initial size to be 0 but got %d", tarBall.Size())
	}

	increase := 1024
	tarBall.AddSize(int64(increase))

	if tarBall.Size() != 1024 {
		t.Errorf("make: Tarball size expected to increase to %d but got %d", increase, tarBall.Size())
	}

	if tarBall.TarWriter() != nil {
		t.Errorf("make: Tarball writer should not be set up without calling SetUp()")
	}

	bundle.NewTarBall(false)
	tarBallCounter += 1

	if tarBall == bundle.TarBall {
		t.Errorf("make: Did not successfully create a new tarball")
	}
}

// Tests S3 dependent functions for S3TarBall such as
// SetUp(), CloseTar() and Finish().
func TestS3DependentFunctions(t *testing.T) {
	bundle := &walg.Bundle{
		TarSizeThreshold: 100,
	}

	tu := testtools.NewLz4MockTarUploader()
	tu.UploaderApi = &mockS3Uploader{}

	bundle.TarBallMaker = &walg.S3TarBallMaker{
		ArchiveDirectory: "",
		BackupName:       "mockBackup",
		TarUploader:      tu,
	}

	bundle.NewTarBall(false)
	tarBall := bundle.TarBall
	tarBall.SetUp(MockArmedCrypter())
	tarWriter := tarBall.TarWriter()

	one := []byte("a")

	// Write mock header.
	hdr := &tar.Header{
		Name: "mock",
		Size: int64(1),
	}
	err := tarWriter.WriteHeader(hdr)
	if err != nil {
		t.Log(err)
	}

	// Write body.
	_, err = tarWriter.Write(one)

	if err != nil {
		t.Errorf("structs: expected to write 1 byte but got %s", err)
	}
	tarBall.CloseTar()

	// Handle write after close.
	_, err = tarBall.TarWriter().Write(one)
	if err == nil {
		t.Errorf("structs: expected WriteAfterClose error but got '<nil>'")
	}

	err = tarBall.Finish(&walg.S3TarBallSentinelDto{})
	if err != nil {
		t.Errorf("structs: tarball did not finish correctly with error %s", err)
	}

	// Test naming property of SetUp().
	bundle.NewTarBall(false)
	tarBall = bundle.TarBall
	tarBall.SetUp(MockArmedCrypter(), "mockTarball")
	tarBall.CloseTar()
	err = tarBall.Finish(&walg.S3TarBallSentinelDto{})
	if err != nil {
		t.Errorf("structs: tarball did not finish correctly with error %s", err)
	}

}

func TestEmptyBundleQueue(t *testing.T) {

	bundle := &walg.Bundle{
		TarSizeThreshold: 100,
	}

	tu := testtools.NewLz4MockTarUploader()
	tu.UploaderApi = &mockS3Uploader{}

	bundle.TarBallMaker = &walg.S3TarBallMaker{
		ArchiveDirectory: "",
		BackupName:       "mockBackup",
		TarUploader:      tu,
	}

	bundle.StartQueue()

	err := bundle.FinishQueue()
	if err != nil {
		t.Log(err)
	}
}

func TestBundleQueue(t *testing.T) {

	queueTest(t)

}

func TestBundleQueueHC(t *testing.T) {

	os.Setenv("WALG_UPLOAD_CONCURRENCY", "100")

	queueTest(t)

	os.Unsetenv("WALG_UPLOAD_CONCURRENCY")
}

func TestBundleQueueLC(t *testing.T) {

	os.Setenv("WALG_UPLOAD_CONCURRENCY", "1")

	queueTest(t)

	os.Unsetenv("WALG_UPLOAD_CONCURRENCY")
}

func queueTest(t *testing.T) {
	bundle := &walg.Bundle{
		TarSizeThreshold: 100,
	}
	tu := testtools.NewLz4MockTarUploader()
	tu.UploaderApi = &mockS3Uploader{}
	bundle.TarBallMaker = &walg.S3TarBallMaker{
		ArchiveDirectory: "",
		BackupName:       "mockBackup",
		TarUploader:      tu,
	}

	f := false
	tr := true
	// For tests there must be at leaest 3 workers

	bundle.StartQueue()

	a := bundle.Deque()
	go func() {
		time.Sleep(10 * time.Millisecond)
		bundle.EnqueueBack(a, &tr)
		time.Sleep(10 * time.Millisecond)
		bundle.EnqueueBack(a, &f)
	}()

	c := bundle.Deque()
	go func() {
		time.Sleep(10 * time.Millisecond)
		bundle.CheckSizeAndEnqueueBack(c)
	}()

	b := bundle.Deque()
	go func() {
		time.Sleep(10 * time.Millisecond)
		bundle.EnqueueBack(b, &f)
	}()

	err := bundle.FinishQueue()
	if err != nil {
		t.Log(err)
	}
}

func TestUserData(t *testing.T) {

	os.Setenv("WALG_SENTINEL_USER_DATA", "1.0")

	data := walg.GetSentinelUserData()
	t.Log(data)
	if 1.0 != data.(float64) {
		t.Fatal("Unable to parse WALG_SENTINEL_USER_DATA")
	}

	os.Setenv("WALG_SENTINEL_USER_DATA", "\"1\"")

	data = walg.GetSentinelUserData()
	t.Log(data)
	if "1" != data.(string) {
		t.Fatal("Unable to parse WALG_SENTINEL_USER_DATA")
	}

	os.Setenv("WALG_SENTINEL_USER_DATA", `{"x":123,"y":["asdasd",123]}`)

	data = walg.GetSentinelUserData()
	t.Log(data)
	if nil == data {
		t.Fatal("Unable to parse WALG_SENTINEL_USER_DATA")
	}

	os.Unsetenv("WALG_UPLOAD_CONCURRENCY")
}

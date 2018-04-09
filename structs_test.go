package walg_test

import (
	"archive/tar"
	"testing"

	"github.com/wal-g/wal-g"
)

// Tests S3 get and set methods.
func TestS3TarBall(t *testing.T) {
	tarBallCounter := 0
	bundle := &walg.Bundle{
		MinSize: int64(10),
	}

	bundle.Tbm = &walg.S3TarBallMaker{
		BaseDir:  "tmp",
		Trim:     "/usr/local",
		BkupName: "test",
	}

	bundle.NewTarBall()
	tarBallCounter += 1

	if bundle.Tb == nil {
		t.Errorf("make: Did not successfully create a new tarball.")
	}

	tarBall := bundle.Tb

	if tarBall.BaseDir() != "tmp" {
		t.Errorf("make: Expected base directory to be '%s' but got '%s'", "tmp", tarBall.BaseDir())
	}

	if tarBall.Trim() != "/usr/local" {
		t.Errorf("make: Expected trim to be '%s' but got '%s'", "/usr/local", tarBall.Trim())
	}

	if tarBall.Nop() {
		t.Errorf("make: S3TarBall expected NOP to be false but got %v", tarBall.Nop())
	}

	if tarBall.Number() != tarBallCounter {
		t.Errorf("make: Expected tarball number to be %d but got %d", tarBallCounter, tarBall.Number())
	}

	if tarBall.Size() != 0 {
		t.Errorf("make: Expected tarball initial size to be 0 but got %d", tarBall.Size())
	}

	increase := 1024
	tarBall.AddSize(int64(increase))

	if tarBall.Size() != 1024 {
		t.Errorf("make: Tarball size expected to increase to %d but got %d", increase, tarBall.Size())
	}

	if tarBall.Tw() != nil {
		t.Errorf("make: Tarball writer should not be set up without calling SetUp()")
	}

	bundle.NewTarBall()
	tarBallCounter += 1

	if tarBall == bundle.Tb {
		t.Errorf("make: Did not successfully create a new tarball")
	}

	if bundle.Tb.Number() != tarBallCounter {
		t.Errorf("make: Expected tarball number to increase to %d but got %d", tarBallCounter, tarBall.Number())
	}

}

// Tests S3 dependent functions for S3TarBall such as
// SetUp(), CloseTar() and Finish().
func TestS3DependentFunctions(t *testing.T) {
	bundle := &walg.Bundle{
		MinSize: 100,
	}

	tu := walg.NewTarUploader(&mockS3Client{}, "bucket", "server", "region", 1, float64(1))
	tu.Upl = &mockS3Uploader{}

	bundle.Tbm = &walg.S3TarBallMaker{
		BaseDir:  "mockDirectory",
		Trim:     "",
		BkupName: "mockBackup",
		Tu:       tu,
	}

	bundle.NewTarBall()
	tarBall := bundle.Tb
	tarBall.SetUp(walg.MockArmedCrypter())
	tarWriter := tarBall.Tw()

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
	_, err = tarBall.Tw().Write(one)
	if err == nil {
		t.Errorf("structs: expected WriteAfterClose error but got '<nil>'")
	}

	err = tarBall.Finish(&walg.S3TarBallSentinelDto{})
	if err != nil {
		t.Errorf("structs: tarball did not finish correctly with error %s", err)
	}

	// Test naming property of SetUp().
	bundle.NewTarBall()
	tarBall = bundle.Tb
	tarBall.SetUp(walg.MockArmedCrypter(), "mockTarball")
	tarBall.CloseTar()
	err = tarBall.Finish(&walg.S3TarBallSentinelDto{})
	if err != nil {
		t.Errorf("structs: tarball did not finish correctly with error %s", err)
	}

}

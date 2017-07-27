package walg_test

import (
	"github.com/katie31/wal-g"
	"testing"
)

/**
 *  Tests the various S3TarBall get and set methods.
 */
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

	if tarBall.Nop() != false {
		t.Errorf("make: S3TarBall expected NOP to be false but got %v", tarBall.Nop())
	}

	if tarBall.Number() != tarBallCounter {
		t.Errorf("make: Expected tarball number to be %d but got %d", tarBallCounter, tarBall.Number())
	}

	if tarBall.Size() != 0 {
		t.Errorf("make: Expected tarball initial size to be 0 but got %d", tarBall.Size())
	}

	increase := 1024
	tarBall.SetSize(int64(increase))

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

package walg_test

import (
	"archive/tar"
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g"
	"github.com/wal-g/wal-g/testtools"
	"io"
	"strings"
	"testing"
)

// TODO : this test is broken now
// Tests S3 get and set methods.
func TestS3TarBall(t *testing.T) {
	tarBallCounter := 0
	bundle := &walg.Bundle{
		ArchiveDirectory: "/usr/local",
		TarSizeThreshold: int64(10),
	}

	bundle.TarBallMaker = &walg.S3TarBallMaker{
		BackupName: "test",
		Uploader:   testtools.NewMockTarUploader(false, false),
	}

	bundle.NewTarBall(false)
	tarBallCounter += 1

	assert.NotNil(t, bundle.TarBall)

	tarBall := bundle.TarBall

	assert.Equal(t, int64(0), tarBall.Size())
	assert.Nil(t, tarBall.TarWriter())

	bundle.NewTarBall(false)
	tarBallCounter += 1
	//assert.Equal(t, bundle.TarBall, tarBall)
}

// Tests S3 dependent functions for S3TarBall such as
// SetUp(), CloseTar() and Finish().
func TestS3DependentFunctions(t *testing.T) {
	bundle := &walg.Bundle{
		ArchiveDirectory: "",
		TarSizeThreshold: 100,
	}

	uploader := testtools.NewMockTarUploader(false, false)

	bundle.TarBallMaker = &walg.S3TarBallMaker{
		BackupName: "mockBackup",
		Uploader:   uploader,
	}

	bundle.NewTarBall(false)
	tarBall := bundle.TarBall
	tarBall.SetUp(MockArmedCrypter())
	tarWriter := tarBall.TarWriter()

	mockData := []byte("a")

	// Write mock header.
	mockHeader := &tar.Header{
		Name: "mock",
		Size: int64(len(mockData)),
	}
	err := tarWriter.WriteHeader(mockHeader)
	if err != nil {
		t.Log(err)
	}

	// Write body.
	_, err = tarWriter.Write(mockData)

	assert.NoError(t, err)
	tarBall.CloseTar()

	// Handle write after close.
	_, err = tarBall.TarWriter().Write(mockData)
	assert.Error(t, err)

	err = tarBall.Finish(&walg.S3TarBallSentinelDto{})
	assert.NoError(t, err)

	// Test naming property of SetUp().
	bundle.NewTarBall(false)
	tarBall = bundle.TarBall
	tarBall.SetUp(MockArmedCrypter(), "mockTarball")
	tarBall.CloseTar()
	err = tarBall.Finish(&walg.S3TarBallSentinelDto{})
	assert.NoError(t, err)
}

func TestPackFileTo(t *testing.T) {
	mockData := "mock"
	mockHeader := &tar.Header{
		Name:     "mock",
		Mode:     int64(0600),
		Size:     int64(len(mockData)),
		Typeflag: tar.TypeReg,
	}
	buffer := bytes.NewBuffer(make([]byte, 0))
	tarBallMaker := testtools.BufferTarBallMaker{
		BufferToWrite: buffer,
	}
	tarBall := tarBallMaker.Make(false)
	tarBall.SetUp(MockDisarmedCrypter())
	size, err := walg.PackFileTo(tarBall, mockHeader, strings.NewReader(mockData))
	assert.Equal(t, int64(len(mockData)), size)
	assert.NoError(t, err)
	assert.Equal(t, tarBall.Size(), size)

	reader := tar.NewReader(buffer)
	interpreter := testtools.BufferTarInterpreter{}
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)
		err = interpreter.Interpret(reader, header)
		assert.NoError(t, err)
	}
	assert.Equal(t, []byte(mockData), interpreter.Out)
}

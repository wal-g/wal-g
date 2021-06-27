package internal_test

import (
	"archive/tar"
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/testtools"
)

// TODO : this test is broken now
// Tests S3 get and set methods.
func TestS3TarBall(t *testing.T) {
	tarSizeThreshold := int64(10)

	tarBallMaker := internal.NewStorageTarBallMaker("test", false, testtools.NewMockUploader(false, false))

	tarBallQueue := internal.NewTarBallQueue(tarSizeThreshold, tarBallMaker)
	_ = tarBallQueue.StartQueue()
	tarBallQueue.NewTarBall(false)

	assert.NotNil(t, tarBallQueue.LastCreatedTarball)

	tarBall := tarBallQueue.LastCreatedTarball

	assert.Equal(t, int64(0), tarBall.Size())
	assert.Nil(t, tarBall.TarWriter())

	tarBallQueue.NewTarBall(false)
	//assert.Equal(t, bundle.TarBall, tarBall)
}

// Tests S3 dependent functions for StorageTarBall such as
// SetUp(), CloseTar() and Finish().
func TestS3DependentFunctions(t *testing.T) {
	tarSizeThreshold := int64(100)

	uploader := testtools.NewMockUploader(false, false)

	tarBallMaker := internal.NewStorageTarBallMaker("mockBackup", false, uploader)
	tarBallQueue := internal.NewTarBallQueue(tarSizeThreshold, tarBallMaker)
	_ = tarBallQueue.StartQueue()

	tarBallQueue.NewTarBall(false)
	tarBall := tarBallQueue.LastCreatedTarball
	tarBall.SetUp(nil)
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
	err = tarBall.CloseTar()
	assert.NoError(t, err)

	// Handle write after close.
	_, err = tarBall.TarWriter().Write(mockData)
	assert.Error(t, err)
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
	size := int64(0)

	tarBallMaker := testtools.BufferTarBallMaker{
		BufferToWrite: buffer,
		Size:          &size,
	}
	tarBall := tarBallMaker.Make(false)
	tarBall.SetUp(nil)
	size, err := internal.PackFileTo(tarBall, mockHeader, strings.NewReader(mockData))
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

package testutils

import (
	"bytes"
	"errors"
	"io"
	"sync"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func NewS3ErrorFolder(sourceFolder storage.Folder, S3ErrorAfterByteSize int) storage.Folder {
	return &S3TestFolder{
		Folder:          sourceFolder,
		errorEachBytes:  int64(S3ErrorAfterByteSize),
		bytesLeft:       int64(S3ErrorAfterByteSize),
		wasSkippedBlock: true,
	}
}

type S3TestFolder struct {
	storage.Folder
	errorEachBytes  int64
	bytesLeft       int64
	wasSkippedBlock bool

	mutex sync.Mutex
}

func (tf *S3TestFolder) PutObject(name string, content io.Reader) error {
	buf := &bytes.Buffer{}
	count, err := io.Copy(buf, content)
	if err != nil {
		return err
	}
	tf.mutex.Lock()
	defer tf.mutex.Unlock()
	if count >= tf.bytesLeft && !tf.wasSkippedBlock {
		tf.wasSkippedBlock = true
		tf.bytesLeft = tf.errorEachBytes
		return errors.New("S3 error")
	}
	tf.wasSkippedBlock = false
	tf.bytesLeft -= count

	err = tf.Folder.PutObject(name, content)

	return err
}

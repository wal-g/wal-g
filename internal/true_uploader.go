package internal

import (
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/utility"
)

// TrueUploader contains fields associated with uploading tarballs.
// Multiple tarballs can share one trueUploader.
type TrueUploader struct {
	UploadingFolder      storage.Folder
	Compressor           compression.Compressor
	waitGroup            *sync.WaitGroup
	ArchiveStatusManager ArchiveStatusManager
	Failed               atomic.Value
	tarSize              *int64
}

// UploadObject
type UploadObject struct {
	Path    string
	Content io.Reader
}

func NewTrueUploader(
	compressor compression.Compressor,
	uploadingLocation storage.Folder,
) *TrueUploader {
	size := int64(0)
	trueUploader := &TrueUploader{
		UploadingFolder: uploadingLocation,
		Compressor:      compressor,
		waitGroup:       &sync.WaitGroup{},
		tarSize:         &size,
	}
	trueUploader.Failed.Store(false)
	return trueUploader
}

// finish waits for all waiting parts to be uploaded. If an error occurs,
// prints alert to stderr.
func (trueUploader *TrueUploader) finish() {
	trueUploader.waitGroup.Wait()
	if trueUploader.Failed.Load().(bool) {
		tracelog.ErrorLogger.Printf("WAL-G could not complete upload.\n")
	}
}

// Clone creates similar TrueUploader with new WaitGroup
func (trueUploader *TrueUploader) clone() *TrueUploader {
	return &TrueUploader{
		trueUploader.UploadingFolder,
		trueUploader.Compressor,
		&sync.WaitGroup{},
		trueUploader.ArchiveStatusManager,
		trueUploader.Failed,
		trueUploader.tarSize,
	}
}

// TODO : unit tests
// UploadFile compresses a file and uploads it.
func (trueUploader *TrueUploader) UploadFile(file NamedReader) error {
	compressedFile := CompressAndEncrypt(file, trueUploader.Compressor, ConfigureCrypter())
	dstPath := utility.SanitizePath(filepath.Base(file.Name()) + "." + trueUploader.Compressor.FileExtension())

	err := trueUploader.Upload(dstPath, compressedFile)
	tracelog.InfoLogger.Println("FILE PATH:", dstPath)
	return err
}

// TODO : unit tests
func (trueUploader *TrueUploader) Upload(path string, content io.Reader) error {
	err := trueUploader.UploadingFolder.PutObject(path, &WithSizeReader{content, trueUploader.tarSize})
	if err == nil {
		return nil
	}
	trueUploader.Failed.Store(true)
	tracelog.ErrorLogger.Printf(tracelog.GetErrorFormatter()+"\n", err)
	return err
}

// UploadMultiple uploads multiple objects from the start of the slice,
// returning the first error if any. Note that this operation is not atomic
// TODO : unit tests
func (trueUploader *TrueUploader) uploadMultiple(objects []UploadObject) error {
	for _, object := range objects {
		err := trueUploader.Upload(object.Path, object.Content)
		if err != nil {
			// possibly do a retry here
			return err
		}
	}
	return nil
}

package internal

import (
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/asm"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/utility"
)

type UploaderProvider interface {
	Upload(path string, content io.Reader) error
	UploadFile(file ioextensions.NamedReader) error
	PushStream(stream io.Reader) (string, error)
	PushStreamToDestination(stream io.Reader, dstPath string) error
	Compression() compression.Compressor
	DisableSizeTracking()
	GetBackupSize() *int64
	GetDataSize() *int64
}

// Uploader contains fields associated with uploading tarballs.
// Multiple tarballs can share one uploader.
type Uploader struct {
	UploadingFolder      storage.Folder
	Compressor           compression.Compressor
	waitGroup            *sync.WaitGroup
	ArchiveStatusManager asm.ArchiveStatusManager
	Failed               atomic.Value
	tarSize              *int64
	dataSize             *int64
}

// UploadObject
type UploadObject struct {
	Path    string
	Content io.Reader
}

func NewUploader(
	compressor compression.Compressor,
	uploadingLocation storage.Folder,
) *Uploader {
	uploader := &Uploader{
		UploadingFolder: uploadingLocation,
		Compressor:      compressor,
		waitGroup:       &sync.WaitGroup{},
		tarSize:         new(int64),
		dataSize:        new(int64),
	}
	uploader.Failed.Store(false)
	return uploader
}

// GetBackupSize returns nil when SizeTracking disabled see DisableSizeTracking
func (uploader *Uploader) GetBackupSize() *int64 {
	return uploader.tarSize
}

// GetDataSize returns nil when SizeTracking disabled see DisableSizeTracking
func (uploader *Uploader) GetDataSize() *int64 {
	return uploader.dataSize
}

// Finish waits for all waiting parts to be uploaded. If an error occurs,
// prints alert to stderr.
func (uploader *Uploader) Finish() {
	uploader.waitGroup.Wait()
	if uploader.Failed.Load().(bool) {
		tracelog.ErrorLogger.Printf("WAL-G could not complete upload.\n")
	}
}

// Clone creates similar Uploader with new WaitGroup
func (uploader *Uploader) Clone() *Uploader {
	return &Uploader{
		UploadingFolder:      uploader.UploadingFolder,
		Compressor:           uploader.Compressor,
		waitGroup:            &sync.WaitGroup{},
		ArchiveStatusManager: uploader.ArchiveStatusManager,
		Failed:               uploader.Failed,
		tarSize:              uploader.tarSize,
		dataSize:             uploader.dataSize,
	}
}

// TODO : unit tests
// UploadFile compresses a file and uploads it.
func (uploader *Uploader) UploadFile(file ioextensions.NamedReader) error {
	filename := file.Name()

	fileReader := file.(io.Reader)
	if uploader.dataSize != nil {
		fileReader = NewWithSizeReader(fileReader, uploader.dataSize)
	}
	compressedFile := CompressAndEncrypt(fileReader, uploader.Compressor, ConfigureCrypter())
	dstPath := utility.SanitizePath(filepath.Base(filename) + "." + uploader.Compressor.FileExtension())

	err := uploader.Upload(dstPath, compressedFile)
	tracelog.InfoLogger.Println("FILE PATH:", dstPath)
	return err
}

// DisableSizeTracking stops bandwidth tracking
func (uploader *Uploader) DisableSizeTracking() {
	uploader.tarSize = nil
	uploader.dataSize = nil
}

// Compression returns configured compressor
func (uploader *Uploader) Compression() compression.Compressor {
	return uploader.Compressor
}

// TODO : unit tests
func (uploader *Uploader) Upload(path string, content io.Reader) error {
	if uploader.tarSize != nil {
		content = NewWithSizeReader(content, uploader.tarSize)
	}
	err := uploader.UploadingFolder.PutObject(path, content)
	if err == nil {
		return nil
	}
	uploader.Failed.Store(true)
	tracelog.ErrorLogger.Printf(tracelog.GetErrorFormatter()+"\n", err)
	return err
}

// UploadMultiple uploads multiple objects from the start of the slice,
// returning the first error if any. Note that this operation is not atomic
// TODO : unit tests
func (uploader *Uploader) UploadMultiple(objects []UploadObject) error {
	for _, object := range objects {
		err := uploader.Upload(object.Path, object.Content)
		if err != nil {
			// possibly do a retry here
			return err
		}
	}
	return nil
}

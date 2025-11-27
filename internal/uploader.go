package internal

import (
	"context"
	json2 "encoding/json/v2"
	"fmt"
	"golang.org/x/sync/errgroup"

	"io"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/wal-g/wal-g/internal/statistics"

	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

var ErrorSizeTrackingDisabled = fmt.Errorf("size tracking disabled by DisableSizeTracking method")

type Uploader interface {
	// Upload uploads stream AS IS
	Upload(ctx context.Context, path string, content io.Reader) error
	UploadJSON(ctx context.Context, path string, data any) error
	// UploadFile compresses a file and uploads it
	UploadFile(ctx context.Context, file ioextensions.NamedReader) error
	UploadExactFile(ctx context.Context, file ioextensions.NamedReader) error
	// PushStream compresses a stream and push it
	PushStream(ctx context.Context, stream io.Reader) (string, error)
	// PushStreamToDestination compresses a stream and push it to specifyed destination
	PushStreamToDestination(ctx context.Context, stream io.Reader, dstPath string) error
	// Compression returns configured compressor
	Compression() compression.Compressor
	DisableSizeTracking()
	UploadedDataSize() (int64, error)
	RawDataSize() (int64, error)
	ChangeDirectory(relativePath string)
	Folder() storage.Folder
	Clone() Uploader
	Failed() bool
	Finish()
}

// RegularUploader contains fields associated with uploading tarballs.
// Multiple tarballs can share one Uploader.
type RegularUploader struct {
	UploadingFolder storage.Folder
	Compressor      compression.Compressor
	waitGroup       *sync.WaitGroup
	failed          atomic.Bool
	tarSize         *int64
	dataSize        *int64
}

var _ Uploader = &RegularUploader{}

// SplitStreamUploader - new Uploader implementation that enable us to split upload streams into blocks
//
//	of blockSize bytes, then puts it in at most `partitions` streams that are compressed and pushed to storage
type SplitStreamUploader struct {
	Uploader
	partitions  int
	blockSize   int
	maxFileSize int
}

var _ Uploader = &SplitStreamUploader{}

// UploadObject
type UploadObject struct {
	Path    string
	Content io.Reader
}

func NewRegularUploader(
	compressor compression.Compressor,
	uploadingLocation storage.Folder,
) *RegularUploader {
	uploader := &RegularUploader{
		UploadingFolder: uploadingLocation,
		Compressor:      compressor,
		waitGroup:       &sync.WaitGroup{},
		tarSize:         new(int64),
		dataSize:        new(int64),
		failed:          atomic.Bool{},
	}
	return uploader
}

func NewSplitStreamUploader(
	uploader Uploader,
	partitions int,
	blockSize int,
	maxFileSize int,
) Uploader {
	if partitions <= 1 && maxFileSize == 0 {
		// Fallback to old implementation in order to skip unneeded steps:
		return uploader
	}

	return &SplitStreamUploader{
		Uploader:    uploader,
		partitions:  partitions,
		blockSize:   blockSize,
		maxFileSize: maxFileSize,
	}
}

// UploadedDataSize returns 0 and error when SizeTracking disabled (see DisableSizeTracking)
func (uploader *RegularUploader) UploadedDataSize() (int64, error) {
	if uploader.tarSize == nil {
		return 0, ErrorSizeTrackingDisabled
	}
	return atomic.LoadInt64(uploader.tarSize), nil
}

// RawDataSize returns 0 and error when SizeTracking disabled (see DisableSizeTracking)
func (uploader *RegularUploader) RawDataSize() (int64, error) {
	if uploader.dataSize == nil {
		return 0, ErrorSizeTrackingDisabled
	}
	return atomic.LoadInt64(uploader.dataSize), nil
}

// Finish waits for all waiting parts to be uploaded. If an error occurs,
// prints alert to stderr.
func (uploader *RegularUploader) Finish() {
	uploader.waitGroup.Wait()
	if uploader.failed.Load() {
		tracelog.ErrorLogger.Printf("WAL-G could not complete upload.\n")
	}
}

// Clone creates similar Uploader with new WaitGroup
func (uploader *RegularUploader) Clone() Uploader {
	clone := &RegularUploader{
		UploadingFolder: uploader.UploadingFolder,
		Compressor:      uploader.Compressor,
		waitGroup:       &sync.WaitGroup{},
		failed:          atomic.Bool{},
		tarSize:         uploader.tarSize,
		dataSize:        uploader.dataSize,
	}
	clone.failed.Store(uploader.Failed())
	return clone
}

// TODO : unit tests
// UploadFile compresses a file and uploads it.
func (uploader *RegularUploader) uploadFile(ctx context.Context, file ioextensions.NamedReader, isExactPath bool) error {
	filename := file.Name()

	fileReader := file.(io.Reader)
	if uploader.dataSize != nil {
		fileReader = utility.NewWithSizeReader(fileReader, uploader.dataSize)
	}
	compressedFile := CompressAndEncrypt(fileReader, uploader.Compressor, ConfigureCrypter())

	dstPath := utility.SanitizePath(filename)
	if !isExactPath {
		dstPath = utility.SanitizePath(filepath.Base(filename) + "." + uploader.Compressor.FileExtension())
	}

	err := uploader.Upload(ctx, dstPath, compressedFile)
	tracelog.InfoLogger.Println("FILE PATH:", dstPath)
	return err
}

func (uploader *RegularUploader) UploadFile(ctx context.Context, file ioextensions.NamedReader) error {
	return uploader.uploadFile(ctx, file, false)
}

// UploadFile compresses a file and uploads it by exact path.
func (uploader *RegularUploader) UploadExactFile(ctx context.Context, file ioextensions.NamedReader) error {
	return uploader.uploadFile(ctx, file, true)
}

// DisableSizeTracking stops bandwidth tracking
func (uploader *RegularUploader) DisableSizeTracking() {
	uploader.tarSize = nil
	uploader.dataSize = nil
}

// Compression returns configured compressor
func (uploader *RegularUploader) Compression() compression.Compressor {
	return uploader.Compressor
}

func (uploader *RegularUploader) Upload(ctx context.Context, path string, content io.Reader) error {
	uploader.waitGroup.Add(1)
	defer uploader.waitGroup.Done()

	statistics.WalgMetrics.UploadedFilesTotal.Inc()
	if uploader.tarSize != nil {
		content = utility.NewWithSizeReader(content, uploader.tarSize)
	}
	err := uploader.UploadingFolder.PutObjectWithContext(ctx, path, content)
	if err != nil {
		statistics.WalgMetrics.UploadedFilesFailedTotal.Inc()
		uploader.failed.Load()
		tracelog.ErrorLogger.Printf(tracelog.GetErrorFormatter()+"\n", err)
		return err
	}
	return nil
}

// UploadJSON uploads raw JSON to storage without allocating memory for whole JSON.
func (uploader *RegularUploader) UploadJSON(ctx context.Context, path string, data any) error {
	reader, writer := io.Pipe()

	errorGroup, _ := errgroup.WithContext(ctx)
	errorGroup.Go(func() error {
		err := json2.MarshalWrite(writer, data)
		if err != nil {
			_ = writer.CloseWithError(err)
			return err
		}
		return writer.Close()
	})
	errorGroup.Go(func() error {
		err := uploader.Upload(ctx, path, reader)
		if err != nil {
			_ = reader.CloseWithError(err)
			return err
		}
		return reader.Close()
	})
	return errorGroup.Wait()
}

// UploadMultiple uploads multiple objects from the start of the slice,
// returning the first error if any. Note that this operation is not atomic
// TODO : unit tests / is it used?
func (uploader *RegularUploader) UploadMultiple(ctx context.Context, objects []UploadObject) error {
	for _, object := range objects {
		err := uploader.Upload(ctx, object.Path, object.Content)
		if err != nil {
			// possibly do a retry here
			return err
		}
	}
	return nil
}

func (uploader *RegularUploader) ChangeDirectory(relativePath string) {
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(relativePath)
}

func (uploader *RegularUploader) Folder() storage.Folder {
	return uploader.UploadingFolder
}

func (uploader *RegularUploader) Failed() bool {
	return uploader.failed.Load()
}

func (uploader *SplitStreamUploader) Clone() Uploader {
	return &SplitStreamUploader{
		Uploader:   uploader.Uploader.Clone(),
		partitions: uploader.partitions,
		blockSize:  uploader.blockSize,
	}
}

package internal

import (
	"fmt"
	"io"
	"math"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wal-g/wal-g/internal/abool"

	"github.com/VividCortex/ewma"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

var ErrorSizeTrackingDisabled = fmt.Errorf("size tracking disabled by DisableSizeTracking method")

type Uploader interface {
	Upload(path string, content io.Reader) error
	UploadFile(file ioextensions.NamedReader) error
	PushStream(stream io.Reader) (string, error)
	PushStreamToDestination(stream io.Reader, dstPath string) error
	Compression() compression.Compressor
	DisableSizeTracking()
	UploadedDataSize() (int64, error)
	RawDataSize() (int64, error)
	ChangeDirectory(relativePath string)
	Folder() storage.Folder
	Clone() Uploader
	Failed() bool
	Finish()
	ShowRemainingTime()
}

// RegularUploader contains fields associated with uploading tarballs.
// Multiple tarballs can share one uploader.
type RegularUploader struct {
	UploadingFolder          storage.Folder
	Compressor               compression.Compressor
	waitGroup                *sync.WaitGroup
	failed                   *abool.AtomicBool
	tarSize                  *int64
	dataSize                 *int64
	compressedUploadSpeeds   ewma.SimpleEWMA
	uncompressedUploadSpeeds ewma.SimpleEWMA
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
		failed:          abool.New(),
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
	uploader.waitGroup.Add(1)
	go uploader.ShowRemainingTime()

	uploader.waitGroup.Wait()
	if uploader.failed.IsSet() {
		tracelog.ErrorLogger.Printf("WAL-G could not complete upload.\n")
	}
}

// Clone creates similar Uploader with new WaitGroup
func (uploader *RegularUploader) Clone() Uploader {
	return &RegularUploader{
		UploadingFolder:          uploader.UploadingFolder,
		Compressor:               uploader.Compressor,
		waitGroup:                &sync.WaitGroup{},
		failed:                   abool.NewBool(uploader.Failed()),
		tarSize:                  uploader.tarSize,
		dataSize:                 uploader.dataSize,
		compressedUploadSpeeds:   uploader.compressedUploadSpeeds,
		uncompressedUploadSpeeds: uploader.uncompressedUploadSpeeds,
	}
}

// TODO : unit tests
// UploadFile compresses a file and uploads it.
func (uploader *RegularUploader) UploadFile(file ioextensions.NamedReader) error {
	filename := file.Name()

	fileReader := file.(io.Reader)
	if uploader.dataSize != nil {
		fileReader = utility.NewWithSizeReader(fileReader, uploader.dataSize)
	}
	compressedFile := CompressAndEncrypt(fileReader, uploader.Compressor, ConfigureCrypter())
	dstPath := utility.SanitizePath(filepath.Base(filename) + "." + uploader.Compressor.FileExtension())

	err := uploader.Upload(dstPath, compressedFile)
	tracelog.InfoLogger.Println("FILE PATH:", dstPath)
	return err
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

// TODO : unit tests
func (uploader *RegularUploader) Upload(path string, content io.Reader) error {
	uploader.waitGroup.Add(1)
	defer uploader.waitGroup.Done()

	WalgMetrics.uploadedFilesTotal.Inc()
	if uploader.tarSize != nil {
		content = utility.NewWithSizeReader(content, uploader.tarSize)
	}
	err := uploader.UploadingFolder.PutObject(path, content)

	if err != nil {
		WalgMetrics.uploadedFilesFailedTotal.Inc()
		uploader.failed.Set()
		tracelog.ErrorLogger.Printf(tracelog.GetErrorFormatter()+"\n", err)
		return err
	}
	return nil
}

// UploadMultiple uploads multiple objects from the start of the slice,
// returning the first error if any. Note that this operation is not atomic
// TODO : unit tests
func (uploader *RegularUploader) UploadMultiple(objects []UploadObject) error {
	for _, object := range objects {
		err := uploader.Upload(object.Path, object.Content)
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
	return uploader.failed.IsSet()
}

func (uploader *SplitStreamUploader) Clone() Uploader {
	return &SplitStreamUploader{
		Uploader:   uploader.Uploader.Clone(),
		partitions: uploader.partitions,
		blockSize:  uploader.blockSize,
	}
}

func (uploader *RegularUploader) ShowRemainingTime() {
	defer uploader.waitGroup.Done()
	startTime := time.Now()
	var prevCompressedSize int64 = 0
	var prevUncompressedSize int64 = 0
	for {
		compressedSize, err := uploader.UploadedDataSize()
		if err != nil {
			return
		}
		uncompressedSize, err := uploader.RawDataSize()
		if err != nil {
			return
		}

		if compressedSize == prevCompressedSize || uncompressedSize == prevUncompressedSize {
			return
		}

		timeElapsed := time.Since(startTime).Seconds()
		uploader.compressedUploadSpeeds.Add(float64(compressedSize) / timeElapsed)
		uploader.uncompressedUploadSpeeds.Add(float64(uncompressedSize) / timeElapsed)
		compressedSpeed := int64(math.Round(uploader.compressedUploadSpeeds.Value()))
		uncompressedSpeed := int64(math.Round(uploader.uncompressedUploadSpeeds.Value()))
		tracelog.InfoLogger.Printf(
			"Uploaded: %s, average %s/s. Raw data read: %s, average %s/s",
			utility.ByteCountSI(compressedSize),
			utility.ByteCountSI(compressedSpeed),
			utility.ByteCountSI(uncompressedSize),
			utility.ByteCountSI(uncompressedSpeed))

		prevUncompressedSize = uncompressedSize
		prevCompressedSize = compressedSize
		time.Sleep(5 * time.Minute)
	}
}

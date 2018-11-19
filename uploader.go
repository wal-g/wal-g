package walg

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"io"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

// Uploader contains fields associated with uploading tarballs.
// Multiple tarballs can share one uploader. Must call CreateUploaderAPI()
// in 'configure.go'.
type Uploader struct {
	uploaderApi          s3manageriface.UploaderAPI
	uploadingFolder      *S3Folder
	serverSideEncryption string
	SSEKMSKeyId          string
	StorageClass         string
	Success              bool
	compressor           Compressor
	useWalDelta          bool
	waitGroup            *sync.WaitGroup
	deltaFileManager     *DeltaFileManager
	verify               bool
}

func NewUploader(
	uploaderAPI s3manageriface.UploaderAPI,
	serverSideEncryption, sseKMSKeyId, storageClass string,
	compressor Compressor,
	uploadingLocation *S3Folder,
	deltaDataFolder DataFolder,
	useWalDelta, verify bool,
) *Uploader {
	var deltaFileManager *DeltaFileManager = nil
	if useWalDelta {
		deltaFileManager = NewDeltaFileManager(deltaDataFolder)
	}
	return &Uploader{
		uploaderApi:          uploaderAPI,
		uploadingFolder:      uploadingLocation,
		serverSideEncryption: serverSideEncryption,
		SSEKMSKeyId:          sseKMSKeyId,
		StorageClass:         storageClass,
		compressor:           compressor,
		useWalDelta:          useWalDelta,
		waitGroup:            &sync.WaitGroup{},
		deltaFileManager:     deltaFileManager,
		verify:               verify,
	}
}

// finish waits for all waiting parts to be uploaded. If an error occurs,
// prints alert to stderr.
func (uploader *Uploader) finish() {
	uploader.waitGroup.Wait()
	if !uploader.Success {
		errorLogger.Printf("WAL-G could not complete upload.\n")
	}
}

// Clone creates similar Uploader with new WaitGroup
func (uploader *Uploader) Clone() *Uploader {
	return &Uploader{
		uploader.uploaderApi,
		uploader.uploadingFolder,
		uploader.serverSideEncryption,
		uploader.SSEKMSKeyId,
		uploader.StorageClass,
		uploader.Success,
		uploader.compressor,
		uploader.useWalDelta,
		&sync.WaitGroup{},
		uploader.deltaFileManager,
		uploader.verify,
	}
}

// TODO : unit tests
func (uploader *Uploader) UploadWalFile(file NamedReader) error {
	var walFileReader io.Reader

	filename := path.Base(file.Name())
	if uploader.useWalDelta && isWalFilename(filename) {
		recordingReader, err := NewWalDeltaRecordingReader(file, filename, uploader.deltaFileManager)
		if err != nil {
			walFileReader = file
		} else {
			walFileReader = recordingReader
			defer recordingReader.Close()
		}
	} else {
		walFileReader = file
	}

	return uploader.UploadFile(&NamedReaderImpl{walFileReader, file.Name()})
}

// TODO : unit tests
// UploadFile compresses a file and uploads it.
func (uploader *Uploader) UploadFile(file NamedReader) error {
	pipeWriter := &CompressingPipeWriter{
		Input:                file,
		NewCompressingWriter: uploader.compressor.NewWriter,
	}

	pipeWriter.Compress(&OpenPGPCrypter{})

	dstPath := sanitizePath(uploader.uploadingFolder.Server + WalPath + filepath.Base(file.Name()) + "." + uploader.compressor.FileExtension())
	reader := pipeWriter.Output

	if uploader.verify {
		reader = newMd5Reader(reader)
	}

	input := uploader.CreateUploadInput(dstPath, reader)

	err := uploader.upload(input, file.Name())
	infoLogger.Println("FILE PATH:", dstPath)
	if uploader.verify {
		sum := reader.(*MD5Reader).Sum()
		archive := &Archive{
			Folder:  uploader.uploadingFolder,
			Archive: aws.String(dstPath),
		}
		eTag, err := archive.getETag()
		if err != nil {
			errorLogger.Panicf("Unable to verify file %s", err)
		}
		if eTag == nil {
			errorLogger.Panicf("Unable to verify file: nil ETag ")
		}

		trimETag := strings.Trim(*eTag, "\"")
		if sum != trimETag {
			errorLogger.Panicf("file verification failed: md5 %s ETag %s", sum, trimETag)
		}
		infoLogger.Println("ETag ", trimETag)
	}
	return err
}

// CreateUploadInput creates a s3manager.UploadInput for a Uploader using
// the specified path and reader.
func (uploader *Uploader) CreateUploadInput(path string, reader io.Reader) *s3manager.UploadInput {
	uploadInput := &s3manager.UploadInput{
		Bucket:       uploader.uploadingFolder.Bucket,
		Key:          aws.String(path),
		Body:         reader,
		StorageClass: aws.String(uploader.StorageClass),
	}

	if uploader.serverSideEncryption != "" {
		uploadInput.ServerSideEncryption = aws.String(uploader.serverSideEncryption)

		if uploader.SSEKMSKeyId != "" {
			// Only aws:kms implies sseKmsKeyId, checked during validation
			uploadInput.SSEKMSKeyId = aws.String(uploader.SSEKMSKeyId)
		}
	}

	return uploadInput
}

// TODO : unit tests
// Helper function to upload to S3. If an error occurs during upload, retries will
// occur in exponentially incremental seconds.
func (uploader *Uploader) upload(input *s3manager.UploadInput, path string) error {
	uploaderAPI := uploader.uploaderApi

	_, err := uploaderAPI.Upload(input)
	if err == nil {
		uploader.Success = true
		return nil
	}

	if multierr, ok := err.(s3manager.MultiUploadFailure); ok {
		errorLogger.Printf("upload: failed to upload '%s' with UploadID '%s'.", path, multierr.UploadID())
	} else {
		errorLogger.Printf("upload: failed to upload '%s': %s.", path, err.Error())
	}
	return err
}

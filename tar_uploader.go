package walg

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/pkg/errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// TarUploader contains fields associated with uploading tarballs.
// Multiple tarballs can share one uploader. Must call CreateUploader()
// in 'upload.go'.
type TarUploader struct {
	UploaderApi          s3manageriface.UploaderAPI
	ServerSideEncryption string
	SSEKMSKeyId          string
	StorageClass         string
	Success              bool
	bucket               string
	server               string
	compressor           Compressor
	waitGroup            *sync.WaitGroup
}

// NewTarUploader creates a new tar uploader without the actual
// S3 uploader. CreateUploader() is used to configure byte size and
// concurrency streams for the uploader.
func NewTarUploader(bucket, server, compressionMethod string) *TarUploader {
	return &TarUploader{
		StorageClass: "STANDARD",
		bucket:       bucket,
		server:       server,
		compressor:   Compressors[compressionMethod],
		waitGroup:    &sync.WaitGroup{},
	}
}

// Finish waits for all waiting parts to be uploaded. If an error occurs,
// prints alert to stderr.
func (tarUploader *TarUploader) Finish() {
	tarUploader.waitGroup.Wait()
	if !tarUploader.Success {
		log.Printf("WAL-G could not complete upload.\n")
	}
}

// Clone creates similar TarUploader with new WaitGroup
func (tarUploader *TarUploader) Clone() *TarUploader {
	return &TarUploader{
		tarUploader.UploaderApi,
		tarUploader.ServerSideEncryption,
		tarUploader.SSEKMSKeyId,
		tarUploader.StorageClass,
		tarUploader.Success,
		tarUploader.bucket,
		tarUploader.server,
		tarUploader.compressor,
		&sync.WaitGroup{},
	}
}

// UploadWal compresses a WAL file and uploads to S3. Returns
// the first error encountered and an empty string upon failure.
func (tarUploader *TarUploader) UploadWal(path string, pre *S3Prefix, verify bool) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", errors.Wrapf(err, "UploadWal: failed to open file %s\n", path)
	}

	var walFileReader io.Reader
	recordingReader, err := NewWalDeltaRecordingReader(file)
	if err != nil {
		if err == DeltaFileExistenceError {
			// it is normal and it means that next backup should use full scan strategy instead of wal scanning
			walFileReader = file
		} else {
			return "", errors.Wrapf(err, "UploadWal: failed to start delta recording\n")
		}
	} else {
		walFileReader = recordingReader
		defer recordingReader.Close()
	}

	pipeWriter := &CompressingPipeWriter{
		Input:                walFileReader,
		NewCompressingWriter: tarUploader.compressor.NewWriter,
	}

	pipeWriter.Compress(&OpenPGPCrypter{})

	dstPath := sanitizePath(tarUploader.server + WalPath + filepath.Base(path) + "." + tarUploader.compressor.FileExtension())
	reader := pipeWriter.Output

	if verify {
		reader = newMd5Reader(reader)
	}

	input := tarUploader.createUploadInput(dstPath, reader)

	tarUploader.waitGroup.Add(1)
	go func() {
		defer tarUploader.waitGroup.Done()
		err = tarUploader.upload(input, path)
	}()

	tarUploader.Finish()
	fmt.Println("WAL PATH:", dstPath)
	if verify {
		sum := reader.(*MD5Reader).Sum()
		archive := &Archive{
			Prefix:  pre,
			Archive: aws.String(dstPath),
		}
		eTag, err := archive.GetETag()
		if err != nil {
			log.Fatalf("Unable to verify WAL %s", err)
		}
		if eTag == nil {
			log.Fatalf("Unable to verify WAL: nil ETag ")
		}

		trimETag := strings.Trim(*eTag, "\"")
		if sum != trimETag {
			log.Fatalf("WAL verification failed: md5 %s ETag %s", sum, trimETag)
		}
		fmt.Println("ETag ", trimETag)
	}
	return dstPath, err
}

// createUploadInput creates a s3manager.UploadInput for a TarUploader using
// the specified path and reader.
func (tarUploader *TarUploader) createUploadInput(path string, reader io.Reader) *s3manager.UploadInput {
	uploadInput := &s3manager.UploadInput{
		Bucket:       aws.String(tarUploader.bucket),
		Key:          aws.String(path),
		Body:         reader,
		StorageClass: aws.String(tarUploader.StorageClass),
	}

	if tarUploader.ServerSideEncryption != "" {
		uploadInput.ServerSideEncryption = aws.String(tarUploader.ServerSideEncryption)

		if tarUploader.SSEKMSKeyId != "" {
			// Only aws:kms implies sseKmsKeyId, checked during validation
			uploadInput.SSEKMSKeyId = aws.String(tarUploader.SSEKMSKeyId)
		}
	}

	return uploadInput
}

// Helper function to upload to S3. If an error occurs during upload, retries will
// occur in exponentially incremental seconds.
func (tarUploader *TarUploader) upload(input *s3manager.UploadInput, path string) (err error) {
	upl := tarUploader.UploaderApi

	_, e := upl.Upload(input)
	if e == nil {
		tarUploader.Success = true
		return nil
	}

	if multierr, ok := e.(s3manager.MultiUploadFailure); ok {
		log.Printf("upload: failed to upload '%s' with UploadID '%s'.", path, multierr.UploadID())
	} else {
		log.Printf("upload: failed to upload '%s': %s.", path, e.Error())
	}
	return e
}

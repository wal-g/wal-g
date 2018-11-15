package walg

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"io"
)

type NoSentinelUploadError struct {
	error
}

func NewNoSentinelUploadError() NoSentinelUploadError {
	return NoSentinelUploadError{errors.New("Sentinel was not uploaded due to timeline change during backup")}
}

func (err NoSentinelUploadError) Error() string {
	return fmt.Sprintf("%+v", err.error)
}

// S3TarBall represents a tar file that is
// going to be uploaded to S3.
type S3TarBall struct {
	backupName  string
	partNumber  int
	size        int64
	writeCloser io.Closer
	tarWriter   *tar.Writer
	uploader    *Uploader
}

// SetUp creates a new tar writer and starts upload to S3.
// Upload will block until the tar file is finished writing.
// If a name for the file is not given, default name is of
// the form `part_....tar.[Compressor file extension]`.
func (tarBall *S3TarBall) SetUp(crypter Crypter, names ...string) {
	if tarBall.tarWriter == nil {
		var name string
		if len(names) > 0 {
			name = names[0]
		} else {
			name = fmt.Sprintf("part_%0.3d.tar.%v", tarBall.partNumber, tarBall.uploader.compressor.FileExtension())
		}
		writeCloser := tarBall.startUpload(name, crypter)

		tarBall.writeCloser = writeCloser
		tarBall.tarWriter = tar.NewWriter(writeCloser)
	}
}

// CloseTar closes the tar writer, flushing any unwritten data
// to the underlying writer before also closing the underlying writer.
func (tarBall *S3TarBall) CloseTar() error {
	err := tarBall.tarWriter.Close()
	if err != nil {
		return errors.Wrap(err, "CloseTar: failed to close tar writer")
	}

	err = tarBall.writeCloser.Close()
	if err != nil {
		return errors.Wrap(err, "CloseTar: failed to close underlying writer")
	}
	infoLogger.Printf("Finished writing part %d.\n", tarBall.partNumber)
	return nil
}

func (tarBall *S3TarBall) AwaitUploads() {
	tarBall.uploader.waitGroup.Wait()
	if !tarBall.uploader.Success {
		errorLogger.Fatal("Unable to complete uploads")
	}
}

// TODO : unit tests
// startUpload creates a compressing writer and runs upload in the background once
// a compressed tar member is finished writing.
func (tarBall *S3TarBall) startUpload(name string, crypter Crypter) io.WriteCloser {
	pipeReader, pipeWriter := io.Pipe()
	uploader := tarBall.uploader

	path := GetBackupPath(uploader.uploadingFolder) + tarBall.backupName + "/tar_partitions/" + name
	input := uploader.CreateUploadInput(path, NewNetworkLimitReader(pipeReader))

	infoLogger.Printf("Starting part %d ...\n", tarBall.partNumber)

	uploader.waitGroup.Add(1)
	go func() {
		defer uploader.waitGroup.Done()

		err := uploader.upload(input, path)
		if compressingError, ok := err.(CompressingPipeWriterError); ok {
			errorLogger.Printf("could not upload '%s' due to compression error\n%+v\n", path, compressingError)
		}
		if err != nil {
			errorLogger.Printf("upload: could not upload '%s'\n", path)
			errorLogger.Printf("%v\n", err)
		}
	}()

	if crypter.IsUsed() {
		encryptedWriter, err := crypter.Encrypt(pipeWriter)

		if err != nil {
			errorLogger.Fatal("upload: encryption error ", err)
		}

		return &CascadeWriteCloser{uploader.compressor.NewWriter(encryptedWriter), &CascadeWriteCloser{encryptedWriter, pipeWriter}}
	}

	return &CascadeWriteCloser{uploader.compressor.NewWriter(pipeWriter), pipeWriter}
}

// Size accumulated in this tarball
func (tarBall *S3TarBall) Size() int64 { return tarBall.size }

// AddSize to total Size
func (tarBall *S3TarBall) AddSize(i int64) { tarBall.size += i }

func (tarBall *S3TarBall) TarWriter() *tar.Writer { return tarBall.tarWriter }

// Finish writes a .json file description and uploads it with the
// the backup name. Finish will wait until all tar file parts
// have been uploaded. The json file will only be uploaded
// if all other parts of the backup are present in S3.
// an alert is given with the corresponding error.
func (tarBall *S3TarBall) Finish(sentinelDto *S3TarBallSentinelDto) error {
	name := tarBall.backupName + SentinelSuffix
	uploader := tarBall.uploader

	uploader.finish()

	var err error
	//If other parts are successful in uploading, upload json file.
	if uploader.Success && sentinelDto != nil {
		sentinelDto.UserData = GetSentinelUserData()
		dtoBody, err := json.Marshal(*sentinelDto)
		if err != nil {
			return err
		}
		path := GetBackupPath(uploader.uploadingFolder) + name
		input := uploader.CreateUploadInput(path, bytes.NewReader(dtoBody))

		uploadingErr := uploader.upload(input, path)
		if uploadingErr != nil {
			errorLogger.Printf("upload: could not upload '%s'\n", path)
			errorLogger.Fatalf("S3TarBall finish: json failed to upload")
		}
	} else {
		infoLogger.Printf("Uploaded %d compressed tar Files.\n", tarBall.partNumber)
		errorLogger.Printf("Sentinel was not uploaded %v", name)
		return NewNoSentinelUploadError()
	}

	if err == nil && uploader.Success {
		infoLogger.Printf("Uploaded %d compressed tar Files.\n", tarBall.partNumber)
	}
	return err
}

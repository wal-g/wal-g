package internal

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/tracelog"
	"io"
)

type NoSentinelUploadError struct {
	error
}

func NewNoSentinelUploadError() NoSentinelUploadError {
	return NoSentinelUploadError{errors.New("Sentinel was not uploaded due to timeline change during backup")}
}

func (err NoSentinelUploadError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// StorageTarBall represents a tar file that is
// going to be uploaded to storage.
type StorageTarBall struct {
	backupName  string
	partNumber  int
	size        int64
	writeCloser io.Closer
	tarWriter   *tar.Writer
	uploader    *Uploader
}

// SetUp creates a new tar writer and starts upload to storage.
// Upload will block until the tar file is finished writing.
// If a name for the file is not given, default name is of
// the form `part_....tar.[Compressor file extension]`.
func (tarBall *StorageTarBall) SetUp(crypter Crypter, names ...string) {
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
func (tarBall *StorageTarBall) CloseTar() error {
	err := tarBall.tarWriter.Close()
	if err != nil {
		return errors.Wrap(err, "CloseTar: failed to close tar writer")
	}

	err = tarBall.writeCloser.Close()
	if err != nil {
		return errors.Wrap(err, "CloseTar: failed to close underlying writer")
	}
	tracelog.InfoLogger.Printf("Finished writing part %d.\n", tarBall.partNumber)
	return nil
}

func (tarBall *StorageTarBall) AwaitUploads() {
	tarBall.uploader.waitGroup.Wait()
	if !tarBall.uploader.Success {
		tracelog.ErrorLogger.Fatal("Unable to complete uploads")
	}
}

// TODO : unit tests
// startUpload creates a compressing writer and runs upload in the background once
// a compressed tar member is finished writing.
func (tarBall *StorageTarBall) startUpload(name string, crypter Crypter) io.WriteCloser {
	pipeReader, pipeWriter := io.Pipe()
	uploader := tarBall.uploader

	path := tarBall.backupName + TarPartitionFolderName + name

	tracelog.InfoLogger.Printf("Starting part %d ...\n", tarBall.partNumber)

	uploader.waitGroup.Add(1)
	go func() {
		defer uploader.waitGroup.Done()

		err := uploader.upload(path, NewNetworkLimitReader(pipeReader))
		if compressingError, ok := err.(CompressingPipeWriterError); ok {
			tracelog.ErrorLogger.Printf("could not upload '%s' due to compression error\n%+v\n", path, compressingError)
		}
		if err != nil {
			tracelog.ErrorLogger.Printf("upload: could not upload '%s'\n", path)
			tracelog.ErrorLogger.Printf("%v\n", err)
		}
	}()

	if crypter.IsUsed() {
		encryptedWriter, err := crypter.Encrypt(pipeWriter)

		if err != nil {
			tracelog.ErrorLogger.Fatal("upload: encryption error ", err)
		}

		return &CascadeWriteCloser{uploader.compressor.NewWriter(encryptedWriter), &CascadeWriteCloser{encryptedWriter, pipeWriter}}
	}

	return &CascadeWriteCloser{uploader.compressor.NewWriter(pipeWriter), pipeWriter}
}

// Size accumulated in this tarball
func (tarBall *StorageTarBall) Size() int64 { return tarBall.size }

// AddSize to total Size
func (tarBall *StorageTarBall) AddSize(i int64) { tarBall.size += i }

func (tarBall *StorageTarBall) TarWriter() *tar.Writer { return tarBall.tarWriter }

// Finish writes a .json file description and uploads it with the
// the backup name. Finish will wait until all tar file parts
// have been uploaded. The json file will only be uploaded
// if all other parts of the backup are present in storage.
// an alert is given with the corresponding error.
func (tarBall *StorageTarBall) Finish(sentinelDto *BackupSentinelDto) error {
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

		uploadingErr := uploader.upload(name, bytes.NewReader(dtoBody))
		if uploadingErr != nil {
			tracelog.ErrorLogger.Printf("upload: could not upload '%s'\n", name)
			tracelog.ErrorLogger.Fatalf("StorageTarBall finish: json failed to upload")
		}
	} else {
		tracelog.InfoLogger.Printf("Uploaded %d compressed tar Files.\n", tarBall.partNumber)
		tracelog.ErrorLogger.Printf("Sentinel was not uploaded %v", name)
		return NewNoSentinelUploadError()
	}

	if err == nil && uploader.Success {
		tracelog.InfoLogger.Printf("Uploaded %d compressed tar Files.\n", tarBall.partNumber)
	}
	return err
}

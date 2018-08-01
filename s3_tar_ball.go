package walg

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"log"
)

// S3TarBall represents a tar file that is
// going to be uploaded to S3.
type S3TarBall struct {
	archiveDirectory string
	backupName       string
	partCount        int
	size             int64
	writeCloser      io.WriteCloser
	tarWriter        *tar.Writer
	uploader         *Uploader
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
			name = fmt.Sprintf("part_%0.3d.tar.%v", tarBall.partCount, tarBall.FileExtension())
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
	fmt.Printf("Finished writing part %d.\n", tarBall.partCount)
	return nil
}
func (tarBall *S3TarBall) AwaitUploads() {
	tarBall.uploader.waitGroup.Wait()
}

// startUpload creates a compressing writer and runs upload in the background once
// a compressed tar member is finished writing.
func (tarBall *S3TarBall) startUpload(name string, crypter Crypter) io.WriteCloser {
	pipeReader, pipeWriter := io.Pipe()
	uploader := tarBall.uploader

	path := *uploader.UploadingFolder.Server + BaseBackupsPath + tarBall.backupName + "/tar_partitions/" + name
	input := uploader.CreateUploadInput(path, pipeReader)

	fmt.Printf("Starting part %d ...\n", tarBall.partCount)

	uploader.waitGroup.Add(1)
	go func() {
		defer uploader.waitGroup.Done()

		err := uploader.upload(input, path)
		if compressingError, ok := err.(CompressingPipeWriterError); ok {
			log.Printf("FATAL: could not upload '%s' due to compression error\n%+v\n", path, compressingError)
		}
		if err != nil {
			log.Printf("upload: could not upload '%s'\n", path)
			log.Printf("FATAL%v\n", err)
		}

	}()

	if crypter.IsUsed() {
		encryptedWriter, err := crypter.Encrypt(pipeWriter)

		if err != nil {
			log.Fatal("upload: encryption error ", err)
		}

		return &CascadeWriteCloser{uploader.compressor.NewWriter(encryptedWriter), &CascadeWriteCloser{encryptedWriter, pipeWriter}}
	}

	return &CascadeWriteCloser{uploader.compressor.NewWriter(pipeWriter), pipeWriter}
}

func (tarBall *S3TarBall) GetFileRelPath(fileAbsPath string) string {
	return GetFileRelativePath(fileAbsPath, tarBall.archiveDirectory)
}

// Size accumulated in this tarball
func (tarBall *S3TarBall) Size() int64 { return tarBall.size }

// AddSize to total Size
func (tarBall *S3TarBall) AddSize(i int64) { tarBall.size += i }

func (tarBall *S3TarBall) TarWriter() *tar.Writer { return tarBall.tarWriter }

func (tarBall *S3TarBall) FileExtension() string {
	return tarBall.uploader.compressor.FileExtension()
}

// Finish writes a .json file description and uploads it with the
// the backup name. Finish will wait until all tar file parts
// have been uploaded. The json file will only be uploaded
// if all other parts of the backup are present in S3.
// an alert is given with the corresponding error.
func (tarBall *S3TarBall) Finish(sentinelDto *S3TarBallSentinelDto) error {
	name := tarBall.backupName + "_backup_stop_sentinel.json"
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
		path := *uploader.UploadingFolder.Server + BaseBackupsPath + name
		input := uploader.CreateUploadInput(path, bytes.NewReader(dtoBody))

		uploadingErr := uploader.upload(input, path)
		if uploadingErr != nil {
			log.Printf("upload: could not upload '%s'\n", path)
			log.Fatalf("S3TarBall finish: json failed to upload")
		}
	} else {
		log.Printf("Uploaded %d compressed tar Files.\n", tarBall.partCount)
		log.Printf("Sentinel was not uploaded %v", name)
		return errors.New("Sentinel was not uploaded due to timeline change during backup")
	}

	if err == nil && uploader.Success {
		fmt.Printf("Uploaded %d compressed tar Files.\n", tarBall.partCount)
	}
	return err
}

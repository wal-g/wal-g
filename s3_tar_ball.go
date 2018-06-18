package walg

import (
	"io"
	"archive/tar"
	"fmt"
	"github.com/pkg/errors"
	"log"
	"github.com/pierrec/lz4"
	"encoding/json"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/aws"
	"bytes"
)

// S3TarBall represents a tar file that is
// going to be uploaded to S3.
type S3TarBall struct {
	baseDir          string
	trim             string
	bkupName         string
	partCount        int
	size             int64
	writeCloser      io.WriteCloser
	tarWriter        *tar.Writer
	tarUploader      *TarUploader
	Lsn              *uint64
	IncrementFromLsn *uint64
	IncrementFrom    string
	Files            BackupFileList
}

// SetUp creates a new tar writer and starts upload to S3.
// Upload will block until the tar file is finished writing.
// If a name for the file is not given, default name is of
// the form `part_....tar.lz4`.
func (tarBall *S3TarBall) SetUp(crypter Crypter, names ...string) {
	if tarBall.tarWriter == nil {
		var name string
		if len(names) > 0 {
			name = names[0]
		} else {
			name = "part_" + fmt.Sprintf("%0.3d", tarBall.partCount) + ".tar.lz4"
		}
		writeCloser := tarBall.StartUpload(name, crypter)

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
	tarBall.tarUploader.waitGroup.Wait()
}

// StartUpload creates a lz4 writer and runs upload in the background once
// a compressed tar member is finished writing.
func (tarBall *S3TarBall) StartUpload(name string, crypter Crypter) io.WriteCloser {
	pr, pw := io.Pipe()
	tupl := tarBall.tarUploader

	path := tupl.server + BaseBackupsPath + tarBall.bkupName + "/tar_partitions/" + name
	input := tupl.createUploadInput(path, pr)

	fmt.Printf("Starting part %d ...\n", tarBall.partCount)

	tupl.waitGroup.Add(1)
	go func() {
		defer tupl.waitGroup.Done()

		err := tupl.upload(input, path)
		if re, ok := err.(Lz4Error); ok {

			log.Printf("FATAL: could not upload '%s' due to compression error\n%+v\n", path, re)
		}
		if err != nil {
			log.Printf("upload: could not upload '%s'\n", path)
			log.Printf("FATAL%v\n", err)
		}

	}()

	if crypter.IsUsed() {
		wc, err := crypter.Encrypt(pw)

		if err != nil {
			log.Fatal("upload: encryption error ", err)
		}

		return &Lz4CascadeClose2{lz4.NewWriter(wc), wc, pw}
	}

	return &Lz4CascadeClose{lz4.NewWriter(pw), pw}
}

// BaseDir of a backup
func (tarBall *S3TarBall) BaseDir() string { return tarBall.baseDir }

// Trim suffix
func (tarBall *S3TarBall) Trim() string { return tarBall.trim }

// Nop is a dummy fonction for test purposes
func (tarBall *S3TarBall) PartCount() int { return tarBall.partCount }

// Size accumulated in this tarball
func (tarBall *S3TarBall) Size() int64     { return tarBall.size }
// AddSize to total Size
func (tarBall *S3TarBall) AddSize(i int64)        { tarBall.size += i }
func (tarBall *S3TarBall) TarWriter() *tar.Writer { return tarBall.tarWriter }

// Finish writes a .json file description and uploads it with the
// the backup name. Finish will wait until all tar file parts
// have been uploaded. The json file will only be uploaded
// if all other parts of the backup are present in S3.
// an alert is given with the corresponding error.
func (tarBall *S3TarBall) Finish(sentinelDto *S3TarBallSentinelDto) error {
	var err error
	name := tarBall.bkupName + "_backup_stop_sentinel.json"
	tupl := tarBall.tarUploader

	tupl.Finish()

	//If other parts are successful in uploading, upload json file.
	if tupl.Success && sentinelDto != nil {
		sentinelDto.UserData = GetSentinelUserData()
		dtoBody, err := json.Marshal(*sentinelDto)
		if err != nil {
			return err
		}
		path := tupl.server + BaseBackupsPath + name
		input := &s3manager.UploadInput{
			Bucket:       aws.String(tupl.bucket),
			Key:          aws.String(path),
			Body:         bytes.NewReader(dtoBody),
			StorageClass: aws.String(tupl.StorageClass),
		}

		if tupl.ServerSideEncryption != "" {
			input.ServerSideEncryption = aws.String(tupl.ServerSideEncryption)

			if tupl.SSEKMSKeyId != "" {
				// Only aws:kms implies sseKmsKeyId, checked during validation
				input.SSEKMSKeyId = aws.String(tupl.SSEKMSKeyId)
			}
		}

		tupl.waitGroup.Add(1)
		go func() {
			defer tupl.waitGroup.Done()

			e := tupl.upload(input, path)
			if e != nil {
				log.Printf("upload: could not upload '%s'\n", path)
				log.Fatalf("S3TarBall Finish: json failed to upload")
			}
		}()

		tupl.Finish()
	} else {
		log.Printf("Uploaded %d compressed tar Files.\n", tarBall.partCount)
		log.Printf("Sentinel was not uploaded %v", name)
		return errors.New("Sentinel was not uploaded due to timeline change during backup")
	}

	if err == nil && tupl.Success {
		fmt.Printf("Uploaded %d compressed tar Files.\n", tarBall.partCount)
	}
	return err
}
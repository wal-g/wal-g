package walg

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
	"io/ioutil"
	"log"
	"fmt"
)

type NoBackupsFoundError struct {
	error
}

func NewNoBackupsFoundError() NoBackupsFoundError {
	return NoBackupsFoundError{errors.New("No backups found")}
}

func (err NoBackupsFoundError) Error() string {
	return fmt.Sprintf("%+v", err.error)
}

// Backup contains information about a valid backup
// generated and uploaded by WAL-G.
type Backup struct {
	Folder *S3Folder
	Name   string
}

func NewBackup(folder *S3Folder, name string) *Backup {
	return &Backup{folder, name}
}

func (backup *Backup) getPath() string {
	return GetBackupPath(backup.Folder) + backup.Name
}

func (backup *Backup) getStopSentinelPath() string {
	return GetBackupPath(backup.Folder) + backup.Name + SentinelSuffix
}

// CheckExistence checks that the specified backup exists.
func (backup *Backup) CheckExistence() (bool, error) {
	stopSentinelObjectInput := &s3.HeadObjectInput{
		Bucket: backup.Folder.Bucket,
		Key:    aws.String(backup.getStopSentinelPath()),
	}

	_, err := backup.Folder.S3API.HeadObject(stopSentinelObjectInput)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case NotFoundAWSErrorCode:
				return false, nil
			default:
				return false, awsErr
			}

		}
	}
	return true, nil
}

// GetKeys returns all the keys for the Files in the specified backup.
func (backup *Backup) GetKeys() ([]string, error) {
	objects := &s3.ListObjectsV2Input{
		Bucket: backup.Folder.Bucket,
		Prefix: aws.String(sanitizePath(backup.getPath() + "/tar_partitions")),
	}

	result := make([]string, 0)

	err := backup.Folder.S3API.ListObjectsV2Pages(objects, func(files *s3.ListObjectsV2Output, lastPage bool) bool {

		arr := make([]string, len(files.Contents))

		for i, ob := range files.Contents {
			key := *ob.Key
			arr[i] = key
		}

		result = append(result, arr...)
		return true
	})
	if err != nil {
		return nil, errors.Wrap(err, "GetKeys: s3.ListObjects failed")
	}

	return result, nil
}

func (backup *Backup) fetchSentinel() (sentinelDto S3TarBallSentinelDto) {
	backupReaderMaker := NewS3ReaderMaker(backup.Folder, backup.getStopSentinelPath())
	backupReader, err := backupReaderMaker.Reader()
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	sentinelDtoData, err := ioutil.ReadAll(backupReader)
	if err != nil {
		log.Fatalf("%+v\n", err)
	}

	err = json.Unmarshal(sentinelDtoData, &sentinelDto)
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	return
}

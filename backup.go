package walg

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/tracelog"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
)

type NoBackupsFoundError struct {
	error
}

func NewNoBackupsFoundError() NoBackupsFoundError {
	return NoBackupsFoundError{errors.New("No backups found")}
}

func (err NoBackupsFoundError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
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

func (backup *Backup) fetchSentinel() (S3TarBallSentinelDto, error) {
	sentinelDto := S3TarBallSentinelDto{}
	backupReaderMaker := NewS3ReaderMaker(backup.Folder, backup.getStopSentinelPath())
	backupReader, err := backupReaderMaker.Reader()
	if err != nil {
		return sentinelDto, err
	}
	sentinelDtoData, err := ioutil.ReadAll(backupReader)
	if err != nil {
		return sentinelDto, errors.Wrap(err, "failed to fetch sentinel")
	}

	err = json.Unmarshal(sentinelDtoData, &sentinelDto)
	return sentinelDto, errors.Wrap(err, "failed to unmarshal sentinel")
}

func checkDbDirectoryForUnwrap(dbDataDirectory string, sentinelDto S3TarBallSentinelDto) error {
	if !sentinelDto.isIncremental() {
		isEmpty, err := IsDirectoryEmpty(dbDataDirectory)
		if err != nil {
			return err
		}
		if !isEmpty {
			return NewNonEmptyDbDataDirectoryError(dbDataDirectory)
		}
	} else {
		tracelog.DebugLogger.Println("DB data directory before increment:")
		filepath.Walk(dbDataDirectory,
			func(path string, info os.FileInfo, err error) error {
				if !info.IsDir() {
					tracelog.DebugLogger.Println(path)
				}
				return nil
			})

		for fileName, fileDescription := range sentinelDto.Files {
			if fileDescription.IsSkipped {
				tracelog.DebugLogger.Printf("Skipped file %v\n", fileName)
			}
		}
	}
	return nil
}

// TODO : unit tests
// Do the job of unpacking Backup object
func (backup *Backup) unwrap(dbDataDirectory string, sentinelDto S3TarBallSentinelDto, filesToUnwrap map[string]bool) error {
	err := checkDbDirectoryForUnwrap(dbDataDirectory, sentinelDto)
	if err != nil {
		return err
	}

	tarInterpreter := NewFileTarInterpreter(dbDataDirectory, sentinelDto, filesToUnwrap)
	tarsToExtract, pgControlKey, err := backup.getTarsToExtract()
	if err != nil {
		return err
	}
	err = ExtractAll(tarInterpreter, tarsToExtract)
	if err != nil {
		return err
	}
	// Check name for backwards compatibility. Will check for `pg_control` if WALG version of backup.
	re := regexp.MustCompile(`^([^_]+._{1}[^_]+._{1})`)
	match := re.FindString(backup.Name)
	if match == "" || sentinelDto.isIncremental() {
		err = ExtractAll(tarInterpreter, []ReaderMaker {NewS3ReaderMaker(backup.Folder, pgControlKey)})
		if err != nil {
			return errors.Wrap(err, "failed to extract pg_control")
		}
	}

	tracelog.InfoLogger.Print("\nBackup extraction complete.\n")
	return nil
}

// TODO : unit tests
func IsDirectoryEmpty(directoryPath string) (bool, error) {
	var isEmpty = true
	searchLambda := func(path string, info os.FileInfo, err error) error {
		if path != directoryPath {
			isEmpty = false
			tracelog.InfoLogger.Printf("found file '%s' in directory: '%s'\n", path, directoryPath)
		}
		return nil
	}
	err := filepath.Walk(directoryPath, searchLambda)
	return isEmpty, errors.Wrapf(err, "can't check, that directory: '%s' is empty", directoryPath)
}

// TODO : init tests
func (backup *Backup) getTarsToExtract() (tarsToExtract []ReaderMaker, pgControlKey string, err error) {
	keys, err := backup.GetKeys()
	if err != nil {
		return nil, "", err
	}
	tarsToExtract = make([]ReaderMaker, 0, len(keys))

	pgControlRe := regexp.MustCompile(`^.*?/tar_partitions/pg_control\.tar(\..+$|$)`)
	for _, key := range keys {
		// Separate the pg_control key from the others to
		// extract it at the end, as to prevent server startup
		// with incomplete backup restoration.  But only if it
		// exists: it won't in the case of WAL-E backup
		// backwards compatibility.
		if pgControlRe.MatchString(key) {
			if pgControlKey != "" {
				panic("expect only one pg_control key match")
			}
			pgControlKey = key
			continue
		}
		tarToExtract := NewS3ReaderMaker(backup.Folder, key)
		tarsToExtract = append(tarsToExtract, tarToExtract)
	}
	if pgControlKey == "" {
		return nil, "", NewPgControlNotFoundError()
	}
	return
}

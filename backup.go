package walg

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
	"io/ioutil"
	"log"
	"sort"
	"strings"
)

// ErrLatestNotFound happens when users asks backup-fetch LATEST, but there is no backups
var ErrLatestNotFound = errors.New("No backups found")

// Backup contains information about a valid backup
// generated and uploaded by WAL-G.
type Backup struct {
	Folder *S3Folder
	Path   *string
	Name   *string
	Js     *string
}

// GetLatest sorts the backups by last modified time
// and returns the latest backup key.
func (backup *Backup) GetLatest() (string, error) {
	sortTimes, err := backup.GetBackups()

	if err != nil {
		return "", err
	}

	return sortTimes[0].Name, nil
}

// GetBackups receives backup descriptions and sorts them by time
func (backup *Backup) GetBackups() ([]BackupTime, error) {
	var sortTimes []BackupTime
	objects := &s3.ListObjectsV2Input{
		Bucket:    backup.Folder.Bucket,
		Prefix:    backup.Path,
		Delimiter: aws.String("/"),
	}

	var backups = make([]*s3.Object, 0)

	err := backup.Folder.S3API.ListObjectsV2Pages(objects, func(files *s3.ListObjectsV2Output, lastPage bool) bool {
		backups = append(backups, files.Contents...)
		return true
	})

	if err != nil {
		return nil, errors.Wrap(err, "GetLatest: s3.ListObjectsV2 failed")
	}

	count := len(backups)

	if count == 0 {
		return nil, ErrLatestNotFound
	}

	sortTimes = GetBackupTimeSlices(backups)

	return sortTimes, nil
}

// CheckExistence checks that the specified backup exists.
func (backup *Backup) CheckExistence() (bool, error) {
	js := &s3.HeadObjectInput{
		Bucket: backup.Folder.Bucket,
		Key:    backup.Js,
	}

	_, err := backup.Folder.S3API.HeadObject(js)
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
		Prefix: aws.String(sanitizePath(*backup.Path + *backup.Name + "/tar_partitions")),
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
		return nil, errors.Wrap(err, "GetKeys: s3.ListObjectsV2 failed")
	}

	return result, nil
}

// GetWals returns all WAL file keys less then key provided
func (backup *Backup) GetWals(before string) ([]*s3.ObjectIdentifier, error) {
	objects := &s3.ListObjectsV2Input{
		Bucket: backup.Folder.Bucket,
		Prefix: aws.String(sanitizePath(*backup.Path)),
	}

	arr := make([]*s3.ObjectIdentifier, 0)

	err := backup.Folder.S3API.ListObjectsV2Pages(objects, func(files *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, ob := range files.Contents {
			key := *ob.Key
			if stripWalName(key) < before {
				arr = append(arr, &s3.ObjectIdentifier{Key: aws.String(key)})
			}
		}
		return true
	})

	if err != nil {
		return nil, errors.Wrap(err, "GetKeys: s3.ListObjectsV2 failed")
	}

	return arr, nil
}

// GetBackupTimeSlices converts S3 objects to backup description
func GetBackupTimeSlices(backups []*s3.Object) []BackupTime {
	sortTimes := make([]BackupTime, len(backups))
	for i, ob := range backups {
		key := *ob.Key
		time := *ob.LastModified
		sortTimes[i] = BackupTime{stripNameBackup(key), time, stripWalFileName(key)}
	}
	slice := TimeSlice(sortTimes)
	sort.Sort(slice)
	return slice
}

// Strips the backup key and returns it in its base form `base_...`.
func stripNameBackup(key string) string {
	all := strings.SplitAfter(key, "/")
	name := strings.Split(all[len(all)-1], "_backup")[0]
	return name
}

// Strips the backup WAL file name.
func stripWalFileName(key string) string {
	name := stripNameBackup(key)
	name = strings.SplitN(name, "_D_", 2)[0]

	if strings.HasPrefix(name, backupNamePrefix) {
		return name[len(backupNamePrefix):]
	}
	return ""
}

func stripWalName(path string) string {
	all := strings.SplitAfter(path, "/")
	name := strings.Split(all[len(all)-1], ".")[0]
	return name
}

func fetchSentinel(backupName string, bk *Backup, folder *S3Folder) (dto S3TarBallSentinelDto) {
	latestSentinel := backupName + SentinelSuffix
	previousBackupReader := S3ReaderMaker{
		Backup:     bk,
		Key:        aws.String(*folder.Server + BaseBackupsPath + latestSentinel),
		FileFormat: GetFileExtension(latestSentinel),
	}
	prevBackup, err := previousBackupReader.Reader()
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	sentinelDto, err := ioutil.ReadAll(prevBackup)
	if err != nil {
		log.Fatalf("%+v\n", err)
	}

	err = json.Unmarshal(sentinelDto, &dto)
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	return
}

// GetBackupPath gets path for basebackup in a bucket
func GetBackupPath(folder *S3Folder) *string {
	path := *folder.Server + BaseBackupsPath
	server := sanitizePath(path)
	return aws.String(server)
}

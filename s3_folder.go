package walg

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/pkg/errors"
	"sort"
	"strings"
)

type S3Folder struct {
	S3API  s3iface.S3API
	Bucket *string
	Server string
}

func NewS3Folder(s3API s3iface.S3API, bucket, server string) *S3Folder {
	return &S3Folder{
		s3API,
		aws.String(bucket),
		server,
	}
}

// GetBackupPath gets path for basebackup in a bucket
func GetBackupPath(folder *S3Folder) string {
	return sanitizePath(folder.Server + BaseBackupsPath)
}

func GetLatestBackupKey(folder *S3Folder) (string, error) {
	sortTimes, err := getBackups(folder)

	if err != nil {
		return "", err
	}

	return sortTimes[0].Name, nil
}

// getBackups receives backup descriptions and sorts them by time
func getBackups(folder *S3Folder) ([]BackupTime, error) {
	var sortTimes []BackupTime
	objects := &s3.ListObjectsV2Input{
		Bucket:    folder.Bucket,
		Prefix:    aws.String(GetBackupPath(folder)),
		Delimiter: aws.String("/"),
	}

	var backups = make([]*s3.Object, 0)

	err := folder.S3API.ListObjectsV2Pages(objects, func(files *s3.ListObjectsV2Output, lastPage bool) bool {
		backups = append(backups, files.Contents...)
		return true
	})

	if err != nil {
		return nil, errors.Wrap(err, "GetLatestBackupKey: s3.ListObjects failed")
	}

	count := len(backups)

	if count == 0 {
		return nil, NoBackupsFoundError
	}

	sortTimes = GetBackupTimeSlices(backups)

	return sortTimes, nil
}

// getWals returns all WAL file keys less then key provided
func getWals(before string, folder *S3Folder) ([]*s3.ObjectIdentifier, error) {
	objects := &s3.ListObjectsV2Input{
		Bucket: folder.Bucket,
		Prefix: aws.String(sanitizePath(folder.Server + WalPath)),
	}

	arr := make([]*s3.ObjectIdentifier, 0)

	err := folder.S3API.ListObjectsV2Pages(objects, func(files *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, ob := range files.Contents {
			key := *ob.Key
			if stripWalName(key) < before {
				arr = append(arr, &s3.ObjectIdentifier{Key: aws.String(key)})
			}
		}
		return true
	})

	if err != nil {
		return nil, errors.Wrap(err, "GetKeys: s3.ListObjects failed")
	}

	return arr, nil
}

// GetBackupTimeSlices converts S3 objects to backup description
func GetBackupTimeSlices(backups []*s3.Object) []BackupTime {
	sortTimes := make([]BackupTime, len(backups))
	for i, ob := range backups {
		key := *ob.Key
		time := *ob.LastModified
		sortTimes[i] = BackupTime{stripBackupName(key), time, stripWalFileName(key)}
	}
	slice := TimeSlice(sortTimes)
	sort.Sort(slice)
	return slice
}

// Strips the backup key and returns it in its base form `base_...`.
func stripBackupName(key string) string {
	all := strings.SplitAfter(key, "/")
	name := strings.Split(all[len(all)-1], "_backup")[0]
	return name
}

// Strips the backup WAL file name.
func stripWalFileName(key string) string {
	name := stripBackupName(key)
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

package walg

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"sort"
	"strings"
)

type WalFiles interface {
	CheckExistence() bool
}

type S3ReaderMaker struct {
	Backup     *Backup
	Key        *string
	FileFormat string
}

func (s *S3ReaderMaker) Format() string { return s.FileFormat }

func (s *S3ReaderMaker) Reader() io.ReadCloser {
	input := &s3.GetObjectInput{
		Bucket: s.Backup.Prefix.Bucket,
		Key:    s.Key,
	}

	rdr, err := s.Backup.Prefix.Svc.GetObject(input)
	if err != nil {
		panic(err)
	}

	return rdr.Body
}

type Prefix struct {
	Creds  *credentials.Credentials
	Svc    *s3.S3
	Bucket *string
	Server *string
}

type Backup struct {
	Prefix *Prefix
	Path   *string
	Name   *string
	Js     *string
}

type Archive struct {
	Prefix  *Prefix
	Archive *string
}

func GetLatest(b *Backup) string {
	objects := &s3.ListObjectsInput{
		Bucket:    b.Prefix.Bucket,
		Prefix:    b.Path,
		Delimiter: aws.String("/"),
	}

	backups, err := b.Prefix.Svc.ListObjects(objects)
	if err != nil {
		panic(err)
	}

	sortTimes := make([]BackupTime, len(backups.Contents))

	for i, ob := range backups.Contents {
		key := *ob.Key
		time := *ob.LastModified
		sortTimes[i] = BackupTime{stripNameBackup(key), time}
	}

	sort.Sort(TimeSlice(sortTimes))

	return sortTimes[0].Name
}

func (b *Backup) CheckExistence() bool {
	js := &s3.HeadObjectInput{
		Bucket: b.Prefix.Bucket,
		Key:    b.Js,
	}

	_, err := b.Prefix.Svc.HeadObject(js)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case "NotFound":
				return false
			}
		}
	}
	return true
}

func (a *Archive) CheckExistence() bool {
	arch := &s3.HeadObjectInput{
		Bucket: a.Prefix.Bucket,
		Key:    a.Archive,
	}

	_, err := a.Prefix.Svc.HeadObject(arch)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case "NotFound":
				return false
			}
		}
	}
	return true
}

func stripNameBackup(key string) string {
	all := strings.SplitAfter(key, "/")
	name := strings.Split(all[2], "_backup")[0]
	return name
}

func GetKeys(b *Backup) []string {
	objects := &s3.ListObjectsInput{
		Bucket: b.Prefix.Bucket,
		Prefix: aws.String(*b.Path + *b.Name + "/tar_partitions"),
	}

	files, err := b.Prefix.Svc.ListObjects(objects)
	if err != nil {
		panic(err)
	}

	arr := make([]string, len(files.Contents))

	for i, ob := range files.Contents {
		key := *ob.Key
		arr[i] = key
	}

	return arr
}

func GetArchive(a *Archive) io.ReadCloser {
	input := &s3.GetObjectInput{
		Bucket: a.Prefix.Bucket,
		Key:    a.Archive,
	}

	archive, err := a.Prefix.Svc.GetObject(input)
	if err != nil {
		panic(err)
	}

	return archive.Body
}

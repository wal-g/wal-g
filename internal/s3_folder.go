package internal

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/pkg/errors"
	"io"
	"strings"
)

type S3Folder struct {
	uploader S3Uploader
	S3API    s3iface.S3API
	Bucket   *string
	Path     string
}

func NewS3Folder(uploader S3Uploader, s3API s3iface.S3API, bucket, path string) *S3Folder {
	return &S3Folder{
		uploader,
		s3API,
		aws.String(bucket),
		addDelimiterToPath(path),
	}
}

func (folder *S3Folder) Exists(objectRelativePath string) (bool, error) {
	objectPath := folder.Path + objectRelativePath
	stopSentinelObjectInput := &s3.HeadObjectInput{
		Bucket: folder.Bucket,
		Key:    aws.String(objectPath),
	}

	_, err := folder.S3API.HeadObject(stopSentinelObjectInput)
	if err != nil {
		if isAwsNotExist(err) {
			return false, nil
		}
		return false, errors.Wrapf(err, "failed to check s3 object '%s' existance", objectPath)
	}
	return true, nil
}

func (folder *S3Folder) PutObject(name string, content io.Reader) error {
	return folder.uploader.upload(*folder.Bucket, folder.Path+name, content)
}

func (folder *S3Folder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	objectPath := folder.Path + objectRelativePath
	input := &s3.GetObjectInput{
		Bucket: folder.Bucket,
		Key:    aws.String(objectPath),
	}

	object, err := folder.S3API.GetObject(input)
	if err != nil {
		if isAwsNotExist(err) {
			return nil, NewObjectNotFoundError(objectPath)
		}
		return nil, errors.Wrapf(err, "failed to read object: '%s' from S3", objectPath)
	}
	return object.Body, nil
}

func (folder *S3Folder) GetSubFolder(subFolderRelativePath string) StorageFolder {
	return NewS3Folder(folder.uploader, folder.S3API, *folder.Bucket, JoinS3Path(folder.Path, subFolderRelativePath)+"/")
}

func JoinS3Path(elem ...string) string {
	var res []string
	for _, e := range elem {
		if e != "" {
			res = append(res, strings.Trim(e, "/"))
		}
	}
	return strings.Join(res, "/")
}

func (folder *S3Folder) GetPath() string {
	return folder.Path
}

func (folder *S3Folder) ListFolder() (objects []StorageObject, subFolders []StorageFolder, err error) {
	s3Objects := &s3.ListObjectsV2Input{
		Bucket:    folder.Bucket,
		Prefix:    aws.String(folder.Path),
		Delimiter: aws.String("/"),
	}

	err = folder.S3API.ListObjectsV2Pages(s3Objects, func(files *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, prefix := range files.CommonPrefixes {
			subFolders = append(subFolders, NewS3Folder(folder.uploader, folder.S3API, *folder.Bucket, *prefix.Prefix))
		}
		for _, object := range files.Contents {
			objectRelativePath := strings.TrimPrefix(*object.Key, folder.Path)
			objects = append(objects, NewS3StorageObject(object.SetKey(objectRelativePath)))
		}
		return true
	})
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to list s3 folder: '%s'", folder.Path)
	}
	return objects, subFolders, nil
}

func (folder *S3Folder) DeleteObjects(objectRelativePaths []string) error {
	parts := partitionStrings(objectRelativePaths, 1000)
	for _, part := range parts {
		input := &s3.DeleteObjectsInput{Bucket: folder.Bucket, Delete: &s3.Delete{
			Objects: folder.partitionToObjects(part),
		}}
		_, err := folder.S3API.DeleteObjects(input)
		if err != nil {
			return errors.Wrapf(err, "failed to delete s3 object: '%s'", part)
		}
	}
	return nil
}

func (folder *S3Folder) partitionToObjects(keys []string) []*s3.ObjectIdentifier {
	objects := make([]*s3.ObjectIdentifier, len(keys))
	for id, key := range keys {
		objects[id] = &s3.ObjectIdentifier{Key: aws.String(folder.Path + key)}
	}
	return objects
}

func isAwsNotExist(err error) bool {
	if awsErr, ok := err.(awserr.Error); ok {
		if awsErr.Code() == NotFoundAWSErrorCode || awsErr.Code() == NoSuchKeyAWSErrorCode {
			return true
		}
	}
	return false
}

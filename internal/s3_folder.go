package internal

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/pkg/errors"
	"io"
	"strconv"
	"strings"
)

// MaxRetries limit upload and download retries during interaction with S3
var MaxRetries = 15

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
	return NewS3Folder(folder.uploader, folder.S3API, *folder.Bucket, JoinStoragePath(folder.Path, subFolderRelativePath)+"/")
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

// TODO : unit tests
// Given an S3 bucket name, attempt to determine its region
func findS3BucketRegion(bucket string, config *aws.Config) (string, error) {
	input := s3.GetBucketLocationInput{
		Bucket: aws.String(bucket),
	}

	sess, err := session.NewSession(config.WithRegion("us-east-1"))
	if err != nil {
		return "", err
	}

	output, err := s3.New(sess).GetBucketLocation(&input)
	if err != nil {
		return "", err
	}

	if output.LocationConstraint == nil {
		// buckets in "US Standard", a.k.a. us-east-1, are returned as a nil region
		return "us-east-1", nil
	}
	// all other regions are strings
	return *output.LocationConstraint, nil
}

// TODO : unit tests
func getAWSRegion(s3Bucket string, config *aws.Config) (string, error) {
	region := getSettingValue("AWS_REGION")
	if region == "" {
		if config.Endpoint == nil ||
			*config.Endpoint == "" ||
			strings.HasSuffix(*config.Endpoint, ".amazonaws.com") {
			var err error
			region, err = findS3BucketRegion(s3Bucket, config)
			if err != nil {
				return "", errors.Wrapf(err, "AWS_REGION is not set and s3:GetBucketLocation failed")
			}
		} else {
			// For S3 compatible services like Minio, Ceph etc. use `us-east-1` as region
			// ref: https://github.com/minio/cookbook/blob/master/docs/aws-sdk-for-go-with-minio.md
			region = "us-east-1"
		}
	}
	return region, nil
}

// TODO : unit tests
func createS3Session(s3Bucket string) (*session.Session, error) {
	config := defaults.Get().Config

	config.MaxRetries = &MaxRetries
	if _, err := config.Credentials.Get(); err != nil {
		return nil, errors.Wrapf(err, "failed to get AWS credentials; please specify AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
	}

	if endpoint := getSettingValue("AWS_ENDPOINT"); endpoint != "" {
		config.Endpoint = aws.String(endpoint)
	}

	if s3ForcePathStyleStr := getSettingValue("AWS_S3_FORCE_PATH_STYLE"); s3ForcePathStyleStr != "" {
		s3ForcePathStyle, err := strconv.ParseBool(s3ForcePathStyleStr)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse AWS_S3_FORCE_PATH_STYLE")
		}
		config.S3ForcePathStyle = aws.Bool(s3ForcePathStyle)
	}

	region, err := getAWSRegion(s3Bucket, config)
	if err != nil {
		return nil, err
	}
	config = config.WithRegion(region)

	return session.NewSession(config)
}

// TODO : unit tests
func ConfigureS3Folder(prefix string) (*S3Folder, error) {
	bucket, path, err := getPathFromPrefix(prefix)
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure S3 path")
	}
	sess, err := createS3Session(bucket)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create new session")
	}
	client := s3.New(sess)
	uploader, err := configureS3Uploader(client)
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure S3 uploader")
	}
	folder := NewS3Folder(*uploader, client, bucket, path)
	return folder, nil
}

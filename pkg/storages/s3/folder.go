package s3

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const (
	NotFoundAWSErrorCode  = "NotFound"
	NoSuchKeyAWSErrorCode = "NoSuchKey"
)

// TODO: Unit tests
type Folder struct {
	s3API    s3iface.S3API
	uploader *Uploader
	bucket   *string
	path     string
	config   *Config
}

func NewFolder(
	s3API s3iface.S3API,
	uploader *Uploader,
	path string,
	config *Config,
) *Folder {
	// Trim leading slash because there's no difference between absolute and relative paths in S3.
	path = strings.TrimPrefix(path, "/")
	return &Folder{
		uploader: uploader,
		s3API:    s3API,
		bucket:   aws.String(config.Bucket),
		path:     storage.AddDelimiterToPath(path),
		config:   config,
	}
}

func (folder *Folder) Exists(objectRelativePath string) (bool, error) {
	objectPath := folder.path + objectRelativePath
	stopSentinelObjectInput := &s3.HeadObjectInput{
		Bucket: folder.bucket,
		Key:    aws.String(objectPath),
	}

	_, err := folder.s3API.HeadObject(stopSentinelObjectInput)
	if err != nil {
		if isAwsNotExist(err) {
			return false, nil
		}
		return false, errors.Wrapf(err, "failed to check s3 object '%s' existence", objectPath)
	}
	return true, nil
}

func (folder *Folder) PutObject(name string, content io.Reader) error {
	return folder.uploader.upload(context.Background(), *folder.bucket, folder.path+name, content) //TODO
}

func (folder *Folder) PutObjectWithContext(ctx context.Context, name string, content io.Reader) error {
	return folder.uploader.upload(ctx, *folder.bucket, folder.path+name, content) //TODO
}

func (folder *Folder) CopyObject(srcPath string, dstPath string) error {
	if exists, err := folder.Exists(srcPath); !exists {
		if err == nil {
			return storage.NewObjectNotFoundError(srcPath)
		}
		return err
	}
	source := path.Join(*folder.bucket, folder.path, srcPath)
	dst := path.Join(folder.path, dstPath)
	input := &s3.CopyObjectInput{CopySource: &source, Bucket: folder.bucket, Key: &dst}
	_, err := folder.s3API.CopyObject(input)
	return err
}

func (folder *Folder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	objectPath := folder.path + objectRelativePath
	input := &s3.GetObjectInput{
		Bucket: folder.bucket,
		Key:    aws.String(objectPath),
	}

	object, err := folder.s3API.GetObject(input)
	if err != nil {
		if isAwsNotExist(err) {
			return nil, storage.NewObjectNotFoundError(objectPath)
		}
		return nil, errors.Wrapf(err, "failed to read object: '%s' from S3", objectPath)
	}

	reader := object.Body
	if folder.config.RangeBatchEnabled {
		reader = NewRangeReader(object.Body, objectPath, folder.config.RangeMaxRetries, folder)
	}
	return reader, nil
}

func (folder *Folder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	subFolder := NewFolder(
		folder.s3API,
		folder.uploader,
		storage.JoinPath(folder.path, subFolderRelativePath)+"/",
		folder.config,
	)
	return subFolder
}

func (folder *Folder) GetPath() string {
	return folder.path
}

func (folder *Folder) ListFolder() (objects []storage.Object, subFolders []storage.Folder, err error) {
	listFunc := func(commonPrefixes []*s3.CommonPrefix, contents []*s3.Object) {
		for _, prefix := range commonPrefixes {
			subFolder := NewFolder(folder.s3API, folder.uploader, *prefix.Prefix, folder.config)
			subFolders = append(subFolders, subFolder)
		}
		for _, object := range contents {
			// Some storages return root tar_partitions folder as a Key.
			// We do not want to fail restoration due to this fact.
			// Keep in mind that skipping files is very dangerous and any decision here must be weighted.
			if *object.Key == folder.path {
				continue
			}
			objectRelativePath := strings.TrimPrefix(*object.Key, folder.path)
			objects = append(objects, storage.NewLocalObject(objectRelativePath, *object.LastModified, *object.Size))
		}
	}

	prefix := aws.String(folder.path)
	delimiter := aws.String("/")
	err = folder.listObjectsPages(prefix, delimiter, nil, listFunc)

	if err != nil {
		// DigitalOcean Spaces compatibility: DO's API complains about NoSuchKey when trying to list folders
		// which don't yet exist.
		if isAwsNotExist(err) {
			return objects, subFolders, nil
		}

		return nil, nil, errors.Wrapf(err, "failed to list s3 folder: '%s'", folder.path)
	}
	return objects, subFolders, nil
}

func (folder *Folder) listObjectsPages(prefix *string, delimiter *string, maxKeys *int64,
	listFunc func(commonPrefixes []*s3.CommonPrefix, contents []*s3.Object)) (err error) {
	if folder.config.UseListObjectsV1 {
		err = folder.listObjectsPagesV1(prefix, delimiter, maxKeys, listFunc)
	} else {
		err = folder.listObjectsPagesV2(prefix, delimiter, maxKeys, listFunc)
	}
	return
}

func (folder *Folder) listObjectsPagesV1(prefix *string, delimiter *string, maxKeys *int64,
	listFunc func(commonPrefixes []*s3.CommonPrefix, contents []*s3.Object)) error {
	s3Objects := &s3.ListObjectsInput{
		Bucket:    folder.bucket,
		Prefix:    prefix,
		Delimiter: delimiter,
		MaxKeys:   maxKeys,
	}

	err := folder.s3API.ListObjectsPages(s3Objects, func(files *s3.ListObjectsOutput, lastPage bool) bool {
		listFunc(files.CommonPrefixes, files.Contents)
		return true
	})
	return err
}

func (folder *Folder) listObjectsPagesV2(prefix *string, delimiter *string, maxKeys *int64,
	listFunc func(commonPrefixes []*s3.CommonPrefix, contents []*s3.Object)) error {
	s3Objects := &s3.ListObjectsV2Input{
		Bucket:    folder.bucket,
		Prefix:    prefix,
		Delimiter: delimiter,
		MaxKeys:   maxKeys,
	}
	err := folder.s3API.ListObjectsV2Pages(s3Objects, func(files *s3.ListObjectsV2Output, lastPage bool) bool {
		listFunc(files.CommonPrefixes, files.Contents)
		return true
	})
	return err
}

func (folder *Folder) DeleteObjects(objectRelativePaths []string) error {
	parts := partitionStrings(objectRelativePaths, 1000)
	for _, part := range parts {
		input := &s3.DeleteObjectsInput{Bucket: folder.bucket, Delete: &s3.Delete{
			Objects: folder.partitionToObjects(part),
		}}
		_, err := folder.s3API.DeleteObjects(input)
		if err != nil {
			return errors.Wrapf(err, "failed to delete s3 object: '%s'", part)
		}
	}
	return nil
}

func (folder *Folder) Validate() error {
	prefix := aws.String(folder.path)
	delimiter := aws.String("/")
	int64One := int64(1)
	input := &s3.ListObjectsInput{
		Bucket:    folder.bucket,
		Prefix:    prefix,
		Delimiter: delimiter,
		MaxKeys:   &int64One,
	}
	_, err := folder.s3API.ListObjects(input)
	if err != nil {
		return fmt.Errorf("bad credentials: %v", err)
	}
	return nil
}

func (folder *Folder) partitionToObjects(keys []string) []*s3.ObjectIdentifier {
	objects := make([]*s3.ObjectIdentifier, len(keys))
	for id, key := range keys {
		objects[id] = &s3.ObjectIdentifier{Key: aws.String(folder.path + key)}
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

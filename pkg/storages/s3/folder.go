package s3

import (
	"fmt"
	"github.com/wal-g/tracelog"
	"hash/fnv"
	"io"
	"path"
	"strconv"
	"strings"
	"time"

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

	EndpointSetting          = "AWS_ENDPOINT"
	RegionSetting            = "AWS_REGION"
	ForcePathStyleSetting    = "AWS_S3_FORCE_PATH_STYLE"
	AccessKeyIdSetting       = "AWS_ACCESS_KEY_ID"
	AccessKeySetting         = "AWS_ACCESS_KEY"
	SecretAccessKeySetting   = "AWS_SECRET_ACCESS_KEY"
	SecretKeySetting         = "AWS_SECRET_KEY"
	SessionTokenSetting      = "AWS_SESSION_TOKEN"
	SseSetting               = "S3_SSE"
	SseCSetting              = "S3_SSE_C"
	SseKmsIdSetting          = "S3_SSE_KMS_ID"
	StorageClassSetting      = "S3_STORAGE_CLASS"
	UploadConcurrencySetting = "UPLOAD_CONCURRENCY"
	s3CertFile               = "S3_CA_CERT_FILE"
	MaxPartSize              = "S3_MAX_PART_SIZE"
	EndpointSourceSetting    = "S3_ENDPOINT_SOURCE"
	EndpointPortSetting      = "S3_ENDPOINT_PORT"
	LogLevel                 = "S3_LOG_LEVEL"
	UseListObjectsV1         = "S3_USE_LIST_OBJECTS_V1"
	RangeBatchEnabled        = "S3_RANGE_BATCH_ENABLED"
	RangeQueriesMaxRetries   = "S3_RANGE_MAX_RETRIES"


	RangeQueriesMaxRetriesDefault = 10
	RangeBatchEnabledDefault      = false
)

var (
	// MaxRetries limit upload and download retries during interaction with S3
	MaxRetries  = 15
	SettingList = []string{
		EndpointPortSetting,
		EndpointSetting,
		EndpointSourceSetting,
		RegionSetting,
		ForcePathStyleSetting,
		AccessKeyIdSetting,
		AccessKeySetting,
		SecretAccessKeySetting,
		SecretKeySetting,
		SessionTokenSetting,
		SseSetting,
		SseCSetting,
		SseKmsIdSetting,
		StorageClassSetting,
		UploadConcurrencySetting,
		s3CertFile,
		MaxPartSize,
		UseListObjectsV1,
		RangeBatchEnabled,
		RangeQueriesMaxRetries,
	}
	S3BufferCounter = 0
)

func NewFolderError(err error, format string, args ...interface{}) storage.Error {
	return storage.NewError(err, "S3", format, args...)
}

func NewConfiguringError(settingName string) storage.Error {
	return NewFolderError(errors.New("Configuring error"),
		"%s setting is not set", settingName)
}

type Folder struct {
	uploader Uploader
	S3API    s3iface.S3API
	Bucket   *string
	Path     string
	settings map[string]string

	useListObjectsV1 bool
}

func NewFolder(uploader Uploader, s3API s3iface.S3API, settings map[string]string, bucket, path string, useListObjectsV1 bool) *Folder {
	return &Folder{
		uploader:			uploader,
		S3API:				s3API,
		settings:			settings,
		Bucket:				aws.String(bucket),
		Path:				storage.AddDelimiterToPath(path),
		useListObjectsV1:	useListObjectsV1,
	}
}

func ConfigureFolder(prefix string, settings map[string]string) (storage.Folder, error) {
	bucket, storagePath, err := storage.GetPathFromPrefix(prefix)
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure S3 path")
	}
	sess, err := createSession(bucket, settings)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create new session")
	}
	client := s3.New(sess)
	uploader, err := configureUploader(client, settings)
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure S3 uploader")
	}
	useListObjectsV1 := false
	if strUseListObjectsV1, ok := settings[UseListObjectsV1]; ok {
		useListObjectsV1, err = strconv.ParseBool(strUseListObjectsV1)
		if err != nil {
			return nil, NewFolderError(err, "Invalid s3 list objects version setting")
		}
	}

	folder := NewFolder(*uploader, client, settings, bucket, storagePath, useListObjectsV1)

	return folder, nil
}

func (folder *Folder) Exists(objectRelativePath string) (bool, error) {
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

func (folder *Folder) PutObject(name string, content io.Reader) error {
	return folder.uploader.upload(*folder.Bucket, folder.Path+name, content)
}

func (folder *Folder) CopyObject(srcPath string, dstPath string) error {
	if exists, err := folder.Exists(srcPath); !exists {
		if err == nil {
			return errors.New("object does not exist")
		} else {
			return err
		}
	}
	source := path.Join(*folder.Bucket, folder.Path, srcPath)
	dst := path.Join(folder.Path, dstPath)
	input := &s3.CopyObjectInput{CopySource: &source, Bucket: folder.Bucket, Key: &dst}
	_, err := folder.S3API.CopyObject(input)
	if err != nil {
		return err
	}
	return nil
}

type s3Reader struct {
	lastBody      io.ReadCloser
	folder        *Folder
	maxRetries    int32
	objectPath    string
	storageCursor int64
	id            string // id useful for logs, cause its shorter than filename
}

func (reader *s3Reader) getObjectRange(from, to int64) (*s3.GetObjectOutput, error) {
	bytesRange := fmt.Sprintf("bytes=%d-", from)
	if to != 0 {
		bytesRange += strconv.Itoa(int(to))
	}
	input := &s3.GetObjectInput{
		Bucket: reader.folder.Bucket,
		Key:    aws.String(reader.objectPath),
		Range:  aws.String(bytesRange),
	}
	tracelog.DebugLogger.Printf("GetObject [%s] with range %s", reader.id, bytesRange)
	return reader.folder.S3API.GetObject(input)
}


func (reader *s3Reader) Read(p []byte) (n int, err error) {
	tracelog.DebugLogger.Printf("s3Reader [%s] Read to buffer [%d] bytes", reader.id, len(p))
	reconnect := false
	if reader.lastBody == nil { // initial connect
		reconnect = true
	}
	for {
		if reconnect {
			reconnect = false
			connErr := reader.reconnect()
			if connErr != nil {
				tracelog.DebugLogger.Printf("s3Reader [%s] reconnect failed %s", reader.id, connErr)
				return 0, connErr
			}
		}

		n, err = reader.lastBody.Read(p)
		tracelog.DebugLogger.Printf("s3Reader [%s] read %d, err %s", reader.id, n, err)
		if err != nil && err != io.EOF {
			reconnect = true
			continue
		}
		reader.storageCursor += int64(n)
		tracelog.DebugLogger.Printf("s3Reader [%s] success read", reader.id)
		return n, err
	}
}

func (reader *s3Reader) reconnect() error {
	failed := int32(0)
	backoffSleepDuration := 0.25 * 1000 * time.Millisecond
	sleepMultiplier := 1

	for  {
		object, err := reader.getObjectRange(reader.storageCursor, 0)
		if err != nil {
			failed += 1
			tracelog.DebugLogger.Printf("s3Reader [%s] reconnect failed: %s", reader.id, err)
			if failed >= reader.maxRetries {
				return errors.Wrap(err, "s3Reader Too much reconnecting retries")
			}
			time.Sleep(time.Duration(sleepMultiplier) * backoffSleepDuration)
			sleepMultiplier *= 2
			continue
		}
		if reader.lastBody != nil {
			err = reader.lastBody.Close()
			if err != nil {
				return errors.Wrap(err, "s3Reader We have problems with closing prev connection, smth strange")
			}
		}
		reader.lastBody = object.Body
		tracelog.DebugLogger.Printf("s3Reader [%s] reconnect succeeded", reader.id)
		break
	}
	return nil
}

func (reader *s3Reader) Close() (err error) {
	return reader.lastBody.Close()
}

func NewS3Reader(objectPath string, maxRetries int32, folder *Folder) *s3Reader {
	S3BufferCounter++
	reader := &s3Reader{objectPath: objectPath, maxRetries: maxRetries,
		folder: folder, id: getId(objectPath, S3BufferCounter)}

	tracelog.DebugLogger.Printf("Init s3reader id %s path %s", reader.id, objectPath)
	return reader
}

func getId(objectPath string, counter int) string {
	hash := fnv.New32a()
	_, err := hash.Write([]byte(objectPath))
	if err != nil {
		tracelog.ErrorLogger.Println("Fatal, can't write buffer to hash")
		return "<INVALID HASH>"
	}
	return fmt.Sprintf("%x_%d", hash.Sum32(), counter)
}

func (folder *Folder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	objectPath := folder.Path + objectRelativePath
	input := &s3.GetObjectInput{
		Bucket: folder.Bucket,
		Key:    aws.String(objectPath),
	}

	object, err := folder.S3API.GetObject(input)
	if err != nil {
		if isAwsNotExist(err) {
			return nil, storage.NewObjectNotFoundError(objectPath)
		}
		return nil, errors.Wrapf(err, "failed to read object: '%s' from S3", objectPath)
	}

	rangeEnabled, maxRetries := folder.getReaderSettings()

	reader := object.Body
	if rangeEnabled {
		_ = object.Body.Close() // we don't need it anymore
		reader = NewS3Reader(objectPath, int32(maxRetries), folder)
	}
	return reader, nil
}

func (folder *Folder) getReaderSettings() (rangeEnabled bool, maxRetries int) {
	rangeEnabled = RangeBatchEnabledDefault
	if rangeBatch, ok := folder.settings[RangeBatchEnabled]; ok {
		if rangeBatch == "true" {
			rangeEnabled = true
		} else {
			rangeEnabled = false
		}
	}

	maxRetries = RangeQueriesMaxRetriesDefault
	if maxRetriesRaw, ok := folder.settings[RangeQueriesMaxRetries]; ok {
		if maxRetriesInt, err := strconv.Atoi(maxRetriesRaw); err == nil {
			maxRetries = maxRetriesInt
		}
	}

	return rangeEnabled, maxRetries
}

func (folder *Folder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	subFolder := NewFolder(folder.uploader, folder.S3API, folder.settings, *folder.Bucket,
		storage.JoinPath(folder.Path, subFolderRelativePath)+"/", folder.useListObjectsV1)
	return subFolder
}

func (folder *Folder) GetPath() string {
	return folder.Path
}

func (folder *Folder) ListFolder() (objects []storage.Object, subFolders []storage.Folder, err error) {
	listFunc := func(commonPrefixes []*s3.CommonPrefix, contents []*s3.Object) {
		for _, prefix := range commonPrefixes {
			subFolder := NewFolder(folder.uploader, folder.S3API, folder.settings, *folder.Bucket,
				*prefix.Prefix, folder.useListObjectsV1)
			subFolders = append(subFolders, subFolder)
		}
		for _, object := range contents {
			// Some storages return root tar_partitions folder as a Key.
			// We do not want to fail restoration due to this fact.
			// Keep in mind that skipping files is very dangerous and any decision here must be weighted.
			if *object.Key == folder.Path {
				continue
			}
			objectRelativePath := strings.TrimPrefix(*object.Key, folder.Path)
			objects = append(objects, storage.NewLocalObject(objectRelativePath, *object.LastModified, *object.Size))
		}
	}

	prefix := aws.String(folder.Path)
	delimiter := aws.String("/")
	if folder.useListObjectsV1 {
		err = folder.listObjectsPagesV1(prefix, delimiter, listFunc)
	} else {
		err = folder.listObjectsPagesV2(prefix, delimiter, listFunc)
	}

	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to list s3 folder: '%s'", folder.Path)
	}
	return objects, subFolders, nil
}

func (folder *Folder) listObjectsPagesV1(prefix *string, delimiter *string,
	listFunc func(commonPrefixes []*s3.CommonPrefix, contents []*s3.Object)) error {
	s3Objects := &s3.ListObjectsInput{
		Bucket:    folder.Bucket,
		Prefix:    prefix,
		Delimiter: delimiter,
	}
	return folder.S3API.ListObjectsPages(s3Objects, func(files *s3.ListObjectsOutput, lastPage bool) bool {
		listFunc(files.CommonPrefixes, files.Contents)
		return true
	})
}

func (folder *Folder) listObjectsPagesV2(prefix *string, delimiter *string,
	listFunc func(commonPrefixes []*s3.CommonPrefix, contents []*s3.Object)) error {
	s3Objects := &s3.ListObjectsV2Input{
		Bucket:    folder.Bucket,
		Prefix:    prefix,
		Delimiter: delimiter,
	}
	return folder.S3API.ListObjectsV2Pages(s3Objects, func(files *s3.ListObjectsV2Output, lastPage bool) bool {
		listFunc(files.CommonPrefixes, files.Contents)
		return true
	})
}

func (folder *Folder) DeleteObjects(objectRelativePaths []string) error {
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

func (folder *Folder) partitionToObjects(keys []string) []*s3.ObjectIdentifier {
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

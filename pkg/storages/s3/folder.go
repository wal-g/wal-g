package s3

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	stderrors "errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const (
	NotFoundAWSErrorCode  = "NotFound"
	NoSuchKeyAWSErrorCode = "NoSuchKey"

	VersioningDefault  = ""
	VersioningEnabled  = "enabled"
	VersioningDisabled = "disabled"
)

// TODO: Unit tests
type Folder struct {
	s3API    API
	uploader *Uploader
	bucket   *string
	path     string
	config   *Config
}

func NewFolder(
	s3API API,
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

func GetSSECustomerKeyMD5(sseCustomerKey string) string {
	hash := md5.Sum([]byte(sseCustomerKey))
	return base64.StdEncoding.EncodeToString(hash[:])
}

func (folder *Folder) Exists(objectRelativePath string) (bool, error) {
	objectPath := folder.path + objectRelativePath
	stopSentinelObjectInput := &s3.HeadObjectInput{
		Bucket: folder.bucket,
		Key:    aws.String(objectPath),
	}

	if folder.uploader.serverSideEncryption != "" && folder.uploader.SSECustomerKey != "" {
		stopSentinelObjectInput.SSECustomerAlgorithm = aws.String(folder.uploader.serverSideEncryption)
		stopSentinelObjectInput.SSECustomerKey = aws.String(sseCustomerKeyB64(folder.uploader.SSECustomerKey))

		customerKeyMD5 := GetSSECustomerKeyMD5(folder.uploader.SSECustomerKey)
		stopSentinelObjectInput.SSECustomerKeyMD5 = aws.String(customerKeyMD5)
	}

	_, err := folder.s3API.HeadObject(context.Background(), stopSentinelObjectInput)
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

	if folder.uploader.serverSideEncryption != "" {
		if folder.uploader.SSECustomerKey != "" {
			customerKeyMD5 := GetSSECustomerKeyMD5(folder.uploader.SSECustomerKey)
			encodedKey := sseCustomerKeyB64(folder.uploader.SSECustomerKey)

			input.CopySourceSSECustomerAlgorithm = aws.String(folder.uploader.serverSideEncryption)
			input.CopySourceSSECustomerKey = aws.String(encodedKey)
			input.CopySourceSSECustomerKeyMD5 = aws.String(customerKeyMD5)

			input.SSECustomerAlgorithm = aws.String(folder.uploader.serverSideEncryption)
			input.SSECustomerKey = aws.String(encodedKey)
			input.SSECustomerKeyMD5 = aws.String(customerKeyMD5)
		} else {
			input.ServerSideEncryption = types.ServerSideEncryption(folder.uploader.serverSideEncryption)
		}

		if folder.uploader.SSEKMSKeyID != "" {
			input.SSEKMSKeyId = aws.String(folder.uploader.SSEKMSKeyID)
		}
	}

	_, err := folder.s3API.CopyObject(context.Background(), input)
	return err
}

func (folder *Folder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	objectPath := folder.path + objectRelativePath
	input := &s3.GetObjectInput{
		Bucket: folder.bucket,
		Key:    aws.String(objectPath),
	}

	if folder.uploader.serverSideEncryption != "" && folder.uploader.SSECustomerKey != "" {
		input.SSECustomerAlgorithm = aws.String(folder.uploader.serverSideEncryption)
		input.SSECustomerKey = aws.String(sseCustomerKeyB64(folder.uploader.SSECustomerKey))

		customerKeyMD5 := GetSSECustomerKeyMD5(folder.uploader.SSECustomerKey)
		input.SSECustomerKeyMD5 = aws.String(customerKeyMD5)
	}

	object, err := folder.s3API.GetObject(context.Background(), input)
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
	return NewContentLengthValidator(reader, aws.ToInt64(object.ContentLength), objectPath), nil
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

// SetShowAllVersions controls whether ListFolder includes deleted objects
// (objects where the latest version is a delete marker) when versioning is enabled.
func (folder *Folder) SetShowAllVersions(show bool) {
	tracelog.DebugLogger.Printf("setting all versions %t for folder %s", show, folder.path)
	folder.config.showAllVersions = show
}

func (folder *Folder) ListFolder() (objects []storage.Object, subFolders []storage.Folder, err error) {
	prefix := aws.String(folder.path)
	delimiter := aws.String("/")

	if folder.isVersioningEnabled() {
		objects, subFolders, err = folder.listVersions(prefix, delimiter)
		if err != nil {
			return nil, nil, err
		}
	} else {
		listFunc := func(commonPrefixes []types.CommonPrefix, contents []types.Object) bool {
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
				objects = append(objects, storage.NewLocalObject(objectRelativePath, *object.LastModified, aws.ToInt64(object.Size)))
			}
			return true
		}

		err = folder.listObjectsPages(prefix, delimiter, nil, nil, listFunc)

		// DigitalOcean Spaces compatibility: DO's API complains about NoSuchKey when trying to list folders
		// which don't yet exist.
		if err != nil && !isAwsNotExist(err) {
			return nil, nil, errors.Wrapf(err, "failed to list s3 folder: '%s'", folder.path)
		}
	}

	return objects, subFolders, nil
}

func (folder *Folder) ListFolderSegment(
	startAfterKey *string,
	endBeforeKey *string,
) (objects []storage.Object, subFolders []storage.Folder, err error) {
	prefix := aws.String(folder.path)
	delimiter := aws.String("/")
	var startAfterPrefix *string
	if startAfterKey != nil {
		startAfterPrefix = aws.String(*prefix + *startAfterKey)
	}

	listFunc := func(commonPrefixes []types.CommonPrefix, contents []types.Object) bool {
		cont := true
		for _, prefix := range commonPrefixes {
			subFolder := NewFolder(folder.s3API, folder.uploader, *prefix.Prefix, folder.config)
			subFolders = append(subFolders, subFolder)
		}
		for _, object := range contents {
			key := *object.Key
			if endBeforeKey != nil && key > *endBeforeKey {
				cont = false
				break
			}
			// Some storages return root tar_partitions folder as a Key.
			// We do not want to fail restoration due to this fact.
			// Keep in mind that skipping files is very dangerous and any decision here must be weighted.
			if key == folder.path {
				continue
			}

			objectRelativePath := strings.TrimPrefix(*object.Key, folder.path)
			objects = append(objects, storage.NewLocalObject(objectRelativePath, *object.LastModified, aws.ToInt64(object.Size)))
		}
		return cont
	}

	err = folder.listObjectsPages(prefix, delimiter, nil, startAfterPrefix, listFunc)

	// DigitalOcean Spaces compatibility: DO's API complains about NoSuchKey when trying to list folders
	// which don't yet exist.
	if err != nil && !isAwsNotExist(err) {
		return nil, nil, errors.Wrapf(err, "failed to list s3 folder: '%s'", folder.path)
	}

	return objects, subFolders, nil
}

// versionInfo holds information about an S3 object version collected during listing.
type versionInfo struct {
	relativePath string
	lastModified time.Time
	size         int64
	versionID    string
	isLatest     bool
}

// addListedSubfolders converts S3 CommonPrefixes to Folder handles.
// These handles are used by callers (e.g. `st ls` non-recursive output, or recursive listing via subfolders).
func (folder *Folder) addListedSubfolders(subFolders *[]storage.Folder, commonPrefixes []types.CommonPrefix) {
	for _, p := range commonPrefixes {
		subFolder := NewFolder(folder.s3API, folder.uploader, *p.Prefix, folder.config)
		*subFolders = append(*subFolders, subFolder)
	}
}

// collectDeleteMarkers gathers delete marker entries and records which keys are "deleted"
// (i.e. their LATEST is a delete marker). `deletedKeys` drives filtering in buildObjectsFromVersions.
func (folder *Folder) collectDeleteMarkers(
	deleteMarkers *[]versionInfo,
	deletedKeys map[string]bool,
	markers []types.DeleteMarkerEntry,
) {
	for _, marker := range markers {
		if *marker.Key == folder.path {
			continue
		}
		objectRelativePath := strings.TrimPrefix(*marker.Key, folder.path)
		isLatest := aws.ToBool(marker.IsLatest)
		if isLatest {
			deletedKeys[objectRelativePath] = true
		}
		// Also collect delete marker info for --all-versions mode
		*deleteMarkers = append(*deleteMarkers, versionInfo{
			relativePath: objectRelativePath,
			lastModified: *marker.LastModified,
			size:         0,
			versionID:    *marker.VersionId,
			isLatest:     isLatest,
		})
	}
}

// collectVersions gathers object versions from S3. Filtering happens later to correctly handle pagination
// (delete markers and corresponding versions can appear on different pages).
func (folder *Folder) collectVersions(allVersions *[]versionInfo, versions []types.ObjectVersion) {
	for i := range versions {
		object := &versions[i]
		// Some storages return root tar_partitions folder as a Key.
		if *object.Key == folder.path {
			continue
		}
		objectRelativePath := strings.TrimPrefix(*object.Key, folder.path)
		*allVersions = append(*allVersions, versionInfo{
			relativePath: objectRelativePath,
			lastModified: *object.LastModified,
			size:         aws.ToInt64(object.Size),
			versionID:    *object.VersionId,
			isLatest:     aws.ToBool(object.IsLatest),
		})
	}
}

// buildObjectsFromVersions turns collected versions into storage objects for output and higher-level logic.
// By default it filters out keys whose LATEST is a delete marker; `showAllVersions` disables that filter.
func (folder *Folder) buildObjectsFromVersions(allVersions []versionInfo, deletedKeys map[string]bool) []storage.Object {
	objects := make([]storage.Object, 0, len(allVersions))
	for _, v := range allVersions {
		// Skip objects where the LATEST version is a delete marker (unless showing all versions)
		if deletedKeys[v.relativePath] && !folder.config.showAllVersions {
			continue
		}
		isLatest := ""
		if v.isLatest {
			isLatest = "LATEST"
		}
		objects = append(objects, storage.NewLocalObjectWithVersion(v.relativePath, v.lastModified, v.size, v.versionID, isLatest))
	}
	return objects
}

// deleteMarkerAdditionalInfo formats marker version metadata for `st ls --all-versions` output.
func deleteMarkerAdditionalInfo(marker versionInfo) string {
	if marker.isLatest {
		return "LATEST DELETE"
	}
	return "DELETE"
}

func (folder *Folder) listVersions(prefix *string, delimiter *string) ([]storage.Object, []storage.Folder, error) {
	subFolders := []storage.Folder{}

	// Collect all versions and delete markers across all pages first,
	// because a delete marker and its corresponding version might be on different pages.
	allVersions := []versionInfo{}
	deleteMarkers := []versionInfo{}

	// Track keys where the LATEST version is a delete marker.
	// These objects are effectively deleted and should not appear in the listing.
	deletedKeys := make(map[string]bool)

	input := &s3.ListObjectVersionsInput{
		Bucket:    folder.bucket,
		Prefix:    prefix,
		Delimiter: delimiter,
	}
	paginator := s3.NewListObjectVersionsPaginator(folder.s3API, input)
	for paginator.HasMorePages() {
		out, err := paginator.NextPage(context.Background())
		if err != nil {
			// DigitalOcean Spaces compatibility: DO's API complains about NoSuchKey when trying to list folders
			// which don't yet exist.
			if !isAwsNotExist(err) {
				return nil, nil, errors.Wrapf(err, "failed to list s3 folder: '%s'", folder.path)
			}
			break
		}
		folder.addListedSubfolders(&subFolders, out.CommonPrefixes)
		folder.collectDeleteMarkers(&deleteMarkers, deletedKeys, out.DeleteMarkers)
		folder.collectVersions(&allVersions, out.Versions)
	}

	// Convert collected versions to storage objects, applying delete-marker filtering unless requested otherwise.
	objects := folder.buildObjectsFromVersions(allVersions, deletedKeys)

	// If showing all versions, also include delete markers themselves
	if folder.config.showAllVersions {
		tracelog.DebugLogger.Println("adding delete markers")
		for _, marker := range deleteMarkers {
			objects = append(objects, storage.NewLocalObjectWithVersion(
				marker.relativePath,
				marker.lastModified,
				0,
				marker.versionID,
				deleteMarkerAdditionalInfo(marker),
			),
			)
		}
	}
	return objects, subFolders, nil
}

func (folder *Folder) listObjectsPages(prefix *string, delimiter *string, maxKeys *int32, startAfter *string,
	listFunc func(commonPrefixes []types.CommonPrefix, contents []types.Object) bool) error {
	input := &s3.ListObjectsV2Input{
		Bucket:     folder.bucket,
		Prefix:     prefix,
		Delimiter:  delimiter,
		MaxKeys:    maxKeys,
		StartAfter: startAfter,
	}
	paginator := s3.NewListObjectsV2Paginator(folder.s3API, input)
	for paginator.HasMorePages() {
		out, err := paginator.NextPage(context.Background())
		if err != nil {
			return err
		}
		if !listFunc(out.CommonPrefixes, out.Contents) {
			return nil
		}
	}
	return nil
}

func (folder *Folder) DeleteObjects(objects []storage.Object) error {
	parts := partitionObjects(objects, folder.config.DeleteBatchSize)

	for _, part := range parts {
		tracelog.DebugLogger.Printf("len of part  %d", len(part))
		input := &s3.DeleteObjectsInput{Bucket: folder.bucket, Delete: &types.Delete{
			Objects: []types.ObjectIdentifier{},
		}}
		for _, obj := range part {
			input.Delete.Objects = append(input.Delete.Objects, types.ObjectIdentifier{
				Key:       aws.String(folder.path + obj.GetName()),
				VersionId: aws.String(obj.GetVersionID()),
			})
		}
		_, err := folder.s3API.DeleteObjects(context.Background(), input)
		if err != nil {
			for _, obj := range part {
				tracelog.DebugLogger.Printf("object %s version %s", obj.GetName(), obj.GetVersionID())
			}
			return errors.Wrapf(err, "failed to delete s3 object: '%s'", part)
		}
	}
	return nil
}

func (folder *Folder) isVersioningEnabled() bool {
	switch folder.config.EnableVersioning {
	case VersioningEnabled:
		return true
	case VersioningDisabled:
		return false
	case VersioningDefault:
		result, err := folder.s3API.GetBucketVersioning(context.Background(), &s3.GetBucketVersioningInput{
			Bucket: folder.bucket,
		})
		if err != nil {
			return false
		}

		if result.Status == types.BucketVersioningStatusEnabled {
			folder.config.EnableVersioning = VersioningEnabled
			return true
		}
		folder.config.EnableVersioning = VersioningDisabled
	}
	return false
}

func (folder *Folder) Validate() error {
	prefix := aws.String(folder.path)
	delimiter := aws.String("/")
	int32One := int32(1)
	input := &s3.ListObjectsInput{
		Bucket:    folder.bucket,
		Prefix:    prefix,
		Delimiter: delimiter,
		MaxKeys:   &int32One,
	}
	_, err := folder.s3API.ListObjects(context.Background(), input)
	if err != nil {
		return fmt.Errorf("bad credentials: %v", err)
	}
	return nil
}

func (folder *Folder) SetVersioningEnabled(enable bool) {
	if enable && folder.isVersioningEnabled() {
		folder.config.EnableVersioning = VersioningEnabled
	} else {
		folder.config.EnableVersioning = VersioningDisabled
	}
}

func (folder *Folder) GetVersioningEnabled() bool {
	return folder.isVersioningEnabled()
}

// isAwsNotExist returns true when err carries an S3 NotFound or NoSuchKey
// signal. v2 surfaces these as typed errors (*types.NotFound, *types.NoSuchKey)
// which v1's Code()-based check no longer reaches; fall back to smithy.APIError
// for compatibility with non-AWS S3 servers that return only the error code.
func isAwsNotExist(err error) bool {
	if _, ok := stderrors.AsType[*types.NotFound](err); ok {
		return true
	}
	if _, ok := stderrors.AsType[*types.NoSuchKey](err); ok {
		return true
	}
	if apiErr, ok := stderrors.AsType[smithy.APIError](err); ok {
		code := apiErr.ErrorCode()
		return code == NotFoundAWSErrorCode || code == NoSuchKeyAWSErrorCode
	}
	return false
}

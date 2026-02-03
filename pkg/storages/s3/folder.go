package s3

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
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
	s3API           s3iface.S3API
	uploader        *Uploader
	bucket          *string
	path            string
	config          *Config
	showAllVersions bool // When true, include deleted objects in listing (for st ls --all-versions)
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
		stopSentinelObjectInput.SSECustomerKey = aws.String(folder.uploader.SSECustomerKey)

		customerKeyMD5 := GetSSECustomerKeyMD5(folder.uploader.SSECustomerKey)
		stopSentinelObjectInput.SSECustomerKeyMD5 = aws.String(customerKeyMD5)
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

	if folder.uploader.serverSideEncryption != "" {
		if folder.uploader.SSECustomerKey != "" {
			customerKeyMD5 := GetSSECustomerKeyMD5(folder.uploader.SSECustomerKey)

			input.CopySourceSSECustomerAlgorithm = aws.String(folder.uploader.serverSideEncryption)
			input.CopySourceSSECustomerKey = aws.String(folder.uploader.SSECustomerKey)
			input.CopySourceSSECustomerKeyMD5 = aws.String(customerKeyMD5)

			input.SSECustomerAlgorithm = aws.String(folder.uploader.serverSideEncryption)
			input.SSECustomerKey = aws.String(folder.uploader.SSECustomerKey)
			input.SSECustomerKeyMD5 = aws.String(customerKeyMD5)
		} else {
			input.ServerSideEncryption = aws.String(folder.uploader.serverSideEncryption)
		}

		if folder.uploader.SSEKMSKeyID != "" {
			input.SSEKMSKeyId = aws.String(folder.uploader.SSEKMSKeyID)
		}
	}

	_, err := folder.s3API.CopyObject(input)
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
		input.SSECustomerKey = aws.String(folder.uploader.SSECustomerKey)

		customerKeyMD5 := GetSSECustomerKeyMD5(folder.uploader.SSECustomerKey)
		input.SSECustomerKeyMD5 = aws.String(customerKeyMD5)
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
	// Propagate the showAllVersions setting to subfolders
	subFolder.showAllVersions = folder.showAllVersions
	return subFolder
}

func (folder *Folder) GetPath() string {
	return folder.path
}

// SetShowAllVersions controls whether ListFolder includes deleted objects
// (objects where the latest version is a delete marker) when versioning is enabled.
func (folder *Folder) SetShowAllVersions(show bool) {
	folder.showAllVersions = show
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
		listFunc := func(commonPrefixes []*s3.CommonPrefix, contents []*s3.Object) {
			for _, prefix := range commonPrefixes {
				subFolder := NewFolder(folder.s3API, folder.uploader, *prefix.Prefix, folder.config)
				// Propagate the showAllVersions setting to subfolders created during listing
				subFolder.showAllVersions = folder.showAllVersions
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

		err = folder.listObjectsPages(prefix, delimiter, nil, listFunc)

		// DigitalOcean Spaces compatibility: DO's API complains about NoSuchKey when trying to list folders
		// which don't yet exist.
		if err != nil && !isAwsNotExist(err) {
			return nil, nil, errors.Wrapf(err, "failed to list s3 folder: '%s'", folder.path)
		}
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
func (folder *Folder) addListedSubfolders(subFolders *[]storage.Folder, commonPrefixes []*s3.CommonPrefix) {
	for _, p := range commonPrefixes {
		subFolder := NewFolder(folder.s3API, folder.uploader, *p.Prefix, folder.config)
		// Propagate the showAllVersions setting to subfolders created during listing
		subFolder.showAllVersions = folder.showAllVersions
		*subFolders = append(*subFolders, subFolder)
	}
}

// collectDeleteMarkers gathers delete marker entries and records which keys are "deleted"
// (i.e. their LATEST is a delete marker). `deletedKeys` drives filtering in buildObjectsFromVersions.
func (folder *Folder) collectDeleteMarkers(
	deleteMarkers *[]versionInfo,
	deletedKeys map[string]bool,
	markers []*s3.DeleteMarkerEntry,
) {
	for _, marker := range markers {
		if *marker.Key == folder.path {
			continue
		}
		objectRelativePath := strings.TrimPrefix(*marker.Key, folder.path)
		if *marker.IsLatest {
			deletedKeys[objectRelativePath] = true
		}
		// Also collect delete marker info for --all-versions mode
		*deleteMarkers = append(*deleteMarkers, versionInfo{
			relativePath: objectRelativePath,
			lastModified: *marker.LastModified,
			size:         0,
			versionID:    *marker.VersionId,
			isLatest:     *marker.IsLatest,
		})
	}
}

// collectVersions gathers object versions from S3. Filtering happens later to correctly handle pagination
// (delete markers and corresponding versions can appear on different pages).
func (folder *Folder) collectVersions(allVersions *[]versionInfo, versions []*s3.ObjectVersion) {
	for _, object := range versions {
		// Some storages return root tar_partitions folder as a Key.
		if *object.Key == folder.path {
			continue
		}
		objectRelativePath := strings.TrimPrefix(*object.Key, folder.path)
		*allVersions = append(*allVersions, versionInfo{
			relativePath: objectRelativePath,
			lastModified: *object.LastModified,
			size:         *object.Size,
			versionID:    *object.VersionId,
			isLatest:     *object.IsLatest,
		})
	}
}

// buildObjectsFromVersions turns collected versions into storage objects for output and higher-level logic.
// By default it filters out keys whose LATEST is a delete marker; `showAllVersions` disables that filter.
func (folder *Folder) buildObjectsFromVersions(allVersions []versionInfo, deletedKeys map[string]bool) []storage.Object {
	objects := make([]storage.Object, 0, len(allVersions))
	for _, v := range allVersions {
		// Skip objects where the LATEST version is a delete marker (unless showing all versions)
		if deletedKeys[v.relativePath] && !folder.showAllVersions {
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
		return fmt.Sprintf("%s LATEST DELETE", marker.versionID)
	}
	return fmt.Sprintf("%s DELETE", marker.versionID)
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

	// Keep the page callback small and side-effect-only: collect state; build output after paging completes.
	versionsListFunc := func(out *s3.ListObjectVersionsOutput, _ bool) bool {
		folder.addListedSubfolders(&subFolders, out.CommonPrefixes)
		folder.collectDeleteMarkers(&deleteMarkers, deletedKeys, out.DeleteMarkers)
		folder.collectVersions(&allVersions, out.Versions)
		return true
	}

	input := &s3.ListObjectVersionsInput{
		Bucket:    folder.bucket,
		Prefix:    prefix,
		Delimiter: delimiter,
	}
	err := folder.s3API.ListObjectVersionsPages(input, versionsListFunc)

	// DigitalOcean Spaces compatibility: DO's API complains about NoSuchKey when trying to list folders
	// which don't yet exist.
	if err != nil && !isAwsNotExist(err) {
		return nil, nil, errors.Wrapf(err, "failed to list s3 folder: '%s'", folder.path)
	}

	// Convert collected versions to storage objects, applying delete-marker filtering unless requested otherwise.
	objects := folder.buildObjectsFromVersions(allVersions, deletedKeys)

	// If showing all versions, also include delete markers themselves
	if folder.showAllVersions {
		for _, marker := range deleteMarkers {
			objects = append(objects, storage.NewLocalObjectWithVersion(
				marker.relativePath, marker.lastModified, 0, deleteMarkerAdditionalInfo(marker)))
		}
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

func (folder *Folder) DeleteObjects(objects []storage.Object) error {
	parts := partitionObjects(objects, folder.config.DeleteBatchSize)

	for _, part := range parts {
		tracelog.DebugLogger.Printf("len of part  %d", len(part))
		input := &s3.DeleteObjectsInput{Bucket: folder.bucket, Delete: &s3.Delete{
			Objects: []*s3.ObjectIdentifier{},
		}}
		for _, obj := range part {
			name := obj.GetName()
			version := obj.GetVersionId()
			input.Delete.Objects = append(input.Delete.Objects, &s3.ObjectIdentifier{Key: &name, VersionId: &version})
		}
		_, err := folder.s3API.DeleteObjects(input)
		if err != nil {
			for _, obj := range part {
				tracelog.DebugLogger.Printf("object %s version %s", obj.GetName(), obj.GetVersionId())
			}
			return errors.Wrapf(err, "failed to delete s3 object: '%s'", part)
		}
	}
	return nil
}

func (folder *Folder) getObjectVersions(key string) ([]*s3.ObjectIdentifier, error) {
	inp := &s3.ListObjectVersionsInput{
		Bucket: folder.bucket,
		Prefix: aws.String(folder.path + key),
	}

	out, err := folder.s3API.ListObjectVersions(inp)
	if err != nil {
		return nil, err
	}
	list := make([]*s3.ObjectIdentifier, 0)
	for _, version := range out.Versions {
		list = append(list, &s3.ObjectIdentifier{Key: version.Key, VersionId: version.VersionId})
	}

	for _, deleteMarker := range out.DeleteMarkers {
		list = append(list, &s3.ObjectIdentifier{Key: deleteMarker.Key, VersionId: deleteMarker.VersionId})
	}

	return list, nil
}

func (folder *Folder) isVersioningEnabled() bool {
	switch folder.config.EnableVersioning {
	case VersioningEnabled:
		return true
	case VersioningDisabled:
		return false
	case VersioningDefault:
		result, err := folder.s3API.GetBucketVersioning(&s3.GetBucketVersioningInput{
			Bucket: folder.bucket,
		})
		if err != nil {
			return false
		}

		if result.Status != nil && *result.Status == s3.BucketVersioningStatusEnabled {
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

func (folder *Folder) partitionToObjects(keys []string, versioningEnabled bool) []*s3.ObjectIdentifier {
	objects := make([]*s3.ObjectIdentifier, 0, len(keys))
	for _, key := range keys {
		objects = append(objects, &s3.ObjectIdentifier{Key: aws.String(folder.path + key)})
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

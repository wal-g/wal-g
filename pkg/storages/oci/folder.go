package oci

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/oracle/oci-go-sdk/v65/objectstorage/transfer"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

var _ storage.Folder = &Folder{}

// formatOCIError formats an OCI error with request ID for better debuggability.
func formatOCIError(operation string, err error) error {
	serviceErr, ok := common.IsServiceError(err)
	if ok {
		return fmt.Errorf("OCI %s failed: code=%s, status=%d, opc-request-id=%s, message=%s",
			operation, serviceErr.GetCode(), serviceErr.GetHTTPStatusCode(),
			serviceErr.GetOpcRequestID(), serviceErr.GetMessage())
	}
	return fmt.Errorf("%s: %w", operation, err)
}

// defaultRetryPolicy returns the default retry policy for OCI requests.
func defaultRetryPolicy() *common.RetryPolicy {
	policy := common.DefaultRetryPolicy()
	return &policy
}

type Folder struct {
	client *ociClient
	bucket string
	path   string
	region string
}

// ctxReadCloser wraps an io.ReadCloser and cancels its context on Close.
// This keeps the context alive for the lifetime of the reader, which is
// owned by the caller and may outlive the function that created it.
type ctxReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *ctxReadCloser) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}

// NewFolder creates a new folder instance for the given bucket and path.
func NewFolder(client *ociClient, bucket string, path string, region string) *Folder {
	// Ensure path has trailing slash for consistency
	if path != "" && !strings.HasSuffix(path, "/") {
		path += "/"
	}
	return &Folder{
		client: client,
		bucket: bucket,
		path:   path,
		region: region,
	}
}

// getNamespace retrieves the OCI namespace for this folder's operations.
func (f *Folder) getNamespace(ctx context.Context) (string, error) {
	if f.client == nil {
		return "", fmt.Errorf("OCI client not initialized")
	}
	return f.client.getNamespace(ctx)
}

// GetPath returns the folder path with a trailing slash.
func (f *Folder) GetPath() string {
	return f.path
}

// ListFolder lists all objects and subfolders in this folder.
func (f *Folder) ListFolder(ctx context.Context) (objects []storage.Object, subFolders []storage.Folder, err error) {
	prefix := f.GetPath()
	delimiter := "/"
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	var nextStartWith *string

	tracelog.DebugLogger.Printf("Listing OCI objects with prefix: %s", prefix)

	namespace, err := f.getNamespace(ctx)
	if err != nil {
		return nil, nil, err
	}

	for {
		req := objectstorage.ListObjectsRequest{
			NamespaceName: common.String(namespace),
			BucketName:    common.String(f.bucket),
			Prefix:        common.String(prefix),
			Delimiter:     common.String(delimiter),
			Start:         nextStartWith,
			Fields:        common.String("name,size,timeModified"),
			RequestMetadata: common.RequestMetadata{
				RetryPolicy: defaultRetryPolicy(),
			},
		}

		resp, err := f.client.objectStorageClient.ListObjects(ctx, req)
		if err != nil {
			return nil, nil, formatOCIError("listing objects", err)
		}

		for _, commonPrefix := range resp.Prefixes {
			subFolder := NewFolder(f.client, f.bucket, commonPrefix, f.region)
			subFolders = append(subFolders, subFolder)
		}

		for _, object := range resp.Objects {
			if *object.Name == prefix {
				continue
			}
			objectRelativePath := strings.TrimPrefix(*object.Name, prefix)

			// Size and TimeModified are explicitly requested via Fields parameter
			// and should always be populated by OCI API
			size := int64(0)
			if object.Size != nil {
				size = *object.Size
			}

			var modTime time.Time
			if object.TimeModified != nil {
				modTime = object.TimeModified.Time
			}

			objects = append(objects, storage.NewLocalObject(objectRelativePath, modTime, size))
		}

		if resp.NextStartWith == nil {
			break
		}
		// Detect infinite loop if API returns same pagination token
		if nextStartWith != nil && *nextStartWith == *resp.NextStartWith {
			return nil, nil, fmt.Errorf("OCI returned identical pagination token, possible API bug")
		}
		nextStartWith = resp.NextStartWith
	}

	return objects, subFolders, nil
}

// DeleteObjects deletes multiple objects by their relative paths.
func (f *Folder) DeleteObjects(ctx context.Context, objects []storage.Object) error {
	timeout := time.Duration(len(objects)) * 30 * time.Second
	if timeout < 5*time.Minute {
		timeout = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	namespace, err := f.getNamespace(ctx)
	if err != nil {
		return err
	}

	for _, object := range objects {
		fullPath := f.GetPath() + object.GetName()
		tracelog.DebugLogger.Printf("Deleting OCI object: %s", fullPath)

		req := objectstorage.DeleteObjectRequest{
			NamespaceName: common.String(namespace),
			BucketName:    common.String(f.bucket),
			ObjectName:    common.String(fullPath),
			RequestMetadata: common.RequestMetadata{
				RetryPolicy: defaultRetryPolicy(),
			},
		}

		_, err := f.client.objectStorageClient.DeleteObject(ctx, req)
		if err != nil {
			return formatOCIError(fmt.Sprintf("deleting object %q", fullPath), err)
		}
	}

	return nil
}

// Exists checks if an object exists at the given relative path.
func (f *Folder) Exists(ctx context.Context, objectRelativePath string) (bool, error) {
	objectPath := f.GetPath() + objectRelativePath
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	namespace, err := f.getNamespace(ctx)
	if err != nil {
		return false, err
	}

	req := objectstorage.HeadObjectRequest{
		NamespaceName: common.String(namespace),
		BucketName:    common.String(f.bucket),
		ObjectName:    common.String(objectPath),
		RequestMetadata: common.RequestMetadata{
			RetryPolicy: defaultRetryPolicy(),
		},
	}

	_, err = f.client.objectStorageClient.HeadObject(ctx, req)
	if err != nil {
		serviceErr, ok := common.IsServiceError(err)
		if ok && serviceErr.GetHTTPStatusCode() == 404 {
			return false, nil
		}
		return false, formatOCIError(fmt.Sprintf("checking object %q existence", objectPath), err)
	}

	return true, nil
}

// GetSubFolder returns a folder instance for a subdirectory.
func (f *Folder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	return NewFolder(f.client, f.bucket, storage.JoinPath(f.path, subFolderRelativePath), f.region)
}

// ReadObject downloads an object and returns its content as a reader.
func (f *Folder) ReadObject(ctx context.Context, objectRelativePath string) (io.ReadCloser, error) {
	objectPath := f.GetPath() + objectRelativePath
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)

	tracelog.DebugLogger.Printf("Reading OCI object: %s", objectPath)

	namespace, err := f.getNamespace(ctx)
	if err != nil {
		cancel()
		return nil, err
	}

	req := objectstorage.GetObjectRequest{
		NamespaceName: common.String(namespace),
		BucketName:    common.String(f.bucket),
		ObjectName:    common.String(objectPath),
		RequestMetadata: common.RequestMetadata{
			RetryPolicy: defaultRetryPolicy(),
		},
	}

	resp, err := f.client.objectStorageClient.GetObject(ctx, req)
	if err != nil {
		cancel()
		serviceErr, ok := common.IsServiceError(err)
		if ok && serviceErr.GetHTTPStatusCode() == 404 {
			return nil, storage.NewObjectNotFoundError(objectPath)
		}
		return nil, formatOCIError(fmt.Sprintf("reading object %q", objectPath), err)
	}

	return &ctxReadCloser{ReadCloser: resp.Content, cancel: cancel}, nil
}

// PutObject uploads an object with context for cancellation.
// Uses OCI's transfer.UploadManager to handle large objects without buffering in memory.
func (f *Folder) PutObject(ctx context.Context, name string, content io.Reader) error {
	objectPath := f.GetPath() + name

	tracelog.DebugLogger.Printf("Putting OCI object: %s", objectPath)

	namespace, err := f.getNamespace(ctx)
	if err != nil {
		return err
	}

	uploadManager := transfer.NewUploadManager()
	req := transfer.UploadStreamRequest{
		UploadRequest: transfer.UploadRequest{
			NamespaceName:         common.String(namespace),
			BucketName:            common.String(f.bucket),
			ObjectName:            common.String(objectPath),
			ObjectStorageClient:   f.client.objectStorageClient,
			PartSize:              common.Int64(64 * 1024 * 1024), // 64MB parts
			AllowMultipartUploads: common.Bool(true),
		},
		StreamReader: content,
	}

	_, err = uploadManager.UploadStream(ctx, req)
	if err != nil {
		return formatOCIError(fmt.Sprintf("putting object %q", objectPath), err)
	}

	return nil
}

// CopyObject copies an object from source to destination path within the bucket.
func (f *Folder) CopyObject(ctx context.Context, srcPath string, dstPath string) error {
	src := path.Join(f.GetPath(), srcPath)
	dst := path.Join(f.GetPath(), dstPath)
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	tracelog.DebugLogger.Printf("Copying OCI object from %s to %s", src, dst)

	namespace, err := f.getNamespace(ctx)
	if err != nil {
		return err
	}

	req := objectstorage.CopyObjectRequest{
		NamespaceName: common.String(namespace),
		BucketName:    common.String(f.bucket),
		CopyObjectDetails: objectstorage.CopyObjectDetails{
			SourceObjectName:      common.String(src),
			DestinationBucket:     common.String(f.bucket),
			DestinationNamespace:  common.String(namespace),
			DestinationObjectName: common.String(dst),
			DestinationRegion:     common.String(f.region),
		},
		RequestMetadata: common.RequestMetadata{
			RetryPolicy: defaultRetryPolicy(),
		},
	}

	_, err = f.client.objectStorageClient.CopyObject(ctx, req)
	if err != nil {
		if serviceErr, ok := common.IsServiceError(err); ok && serviceErr.GetHTTPStatusCode() == 404 {
			return storage.NewObjectNotFoundError(srcPath)
		}
		return formatOCIError(fmt.Sprintf("copying object from %q to %q", src, dst), err)
	}

	return nil
}

func (f *Folder) Validate(ctx context.Context) error {
	return nil
}

// SetVersioningEnabled is a no-op for OCI storage.
func (f *Folder) SetVersioningEnabled(ctx context.Context, enable bool) {}

// GetVersioningEnabled always returns false because OCI versioning is unsupported.
func (f *Folder) GetVersioningEnabled(ctx context.Context) bool {
	return false
}

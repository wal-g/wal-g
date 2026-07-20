package oss

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

var _ storage.Folder = &Folder{}

const (
	VersioningDefault  = ""
	VersioningEnabled  = "enabled"
	VersioningDisabled = "disabled"
)

type Folder struct {
	ossAPI *oss.Client
	bucket string
	path   string
	config *Config

	uploader *oss.Uploader
	copier   *oss.Copier
}

func NewFolder(ossAPI *oss.Client, bucket string, path string, config *Config) *Folder {
	uploader := oss.NewUploader(ossAPI, func(uo *oss.UploaderOptions) {
		uo.PartSize = config.UploadPartSize
	})
	copier := oss.NewCopier(ossAPI, func(co *oss.CopierOptions) {
		co.PartSize = config.CopyPartSize
	})
	return &Folder{
		ossAPI:   ossAPI,
		bucket:   bucket,
		path:     path,
		config:   config,
		uploader: uploader,
		copier:   copier,
	}
}

func (f *Folder) GetPath() string {
	if !strings.HasSuffix(f.path, "/") {
		f.path += "/"
	}
	return f.path
}

func (f *Folder) ListFolder(ctx context.Context) (objects []storage.Object, subFolders []storage.Folder, err error) {
	prefix := f.GetPath()
	delimiter := "/"

	if f.isVersioningEnabled(ctx) {
		return nil, nil, fmt.Errorf("versioning is not supported for oss")
	}

	var continuationToken *string
	for {
		result, err := f.ossAPI.ListObjectsV2(ctx, &oss.ListObjectsV2Request{
			Bucket:            oss.Ptr(f.bucket),
			Prefix:            oss.Ptr(prefix),
			Delimiter:         oss.Ptr(delimiter),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("failed to list oss folder: %w", err)
		}

		for _, commonPrefix := range result.CommonPrefixes {
			subFolder := NewFolder(f.ossAPI, f.bucket, *commonPrefix.Prefix, f.config)
			subFolders = append(subFolders, subFolder)
		}

		for _, object := range result.Contents {
			if *object.Key == prefix {
				continue
			}
			objectRelativePath := strings.TrimPrefix(*object.Key, prefix)
			objects = append(objects, storage.NewLocalObject(objectRelativePath, *object.LastModified, object.Size))
		}

		if !result.IsTruncated {
			break
		}
		continuationToken = result.NextContinuationToken
	}

	return objects, subFolders, nil
}

func (f *Folder) DeleteObjects(ctx context.Context, objectRelativePaths []storage.Object) error {
	if f.isVersioningEnabled(ctx) {
		return fmt.Errorf("versioning is not supported for oss")
	}

	for _, part := range partitionObjects(objectRelativePaths, 1000) {
		var objectsToDelete []oss.DeleteObject
		for _, key := range part {
			fullPath := f.GetPath() + key.GetName()
			tracelog.DebugLogger.Println("Deleting OSS object:", fullPath)
			objectsToDelete = append(objectsToDelete, oss.DeleteObject{Key: oss.Ptr(fullPath)})
		}

		_, err := f.ossAPI.DeleteMultipleObjects(ctx, &oss.DeleteMultipleObjectsRequest{
			Bucket:  oss.Ptr(f.bucket),
			Objects: objectsToDelete,
		})
		if err != nil {
			return fmt.Errorf("failed to delete oss objects: %w", err)
		}
	}

	return nil
}

func partitionObjects(keys []storage.Object, size int) [][]storage.Object {
	if len(keys) == 0 {
		return nil
	}
	if size <= 0 {
		size = 1
	}
	var parts [][]storage.Object
	for i := 0; i < len(keys); i += size {
		end := i + size
		if end > len(keys) {
			end = len(keys)
		}
		parts = append(parts, keys[i:end])
	}
	return parts
}

func (f *Folder) Exists(ctx context.Context, objectRelativePath string) (bool, error) {
	objectPath := f.GetPath() + objectRelativePath
	_, err := f.ossAPI.HeadObject(ctx, &oss.HeadObjectRequest{
		Bucket: oss.Ptr(f.bucket),
		Key:    oss.Ptr(objectPath),
	})

	if err != nil {
		var serviceError *oss.ServiceError
		if errors.As(err, &serviceError) && serviceError.Code == "NoSuchKey" {
			return false, nil
		}
		return false, fmt.Errorf("failed to check oss object '%s' existence: %w", objectPath, err)
	}

	return true, nil
}

func (f *Folder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	return NewFolder(f.ossAPI, f.bucket, storage.JoinPath(f.path, subFolderRelativePath), f.config)
}

func (f *Folder) ReadObject(ctx context.Context, objectRelativePath string) (io.ReadCloser, error) {
	objectPath := f.GetPath() + objectRelativePath
	req := &oss.GetObjectRequest{
		Bucket: oss.Ptr(f.bucket),
		Key:    oss.Ptr(objectPath),
	}
	result, err := f.ossAPI.GetObject(ctx, req)
	if err != nil {
		var serviceError *oss.ServiceError
		if errors.As(err, &serviceError) && serviceError.Code == "NoSuchKey" {
			return nil, storage.NewObjectNotFoundError(objectPath)
		}
		return nil, fmt.Errorf("failed to read oss object '%s': %w", objectPath, err)
	}

	return result.Body, nil
}

func (f *Folder) PutObject(ctx context.Context, name string, content io.Reader) error {
	objectPath := f.GetPath() + name

	_, err := f.uploader.UploadFrom(ctx, &oss.PutObjectRequest{
		Bucket: oss.Ptr(f.bucket),
		Key:    oss.Ptr(objectPath),
	}, content)
	if err != nil {
		return fmt.Errorf("failed to put oss object %q: %w", objectPath, err)
	}
	return nil
}

func (f *Folder) CopyObject(ctx context.Context, srcPath string, dstPath string) error {
	if exists, err := f.Exists(ctx, srcPath); !exists {
		if err == nil {
			return storage.NewObjectNotFoundError(srcPath)
		}
		return err
	}
	src := path.Join(f.GetPath(), srcPath)
	dst := path.Join(f.GetPath(), dstPath)

	_, err := f.copier.Copy(ctx, &oss.CopyObjectRequest{
		Bucket:       oss.Ptr(f.bucket),
		Key:          oss.Ptr(dst),
		SourceBucket: oss.Ptr(f.bucket),
		SourceKey:    oss.Ptr(src),
	})
	return err
}

func (f *Folder) Validate(ctx context.Context) error {
	prefix := f.GetPath()
	delimiter := "/"
	_, err := f.ossAPI.ListObjectsV2(ctx, &oss.ListObjectsV2Request{
		Bucket:    oss.Ptr(f.bucket),
		Prefix:    oss.Ptr(prefix),
		Delimiter: oss.Ptr(delimiter),
	})
	if err != nil {
		return fmt.Errorf("failed to list oss folder: %w", err)
	}
	return nil
}

func (f *Folder) isVersioningEnabled(ctx context.Context) bool {
	switch f.config.EnableVersioning {
	case VersioningEnabled:
		return true
	case VersioningDisabled:
		return false
	case VersioningDefault:
		result, err := f.ossAPI.GetBucketVersioning(ctx, &oss.GetBucketVersioningRequest{
			Bucket: oss.Ptr(f.bucket),
		})
		if err != nil {
			return false
		}

		if result.VersionStatus != nil && *result.VersionStatus == "Enabled" {
			f.config.EnableVersioning = VersioningEnabled
			return true
		}
		f.config.EnableVersioning = VersioningDisabled
	}
	return false
}

func (f *Folder) SetVersioningEnabled(ctx context.Context, enable bool) {
	if enable && f.isVersioningEnabled(ctx) {
		f.config.EnableVersioning = VersioningEnabled
	} else {
		f.config.EnableVersioning = VersioningDisabled
	}
}

func (f *Folder) GetVersioningEnabled(ctx context.Context) bool {
	return f.isVersioningEnabled(ctx)
}

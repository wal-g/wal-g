package azure

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/pkg/errors"
)

// TODO: Unit tests
type Folder struct {
	path                string
	containerClient     azblob.ContainerClient
	uploadStreamOptions azblob.UploadStreamOptions
	timeout             time.Duration
}

func NewFolder(
	path string,
	containerClient azblob.ContainerClient,
	uploadStreamOptions azblob.UploadStreamOptions,
	timeout time.Duration,
) *Folder {
	// Trim leading slash because there's no difference between absolute and relative paths in Azure.
	path = strings.TrimPrefix(path, "/")
	return &Folder{
		path,
		containerClient,
		uploadStreamOptions,
		timeout,
	}
}

func (folder *Folder) GetPath() string {
	return folder.path
}

func (folder *Folder) Exists(objectRelativePath string) (bool, error) {
	path := storage.JoinPath(folder.path, objectRelativePath)
	ctx := context.Background()
	blobClient, err := folder.containerClient.NewBlockBlobClient(path)
	if err != nil {
		return false, fmt.Errorf("init Azure Blob client to check object %q for existence: %w", path, err)
	}
	_, err = blobClient.GetProperties(ctx, nil)
	var stgErr *azblob.StorageError
	if err != nil && errors.As(err, &stgErr) && stgErr.ErrorCode == azblob.StorageErrorCodeBlobNotFound {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("get Azure object stats %q: %w", path, err)
	}
	return true, nil
}

func (folder *Folder) ListFolder() (objects []storage.Object, subFolders []storage.Folder, err error) {
	blobPager := folder.containerClient.ListBlobsHierarchy("/", &azblob.ContainerListBlobsHierarchyOptions{Prefix: &folder.path})
	for blobPager.NextPage(context.Background()) {
		blobs := blobPager.PageResponse()
		//add blobs to the list of storage objects
		for _, blob := range blobs.Segment.BlobItems {
			objName := strings.TrimPrefix(*blob.Name, folder.path)
			updated := *blob.Properties.LastModified

			objects = append(objects, storage.NewLocalObject(objName, updated, *blob.Properties.ContentLength))
		}

		//Get subFolder names
		blobPrefixes := blobs.Segment.BlobPrefixes
		//add subFolders to the list of storage folders
		for _, blobPrefix := range blobPrefixes {
			subFolderPath := *blobPrefix.Name

			subFolders = append(subFolders, NewFolder(
				subFolderPath,
				folder.containerClient,
				folder.uploadStreamOptions,
				folder.timeout,
			))
		}
	}
	err = blobPager.Err()
	if err != nil {
		return nil, nil, fmt.Errorf("iterate through folder %q: %w", folder.path, err)
	}
	return objects, subFolders, err
}

func (folder *Folder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	return NewFolder(
		storage.AddDelimiterToPath(storage.JoinPath(folder.path, subFolderRelativePath)),
		folder.containerClient,
		folder.uploadStreamOptions,
		folder.timeout)
}

func (folder *Folder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	path := storage.JoinPath(folder.path, objectRelativePath)
	blobClient, err := folder.containerClient.NewBlockBlobClient(path)
	if err != nil {
		return nil, fmt.Errorf("init Azure Blob client to read object %q: %w", path, err)
	}

	get, err := blobClient.Download(context.Background(), nil)
	if err != nil {
		var storageError *azblob.StorageError
		errors.As(err, &storageError)
		if storageError.ErrorCode == azblob.StorageErrorCodeBlobNotFound {
			return nil, storage.NewObjectNotFoundError(path)
		}
		return nil, fmt.Errorf("download blob %q: %w", path, err)
	}
	reader := get.Body(nil)
	return reader, nil
}

func (folder *Folder) PutObject(name string, content io.Reader) error {
	return folder.PutObjectWithContext(context.Background(), name, content)
}

func (folder *Folder) PutObjectWithContext(ctx context.Context, name string, content io.Reader) error {
	tracelog.DebugLogger.Printf("Put %v into %v\n", name, folder.path)
	//Upload content to a block blob using full path
	path := storage.JoinPath(folder.path, name)
	blobClient, err := folder.containerClient.NewBlockBlobClient(path)
	if err != nil {
		return fmt.Errorf("init Azure Blob client to upload object %q: %w", path, err)
	}

	_, err = blobClient.UploadStream(ctx, content, folder.uploadStreamOptions)
	if err != nil {
		return fmt.Errorf("upload blob %q: %w", path, err)
	}

	tracelog.DebugLogger.Printf("Put %v done\n", name)
	return nil
}

func (folder *Folder) CopyObject(srcPath string, dstPath string) error {
	var exists bool
	var err error
	if exists, err = folder.Exists(srcPath); !exists {
		if err == nil {
			return storage.NewObjectNotFoundError(srcPath)
		}
		return err
	}
	var srcClient, dstClient *azblob.BlockBlobClient
	srcClient, err = folder.containerClient.NewBlockBlobClient(srcPath)
	if err != nil {
		return fmt.Errorf("init Azure Blob client for copy source %q: %w", srcPath, err)
	}
	dstClient, err = folder.containerClient.NewBlockBlobClient(dstPath)
	if err != nil {
		return fmt.Errorf("init Azure Blob client for copy destination %q: %w", dstPath, err)
	}
	_, err = dstClient.StartCopyFromURL(context.Background(), srcClient.URL(),
		&azblob.BlobStartCopyOptions{Tier: azblob.AccessTierHot.ToPtr()})
	return err
}

func (folder *Folder) DeleteObjects(objectRelativePaths []string) error {
	for _, objectRelativePath := range objectRelativePaths {
		//Delete blob using blobClient obtained from full path to blob
		path := storage.JoinPath(folder.path, objectRelativePath)
		blobClient, err := folder.containerClient.NewBlockBlobClient(path)
		if err != nil {
			return fmt.Errorf("init Azure Blob client to delete object %q: %w", path, err)
		}
		tracelog.DebugLogger.Printf("Delete %v\n", path)
		_, err = blobClient.Delete(context.Background(),
			&azblob.BlobDeleteOptions{DeleteSnapshots: azblob.DeleteSnapshotsOptionTypeInclude.ToPtr()})
		var stgErr *azblob.StorageError
		if err != nil && errors.As(err, &stgErr) && stgErr.ErrorCode == azblob.StorageErrorCodeBlobNotFound {
			continue
		}
		if err != nil {
			return fmt.Errorf("delete object %q: %w", path, err)
		}
		//blob is deleted
	}
	return nil
}

func (folder *Folder) Validate() error {
	return nil
}

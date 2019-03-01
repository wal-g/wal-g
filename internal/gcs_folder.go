package internal

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/tracelog"
	"google.golang.org/api/iterator"
)

type GSFolderError struct {
	error
}

func NewGSFolderError(err error, format string, args ...interface{}) GSFolderError {
	return GSFolderError{errors.Wrapf(err, format, args...)}
}

func (err GSFolderError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

func NewGSFolder(bucket *storage.BucketHandle, path string) *GSFolder {
	return &GSFolder{bucket, path}
}

func ConfigureGSFolder(prefix string) (StorageFolder, error) {
	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, NewGSFolderError(err, "Unable to create GS Client")
	}

	bucketName, path, err := getPathFromPrefix(prefix)
	if err != nil {
		return nil, NewGSFolderError(err, "Unable to parse prefix %v", prefix)
	}

	bucket := client.Bucket(bucketName)

	path = addDelimiterToPath(path)
	return NewGSFolder(bucket, path), nil
}

// GSFolder represents folder in GCP
type GSFolder struct {
	bucket *storage.BucketHandle
	path   string
}

func (folder *GSFolder) GetPath() string {
	return folder.path
}

func (folder *GSFolder) ListFolder() (objects []StorageObject, subFolders []StorageFolder, err error) {
	it := folder.bucket.Objects(context.Background(), &storage.Query{Delimiter: "/", Prefix: addDelimiterToPath(folder.path)})
	for {
		objAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, nil, NewGSFolderError(err, "Unable to iterate %v", folder.path)
		}
		if objAttrs.Prefix != "" {
			subFolders = append(subFolders, NewGSFolder(folder.bucket, objAttrs.Prefix))
		} else {
			objName := strings.TrimPrefix(objAttrs.Name, folder.path)
			objects = append(objects, &GSStorageObject{objAttrs.Updated, objName})
		}
	}
	return
}

func (folder *GSFolder) DeleteObjects(objectRelativePaths []string) error {
	for _, objectRelativePath := range objectRelativePaths {
		path := JoinStoragePath(folder.path, objectRelativePath)
		object := folder.bucket.Object(path)
		tracelog.DebugLogger.Printf("Delete %v\n", path)
		err := object.Delete(context.Background())
		if err != nil && err != storage.ErrObjectNotExist {
			return NewGSFolderError(err, "Unable to delete object %v", path)
		}
	}
	return nil
}

func (folder *GSFolder) Exists(objectRelativePath string) (bool, error) {
	path := JoinStoragePath(folder.path, objectRelativePath)
	object := folder.bucket.Object(path)
	_, err := object.Attrs(context.Background())
	if err == storage.ErrObjectNotExist {
		return false, nil
	}
	if err != nil {
		return false, NewGSFolderError(err, "Unable to stat object %v", path)
	}
	return true, nil
}

func (folder *GSFolder) GetSubFolder(subFolderRelativePath string) StorageFolder {
	return NewGSFolder(folder.bucket, JoinStoragePath(folder.path, subFolderRelativePath))
}

func (folder *GSFolder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	path := JoinStoragePath(folder.path, objectRelativePath)
	object := folder.bucket.Object(path)
	reader, err := object.NewReader(context.Background())
	if err == storage.ErrObjectNotExist {
		return nil, NewObjectNotFoundError(path)
	}
	return ioutil.NopCloser(reader), err
}

func (folder *GSFolder) PutObject(name string, content io.Reader) error {
	tracelog.DebugLogger.Printf("Put %v into %v\n", name, folder.path)
	object := folder.bucket.Object(JoinStoragePath(folder.path, name))
	writer := object.NewWriter(context.Background())
	_, err := io.Copy(writer, content)
	if err != nil {
		return NewGSFolderError(err, "Unable to copy to object")
	}
	tracelog.DebugLogger.Printf("Put %v done\n", name)
	err = writer.Close()
	if err != nil {
		return NewGSFolderError(err, "Unable to Close object")
	}
	return nil
}

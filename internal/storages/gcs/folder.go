package gcs

import (
	"context"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"io"
	"io/ioutil"
	"strings"

	gcs "cloud.google.com/go/storage"
	"github.com/wal-g/wal-g/internal/tracelog"
	"google.golang.org/api/iterator"
)

func NewError(err error, format string, args ...interface{}) storage.Error {
	return storage.NewError(err, "GCS", format, args...)
}

func NewFolder(bucket *gcs.BucketHandle, path string) *Folder {
	return &Folder{bucket, path}
}

func ConfigureFolder(prefix string, settings map[string]string) (storage.Folder, error) {
	ctx := context.Background()

	client, err := gcs.NewClient(ctx)
	if err != nil {
		return nil, NewError(err, "Unable to create client")
	}

	bucketName, path, err := storage.GetPathFromPrefix(prefix)
	if err != nil {
		return nil, NewError(err, "Unable to parse prefix %v", prefix)
	}

	bucket := client.Bucket(bucketName)

	path = storage.AddDelimiterToPath(path)
	return NewFolder(bucket, path), nil
}

// Folder represents folder in GCP
type Folder struct {
	bucket *gcs.BucketHandle
	path   string
}

func (folder *Folder) GetPath() string {
	return folder.path
}

func (folder *Folder) ListFolder() (objects []storage.Object, subFolders []storage.Folder, err error) {
	prefix := storage.AddDelimiterToPath(folder.path)
	it := folder.bucket.Objects(context.Background(), &gcs.Query{Delimiter: "/", Prefix: prefix})
	for {
		objAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, nil, NewError(err, "Unable to iterate %v", folder.path)
		}
		if objAttrs.Prefix != "" {
			subFolders = append(subFolders, NewFolder(folder.bucket, objAttrs.Prefix))
		} else {
			objName := strings.TrimPrefix(objAttrs.Name, prefix)
			objects = append(objects, storage.NewLocalObject(objName, objAttrs.Updated))
		}
	}
	return
}

func (folder *Folder) DeleteObjects(objectRelativePaths []string) error {
	for _, objectRelativePath := range objectRelativePaths {
		path := storage.JoinPath(folder.path, objectRelativePath)
		object := folder.bucket.Object(path)
		tracelog.DebugLogger.Printf("Delete %v\n", path)
		err := object.Delete(context.Background())
		if err != nil && err != gcs.ErrObjectNotExist {
			return NewError(err, "Unable to delete object %v", path)
		}
	}
	return nil
}

func (folder *Folder) Exists(objectRelativePath string) (bool, error) {
	path := storage.JoinPath(folder.path, objectRelativePath)
	object := folder.bucket.Object(path)
	_, err := object.Attrs(context.Background())
	if err == gcs.ErrObjectNotExist {
		return false, nil
	}
	if err != nil {
		return false, NewError(err, "Unable to stat object %v", path)
	}
	return true, nil
}

func (folder *Folder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	return NewFolder(folder.bucket, storage.JoinPath(folder.path, subFolderRelativePath))
}

func (folder *Folder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	path := storage.JoinPath(folder.path, objectRelativePath)
	object := folder.bucket.Object(path)
	reader, err := object.NewReader(context.Background())
	if err == gcs.ErrObjectNotExist {
		return nil, storage.NewObjectNotFoundError(path)
	}
	return ioutil.NopCloser(reader), err
}

func (folder *Folder) PutObject(name string, content io.Reader) error {
	tracelog.DebugLogger.Printf("Put %v into %v\n", name, folder.path)
	object := folder.bucket.Object(storage.JoinPath(folder.path, name))
	writer := object.NewWriter(context.Background())
	_, err := io.Copy(writer, content)
	if err != nil {
		return NewError(err, "Unable to copy to object")
	}
	tracelog.DebugLogger.Printf("Put %v done\n", name)
	err = writer.Close()
	if err != nil {
		return NewError(err, "Unable to Close object")
	}
	return nil
}

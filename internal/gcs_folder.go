package internal

import (
	"cloud.google.com/go/storage"
	"context"
	"github.com/wal-g/wal-g/internal/tracelog"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"io"
	"strings"
)

func NewGSFolder(bucket *storage.BucketHandle, path string) *GSFolder {
	return &GSFolder{bucket, path}
}

func ConfigureGSFolder(prefix string) (StorageFolder, error) {
	credentials := getSettingValue("GOOGLE_APPLICATION_CREDENTIALS")
	if credentials == "" {
		return nil, NewUnsetEnvVarError([]string{"GOOGLE_APPLICATION_CREDENTIALS"})
	}

	ctx := context.Background()

	client, err := storage.NewClient(ctx, option.WithCredentialsFile(credentials))
	if err != nil {
		return nil, err
	}

	bucketName, path, err := getPathFromPrefix(prefix)
	if err != nil {
		return nil, err
	}

	bucket := client.Bucket(bucketName)

	path = addDelimiterToPath(path)
	return NewGSFolder(bucket, path), nil
}

func addDelimiterToPath(path string) string {
	if strings.HasSuffix(path, "/") || path == "" {
		return path
	}
	return path + "/"
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
			return nil, nil, err
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
	for _, o := range objectRelativePaths {
		path := JoinS3Path(folder.path, o)
		object := folder.bucket.Object(path)
		tracelog.DebugLogger.Printf("Delete %v\n", path)
		err := object.Delete(context.Background())
		if err != nil && err != storage.ErrObjectNotExist {
			return err
		}
	}
	return nil
}

func (folder *GSFolder) Exists(objectRelativePath string) (bool, error) {
	object := folder.bucket.Object(JoinS3Path(folder.path, objectRelativePath))
	_, err := object.Attrs(context.Background())
	if err == storage.ErrObjectNotExist {
		return false, nil
	}
	return true, err
}

func (folder *GSFolder) GetSubFolder(subFolderRelativePath string) StorageFolder {
	return NewGSFolder(folder.bucket, JoinS3Path(folder.path, subFolderRelativePath))
}

func (folder *GSFolder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	object := folder.bucket.Object(JoinS3Path(folder.path, objectRelativePath))
	reader, err := object.NewReader(context.Background())
	return &ReaderNopCloser{reader}, err
}

func (folder *GSFolder) PutObject(name string, content io.Reader) error {
	tracelog.DebugLogger.Printf("Put %v into %v\n", name, folder.path)
	object := folder.bucket.Object(JoinS3Path(folder.path, name))
	w := object.NewWriter(context.Background())
	_, err := io.Copy(w, content)
	if err != nil {
		return err
	}
	tracelog.DebugLogger.Printf("Put %v done\n", name)
	return w.Close()
}

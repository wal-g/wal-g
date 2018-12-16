package internal

import (
	"cloud.google.com/go/storage"
	"context"
	"github.com/wal-g/wal-g/internal/tracelog"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"io"
	"strings"
	"time"
)

func NewGSFolder(prefix string) (StorageFolder, error) {
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

	return &GSFolder{bucket, addDelimiterToPath(path)}, nil
}

func addDelimiterToPath(path string) string {
	if strings.HasSuffix(path, "/") || path == "" {
		return path
	}
	return path + "/"
}

type GSStorageObject struct {
	updated time.Time
	name    string
}

func (o *GSStorageObject) GetName() string {
	return o.name
}

func (o *GSStorageObject) GetLastModified() time.Time {
	return o.updated
}

// GSFolder represents folder in GCP
type GSFolder struct {
	bucket *storage.BucketHandle
	path   string
}

func (f *GSFolder) GetPath() string {
	return f.path
}

func (f *GSFolder) ListFolder() (objects []StorageObject, subFolders []StorageFolder, err error) {
	it := f.bucket.Objects(context.Background(), &storage.Query{Delimiter: "/", Prefix: addDelimiterToPath(f.path)})
	for {
		objAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		if objAttrs.Prefix != "" {
			subFolders = append(subFolders, &GSFolder{f.bucket, objAttrs.Prefix})
		} else {
			objName := strings.TrimPrefix(objAttrs.Name, f.path)
			objects = append(objects, &GSStorageObject{objAttrs.Updated, objName})
		}
	}
	return
}

func (f *GSFolder) DeleteObjects(objectRelativePaths []string) error {
	for _, o := range objectRelativePaths {
		path := JoinS3Path(f.path, o)
		object := f.bucket.Object(path)
		tracelog.DebugLogger.Printf("Delete %v\n",path)
		err := object.Delete(context.Background())
		if err != nil && err != storage.ErrObjectNotExist {
			return err
		}
	}
	return nil
}

func (f *GSFolder) Exists(objectRelativePath string) (bool, error) {
	object := f.bucket.Object(JoinS3Path(f.path, objectRelativePath))
	_, err := object.Attrs(context.Background())
	if err == storage.ErrObjectNotExist {
		return false, nil
	}
	return true, err
}

func (f *GSFolder) GetSubFolder(subFolderRelativePath string) StorageFolder {
	return &GSFolder{f.bucket, JoinS3Path(f.path, subFolderRelativePath)}
}

type gSObjectReader struct {
	io.Reader
}

func (gSObjectReader) Close() error {
	return nil
}

func (f *GSFolder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	object := f.bucket.Object(JoinS3Path(f.path, objectRelativePath))
	reader, err := object.NewReader(context.Background())
	return &gSObjectReader{reader}, err
}

func (f *GSFolder) PutObject(name string, content io.Reader) error {
	tracelog.DebugLogger.Printf("Put %v into %v\n", name, f.path)
	object := f.bucket.Object(JoinS3Path(f.path, name))
	w := object.NewWriter(context.Background())
	_, err := io.Copy(w, content)
	if err != nil {
		return err
	}
	tracelog.DebugLogger.Printf("Put %v done\n", name)
	return w.Close()
}

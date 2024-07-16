package gcs

import (
	"context"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"

	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/pkg/storages/storage"

	gcs "cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

const composeChunkLimit = 32

func NewFolder(bucket *gcs.BucketHandle, path string, encryptionKey []byte, config *Config) *Folder {
	// Trim leading slash because there's no difference between absolute and relative paths in GCS.
	path = strings.TrimPrefix(path, "/")

	encryptionKeyCopy := make([]byte, len(encryptionKey))
	copy(encryptionKeyCopy, encryptionKey)

	return &Folder{
		bucket:        bucket,
		path:          path,
		encryptionKey: encryptionKeyCopy,
		config:        config,
	}
}

// Folder represents folder in GCP
// TODO: Unit tests
type Folder struct {
	bucket        *gcs.BucketHandle
	path          string
	encryptionKey []byte
	config        *Config
}

func (folder *Folder) GetPath() string {
	return folder.path
}

// BuildObjectHandle creates a new object handle.
func (folder *Folder) BuildObjectHandle(path string) *gcs.ObjectHandle {
	objectHandle := folder.bucket.Object(path)

	if len(folder.encryptionKey) != 0 {
		objectHandle = objectHandle.Key(folder.encryptionKey)
	}

	return objectHandle
}

func (folder *Folder) ListFolder() (objects []storage.Object, subFolders []storage.Folder, err error) {
	prefix := storage.AddDelimiterToPath(folder.path)
	ctx, cancel := folder.createTimeoutContext(context.Background())
	defer cancel()
	iter := folder.bucket.Objects(ctx, &gcs.Query{Delimiter: "/", Prefix: prefix})
	for {
		objAttrs, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("iterate GCS folder %q: %w", folder.path, err)
		}
		if objAttrs.Prefix != "" {
			if objAttrs.Prefix != prefix+"/" {
				// Sometimes GCS returns "//" folder - skip it
				subFolders = append(subFolders,
					NewFolder(
						folder.bucket,
						objAttrs.Prefix,
						folder.encryptionKey,
						folder.config,
					))
			}
		} else {
			objName := strings.TrimPrefix(objAttrs.Name, prefix)
			if objName != "" {
				// GCS returns the current directory - skip it.
				objects = append(objects, storage.NewLocalObject(objName, objAttrs.Updated, objAttrs.Size))
			}
		}
	}
	return objects, subFolders, err
}

func (folder *Folder) createTimeoutContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, folder.config.ContextTimeout)
}

func (folder *Folder) DeleteObjects(objectRelativePaths []string) error {
	for _, objectRelativePath := range objectRelativePaths {
		objPath := folder.joinPath(folder.path, objectRelativePath)
		object := folder.BuildObjectHandle(objPath)
		tracelog.DebugLogger.Printf("Delete %v\n", objPath)
		ctx, ctxCancel := folder.createTimeoutContext(context.Background())
		err := object.Delete(ctx)
		if err != nil && err != gcs.ErrObjectNotExist {
			ctxCancel()
			return fmt.Errorf("delete GCS object %q: %w", objPath, err)
		}
		ctxCancel()
	}
	return nil
}

func (folder *Folder) Exists(objectRelativePath string) (bool, error) {
	objPath := folder.joinPath(folder.path, objectRelativePath)
	object := folder.BuildObjectHandle(objPath)
	ctx, cancel := folder.createTimeoutContext(context.Background())
	defer cancel()
	_, err := object.Attrs(ctx)
	if err == gcs.ErrObjectNotExist {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("get GCS object stats %q: %w", objPath, err)
	}
	return true, nil
}

func (folder *Folder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	return NewFolder(
		folder.bucket,
		folder.joinPath(folder.path, subFolderRelativePath),
		folder.encryptionKey,
		folder.config,
	)
}

func (folder *Folder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	objPath := folder.joinPath(folder.path, objectRelativePath)
	object := folder.BuildObjectHandle(objPath)
	reader, err := object.NewReader(context.Background())
	if err == gcs.ErrObjectNotExist {
		return nil, storage.NewObjectNotFoundError(objPath)
	}
	return io.NopCloser(reader), err
}

func (folder *Folder) PutObject(name string, content io.Reader) error {
	ctx, cancel := folder.createTimeoutContext(context.Background())
	defer cancel()

	return folder.PutObjectWithContext(ctx, name, content)
}

func (folder *Folder) PutObjectWithContext(ctx context.Context, name string, content io.Reader) error {
	tracelog.DebugLogger.Printf("Put %v into %v\n", name, folder.path)
	objectPath := folder.joinPath(folder.path, name)
	object := folder.BuildObjectHandle(objectPath)

	ctx, cancel := folder.createTimeoutContext(ctx)
	defer cancel()

	chunkNum := 0
	tmpChunks := make([]*gcs.ObjectHandle, 0)

	for {
		tmpChunkName := folder.joinPath(name+"_chunks", "chunk"+strconv.Itoa(chunkNum))
		objectChunk := folder.BuildObjectHandle(folder.joinPath(folder.path, tmpChunkName))
		chunkUploader := NewUploader(objectChunk, folder.config.Uploader)
		dataChunk := chunkUploader.allocateBuffer()

		n, err := fillBuffer(content, dataChunk)
		if err != nil && err != io.EOF {
			tracelog.ErrorLogger.Printf("Unable to read content of %s, err: %v", objectPath, err)
			return fmt.Errorf("read a chunk of object %q to upload to GCS: %w", objectPath, err)
		}

		if n == 0 {
			break
		}

		chunk := chunk{
			name:  tmpChunkName,
			index: chunkNum,
			data:  dataChunk,
			size:  n,
		}

		if err := chunkUploader.UploadChunk(ctx, chunk); err != nil {
			return fmt.Errorf("upload a chunk of object %q to GCS: %w", objectPath, err)
		}

		tmpChunks = append(tmpChunks, objectChunk)

		chunkNum++

		if err == io.EOF {
			break
		}

		if len(tmpChunks) == composeChunkLimit {
			// Since there is a limit to the number of components that can be composed in a single operation, merge chunks partially.
			compositeChunkName := folder.joinPath(name+"_chunks", "composite"+strconv.Itoa(chunkNum))
			compositeChunk := folder.BuildObjectHandle(folder.joinPath(folder.path, compositeChunkName))

			tracelog.DebugLogger.Printf("Compose temporary chunks into an intermediate chunk %v\n", compositeChunkName)

			if err := composeChunks(ctx, NewUploader(compositeChunk, folder.config.Uploader), tmpChunks); err != nil {
				return fmt.Errorf("compose GCS temporary chunks into an intermediate chunk: %w", err)
			}

			tmpChunks = []*gcs.ObjectHandle{compositeChunk}
		}
	}

	tracelog.DebugLogger.Printf("Compose file %v from chunks\n", object.ObjectName())

	if err := composeChunks(ctx, NewUploader(object, folder.config.Uploader), tmpChunks); err != nil {
		return fmt.Errorf("compose GCS temporary chunks into an object: %w", err)
	}

	tracelog.DebugLogger.Printf("Put %v done\n", name)

	return nil
}

func (folder *Folder) CopyObject(srcPath string, dstPath string) error {
	if exists, err := folder.Exists(srcPath); !exists {
		if err == nil {
			return storage.NewObjectNotFoundError(srcPath)
		}
		return fmt.Errorf("check the existence of %q for copying in GCS: %w", srcPath, err)
	}
	source := path.Join(folder.path, srcPath)
	dst := path.Join(folder.path, dstPath)

	ctx := context.Background()
	_, err := folder.bucket.Object(dst).CopierFrom(folder.bucket.Object(source)).Run(ctx)
	if err != nil {
		return fmt.Errorf("copy GCS object %q to %q: %w", srcPath, dstPath, err)
	}
	return nil
}

func (folder *Folder) joinPath(one string, another string) string {
	if folder.config.NormalizePrefix {
		return storage.JoinPath(one, another)
	}
	if one[len(one)-1] == '/' {
		one = one[:len(one)-1]
	}
	if another[0] == '/' {
		another = another[1:]
	}
	return one + "/" + another
}

// composeChunks merges uploaded chunks into a new one and cleans up temporary objects.
func composeChunks(ctx context.Context, uploader *Uploader, chunks []*gcs.ObjectHandle) error {
	if err := uploader.ComposeObject(ctx, chunks); err != nil {
		return fmt.Errorf("compose object %q: %w", uploader.objHandle.ObjectName(), err)
	}

	tracelog.DebugLogger.Printf("Remove temporary chunks for %v\n", uploader.objHandle.ObjectName())

	uploader.CleanUpChunks(ctx, chunks)

	return nil
}

// fillBuffer fills the buffer with data from the reader.
func fillBuffer(r io.Reader, b []byte) (int, error) {
	var (
		err       error
		n, offset int
	)

	for offset < len(b) {
		n, err = r.Read(b[offset:])
		offset += n
		if err != nil {
			break
		}
	}

	return offset, err
}

func (folder *Folder) Validate() error {
	return nil
}

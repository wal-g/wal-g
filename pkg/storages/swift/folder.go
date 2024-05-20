package swift

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"

	"github.com/ncw/swift/v2"
)

// TODO: Unit tests
type Folder struct {
	connection *swift.Connection
	container  swift.Container
	path       string
}

func NewFolder(connection *swift.Connection, container swift.Container, path string) *Folder {
	// Trim leading slash because there's no difference between absolute and relative paths in Swift.
	path = strings.TrimPrefix(path, "/")
	return &Folder{connection, container, path}
}

func (folder *Folder) GetPath() string {
	return folder.path
}

func (folder *Folder) Exists(objectRelativePath string) (bool, error) {
	path := storage.JoinPath(folder.path, objectRelativePath)
	_, _, err := folder.connection.Object(context.Background(), folder.container.Name, path)
	if err == swift.ObjectNotFound {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("get Swift object stats %q: %w", path, err)
	}
	return true, nil
}

func (folder *Folder) ListFolder() (objects []storage.Object, subFolders []storage.Folder, err error) {
	//Iterate
	err = folder.connection.ObjectsWalk(
		context.Background(),
		folder.container.Name,
		&swift.ObjectsOpts{Delimiter: int32('/'), Prefix: folder.path},
		func(ctx context.Context, opts *swift.ObjectsOpts) (interface{}, error) {
			objectNames, err := folder.connection.ObjectNames(ctx, folder.container.Name, opts)
			if err != nil {
				return nil, fmt.Errorf("retrieve Swift object names in container %q: %w", folder.container.Name, err)
			}
			// Retrieved object names successfully.
			for _, objectName := range objectNames {
				if strings.HasSuffix(objectName, "/") {
					//It is a subFolder name
					subFolders = append(subFolders, NewFolder(folder.connection, folder.container, objectName))
				} else {
					//It is a storage object name
					obj, _, err := folder.connection.Object(ctx, folder.container.Name, objectName)
					// Some files can disappear during ListFolder execution - they can be deleted by another process
					// for example. We can ignore that and return only files that really exist.
					if err == swift.ObjectNotFound {
						continue
					}
					if err != nil {
						return nil, fmt.Errorf("get Swift object %q: %w", obj.Name, err)
					}
					//trim prefix to get object's standalone name
					objName := strings.TrimPrefix(obj.Name, folder.path)
					objects = append(objects, storage.NewLocalObject(objName, obj.LastModified, obj.Bytes))
				}
			}
			//return objectNames if a further iteration is required.
			return objectNames, err
		},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("iterate Swift folder %q: %w", folder.path, err)
	}
	return objects, subFolders, nil
}

func (folder *Folder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	return NewFolder(folder.connection, folder.container, storage.AddDelimiterToPath(storage.JoinPath(folder.path, subFolderRelativePath)))
}

func (folder *Folder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	path := storage.JoinPath(folder.path, objectRelativePath)
	//get the object from the cloud using full path
	readContents, _, err := folder.connection.ObjectOpen(context.Background(), folder.container.Name, path, true, nil)
	if err == swift.ObjectNotFound {
		return nil, storage.NewObjectNotFoundError(path)
	}
	if err != nil {
		return nil, fmt.Errorf("open Swift object %q: %w", path, err)
	}
	//retrieved object from  the cloud
	return io.NopCloser(readContents), nil
}

func (folder *Folder) PutObject(name string, content io.Reader) error {
	return folder.PutObjectWithContext(context.Background(), name, content)
}

func (folder *Folder) PutObjectWithContext(ctx context.Context, name string, content io.Reader) error {
	tracelog.DebugLogger.Printf("Put %v into %v\n", name, folder.path)
	path := storage.JoinPath(folder.path, name)
	//put the object in the cloud using full path
	_, err := folder.connection.ObjectPut(ctx, folder.container.Name, path, content, false, "", "", nil)
	if err != nil {
		return fmt.Errorf("put Swift object %q: %w", path, err)
	}
	//Object stored successfully
	return nil
}

func (folder *Folder) CopyObject(srcPath string, dstPath string) error {
	if exists, err := folder.Exists(srcPath); !exists {
		if err == nil {
			return storage.NewObjectNotFoundError(srcPath)
		}
		return fmt.Errorf("check if Swift object exists %q: %w", srcPath, err)
	}
	srcPath = storage.JoinPath(folder.path, srcPath)
	dstPath = storage.JoinPath(folder.path, dstPath)
	_, err := folder.connection.ObjectCopy(context.Background(), folder.container.Name, srcPath, folder.container.Name, dstPath, nil)
	if err != nil {
		return fmt.Errorf("copy Swift object %q -> %q: %w", srcPath, dstPath, err)
	}
	return nil
}

func (folder *Folder) DeleteObjects(objectRelativePaths []string) error {
	for _, objectRelativePath := range objectRelativePaths {
		path := storage.JoinPath(folder.path, objectRelativePath)
		tracelog.DebugLogger.Printf("Delete object %v\n", path)
		err := folder.connection.ObjectDelete(context.Background(), folder.container.Name, path)
		if err == swift.ObjectNotFound {
			continue
		}
		if err != nil {
			return fmt.Errorf("delete Swift object %q: %w", path, err)
		}
	}
	return nil
}

func (folder *Folder) Validate() error {
	return nil
}

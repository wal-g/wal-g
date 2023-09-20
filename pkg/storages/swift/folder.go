package swift

import (
	"context"
	"hash/fnv"
	"io"
	"os"
	"strings"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"

	"github.com/ncw/swift/v2"
)

var SettingList = []string{
	"OS_USERNAME",
	"OS_PASSWORD",
	"OS_AUTH_URL",
	"OS_TENANT_NAME",
	"OS_REGION_NAME",
}

func NewError(err error, format string, args ...interface{}) storage.Error {
	return storage.NewError(err, "Swift", format, args...)
}

func NewFolderError(err error, format string, args ...interface{}) storage.Error {
	return storage.NewError(err, "Swift", format, args...)
}

func NewFolder(connection *swift.Connection, container swift.Container, path string) *Folder {
	path = strings.TrimPrefix(path, "/")
	return &Folder{connection, container, path}
}

func ConfigureFolder(prefix string, settings map[string]string) (storage.HashableFolder, error) {
	connection := new(swift.Connection)
	//Set settings as env variables
	for prop, value := range settings {
		os.Setenv(prop, value)
	}
	ctx := context.Background()
	//users must set conventional openStack environment variables: username, key, auth-url, tenantName, region etc
	err := connection.ApplyEnvironment()
	if err != nil {
		return nil, NewError(err, "Unable to apply env variables")
	}
	err = connection.Authenticate(ctx)
	if err != nil {
		return nil, NewError(err, "Unable to authenticate connection")
	}
	containerName, path, err := storage.GetPathFromPrefix(prefix)
	if err != nil {
		return nil, NewError(err, "Unable to get container name and path from prefix %v", prefix)
	}
	path = storage.AddDelimiterToPath(path)

	container, _, err := connection.Container(ctx, containerName)
	if err != nil {
		return nil, NewError(err, "Unable to fetch container from name %v", containerName)
	}

	return NewFolder(connection, container, path), nil
}

type Folder struct {
	connection *swift.Connection
	container  swift.Container
	path       string
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
		return false, NewError(err, "Unable to stat object %v", path)
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
				return nil, err
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
						return nil, err
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
		return nil, nil, NewError(err, "Unable to iterate %v", folder.path)
	}
	return objects, subFolders, err
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
		return nil, NewError(err, "Unable to OPEN Object %v", path)
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
		return NewError(err, "Unable to write content.")
	}
	//Object stored successfully
	return nil
}

func (folder *Folder) CopyObject(srcPath string, dstPath string) error {
	if exists, err := folder.Exists(srcPath); !exists {
		if err == nil {
			return storage.NewObjectNotFoundError(srcPath)
		}
		return err
	}
	srcPath = storage.JoinPath(folder.path, srcPath)
	dstPath = storage.JoinPath(folder.path, dstPath)
	_, err := folder.connection.ObjectCopy(context.Background(), folder.container.Name, srcPath, folder.container.Name, dstPath, nil)
	return err
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
			return NewError(err, "Unable to delete object %v", path)
		}
	}
	return nil
}

func (folder *Folder) Hash() storage.Hash {
	hash := fnv.New64a()

	addToHash := func(data []byte) {
		_, err := hash.Write(data)
		if err != nil {
			// Writing to the hash function is always successful, so it mustn't be a problem that we panic here
			panic(err)
		}
	}

	addToHash([]byte("swift"))
	addToHash([]byte(folder.container.Name))
	addToHash([]byte(folder.path))
	addToHash([]byte(folder.connection.UserName))

	return storage.Hash(hash.Sum64())
}

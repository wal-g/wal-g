package internal

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"github.com/ncw/swift"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/tracelog"
)

type SwiftFolderError struct {
	error
}

func NewSwiftFolderError(err error, format string, args ...interface{}) SwiftFolderError {
	return SwiftFolderError{errors.Wrapf(err, format, args...)}
}

func (err SwiftFolderError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

func NewSwiftFolder(connection *swift.Connection, container swift.Container, path string) *SwiftFolder {
	return &SwiftFolder{connection, container, path}
}

func ConfigureSwiftFolder(prefix string) (StorageFolder, error) {
	connection := new(swift.Connection)
	//users must set conventional openStack environment variables: username, key, auth-url, tenantName, region etc
	err := connection.ApplyEnvironment()
	if err != nil {
		return nil, NewSwiftFolderError(err, "Unable to apply env variables")
	}
	err = connection.Authenticate()
	if err != nil {
		return nil, NewSwiftFolderError(err, "Unable to authenticate Swift connection")
	}
	containerName, path, err := getPathFromPrefix(prefix)
	if err != nil {
		return nil, NewSwiftFolderError(err, "Unable to get container name and path from prefix %v", prefix)
	}
	path = addDelimiterToSwiftPath(path)

	container, _, err := connection.Container(containerName)
	if err != nil {
		return nil, NewSwiftFolderError(err, "Unable to fetch Swift container from name %v", containerName)
	}

	return NewSwiftFolder(connection, container, path), nil
}

func addDelimiterToSwiftPath(path string) string {
	if strings.HasSuffix(path, "/") || path == "" {
		return path
	}
	return path + "/"
}

type SwiftFolder struct {
	connection *swift.Connection
	container  swift.Container
	path       string
}

func (folder *SwiftFolder) GetPath() string {
	return folder.path
}

func (folder *SwiftFolder) Exists(objectRelativePath string) (bool, error) {
	path := JoinS3Path(folder.path, objectRelativePath)
	_, _, err := folder.connection.Object(folder.container.Name, path)
	if err == swift.ObjectNotFound {
		return false, nil
	}
	if err != nil {
		return false, NewSwiftFolderError(err, "Unable to stat object %v", path)
	}
	return true, nil
}

func (folder *SwiftFolder) ListFolder() (objects []StorageObject, subFolders []StorageFolder, err error) {
	//Iterate
	err = folder.connection.ObjectsWalk(folder.container.Name, &swift.ObjectsOpts{Delimiter: int32('/'), Prefix: folder.path}, func(opts *swift.ObjectsOpts) (interface{}, error) {

		objectNames, err := folder.connection.ObjectNames(folder.container.Name, opts)
		if err != nil {
			return nil, err
		} else {
			// Retrieved object names successfully.
		}
		for _, objectName := range objectNames {
			if strings.HasSuffix(objectName, "/") {
				//It is a subFolder name
				subFolders = append(subFolders, NewSwiftFolder(folder.connection, folder.container, objectName))
			} else {
				//It is a storage object name
				obj, _, err := folder.connection.Object(folder.container.Name, objectName)
				if err != nil {
					return nil, err
				}
				//trim prefix to get object's standalone name
				objName := strings.TrimPrefix(obj.Name, folder.path)
				objects = append(objects, &SwiftStorageObject{name: objName, updated: obj.LastModified})
			}
		}
		//return objectNames if a further iteration is required.
		return objectNames, err
	})
	if err != nil {
		return nil, nil, NewSwiftFolderError(err, "Unable to iterate %v", folder.path)
	}
	return
}

func (folder *SwiftFolder) GetSubFolder(subFolderRelativePath string) StorageFolder {
	return NewSwiftFolder(folder.connection, folder.container, addDelimiterToSwiftPath(JoinS3Path(folder.path, subFolderRelativePath)))
}

func (folder *SwiftFolder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	path := JoinS3Path(folder.path, objectRelativePath)
	//get the object from the cloud using full path
	cBytes, err := folder.connection.ObjectGetBytes(folder.container.Name, path)
	if err == swift.ObjectNotFound {
		return nil, NewObjectNotFoundError(path)
	}
	if err != nil {
		return nil, NewSwiftFolderError(err, "Unable to OPEN Object %v", path)
	} else {
		//retrieved object from  the cloud
	}
	readContents := bytes.NewReader(cBytes)
	return ioutil.NopCloser(readContents), nil
}

func (folder *SwiftFolder) PutObject(name string, content io.Reader) error {
	tracelog.DebugLogger.Printf("Put %v into %v\n", name, folder.path)
	path := JoinS3Path(folder.path, name)
	//put the object in the cloud using full path
	_, err := folder.connection.ObjectPut(folder.container.Name, path, content, false, "", "", nil)
	if err != nil {
		return NewSwiftFolderError(err, "Unable to write content.")
	} else {
		//Object stored successfully
	}
	return nil
}

func (folder *SwiftFolder) DeleteObjects(objectRelativePaths []string) error {
	for _, objectRelativePath := range objectRelativePaths {
		path := JoinS3Path(folder.path, objectRelativePath)
		tracelog.DebugLogger.Printf("Delete object %v\n", path)
		err := folder.connection.ObjectDelete(folder.container.Name, path)
		if err == swift.ObjectNotFound {
			continue
		}
		if err != nil {
			return NewSwiftFolderError(err, "Unable to delete object %v", path)
		}
	}
	return nil
}

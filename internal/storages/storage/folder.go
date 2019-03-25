package storage

import (
	"io"
	"path"
)

type Folder interface {
	// Path should always ends with '/'
	GetPath() string

	// Should return objects with relative paths
	ListFolder() (objects []Object, subFolders []Folder, err error)

	// Delete object, if exists
	DeleteObjects(objectRelativePaths []string) error

	Exists(objectRelativePath string) (bool, error)

	// Returns handle to subfolder. Does not have to instantiate subfolder in any material form
	GetSubFolder(subFolderRelativePath string) Folder

	// Should return ObjectNotFoundError in case, there is no such object
	ReadObject(objectRelativePath string) (io.ReadCloser, error)

	PutObject(name string, content io.Reader) error
}

func ListFolderRecursively(folder Folder) (relativePathObjects []Object, err error) {
	queue := make([]Folder, 0)
	queue = append(queue, folder)
	for len(queue) > 0 {
		currentFolder := queue[0]
		queue = queue[1:]
		objects, subFolders, err := currentFolder.ListFolder()
		relativePathObjects = append(relativePathObjects, GetRelativePathObjects(objects, currentFolder)...)
		if err != nil {
			return nil, err
		}
		queue = append(queue, subFolders...)
	}
	return relativePathObjects, nil
}

func GetRelativePathObjects(objects []Object, folder Folder) []Object {
	for i, object := range objects {
		// meaning: now it's objects from root of folder tree.
		// I think that []storage.Object is right returning type for ListFolderRecursively
		objects[i] = NewLocalObject(GetRelativePath(object, folder), object.GetLastModified())
	}
	return objects
}

func GetRelativePath(object Object, folder Folder) string {
	return path.Join(folder.GetPath(), object.GetName())
}

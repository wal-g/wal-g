package multistorage

import (
	"fmt"
	"path"
	"strings"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// ListFolderRecursively is like storage.ListFolderRecursively but it preserves the information about what storage each
// object is stored in.
func ListFolderRecursively(folder storage.Folder) (relativePathObjects []storage.Object, err error) {
	noFilter := func(string) bool { return true }
	return ListFolderRecursivelyWithFilter(folder, noFilter)
}

// ListFolderRecursivelyWithFilter is like storage.ListFolderRecursivelyWithFilter but it preserves the information
// about what storage each object is stored in.
func ListFolderRecursivelyWithFilter(
	folder storage.Folder,
	folderSelector func(path string) bool,
) (relativePathObjects []storage.Object, err error) {
	queue := make([]storage.Folder, 0)
	queue = append(queue, folder)
	for len(queue) > 0 {
		subFolder := queue[0]
		queue = queue[1:]
		objects, subFolders, err := subFolder.ListFolder()
		folderPrefix := strings.TrimPrefix(subFolder.GetPath(), folder.GetPath())
		relativePathObjects = append(relativePathObjects, prependPaths(objects, folderPrefix)...)
		if err != nil {
			return nil, err
		}

		selectedSubfolders := filterSubfolders(folder.GetPath(), subFolders, folderSelector)
		queue = append(queue, selectedSubfolders...)
	}
	return relativePathObjects, nil
}

// ListFolderRecursivelyWithPrefix is like storage.ListFolderRecursivelyWithPrefix but it preserves the information
// about what storage each object is stored in.
func ListFolderRecursivelyWithPrefix(folder storage.Folder, prefix string) (relativePathObjects []storage.Object, err error) {
	checkFile := len(prefix) > 0 && !strings.HasSuffix(prefix, "/")
	prefix = strings.Trim(prefix, "/")

	if checkFile {
		dirName, fileName := path.Split(prefix)
		parentFolder := folder.GetSubFolder(dirName)
		objects, _, err := parentFolder.ListFolder()
		if err != nil {
			return nil, fmt.Errorf("can't list folder %q: %w", dirName, err)
		}
		for _, obj := range objects {
			if obj.GetName() == fileName {
				return prependPaths([]storage.Object{obj}, dirName), nil
			}
		}
	}

	parentFolder := folder.GetSubFolder(prefix)
	objects, err := ListFolderRecursively(parentFolder)
	if err != nil {
		return nil, fmt.Errorf("can't list folder %q: %w", prefix, err)
	}
	return prependPaths(objects, prefix), nil
}

func prependPaths(objects []storage.Object, folderPrefix string) []storage.Object {
	relativePathObjects := make([]storage.Object, len(objects))
	for i, object := range objects {
		relativePathObjects[i] = multiObject{
			Object: storage.NewLocalObject(
				path.Join(folderPrefix, object.GetName()),
				object.GetLastModified(),
				object.GetSize(),
			),
			storageName: GetStorage(object),
		}
	}
	return relativePathObjects
}

// filterSubfolders returns subfolders matching the provided path selector
func filterSubfolders(
	rootFolderPath string,
	folders []storage.Folder,
	selector func(path string) bool,
) []storage.Folder {
	result := make([]storage.Folder, 0)
	for i := range folders {
		folderPath := strings.TrimPrefix(folders[i].GetPath(), rootFolderPath)
		if selector(folderPath) {
			result = append(result, folders[i])
		}
	}
	return result
}

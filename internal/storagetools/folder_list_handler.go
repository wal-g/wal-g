package storagetools

import (
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type ListElementType string

const (
	Object    ListElementType = "obj"
	Directory ListElementType = "dir"
)

type ListElement interface {
	storage.Object
	Type() ListElementType
}

type ListObject struct {
	storage.Object
}

func NewListObject(object storage.Object) *ListObject {
	return &ListObject{object}
}

func (lo *ListObject) Type() ListElementType {
	return Object
}

type ListDirectory struct {
	name string
}

func NewListDirectory(folder storage.Folder, rootFolder storage.Folder) *ListDirectory {
	return &ListDirectory{name: strings.TrimPrefix(folder.GetPath(), rootFolder.GetPath())}
}

func (ld *ListDirectory) GetName() string {
	return ld.name
}

func (ld *ListDirectory) GetLastModified() time.Time {
	return time.Time{}
}

func (ld *ListDirectory) GetSize() int64 {
	return 0
}

func (ld *ListDirectory) Type() ListElementType {
	return Directory
}

func HandleFolderList(folder storage.Folder, recursive bool) error {
	var list []ListElement
	var folderObjects []storage.Object
	var err error

	if recursive {
		folderObjects, err = storage.ListFolderRecursively(folder)
	} else {
		var subFolders []storage.Folder
		folderObjects, subFolders, err = folder.ListFolder()
		for i := range subFolders {
			list = append(list, NewListDirectory(subFolders[i], folder))
		}
	}
	if err != nil {
		return fmt.Errorf("list folder: %v", err)
	}

	for i := range folderObjects {
		list = append(list, NewListObject(folderObjects[i]))
	}

	err = WriteObjectsList(list, os.Stdout)
	if err != nil {
		return fmt.Errorf("write folder listing: %v", err)
	}

	return nil
}

func WriteObjectsList(objects []ListElement, output io.Writer) error {
	writer := tabwriter.NewWriter(output, 0, 0, 1, ' ', 0)
	defer writer.Flush()
	_, err := fmt.Fprintln(writer, "type\tsize\tlast modified\tname")
	if err != nil {
		return err
	}
	for _, o := range objects {
		_, err = fmt.Fprintf(writer, "%s\t%d\t%s\t%s\n", o.Type(), o.GetSize(), o.GetLastModified(), o.GetName())
		if err != nil {
			return err
		}
	}
	return nil
}

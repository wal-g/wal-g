package dirtyhands

import (
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func HandleFolderList(folder storage.Folder) {
	folderObjects, err := storage.ListFolderRecursively(folder)
	tracelog.ErrorLogger.FatalfOnError("Failed to list the folder: %v", err)

	err = WriteObjectsList(folderObjects, os.Stdout)
	tracelog.ErrorLogger.FatalfOnError("Failed to write the folder listing: %v", err)
}

func WriteObjectsList(objects []storage.Object, output io.Writer) error {
	writer := tabwriter.NewWriter(output, 0, 0, 1, ' ', 0)
	defer writer.Flush()
	_, err := fmt.Fprintln(writer, "size\tlast modified\tname")
	if err != nil {
		return err
	}
	for _, o := range objects {
		_, err = fmt.Fprintf(writer, "%d\t%s\t%s\n", o.GetSize(), o.GetLastModified(), o.GetName())
		if err != nil {
			return err
		}
	}
	return nil
}

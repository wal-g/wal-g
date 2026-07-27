package storagetools

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// HandleStatObject prints metadata of a single object in the same format as the folder listing does. Unlike listing,
// it fetches the metadata with a cheap point lookup (HEAD/stat) instead of enumerating the whole folder.
func HandleStatObject(ctx context.Context, objectPath string, folder storage.Folder) error {
	return statObject(ctx, objectPath, folder, os.Stdout)
}

func statObject(ctx context.Context, objectPath string, folder storage.Folder, output io.Writer) error {
	object, err := folder.StatObject(ctx, objectPath)
	if err != nil {
		return fmt.Errorf("stat object %q: %w", objectPath, err)
	}

	err = WriteObjectsList([]ListElement{NewListObject(object)}, output)
	if err != nil {
		return fmt.Errorf("write object stat: %w", err)
	}
	return nil
}

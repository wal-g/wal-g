package st

import (
	"fmt"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func handleGlobPattern(folder storage.Folder, pathPattern string, handleCommandObject func(string) error) error {
	objectPaths, err := storage.Glob(folder, pathPattern)
	if err != nil {
		return err
	}
	var allErrors []error
	for _, path := range objectPaths {
		err := handleCommandObject(path)
		if err != nil {
			allErrors = append(allErrors, err)
		}
	}
	if len(allErrors) > 0 {
		return fmt.Errorf("%v", allErrors)
	}
	return nil
}

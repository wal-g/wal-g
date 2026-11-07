package fs

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func (folder *Folder) WalkObjects(ctx context.Context, visit func(storage.Object) error) error {
	var walk func(string) error
	walk = func(prefix string) error {
		f, err := os.Open(folder.GetFilePath(prefix))
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		defer f.Close()
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			entries, err := f.ReadDir(256)
			if err != nil && !errors.Is(err, io.EOF) {
				return err
			}
			for _, entry := range entries {
				key := filepath.Join(prefix, entry.Name())
				if entry.IsDir() {
					if err := walk(key); err != nil {
						return err
					}
					continue
				}
				if storage.HasTimestampRandomTmpSuffix(entry.Name()) {
					continue
				}
				info, err := entry.Info()
				if err != nil {
					return err
				}
				if err := visit(storage.NewLocalObject(filepath.ToSlash(key), info.ModTime(), info.Size())); err != nil {
					return err
				}
			}
			if errors.Is(err, io.EOF) {
				return nil
			}
		}
	}
	return walk("")
}

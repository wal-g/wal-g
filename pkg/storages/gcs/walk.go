package gcs

import (
	"context"
	"strings"

	gcs "cloud.google.com/go/storage"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"google.golang.org/api/iterator"
)

func (folder *Folder) WalkObjects(ctx context.Context, visit func(storage.Object) error) error {
	prefix := storage.AddDelimiterToPath(folder.path)
	ctx, cancel := folder.createTimeoutContext(ctx)
	defer cancel()
	objects := folder.bucket.Objects(ctx, &gcs.Query{Prefix: prefix})
	for {
		obj, err := objects.Next()
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return err
		}
		key := strings.TrimPrefix(obj.Name, prefix)
		if err := visit(storage.NewLocalObject(key, obj.Updated, obj.Size)); err != nil {
			return err
		}
	}
}

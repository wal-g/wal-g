package swift

import (
	"context"
	"strings"

	"github.com/ncw/swift/v2"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func (folder *Folder) WalkObjects(ctx context.Context, visit func(storage.Object) error) error {
	prefix := storage.AddDelimiterToPath(folder.path)
	return folder.connection.ObjectsWalk(ctx, folder.container.Name, &swift.ObjectsOpts{Prefix: prefix},
		func(ctx context.Context, opts *swift.ObjectsOpts) (interface{}, error) {
			objects, err := folder.connection.Objects(ctx, folder.container.Name, opts)
			if err != nil {
				return nil, err
			}
			for i := range objects {
				obj := &objects[i]
				key := strings.TrimPrefix(obj.Name, prefix)
				if err := visit(storage.NewLocalObject(key, obj.LastModified, obj.Bytes)); err != nil {
					return nil, err
				}
			}
			return objects, nil
		})
}

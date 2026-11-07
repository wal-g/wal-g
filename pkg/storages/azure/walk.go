package azure

import (
	"context"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func (folder *Folder) WalkObjects(ctx context.Context, visit func(storage.Object) error) error {
	prefix := storage.AddDelimiterToPath(folder.path)
	pages := folder.containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{Prefix: &prefix})
	for pages.More() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return err
		}
		for _, obj := range page.Segment.BlobItems {
			key := strings.TrimPrefix(*obj.Name, prefix)
			if err := visit(storage.NewLocalObject(key, *obj.Properties.LastModified, *obj.Properties.ContentLength)); err != nil {
				return err
			}
		}
	}
	return nil
}

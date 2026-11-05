package internal

import (
	"context"
	"fmt"
	"strings"

	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func deleteObjectsPhased(ctx context.Context, folder storage.Folder, objects []storage.Object) error {
	var sentinels, markers, payload []storage.Object
	for _, obj := range objects {
		if strings.HasSuffix(obj.GetName(), utility.SentinelSuffix) {
			if strings.Contains(obj.GetAdditionalInfo(), "DELETE") {
				markers = append(markers, obj)
			} else {
				sentinels = append(sentinels, obj)
			}
		} else {
			payload = append(payload, obj)
		}
	}
	for _, batch := range [][]storage.Object{sentinels, markers} {
		if len(batch) > 0 {
			if err := folder.DeleteObjects(ctx, batch); err != nil {
				return err
			}
		}
	}
	seen := map[string]bool{}
	for _, obj := range append(sentinels, markers...) {
		identity := multistorage.GetStorage(obj) + "/" + obj.GetName()
		if seen[identity] {
			continue
		}
		seen[identity] = true
		selected, err := multistorage.UseSpecificStorage(ctx, multistorage.GetStorage(obj), folder)
		if err != nil {
			return err
		}
		exists, err := selected.Exists(ctx, obj.GetName())
		if err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("sentinel deletion unconfirmed: %s", identity)
		}
	}
	for len(payload) > 0 {
		n := min(len(payload), 500)
		if err := folder.DeleteObjects(ctx, payload[:n]); err != nil {
			return err
		}
		payload = payload[n:]
	}
	return nil
}

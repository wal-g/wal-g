package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// syntheticRootFolder serves a generated WAL folder instead of storing millions
// of objects in the in-memory KVS, whose Store is linear in its size.
type syntheticRootFolder struct {
	storage.Folder
	wal storage.Folder
}

func (f syntheticRootFolder) GetSubFolder(path string) storage.Folder {
	if path == utility.WalPath {
		return f.wal
	}
	return f.Folder.GetSubFolder(path)
}

// syntheticWalFolder lists segments [1, count] of timeline 1 with a gap every
// gapEvery segments. ListFolder materializes all objects like the S3 backend does;
// WalkObjects yields them one by one like a paginated S3 walk.
type syntheticWalFolder struct {
	storage.Folder
	count, gapEvery int
}

func (f syntheticWalFolder) names(visit func(string) error) error {
	for no := 1; no <= f.count; no++ {
		if no%f.gapEvery == 0 {
			continue
		}
		if err := visit(postgres.WalSegmentNo(no).GetFilename(1) + ".br"); err != nil {
			return err
		}
	}
	return nil
}

func (f syntheticWalFolder) ListFolder(context.Context) ([]storage.Object, []storage.Folder, error) {
	var objects []storage.Object
	err := f.names(func(name string) error {
		objects = append(objects, storage.NewLocalObject(name, time.Time{}, 0))
		return nil
	})
	return objects, nil, err
}

func (f syntheticWalFolder) WalkObjects(_ context.Context, visit func(storage.Object) error) error {
	return f.names(func(name string) error {
		return visit(storage.NewLocalObject(name, time.Time{}, 0))
	})
}

// BenchmarkWalVerifyLargeWalFolder measures wal-verify allocations for a WAL
// folder with many segments. Memory must scale with timelines and gaps, not
// with the number of WAL objects, so watch B/op when changing this path.
func BenchmarkWalVerifyLargeWalFolder(b *testing.B) {
	for _, segmentsCount := range []int{100_000, 1_000_000, 3_000_000} {
		b.Run(fmt.Sprintf("segments=%d", segmentsCount), func(b *testing.B) {
			ctx := context.Background()
			memRoot := setupTestStorageFolder()
			rootFolder := syntheticRootFolder{
				Folder: memRoot,
				wal:    syntheticWalFolder{Folder: memRoot.GetSubFolder(utility.WalPath), count: segmentsCount, gapEvery: 250_000},
			}
			current := postgres.WalSegmentDescription{Timeline: 1, Number: postgres.WalSegmentNo(segmentsCount + 1)}
			checkTypes := []postgres.WalVerifyCheckType{postgres.WalVerifyTimelineCheck, postgres.WalVerifyIntegrityCheck}
			params := postgres.BackupSearchParams{FindEarliestBackup: true}

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				writer := &MockWalVerifyOutputWriter{}
				postgres.HandleWalVerify(ctx, checkTypes, rootFolder, current, params, writer)
				if writer.writeCallsCount != 1 {
					b.Fatal("wal-verify did not write a result")
				}
			}
		})
	}
}

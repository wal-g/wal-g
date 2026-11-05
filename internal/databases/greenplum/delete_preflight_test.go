package greenplum

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type preflightObservedFolder struct {
	storage.Folder
	writes     *atomic.Int64
	unreadable string
}

func (f preflightObservedFolder) GetSubFolder(n string) storage.Folder {
	return preflightObservedFolder{f.Folder.GetSubFolder(n), f.writes, f.unreadable}
}
func (f preflightObservedFolder) DeleteObjects(ctx context.Context, objects []storage.Object) error {
	f.writes.Add(1)
	return f.Folder.DeleteObjects(ctx, objects)
}
func (f preflightObservedFolder) PutObject(ctx context.Context, key string, r io.Reader) error {
	f.writes.Add(1)
	return f.Folder.PutObject(ctx, key, r)
}
func (f preflightObservedFolder) ReadObject(ctx context.Context, key string) (io.ReadCloser, error) {
	if f.unreadable != "" && strings.Contains(f.GetPath()+key, f.unreadable) {
		return nil, errors.New("GET 500")
	}
	return f.Folder.ReadObject(ctx, key)
}

func preflightClusterFixture(t *testing.T) storage.Folder {
	t.Helper()
	now := time.Unix(100, 0)
	root := memory.NewFolder("", memory.NewKVS(memory.WithCustomTime(func() time.Time { now = now.Add(time.Second); return now })))
	for i, name := range []string{"backup_1", "backup_2"} {
		s := BackupSentinelDto{StartTime: time.Unix(int64(i+1), 0), FinishTime: time.Unix(int64(i+2), 0)}
		pgName := []string{firstSegBackup, secondSegBackup}[i]
		for id := 0; id < 2; id++ {
			identity := fmt.Sprintf("backup-%d-segment-%d", i, id)
			s.Segments = append(s.Segments, SegmentMetadata{ContentID: id, BackupID: identity, BackupName: pgName})
			base := root.GetSubFolder(FormatSegmentStoragePrefix(id)).GetSubFolder(utility.BaseBackupPath)
			start, end := postgres.LSN(i+1), postgres.LSN(i+2)
			require.NoError(t, internal.UploadDto(t.Context(), base, postgres.BackupSentinelDto{BackupStartLSN: &start, BackupFinishLSN: &end}, pgName+utility.SentinelSuffix))
			require.NoError(t, internal.UploadDto(t.Context(), base, postgres.ExtendedMetadataDto{StartTime: s.StartTime, FinishTime: s.FinishTime, StartLsn: start, FinishLsn: end, UserData: NewSegmentUserDataFromID(identity)}, pgName+"/metadata.json"))
			require.NoError(t, base.PutObject(t.Context(), pgName+"/payload", strings.NewReader("payload")))
			for _, manifest := range []string{"ao_files_metadata.json", "pax_files_metadata.json"} {
				require.NoError(t, base.PutObject(t.Context(), pgName+"/"+manifest, strings.NewReader(`{"Files":{}}`)))
			}
		}
		require.NoError(t, internal.UploadDto(t.Context(), root.GetSubFolder(utility.BaseBackupPath), s, name+utility.SentinelSuffix))
	}
	return root
}

func TestOrdinaryDeletePreflightsAllSegments(t *testing.T) {
	temporary := t.TempDir()
	t.Setenv("TMPDIR", temporary)
	old := viper.Get(conf.GPDeleteConcurrency)
	defer viper.Set(conf.GPDeleteConcurrency, old)
	for _, concurrency := range []int{1, 2} {
		viper.Set(conf.GPDeleteConcurrency, concurrency)
		for _, kind := range []SegDeleteType{SegDeleteTarget, SegDeleteBefore} {
			for _, damage := range []string{"missing", "malformed", "unreadable"} {
				for _, confirmed := range []bool{false, true} {
					t.Run(fmt.Sprintf("%d/%d/%s/%t", concurrency, kind, damage, confirmed), func(t *testing.T) {
						root := preflightClusterFixture(t)
						key := path.Join(FormatSegmentStoragePrefix(1), utility.BaseBackupPath, secondSegBackup, "ao_files_metadata.json")
						switch damage {
						case "missing":
							require.NoError(t, root.DeleteObjects(t.Context(), []storage.Object{storage.NewLocalObject(key, time.Time{}, 0)}))
						case "malformed":
							require.NoError(t, root.PutObject(t.Context(), key, strings.NewReader("broken")))
						}
						var writes atomic.Int64
						observed := preflightObservedFolder{Folder: root, writes: &writes}
						if damage == "unreadable" {
							observed.unreadable = key
						}
						h, err := NewDeleteHandler(t.Context(), observed, DeleteArgs{Confirmed: confirmed, Force: true})
						require.NoError(t, err)
						targetName := "backup_1"
						if kind == SegDeleteBefore {
							targetName = "backup_2"
						}
						target, err := h.FindTargetByName(targetName)
						require.NoError(t, err)
						err = h.dispatchDeleteCmd(t.Context(), target, kind)
						require.Error(t, err)
						require.Zero(t, writes.Load(), "preflight must stop the entire command before any storage writes")
						entries, readErr := os.ReadDir(temporary)
						require.NoError(t, readErr)
						require.Empty(t, entries, "failed preparation must close and remove every temporary inventory")
						exists, err := root.GetSubFolder(FormatSegmentStoragePrefix(0)).GetSubFolder(utility.BaseBackupPath).Exists(t.Context(), firstSegBackup+utility.SentinelSuffix)
						require.NoError(t, err)
						require.True(t, exists)
					})
				}
			}
		}
	}
}

func TestOrdinaryDeleteExecutesPreparedSegments(t *testing.T) {
	old := viper.Get(conf.GPDeleteConcurrency)
	viper.Set(conf.GPDeleteConcurrency, 2)
	defer viper.Set(conf.GPDeleteConcurrency, old)
	for _, kind := range []SegDeleteType{SegDeleteTarget, SegDeleteBefore} {
		for _, confirmed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/%t", kind, confirmed), func(t *testing.T) {
				root := preflightClusterFixture(t)
				h, err := NewDeleteHandler(t.Context(), root, DeleteArgs{Confirmed: confirmed})
				require.NoError(t, err)
				name := "backup_1"
				if kind == SegDeleteBefore {
					name = "backup_2"
				}
				target, err := h.FindTargetByName(name)
				require.NoError(t, err)
				require.NoError(t, h.dispatchDeleteCmd(t.Context(), target, kind))
				for id := 0; id < 2; id++ {
					base := root.GetSubFolder(FormatSegmentStoragePrefix(id)).GetSubFolder(utility.BaseBackupPath)
					exists, err := base.Exists(t.Context(), firstSegBackup+"/payload")
					require.NoError(t, err)
					require.Equal(t, !confirmed, exists)
					exists, err = base.Exists(t.Context(), secondSegBackup+utility.SentinelSuffix)
					require.NoError(t, err)
					require.True(t, exists)
				}
			})
		}
	}
}

func TestSegmentProtectionEmptyCatalogAndMissingMapping(t *testing.T) {
	root := memory.NewFolder("", memory.NewKVS())
	_, err := NewSegDeleteHandler(t.Context(), root, 0, DeleteArgs{}, SegDeleteTarget)
	require.NoError(t, err)
	populated := preflightClusterFixture(t)
	base := populated.GetSubFolder(FormatSegmentStoragePrefix(0)).GetSubFolder(utility.BaseBackupPath)
	require.NoError(t, base.DeleteObjects(t.Context(), []storage.Object{storage.NewLocalObject(firstSegBackup+utility.SentinelSuffix, time.Time{}, 0)}))
	_, err = NewSegDeleteHandler(t.Context(), populated, 0, DeleteArgs{}, SegDeleteTarget)
	require.ErrorContains(t, err, "missing mapped segment backup")
}

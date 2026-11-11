package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression/lz4"
	"github.com/wal-g/wal-g/pkg/storages/fs"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type noListTrace struct {
	root, catalog string
	lists         map[string]int
	allowed       []string
	failList      string
	deleted       []string
	largestBatch  int
	failKey       string
	keepSentinels bool
	versioned     bool
}

type noListTestFolder struct {
	storage.Folder
	trace *noListTrace
}

func (f noListTestFolder) GetSubFolder(name string) storage.Folder {
	return noListTestFolder{f.Folder.GetSubFolder(name), f.trace}
}

func (f noListTestFolder) ListFolder(ctx context.Context) ([]storage.Object, []storage.Folder, error) {
	prefix := f.GetPath()
	f.trace.lists[prefix]++
	allowed := prefix == f.trace.catalog
	for _, selected := range f.trace.allowed {
		allowed = allowed || strings.HasPrefix(prefix, selected)
	}
	if !allowed || f.trace.lists[prefix] > 1 {
		return nil, nil, fmt.Errorf("unexpected LIST %s", f.GetPath())
	}
	if prefix == f.trace.failList {
		return nil, nil, fmt.Errorf("injected listing error: %s", prefix)
	}
	objects, folders, err := f.Folder.ListFolder(ctx)
	for i := range folders {
		folders[i] = noListTestFolder{folders[i], f.trace}
	}
	return objects, folders, err
}

func (f noListTestFolder) DeleteObjects(ctx context.Context, objects []storage.Object) error {
	f.trace.largestBatch = max(f.trace.largestBatch, len(objects))
	for _, object := range objects {
		key := strings.TrimPrefix(f.GetPath(), f.trace.root) + object.GetName()
		if key == f.trace.failKey {
			return fmt.Errorf("injected deletion error: %s", key)
		}
		f.trace.deleted = append(f.trace.deleted, key)
	}
	if f.trace.keepSentinels && strings.HasSuffix(objects[0].GetName(), utility.SentinelSuffix) {
		return nil // The backend returned success but the sentinel is still visible.
	}
	return f.Folder.DeleteObjects(ctx, objects)
}

func (f noListTestFolder) GetVersioningEnabled(context.Context) bool { return f.trace.versioned }

func traceNoListFolder(root storage.Folder, selected ...string) noListTestFolder {
	trace := &noListTrace{
		root: root.GetPath(), catalog: root.GetSubFolder(utility.BaseBackupPath).GetPath(),
		lists: map[string]int{},
	}
	for _, name := range selected {
		trace.allowed = append(trace.allowed, root.GetSubFolder(utility.BaseBackupPath+name).GetPath())
	}
	return noListTestFolder{root, trace}
}

func putNoListBackup(t *testing.T, root storage.Folder, segment uint64, parent string, permanent bool) string {
	t.Helper()
	return putNoListTimelineBackup(t, root, 1, segment, parent, permanent)
}

func putNoListTimelineBackup(t *testing.T, root storage.Folder, timeline uint32,
	segment uint64, parent string, permanent bool) string {
	t.Helper()
	name := "base_" + WalSegmentNo(segment).GetFilename(timeline)
	if parent != "" {
		name += "_D_" + strings.TrimPrefix(parent, "base_")
	}
	start, finish := LSN(segment*WalSegmentSize+1), LSN((segment+1)*WalSegmentSize+42)
	systemID := uint64(42)
	sentinel := BackupSentinelDto{BackupStartLSN: &start, BackupFinishLSN: &finish, SystemIdentifier: &systemID,
		StartWalFileName: WalSegmentNo(segment).GetFilename(timeline),
		EndWalFileName:   WalSegmentNo(segment + 1).GetFilename(timeline)}
	if parent != "" {
		_, parentSegment, err := ParseWALFilename(strings.TrimPrefix(parent, "base_"))
		require.NoError(t, err)
		count, from := 1, LSN(parentSegment*WalSegmentSize+1)
		sentinel.IncrementFrom, sentinel.IncrementFullName = &parent, &parent
		sentinel.IncrementCount, sentinel.IncrementFromLSN = &count, &from
	}
	files := FilesMetadataDto{TarFileSets: map[string][]string{
		"part_001.tar.lz4": {"base/1/123"}, "part_999.tar.zst": {"base/1/456"}, "backup_label.tar.lz4": {"backup_label"},
	}}
	base := root.GetSubFolder(utility.BaseBackupPath)
	for _, tar := range []string{"part_001.tar.lz4", "part_999.tar.zst", "backup_label.tar.lz4", "pg_control.tar.lz4"} {
		require.NoError(t, base.PutObject(t.Context(), internal.GetBackupTarPath(name, tar), strings.NewReader("tar")))
	}
	handler := BackupHandler{
		CurBackupInfo: CurBackupInfo{Name: name, StartTime: time.Unix(int64(segment), 0)},
		Arguments:     BackupArguments{Uploader: internal.NewRegularUploader(lz4.Compressor{}, base), isPermanent: permanent},
	}
	handler.uploadMetadata(t.Context(), sentinel, files)
	require.NoError(t, base.PutObject(t.Context(), internal.JournalPrefix+name, strings.NewReader("{}")))
	return name
}

func TestDeleteWithoutListing(t *testing.T) {
	for _, backend := range []string{"memory", "fs"} {
		for _, size := range []uint64{16, 64} {
			t.Run(fmt.Sprintf("%s/%dMB", backend, size), func(t *testing.T) {
				oldSize := WalSegmentSize
				SetWalSize(size)
				t.Cleanup(func() { SetWalSize(oldSize >> 20) })
				var root storage.Folder = memory.NewFolder("tenant/postgres/", memory.NewKVS())
				if backend == "fs" {
					root = fs.NewFolder(t.TempDir(), "").GetSubFolder("tenant/postgres")
				}
				old := putNoListBackup(t, root, 1, "", false)
				delta := putNoListBackup(t, root, 3, old, false)
				permanent := putNoListBackup(t, root, 5, "", true)
				keep := putNoListBackup(t, root, 66, "", false)
				putNoListBackup(t, root, 70, keep, false)
				marker := path.Join(backupReservationsPath, utility.BaseBackupPath, old+".json")
				require.NoError(t, root.PutObject(t.Context(), marker, strings.NewReader("{}")))
				wal := root.GetSubFolder(utility.WalPath)
				for n := uint64(0); n <= 67; n++ {
					if n == 8 { // Missing WAL must not terminate arithmetic enumeration.
						continue
					}
					key := WalSegmentNo(n).GetFilename(1)
					for _, suffix := range []string{".lz4", ".zst", ".json", "_part.lz4"} {
						require.NoError(t, wal.PutObject(t.Context(), key+suffix, strings.NewReader("wal")))
					}
				}
				for _, key := range []string{"00000002.history", WalSegmentNo(2).GetFilename(2) + ".lz4",
					WalSegmentNo(16).GetFilename(1) + "_delta.lz4", WalSegmentNo(64).GetFilename(1) + "_delta.lz4"} {
					require.NoError(t, wal.PutObject(t.Context(), key, strings.NewReader("wal")))
				}
				// Neither numbered tar gaps nor unknown nested sidecars should survive.
				require.NoError(t, root.PutObject(t.Context(), utility.BaseBackupPath+old+"/extra/nested/object",
					strings.NewReader("sidecar")))
				folder := traceNoListFolder(root, old, delta)
				plan, err := PlanDeleteWithoutListing(t.Context(), folder, NoListDeleteOptions{
					Retain: 1, Modifier: internal.FullDeleteModifier,
				})
				require.NoError(t, err)
				t.Cleanup(plan.Close)
				require.Equal(t, 1, folder.trace.lists[folder.trace.catalog])
				listingCount := len(folder.trace.lists)
				require.NoError(t, plan.Execute(t.Context(), false))
				require.Empty(t, folder.trace.deleted)
				require.NoError(t, plan.Execute(t.Context(), true))
				require.Len(t, folder.trace.lists, listingCount)
				require.Equal(t, 500, folder.trace.largestBatch)
				for _, name := range []string{old, delta} {
					objects, err := storage.ListFolderRecursively(t.Context(), root.GetSubFolder(utility.BaseBackupPath+name))
					require.NoError(t, err)
					require.Empty(t, objects)
					assertNoListExists(t, root, utility.BaseBackupPath+name+utility.SentinelSuffix, false)
					assertNoListExists(t, root, utility.BaseBackupPath+internal.JournalPrefix+name, false)
				}
				for _, name := range []string{permanent, keep} {
					assertNoListExists(t, root, utility.BaseBackupPath+name+utility.SentinelSuffix, true)
					assertNoListExists(t, root, utility.BaseBackupPath+name+"/tar_partitions/pg_control.tar.lz4", true)
				}
				assertNoListExists(t, root, marker, true)
				for n := uint64(0); n <= 67; n++ {
					if n == 8 {
						continue
					}
					for _, suffix := range []string{".lz4", ".zst", ".json", "_part.lz4"} {
						assertNoListExists(t, wal, WalSegmentNo(n).GetFilename(1)+suffix, n == 0 || n == 5 || n == 6 || n >= 66)
					}
				}
				assertNoListExists(t, wal, "00000002.history", true)
				assertNoListExists(t, wal, WalSegmentNo(2).GetFilename(2)+".lz4", true)
				assertNoListExists(t, wal, WalSegmentNo(16).GetFilename(1)+"_delta.lz4", false)
				assertNoListExists(t, wal, WalSegmentNo(64).GetFilename(1)+"_delta.lz4", true)
				// Reusing an already prepared plan is idempotent, including absent keys.
				require.NoError(t, plan.Execute(t.Context(), true))
			})
		}
	}
}

func assertNoListExists(t *testing.T, root storage.Folder, key string, want bool) {
	t.Helper()
	exists, err := root.Exists(t.Context(), key)
	require.NoError(t, err)
	require.Equal(t, want, exists, key)
}

func TestDeleteWithoutListingUsesExistingMetadata(t *testing.T) {
	for _, format := range []string{"legacy_sentinel", "files_metadata_disabled"} {
		t.Run(format, func(t *testing.T) {
			root := memory.NewFolder("tenant/", memory.NewKVS())
			old := putNoListBackup(t, root, 1, "", false)
			keep := putNoListBackup(t, root, 10, "", false)
			base := root.GetSubFolder(utility.BaseBackupPath)
			var sentinel map[string]json.RawMessage
			require.NoError(t, internal.FetchDto(t.Context(), base, &sentinel, old+utility.SentinelSuffix))
			require.NotContains(t, sentinel, "TarFiles")
			if format == "legacy_sentinel" {
				delete(sentinel, "Version")
				delete(sentinel, "IsPermanent")
			} else {
				sentinel["FilesMetadataDisabled"] = json.RawMessage(`true`)
				require.NoError(t, base.DeleteObjects(t.Context(), []storage.Object{
					storage.NewLocalObject(getFilesMetadataPath(old), time.Time{}, 0),
				}))
			}
			require.NoError(t, internal.UploadDto(t.Context(), base, sentinel, old+utility.SentinelSuffix))
			wal := root.GetSubFolder(utility.WalPath)
			key := WalSegmentNo(1).GetFilename(1) + ".lz4"
			require.NoError(t, wal.PutObject(t.Context(), key, strings.NewReader("wal")))
			folder := traceNoListFolder(root, old)
			plan, err := PlanDeleteWithoutListing(t.Context(), folder, NoListDeleteOptions{Before: keep})
			require.NoError(t, err)
			t.Cleanup(plan.Close)
			require.NoError(t, plan.Execute(t.Context(), true))
			assertNoListExists(t, wal, key, false)
			assertNoListExists(t, base, old+utility.SentinelSuffix, false)
			assertNoListExists(t, base, old+"/tar_partitions/pg_control.tar.lz4", false)
			assertNoListExists(t, base, keep+utility.SentinelSuffix, true)
		})
	}
}

func TestDeleteWithoutListingTimelineRanges(t *testing.T) {
	root := memory.NewFolder("tenant/", memory.NewKVS())
	old := putNoListTimelineBackup(t, root, 1, 1, "", false)
	newer := putNoListTimelineBackup(t, root, 2, 4, "", false)
	keep := putNoListTimelineBackup(t, root, 3, 10, "", false)
	wal := root.GetSubFolder(utility.WalPath)
	for timeline := uint32(1); timeline <= 4; timeline++ {
		for n := uint64(0); n <= 10; n++ {
			require.NoError(t, wal.PutObject(t.Context(), WalSegmentNo(n).GetFilename(timeline)+".lz4", strings.NewReader("wal")))
		}
	}
	plan, err := PlanDeleteWithoutListing(t.Context(), traceNoListFolder(root, old, newer), NoListDeleteOptions{Before: keep})
	require.NoError(t, err)
	t.Cleanup(plan.Close)
	require.NoError(t, plan.Execute(t.Context(), true))
	for timeline := uint32(1); timeline <= 4; timeline++ {
		for n := uint64(0); n <= 10; n++ {
			deleted := n < 10 && ((timeline == 1 && n >= 1) || (timeline == 2 && n >= 4))
			assertNoListExists(t, wal, WalSegmentNo(n).GetFilename(timeline)+".lz4", !deleted)
		}
	}
}

func TestDeleteWithoutListingBoundsFollowSelectedMetadata(t *testing.T) {
	root := memory.NewFolder("tenant/", memory.NewKVS())
	putNoListBackup(t, root, 1, "", true)
	old := putNoListBackup(t, root, 5, "", false)
	keep := putNoListBackup(t, root, 10, "", false)
	base := root.GetSubFolder(utility.BaseBackupPath)
	// Exact LSN boundaries belong to the preceding WAL segment. The permanent
	// backup must not make each deletion enumerate the entire archive history.
	for name, first := range map[string]uint64{old: 5, keep: 10} {
		start := LSN((first + 1) * WalSegmentSize)
		var sentinel map[string]json.RawMessage
		require.NoError(t, internal.FetchDto(t.Context(), base, &sentinel, name+utility.SentinelSuffix))
		sentinel["LSN"], _ = json.Marshal(start)
		require.NoError(t, internal.UploadDto(t.Context(), base, sentinel, name+utility.SentinelSuffix))
		var meta ExtendedMetadataDto
		require.NoError(t, internal.FetchDto(t.Context(), base, &meta, name+"/metadata.json"))
		meta.StartLsn = start
		require.NoError(t, internal.UploadDto(t.Context(), base, meta, name+"/metadata.json"))
	}
	wal := root.GetSubFolder(utility.WalPath)
	for n := uint64(0); n <= 10; n++ {
		require.NoError(t, wal.PutObject(t.Context(), WalSegmentNo(n).GetFilename(1)+".lz4", strings.NewReader("wal")))
	}
	plan, err := PlanDeleteWithoutListing(t.Context(), traceNoListFolder(root, old), NoListDeleteOptions{Before: keep})
	require.NoError(t, err)
	t.Cleanup(plan.Close)
	require.NoError(t, plan.Execute(t.Context(), true))
	for n := uint64(0); n <= 10; n++ {
		assertNoListExists(t, wal, WalSegmentNo(n).GetFilename(1)+".lz4", n < 5 || n >= 10)
	}
	// With no selected backups, a fresh run has no WAL interval to enumerate.
	folder := traceNoListFolder(root)
	again, err := PlanDeleteWithoutListing(t.Context(), folder, NoListDeleteOptions{Before: keep})
	require.NoError(t, err)
	t.Cleanup(again.Close)
	require.NoError(t, again.Execute(t.Context(), true))
	require.Empty(t, folder.trace.deleted)
}

func TestDeleteWithoutListingRejectsUnknownMetadata(t *testing.T) {
	for _, failure := range []string{"missing_wal_boundaries", "unknown_protection", "incremental_boundary",
		"retained_dependency", "segment_size", "system_id", "selected_system_id", "versioned", "listing"} {
		t.Run(failure, func(t *testing.T) {
			root := memory.NewFolder("tenant/", memory.NewKVS())
			old := putNoListBackup(t, root, 1, "", false)
			keep := putNoListBackup(t, root, 10, "", false)
			base := root.GetSubFolder(utility.BaseBackupPath)
			folder := traceNoListFolder(root, old)
			options := NoListDeleteOptions{Before: keep}
			var sentinel map[string]json.RawMessage
			require.NoError(t, internal.FetchDto(t.Context(), base, &sentinel, old+utility.SentinelSuffix))
			switch failure {
			case "missing_wal_boundaries":
				delete(sentinel, "StartWalFileName")
				delete(sentinel, "EndWalFileName")
			case "unknown_protection":
				sentinel["Version"] = json.RawMessage(`1`)
				require.NoError(t, base.PutObject(t.Context(), old+"/metadata.json", strings.NewReader("{}")))
			case "incremental_boundary":
				options.Before = putNoListBackup(t, root, 11, keep, false)
			case "retained_dependency":
				putNoListBackup(t, root, 11, old, true)
			case "segment_size":
				oldSize := WalSegmentSize
				SetWalSize(64)
				t.Cleanup(func() { SetWalSize(oldSize >> 20) })
			case "system_id", "selected_system_id":
				// Both retained and selected backups must belong to the same cluster.
				name := old
				if failure == "system_id" {
					name = putNoListBackup(t, root, 12, "", false)
				}
				var s map[string]json.RawMessage
				require.NoError(t, internal.FetchDto(t.Context(), base, &s, name+utility.SentinelSuffix))
				s["SystemIdentifier"] = json.RawMessage(`99`)
				require.NoError(t, internal.UploadDto(t.Context(), base, s, name+utility.SentinelSuffix))
				if name == old {
					sentinel = s
				}
				var meta ExtendedMetadataDto
				require.NoError(t, internal.FetchDto(t.Context(), base, &meta, name+"/metadata.json"))
				sid := uint64(99)
				meta.SystemIdentifier = &sid
				require.NoError(t, internal.UploadDto(t.Context(), base, meta, name+"/metadata.json"))
			case "versioned":
				folder.trace.versioned = true
			case "listing":
				folder.trace.failList = base.GetSubFolder(old + "/tar_partitions").GetPath()
			}
			require.NoError(t, internal.UploadDto(t.Context(), base, sentinel, old+utility.SentinelSuffix))
			_, err := PlanDeleteWithoutListing(t.Context(), folder, options)
			messages := map[string]string{
				"missing_wal_boundaries": "recorded WAL boundaries",
				"unknown_protection":     "cannot establish retention", "incremental_boundary": "is incremental",
				"retained_dependency": "depends on deletion target", "segment_size": "segment size",
				"system_id": "conflicting system identifiers", "selected_system_id": "conflicting system identifiers",
				"versioned": "versioned storage", "listing": "injected listing error",
			}
			require.ErrorContains(t, err, messages[failure])
			require.Empty(t, folder.trace.deleted)
			assertNoListExists(t, base, old+utility.SentinelSuffix, true)
		})
	}
}

func TestDeleteWithoutListingBulkWALProtection(t *testing.T) {
	p := NoListDeletePlan{
		ranges: []noListWALRange{{timeline: 7, first: 0}},
		retention: deletionWALRetention{
			knownTimelines: map[uint32]bool{7: true}, segmentSize: 16 << 20, cutoff: 34, hasCutoff: true,
			protected: []deletionWALInterval{{timeline: 7, first: 5, last: 6}},
		},
	}
	keys := map[string]bool{}
	require.NoError(t, p.walkWALKeys(t.Context(), func(key string) error {
		require.False(t, keys[key], "duplicate deletion key %s", key)
		keys[key] = true
		return nil
	}))
	for _, key := range []string{"000000070000000000000010_delta.lz4", "00000007000000000000001.json",
		"wal_metadata/00000007000000000000001.json", "000000070000000000000021.lz4"} {
		require.True(t, keys[key], key)
	}
	for _, key := range []string{"000000070000000000000000_delta.lz4", "00000007000000000000000.json",
		"000000070000000000000020_delta.lz4", "00000007000000000000002.json",
		"000000070000000000000005.lz4", "wal_metadata/000000070000000000000006.json",
		"000000070000000000000022.lz4", "00000007.history"} {
		require.False(t, keys[key], key)
	}
}

func TestDeleteWithoutListingStopsOnDeleteFailure(t *testing.T) {
	for _, failure := range []string{"sentinel", "unconfirmed_sentinel", "payload", "cancelled"} {
		t.Run(failure, func(t *testing.T) {
			root := memory.NewFolder("tenant/", memory.NewKVS())
			old := putNoListBackup(t, root, 1, "", false)
			keep := putNoListBackup(t, root, 10, "", false)
			walKey := utility.WalPath + WalSegmentNo(1).GetFilename(1) + ".lz4"
			require.NoError(t, root.PutObject(t.Context(), walKey, strings.NewReader("wal")))
			folder := traceNoListFolder(root, old)
			plan, err := PlanDeleteWithoutListing(t.Context(), folder, NoListDeleteOptions{
				Before: keep,
			})
			require.NoError(t, err)
			t.Cleanup(plan.Close)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch failure {
			case "sentinel":
				folder.trace.failKey = utility.BaseBackupPath + old + utility.SentinelSuffix
			case "unconfirmed_sentinel":
				folder.trace.keepSentinels = true
			case "payload":
				folder.trace.failKey = utility.BaseBackupPath + old + "/tar_partitions/part_001.tar.lz4"
			case "cancelled":
				cancel()
			}
			require.Error(t, plan.Execute(ctx, true))
			assertNoListExists(t, root, walKey, true)
			assertNoListExists(t, root, utility.BaseBackupPath+old+"/metadata.json", true)
			assertNoListExists(t, root, utility.BaseBackupPath+old+"/tar_partitions/pg_control.tar.lz4", true)
		})
	}
}

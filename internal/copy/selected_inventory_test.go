package copy_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	copyutil "github.com/wal-g/wal-g/internal/copy"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

type listingGuard struct {
	storage.Folder
	root  string
	allow func(string) bool
}

func guardListings(folder storage.Folder, allow func(string) bool) storage.Folder {
	return listingGuard{Folder: folder, root: folder.GetPath(), allow: allow}
}

func (f listingGuard) GetSubFolder(name string) storage.Folder {
	return listingGuard{Folder: f.Folder.GetSubFolder(name), root: f.root, allow: f.allow}
}

func (f listingGuard) ListFolder(ctx context.Context) ([]storage.Object, []storage.Folder, error) {
	if !f.allow(strings.Trim(strings.TrimPrefix(f.GetPath(), f.root), "/")) {
		return nil, nil, fmt.Errorf("unexpected LIST of %q", f.GetPath())
	}
	objects, children, err := f.Folder.ListFolder(ctx)
	for i := range children {
		children[i] = listingGuard{Folder: children[i], root: f.root, allow: f.allow}
	}
	return objects, children, err
}

func TestExecuteDoesNotListDestination(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	target := testtools.MakeDefaultInMemoryStorageFolder()
	require.NoError(t, from.PutObject(t.Context(), "payload", bytes.NewBufferString("data")))
	to := guardListings(target, func(string) bool { return false })
	plan, err := copyutil.NewPlan(t.Context(), from, to)
	require.NoError(t, err)
	require.NoError(t, plan.AddObject("payload", "payload", copyutil.PayloadPhase, false))
	require.NoError(t, copyutil.ExecuteRaw(t.Context(), plan))
}

type statFailureFolder struct {
	storage.Folder
	err error
}

func (f statFailureFolder) StatObject(context.Context, string) (storage.Object, error) {
	return nil, f.err
}

func TestDestinationStatFailureStopsBeforePublication(t *testing.T) {
	for _, failure := range []error{errors.New("access denied"), context.Canceled} {
		t.Run(failure.Error(), func(t *testing.T) {
			from := testtools.MakeDefaultInMemoryStorageFolder()
			target := testtools.MakeDefaultInMemoryStorageFolder()
			to := &recordingPutFolder{Folder: statFailureFolder{Folder: target, err: failure}}
			plan, err := copyutil.NewPlan(t.Context(), from, to)
			require.NoError(t, err)
			plan.AddInline("payload", []byte("data"), copyutil.PayloadPhase, false)
			plan.AddInline("sentinel", []byte("{}"), copyutil.BackupCommitPhase, false)
			require.ErrorIs(t, copyutil.ExecuteRaw(t.Context(), plan), failure)
			require.Empty(t, to.Writes())
		})
	}
}

func TestScopedPlanRejectsPathsOutsideSource(t *testing.T) {
	source := testtools.MakeDefaultInMemoryStorageFolder()
	plan := copyutil.NewScopedPlan(source, source)
	for _, name := range []string{"", ".", "..", "../other", "/other"} {
		require.Error(t, plan.InventoryPrefix(t.Context(), name))
		_, err := plan.FindObject(t.Context(), name)
		require.Error(t, err)
	}
}

func TestSelectedGreenplumCopyDoesNotScanStorageRoot(t *testing.T) {
	postgres.SetWalSize(16)
	t.Cleanup(func() { postgres.SetWalSize(16) })
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := testtools.MakeDefaultInMemoryStorageFolder()
	const backup = "backup_selected"
	const segment = "base_000000010000000000000002"
	base := greenplum.FormatSegmentBackupPath(0)
	put := func(key, value string) {
		require.NoError(t, from.PutObject(t.Context(), key, strings.NewReader(value)))
	}
	put(path.Join(utility.BaseBackupPath, backup+utility.SentinelSuffix),
		`{"segments":[{"content_id":0,"backup_name":"`+segment+`"}],"restore_point":"rp"}`)
	put(path.Join(utility.BaseBackupPath, greenplum.RestorePointMetadataFileName("rp")),
		`{"name":"rp","lsn_by_segment":{"0":"0/3000001"},"timeline_by_segment":{"0":1}}`)
	put(path.Join(base, segment+utility.SentinelSuffix), "{}")
	put(path.Join(base, segment, utility.MetadataFileName), `{"start_lsn":33554433,"finish_lsn":50331649}`)
	put(path.Join(base, segment, "tar_partitions/part_001.tar.lz4"), "data")
	put(path.Join(base, segment, "ao_files_metadata.json"),
		`{"Files":{"data":{"StoragePath":"needed_aoseg","EOF":4}}}`)
	put(path.Join(base, "aosegments/needed_aoseg"), "data")
	put(path.Join(base, "aosegments/unrelated_aoseg"), "do not enumerate")
	for i := 2; i <= 3; i++ {
		put(path.Join(greenplum.FormatSegmentWalPath(0), postgres.WalSegmentNo(i).GetFilename(1)+".lz4"), "wal")
	}
	put("unrelated/old-backup", "do not enumerate")
	selected := path.Join(base, segment)
	wal := strings.Trim(greenplum.FormatSegmentWalPath(0), "/")
	guard := guardListings(from, func(prefix string) bool {
		return prefix == selected || strings.HasPrefix(prefix, selected+"/") || prefix == wal
	})
	plan, err := greenplum.BuildCopyPlan(t.Context(), guard, to, backup, false)
	require.NoError(t, err)
	var shared bool
	for _, entry := range plan.Entries() {
		require.NotContains(t, entry.SourcePath, "unrelated")
		shared = shared || entry.SourcePath == path.Join(base, "aosegments/needed_aoseg")
	}
	require.True(t, shared)
	require.NoError(t, copyutil.ExecuteRaw(t.Context(), plan))
}

func TestSelectedPostgresCopyDoesNotScanStorageRoot(t *testing.T) {
	postgres.SetWalSize(16)
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := testtools.MakeDefaultInMemoryStorageFolder()
	const backup = "base_000000010000000000000002"
	put := func(key, value string) {
		require.NoError(t, from.PutObject(t.Context(), key, strings.NewReader(value)))
	}
	put(path.Join(utility.BaseBackupPath, backup+utility.SentinelSuffix), "{}")
	put(path.Join(utility.BaseBackupPath, backup, utility.MetadataFileName),
		`{"start_lsn":33554433,"finish_lsn":50331649}`)
	put(path.Join(utility.BaseBackupPath, backup, "tar_partitions/part_001.tar.lz4"), "data")
	for i := 2; i <= 3; i++ {
		put(path.Join(utility.WalPath, postgres.WalSegmentNo(i).GetFilename(1)+".lz4"), "wal")
	}
	put(path.Join(utility.BaseBackupPath, "unrelated/payload"), "do not enumerate")
	selected := path.Join(utility.BaseBackupPath, backup)
	for _, requested := range []string{backup, "LATEST", ""} {
		t.Run("requested="+requested, func(t *testing.T) {
			guard := guardListings(from, func(prefix string) bool {
				return prefix == selected || strings.HasPrefix(prefix, selected+"/") ||
					prefix == strings.Trim(utility.WalPath, "/") ||
					requested != backup && prefix == strings.Trim(utility.BaseBackupPath, "/")
			})
			plan, err := postgres.BuildCopyPlan(t.Context(), guard, to, requested, false)
			require.NoError(t, err)
			for _, entry := range plan.Entries() {
				require.NotContains(t, entry.SourcePath, "unrelated")
			}
			require.NoError(t, copyutil.ExecuteRaw(t.Context(), plan))
		})
	}
}

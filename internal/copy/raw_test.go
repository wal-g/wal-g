package copy_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	copyutil "github.com/wal-g/wal-g/internal/copy"
	etcdcopy "github.com/wal-g/wal-g/internal/databases/etcd"
	rediscopy "github.com/wal-g/wal-g/internal/databases/redis"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

func TestExecuteRawPreservesOpaqueBytes(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := testtools.MakeDefaultInMemoryStorageFolder()
	opaque := []byte{0, 1, 2, 3, 0xff, 0, 0x89, 'P', 'G', 'P'}
	require.NoError(t, from.PutObject(t.Context(), "opaque.enc.lz4", bytes.NewReader(opaque)))

	plan, err := copyutil.NewPlan(t.Context(), from, to)
	require.NoError(t, err)
	require.NoError(t, plan.AddObject("opaque.enc.lz4", "opaque.enc.lz4", copyutil.PayloadPhase, false))
	require.NoError(t, copyutil.ExecuteRaw(t.Context(), plan))

	reader, err := to.ReadObject(t.Context(), "opaque.enc.lz4")
	require.NoError(t, err)
	defer reader.Close()
	actual, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, opaque, actual)
}

func TestExecuteRawResumesAndSkipsExistingImmutableObjects(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := testtools.MakeDefaultInMemoryStorageFolder()
	require.NoError(t, from.PutObject(t.Context(), "wal/one", bytes.NewBufferString("source-one")))
	require.NoError(t, to.PutObject(t.Context(), "wal/one", bytes.NewBufferString("target-one")))
	require.NoError(t, from.PutObject(t.Context(), "wal/two", bytes.NewBufferString("source-two")))

	plan, err := copyutil.NewPlan(t.Context(), from, to)
	require.NoError(t, err)
	require.NoError(t, plan.AddObject("wal/one", "wal/one", copyutil.PayloadPhase, false))
	require.NoError(t, plan.AddObject("wal/two", "wal/two", copyutil.PayloadPhase, false))
	require.NoError(t, copyutil.ExecuteRaw(t.Context(), plan))

	one, err := to.ReadObject(t.Context(), "wal/one")
	require.NoError(t, err)
	oneBytes, err := io.ReadAll(one)
	require.NoError(t, err)
	require.NoError(t, one.Close())
	require.Equal(t, []byte("target-one"), oneBytes, "same-sized immutable object must be skipped")

	two, err := to.ReadObject(t.Context(), "wal/two")
	require.NoError(t, err)
	twoBytes, err := io.ReadAll(two)
	require.NoError(t, err)
	require.NoError(t, two.Close())
	require.Equal(t, []byte("source-two"), twoBytes)
}

func TestExecuteRawRejectsDestinationSizeConflict(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := testtools.MakeDefaultInMemoryStorageFolder()
	require.NoError(t, from.PutObject(t.Context(), "object", bytes.NewBufferString("longer")))
	require.NoError(t, to.PutObject(t.Context(), "object", bytes.NewBufferString("short")))

	plan, err := copyutil.NewPlan(t.Context(), from, to)
	require.NoError(t, err)
	require.NoError(t, plan.AddObject("object", "object", copyutil.PayloadPhase, false))
	require.ErrorContains(t, copyutil.ExecuteRaw(t.Context(), plan), "conflicts with source")
}

func TestPlanAddBackupUsesExactBoundariesAndCommitsLast(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := testtools.MakeDefaultInMemoryStorageFolder()
	for _, name := range []string{"base_1", "base_10"} {
		require.NoError(t, from.PutObject(t.Context(), utility.BaseBackupPath+name+"/part.tar.lz4", bytes.NewBufferString(name)))
		require.NoError(t, from.PutObject(t.Context(), utility.BaseBackupPath+name+utility.SentinelSuffix, bytes.NewBufferString("{}")))
	}

	plan, err := copyutil.NewPlan(t.Context(), from, to)
	require.NoError(t, err)
	require.NoError(t, plan.AddBackup("base_1", "base_1"))
	entries := plan.Entries()
	require.Len(t, entries, 2)
	require.Equal(t, utility.BaseBackupPath+"base_1/part.tar.lz4", entries[0].SourcePath)
	require.Equal(t, copyutil.CommitPhase, entries[1].Phase)
}

func TestStandaloneDatabasePlansExcludeUnrelatedArchiveHistory(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	name := "base_20260721T120000Z"
	require.NoError(t, from.PutObject(t.Context(), utility.BaseBackupPath+name+"/part", bytes.NewBufferString("snapshot")))
	require.NoError(t, from.PutObject(t.Context(), utility.BaseBackupPath+internal.SentinelNameFromBackup(name), bytes.NewBufferString("{}")))
	require.NoError(t, from.PutObject(t.Context(), utility.WalPath+"unrelated", bytes.NewBufferString("wal")))

	builders := []struct {
		name  string
		build func() (*copyutil.Plan, error)
	}{
		{"etcd", func() (*copyutil.Plan, error) {
			return etcdcopy.BuildCopyPlan(t.Context(), from, testtools.MakeDefaultInMemoryStorageFolder(), name)
		}},
		{"redis-valkey", func() (*copyutil.Plan, error) {
			return rediscopy.BuildCopyPlan(t.Context(), from, testtools.MakeDefaultInMemoryStorageFolder(), name)
		}},
	}
	for _, builder := range builders {
		t.Run(builder.name, func(t *testing.T) {
			plan, err := builder.build()
			require.NoError(t, err)
			for _, entry := range plan.Entries() {
				require.NotContains(t, entry.SourcePath, utility.WalPath)
			}
		})
	}
}

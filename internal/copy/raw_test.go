package copy_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression"
	copyutil "github.com/wal-g/wal-g/internal/copy"
	etcdcopy "github.com/wal-g/wal-g/internal/databases/etcd"
	rediscopy "github.com/wal-g/wal-g/internal/databases/redis"
	"github.com/wal-g/wal-g/internal/statistics"
	"github.com/wal-g/wal-g/pkg/storages/storage"
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
	require.Equal(t, copyutil.BackupCommitPhase, entries[1].Phase)
}

func TestExecuteRawOrdersStagesBeforeArbitrarySequence(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := &recordingPutFolder{Folder: testtools.MakeDefaultInMemoryStorageFolder()}
	plan, err := copyutil.NewPlan(t.Context(), from, to)
	require.NoError(t, err)

	plan.AddInline("parent-backup", []byte("{}"), copyutil.BackupCommitPhase, false)
	require.NoError(t, plan.SetOrder("parent-backup", copyutil.BackupCommitPhase, 0))
	plan.AddInline("deep-backup", []byte("{}"), copyutil.BackupCommitPhase, false)
	require.NoError(t, plan.SetOrder("deep-backup", copyutil.BackupCommitPhase, 1000))
	plan.AddInline("recovery-metadata", []byte("{}"), copyutil.RecoveryMetadataPhase, false)

	require.NoError(t, copyutil.ExecuteRaw(t.Context(), plan))
	require.Equal(t, []string{"parent-backup", "deep-backup", "recovery-metadata"}, to.Writes())
}

func TestExecuteRawUpdatesUploadMetrics(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	require.NoError(t, from.PutObject(t.Context(), "object", bytes.NewBufferString("payload")))

	totalBefore := counterValue(t, statistics.WalgMetrics.UploadedFilesTotal)
	failedBefore := counterValue(t, statistics.WalgMetrics.UploadedFilesFailedTotal)

	successPlan, err := copyutil.NewPlan(t.Context(), from, testtools.MakeDefaultInMemoryStorageFolder())
	require.NoError(t, err)
	require.NoError(t, successPlan.AddObject("object", "object", copyutil.PayloadPhase, false))
	require.NoError(t, copyutil.ExecuteRaw(t.Context(), successPlan))

	failingTarget := &failingPutFolder{Folder: testtools.MakeDefaultInMemoryStorageFolder()}
	failurePlan, err := copyutil.NewPlan(t.Context(), from, failingTarget)
	require.NoError(t, err)
	require.NoError(t, failurePlan.AddObject("object", "object", copyutil.PayloadPhase, false))
	require.ErrorContains(t, copyutil.ExecuteRaw(t.Context(), failurePlan), "write destination object")

	require.Equal(t, totalBefore+2, counterValue(t, statistics.WalgMetrics.UploadedFilesTotal))
	require.Equal(t, failedBefore+1, counterValue(t, statistics.WalgMetrics.UploadedFilesFailedTotal))
}

func TestExecuteUsesCrossFolderCopy(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	require.NoError(t, from.PutObject(t.Context(), "object", bytes.NewBufferString("payload")))
	to := &recordingCrossCopyFolder{Folder: testtools.MakeDefaultInMemoryStorageFolder()}
	plan, err := copyutil.NewPlan(t.Context(), from, to)
	require.NoError(t, err)
	require.NoError(t, plan.AddObject("object", "object", copyutil.PayloadPhase, false))

	require.NoError(t, copyutil.Execute(t.Context(), plan, copyutil.ExecuteOptions{UseServerSideCopy: true}))
	require.Equal(t, 1, to.CopyCalls())
	require.Equal(t, 0, to.PutCalls())
}

func TestExecuteFallsBackFromCrossFolderCopy(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	require.NoError(t, from.PutObject(t.Context(), "object", bytes.NewBufferString("payload")))
	to := &recordingCrossCopyFolder{
		Folder:  testtools.MakeDefaultInMemoryStorageFolder(),
		copyErr: errors.New("copy API unavailable"),
	}
	plan, err := copyutil.NewPlan(t.Context(), from, to)
	require.NoError(t, err)
	require.NoError(t, plan.AddObject("object", "object", copyutil.PayloadPhase, false))

	require.NoError(t, copyutil.Execute(t.Context(), plan, copyutil.ExecuteOptions{UseServerSideCopy: true}))
	require.Equal(t, 1, to.CopyCalls())
	require.Equal(t, 1, to.PutCalls())
	reader, err := to.ReadObject(t.Context(), "object")
	require.NoError(t, err)
	defer reader.Close()
	content, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), content)
}

func TestExecuteDoesNotFallBackAfterCancellation(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	require.NoError(t, from.PutObject(t.Context(), "object", bytes.NewBufferString("payload")))
	to := &recordingCrossCopyFolder{Folder: testtools.MakeDefaultInMemoryStorageFolder(), copyErr: context.Canceled}
	plan, err := copyutil.NewPlan(t.Context(), from, to)
	require.NoError(t, err)
	require.NoError(t, plan.AddObject("object", "object", copyutil.PayloadPhase, false))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.ErrorIs(t, copyutil.Execute(ctx, plan, copyutil.ExecuteOptions{UseServerSideCopy: true}), context.Canceled)
	require.Equal(t, 0, to.PutCalls())
}

func TestExecuteDoesNotFallBackWhenCopyReturnsCancellation(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	require.NoError(t, from.PutObject(t.Context(), "object", bytes.NewBufferString("payload")))
	to := &recordingCrossCopyFolder{Folder: testtools.MakeDefaultInMemoryStorageFolder(), copyErr: context.Canceled}
	plan, err := copyutil.NewPlan(t.Context(), from, to)
	require.NoError(t, err)
	require.NoError(t, plan.AddObject("object", "object", copyutil.PayloadPhase, false))

	require.ErrorIs(t,
		copyutil.Execute(t.Context(), plan, copyutil.ExecuteOptions{UseServerSideCopy: true}),
		context.Canceled)
	require.Equal(t, 0, to.PutCalls())
}

func TestExecuteReturnsServerAndStreamingErrors(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	require.NoError(t, from.PutObject(t.Context(), "object", bytes.NewBufferString("payload")))
	to := &recordingCrossCopyFolder{
		Folder:  &failingPutFolder{Folder: testtools.MakeDefaultInMemoryStorageFolder()},
		copyErr: errors.New("server copy failed"),
	}
	plan, err := copyutil.NewPlan(t.Context(), from, to)
	require.NoError(t, err)
	require.NoError(t, plan.AddObject("object", "object", copyutil.PayloadPhase, false))

	err = copyutil.Execute(t.Context(), plan, copyutil.ExecuteOptions{UseServerSideCopy: true})
	require.ErrorContains(t, err, "server copy failed")
	require.ErrorContains(t, err, "put failed")
}

func TestStripCompressionExtensionCoversRegisteredDecompressors(t *testing.T) {
	for _, decompressor := range compression.Decompressors {
		extension := decompressor.FileExtension()
		if extension == "" {
			continue
		}
		t.Run(extension, func(t *testing.T) {
			require.Equal(t, "archive", copyutil.StripCompressionExtension("archive."+extension))
		})
	}
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

type failingPutFolder struct {
	storage.Folder
}

func (f *failingPutFolder) PutObject(context.Context, string, io.Reader) error {
	return errors.New("put failed")
}

type recordingPutFolder struct {
	storage.Folder

	mu     sync.Mutex
	writes []string
}

type recordingCrossCopyFolder struct {
	storage.Folder

	mu        sync.Mutex
	copyErr   error
	copyCalls int
	putCalls  int
}

func (f *recordingCrossCopyFolder) CanCopyFrom(storage.Folder) bool { return true }

func (f *recordingCrossCopyFolder) CopyObjectFrom(
	context.Context, storage.Folder, string, string, int64,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.copyCalls++
	return f.copyErr
}

func (f *recordingCrossCopyFolder) PutObject(ctx context.Context, name string, content io.Reader) error {
	f.mu.Lock()
	f.putCalls++
	f.mu.Unlock()
	return f.Folder.PutObject(ctx, name, content)
}

func (f *recordingCrossCopyFolder) CopyCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.copyCalls
}

func (f *recordingCrossCopyFolder) PutCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.putCalls
}

func (f *recordingPutFolder) PutObject(ctx context.Context, name string, content io.Reader) error {
	f.mu.Lock()
	f.writes = append(f.writes, name)
	f.mu.Unlock()
	return f.Folder.PutObject(ctx, name, content)
}

func (f *recordingPutFolder) Writes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.writes...)
}

func counterValue(t *testing.T, counter interface{ Write(*dto.Metric) error }) float64 {
	t.Helper()
	metric := &dto.Metric{}
	require.NoError(t, counter.Write(metric))
	return metric.GetCounter().GetValue()
}

package copy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type statHookFolder struct {
	storage.Folder
	stat func()
}

func (f *statHookFolder) StatObject(ctx context.Context, name string) (storage.Object, error) {
	f.stat()
	return f.Folder.StatObject(ctx, name)
}

func TestExecuteReleasesSourceInventoryBeforeDestinationCheck(t *testing.T) {
	from := memory.NewFolder("in_memory/", memory.NewKVS())
	require.NoError(t, from.PutObject(t.Context(), "wal/one", strings.NewReader("one")))
	var plan *Plan
	var stats, releasedAtStat atomic.Int64
	to := &statHookFolder{Folder: memory.NewFolder("in_memory/", memory.NewKVS()), stat: func() {
		stats.Add(1)
		if plan.source == nil && plan.listed == nil {
			releasedAtStat.Add(1)
		}
	}}
	plan = NewScopedPlan(from, to)
	require.NoError(t, plan.InventoryPrefix(t.Context(), "wal"))
	require.NoError(t, plan.AddObject("wal/one", "wal/one", PayloadPhase, false))

	require.NoError(t, ExecuteRaw(t.Context(), plan))
	require.Equal(t, int64(1), stats.Load())
	require.Equal(t, int64(1), releasedAtStat.Load())
	require.Nil(t, plan.source)
	require.Nil(t, plan.listed)
}

func TestExecuteAgainAfterReleasePublishesPlan(t *testing.T) {
	from := memory.NewFolder("in_memory/", memory.NewKVS())
	require.NoError(t, from.PutObject(t.Context(), "payload", strings.NewReader("payload")))
	base := memory.NewFolder("in_memory/", memory.NewKVS())
	failure := errors.New("injected destination failure")
	fail := true
	to := &retryTestFolder{Folder: base, put: func(ctx context.Context, name string, r io.Reader) error {
		if fail && name == "payload" {
			return failure
		}
		return base.PutObject(ctx, name, r)
	}}
	plan := NewScopedPlan(from, to)
	require.NoError(t, plan.AddSourceObject(t.Context(), "payload", "payload", PayloadPhase, false))
	plan.AddInline("sentinel", []byte("{}"), BackupCommitPhase, false)
	retries := 0
	options := ExecuteOptions{MaxRetries: &retries}

	require.ErrorIs(t, Execute(t.Context(), plan, options), failure)
	exists, err := base.Exists(t.Context(), "sentinel")
	require.NoError(t, err)
	require.False(t, exists)

	fail = false
	require.NoError(t, Execute(t.Context(), plan, options))
	require.Equal(t, "payload", readReleaseTestObject(t, base, "payload"))
	require.Equal(t, "{}", readReleaseTestObject(t, base, "sentinel"))
}

func TestPlanningAfterExecuteFails(t *testing.T) {
	backupPath := utility.BaseBackupPath
	wal := []string{"wal/one"}
	failing := map[string]func(context.Context, *Plan) error{
		"InventoryPrefix": func(ctx context.Context, p *Plan) error { return p.InventoryPrefix(ctx, "wal") },
		"InventoryFolder": func(ctx context.Context, p *Plan) error { return p.InventoryFolder(ctx, backupPath) },
		"InventoryBackup": func(ctx context.Context, p *Plan) error { return p.InventoryBackup(ctx, backupPath, "base_1") },
		"FindObject": func(ctx context.Context, p *Plan) error {
			_, err := p.FindObject(ctx, wal[0])
			return err
		},
		"AddSourceObject": func(ctx context.Context, p *Plan) error {
			return p.AddSourceObject(ctx, wal[0], wal[0], PayloadPhase, false)
		},
		"AddObject": func(_ context.Context, p *Plan) error { return p.AddObject(wal[0], wal[0], PayloadPhase, false) },
		"AddMatching": func(_ context.Context, p *Plan) error {
			return p.AddMatching(func(string) bool { return true }, PayloadPhase)
		},
		"AddBackup":   func(_ context.Context, p *Plan) error { return p.AddBackup("base_1", "base_1") },
		"AddBackupAt": func(_ context.Context, p *Plan) error { return p.AddBackupAt(backupPath, "base_1", "base_1") },
	}
	panicking := map[string]func(*Plan){
		"SourceObjects":   func(p *Plan) { p.SourceObjects() },
		"SourceObjectsAt": func(p *Plan) { p.SourceObjectsAt("wal") },
		"BackupNames":     func(p *Plan) { p.BackupNames() },
	}
	for _, scoped := range []bool{true, false} {
		for name, call := range failing {
			t.Run(fmt.Sprintf("%s/scoped=%t", name, scoped), func(t *testing.T) {
				plan := executedReleaseTestPlan(t, scoped, func(p *Plan) {
					require.NoError(t, call(t.Context(), p), "call must succeed before Execute")
				})
				entries := plan.Entries()

				require.ErrorIs(t, call(t.Context(), plan), errSourceReleased)
				require.Equal(t, entries, plan.Entries())
			})
		}
		for name, call := range panicking {
			t.Run(fmt.Sprintf("%s/scoped=%t", name, scoped), func(t *testing.T) {
				plan := executedReleaseTestPlan(t, scoped, func(p *Plan) {
					require.NotPanics(t, func() { call(p) }, "call must succeed before Execute")
				})

				require.PanicsWithValue(t, errSourceReleased, func() { call(plan) })
			})
		}
		t.Run(fmt.Sprintf("AddSourceObjects/scoped=%t", scoped), func(t *testing.T) {
			plan := executedReleaseTestPlan(t, scoped, nil)
			entries := plan.Entries()

			require.NotPanics(t, func() { plan.AddSourceObjects(t.Context(), wal, wal, PayloadPhase, false, 1) })
			require.Equal(t, entries, plan.Entries())
		})
	}
}

func executedReleaseTestPlan(t *testing.T, scoped bool, beforeExecute func(*Plan)) *Plan {
	backupPath := utility.BaseBackupPath
	from := memory.NewFolder("in_memory/", memory.NewKVS())
	for _, object := range []string{"wal/one", backupPath + "base_1/part", backupPath + "base_1" + utility.SentinelSuffix} {
		require.NoError(t, from.PutObject(t.Context(), object, strings.NewReader(object)))
	}
	to := memory.NewFolder("in_memory/", memory.NewKVS())
	plan := NewScopedPlan(from, to)
	if !scoped {
		var err error
		plan, err = NewPlan(t.Context(), from, to)
		require.NoError(t, err)
	}
	require.NoError(t, plan.InventoryPrefix(t.Context(), "wal"))
	require.NoError(t, plan.InventoryBackup(t.Context(), backupPath, "base_1"))
	if beforeExecute != nil {
		beforeExecute(plan)
	}
	require.NoError(t, ExecuteRaw(t.Context(), plan))
	return plan
}

func readReleaseTestObject(t *testing.T, folder storage.Folder, name string) string {
	reader, err := folder.ReadObject(t.Context(), name)
	require.NoError(t, err)
	defer reader.Close()
	content, err := io.ReadAll(reader)
	require.NoError(t, err)
	return string(content)
}

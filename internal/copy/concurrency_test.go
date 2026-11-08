package copy_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	conf "github.com/wal-g/wal-g/internal/config"
	copyutil "github.com/wal-g/wal-g/internal/copy"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/testtools"
)

type gatedPutFolder struct {
	storage.Folder
	release     chan struct{}
	releaseOnce sync.Once
	inFlight    atomic.Int32
	maxInFlight atomic.Int32
}

func (f *gatedPutFolder) open() {
	f.releaseOnce.Do(func() { close(f.release) })
}

func (f *gatedPutFolder) PutObject(ctx context.Context, name string, content io.Reader) error {
	current := f.inFlight.Add(1)
	defer f.inFlight.Add(-1)
	for peak := f.maxInFlight.Load(); current > peak && !f.maxInFlight.CompareAndSwap(peak, current); {
		peak = f.maxInFlight.Load()
	}
	<-f.release
	return f.Folder.PutObject(ctx, name, content)
}

type copyRunner func(context.Context, storage.Folder, storage.Folder, copyutil.ExecuteOptions) error

func copyAll(ctx context.Context, from, to storage.Folder, options copyutil.ExecuteOptions) error {
	plan, err := copyutil.NewPlan(ctx, from, to)
	if err != nil {
		return err
	}
	if err := plan.AddMatching(func(string) bool { return true }, copyutil.PayloadPhase); err != nil {
		return err
	}
	return copyutil.Execute(ctx, plan, options)
}

func copyAllInfos(ctx context.Context, from, to storage.Folder, options copyutil.ExecuteOptions) error {
	objects, err := storage.ListFolderRecursively(ctx, from)
	if err != nil {
		return err
	}
	infos := copyutil.BuildCopyingInfos(from, to, objects, func(storage.Object) bool { return true },
		copyutil.NoopRenameFunc, copyutil.NoopSourceTransformer)
	return copyutil.InfosWithOptions(ctx, infos, options)
}

var copyRunners = []struct {
	name string
	run  copyRunner
}{
	{"execute", copyAll},
	{"infos", copyAllInfos},
}

func TestCopyLimitsConcurrentTransfers(t *testing.T) {
	const objects, limit = 10, 3
	for _, runner := range copyRunners {
		for _, configured := range []struct {
			name    string
			main    string
			options copyutil.ExecuteOptions
			want    int
		}{
			{"options", "", copyutil.ExecuteOptions{Concurrency: limit}, limit},
			{"main config", strconv.Itoa(limit), copyutil.ExecuteOptions{}, limit},
			{"unconfigured", "", copyutil.ExecuteOptions{}, 8},
		} {
			t.Run(runner.name+"/"+configured.name, func(t *testing.T) {
				viper.Reset()
				t.Cleanup(viper.Reset)
				if configured.main != "" {
					viper.Set(conf.CopyConcurrency, configured.main)
				}
				synctest.Test(t, func(t *testing.T) {
					from := testtools.MakeDefaultInMemoryStorageFolder()
					for i := range objects {
						require.NoError(t, from.PutObject(t.Context(), fmt.Sprintf("object-%02d", i), bytes.NewBufferString("payload")))
					}
					to := &gatedPutFolder{Folder: testtools.MakeDefaultInMemoryStorageFolder(), release: make(chan struct{})}
					defer to.open()
					done := make(chan error, 1)
					go func() { done <- runner.run(t.Context(), from, to, configured.options) }()

					synctest.Wait()
					require.EqualValues(t, configured.want, to.inFlight.Load())
					to.open()
					require.NoError(t, <-done)
					require.EqualValues(t, configured.want, to.maxInFlight.Load())
					copied, err := storage.ListFolderRecursively(t.Context(), to)
					require.NoError(t, err)
					require.Len(t, copied, objects)
				})
			})
		}
	}
}

func TestCopyRejectsInvalidConcurrency(t *testing.T) {
	for _, runner := range copyRunners {
		for _, configured := range []struct {
			name, main, rejected string
			options              copyutil.ExecuteOptions
		}{
			{"options", "", "-1", copyutil.ExecuteOptions{Concurrency: -1}},
			{"main config", "0", `"0"`, copyutil.ExecuteOptions{}},
		} {
			t.Run(runner.name+"/"+configured.name, func(t *testing.T) {
				viper.Reset()
				t.Cleanup(viper.Reset)
				if configured.main != "" {
					viper.Set(conf.CopyConcurrency, configured.main)
				}
				from := testtools.MakeDefaultInMemoryStorageFolder()
				require.NoError(t, from.PutObject(t.Context(), "object", bytes.NewBufferString("payload")))
				to := testtools.MakeDefaultInMemoryStorageFolder()

				err := runner.run(t.Context(), from, to, configured.options)
				require.ErrorContains(t, err, conf.CopyConcurrency)
				require.ErrorContains(t, err, configured.rejected)
				copied, listErr := storage.ListFolderRecursively(t.Context(), to)
				require.NoError(t, listErr)
				require.Empty(t, copied)
			})
		}
	}
}

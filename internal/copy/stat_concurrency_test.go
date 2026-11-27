package copy_test

import (
	"bytes"
	"context"
	"fmt"
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

func TestCopyStatConcurrencyConfigPrecedence(t *testing.T) {
	for _, tc := range []struct {
		name                                   string
		copyEnvironment, copyMain, copySource  string
		environment, main, source, destination string
		wantConcurrency, wantStatConcurrency   int
		invalid                                bool
	}{
		{name: "default", wantConcurrency: 8, wantStatConcurrency: 4},
		{name: "main", copyMain: "4", main: "40", wantConcurrency: 4, wantStatConcurrency: 40},
		{name: "environment", environment: "60", wantConcurrency: 8, wantStatConcurrency: 60},
		{name: "source wins", main: "40", source: "20", wantConcurrency: 8, wantStatConcurrency: 20},
		{name: "source wins over environment", environment: "60", source: "20", wantConcurrency: 8, wantStatConcurrency: 20},
		{name: "destination ignored", main: "40", destination: "90", wantConcurrency: 8, wantStatConcurrency: 40},
		{name: "lower than copy concurrency", copySource: "4", source: "1", wantConcurrency: 4, wantStatConcurrency: 1},
		{name: "invalid environment", environment: "bad", invalid: true},
		{name: "blank environment", environment: " ", invalid: true},
		{name: "blank source", main: "1", source: " ", invalid: true},
		{name: "zero", source: "0", invalid: true},
		{name: "negative", source: "-1", invalid: true},
		{name: "invalid", source: "bad", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)
			t.Setenv(conf.CopyConcurrency, tc.copyEnvironment)
			t.Setenv(conf.CopyStatConcurrency, tc.environment)
			viper.AutomaticEnv()
			source, destination := viper.New(), viper.New()
			for _, config := range []*viper.Viper{viper.GetViper(), source, destination} {
				for setting, value := range conf.CommonDefaultConfigValues {
					config.SetDefault(setting, value)
				}
			}
			for _, setting := range []struct {
				config      *viper.Viper
				name, value string
			}{
				{viper.GetViper(), conf.CopyConcurrency, tc.copyMain},
				{viper.GetViper(), conf.CopyStatConcurrency, tc.main},
				{source, conf.CopyConcurrency, tc.copySource},
				{source, conf.CopyStatConcurrency, tc.source},
				{destination, conf.CopyStatConcurrency, tc.destination},
			} {
				if setting.value != "" {
					require.NoError(t, setting.config.MergeConfigMap(map[string]any{setting.name: setting.value}))
				}
			}

			options, err := copyutil.OptionsFromConfigs(source, destination)
			if tc.invalid {
				require.ErrorContains(t, err, conf.CopyStatConcurrency)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantConcurrency, options.Concurrency)
			require.Equal(t, tc.wantStatConcurrency, options.StatConcurrency)
		})
	}
}

type gatedStatFolder struct {
	storage.Folder
	release     chan struct{}
	releaseOnce sync.Once
	inFlight    atomic.Int32
	maxInFlight atomic.Int32
}

func (f *gatedStatFolder) open() {
	f.releaseOnce.Do(func() { close(f.release) })
}

func (f *gatedStatFolder) StatObject(ctx context.Context, name string) (storage.Object, error) {
	current := f.inFlight.Add(1)
	defer f.inFlight.Add(-1)
	for peak := f.maxInFlight.Load(); current > peak && !f.maxInFlight.CompareAndSwap(peak, current); {
		peak = f.maxInFlight.Load()
	}
	<-f.release
	return f.Folder.StatObject(ctx, name)
}

func TestExecuteLimitsDestinationStatsSeparately(t *testing.T) {
	const objects = 10
	for _, tc := range []struct {
		name                     string
		options                  copyutil.ExecuteOptions
		wantStats, wantTransfers int
	}{
		{"more stats than transfers", copyutil.ExecuteOptions{Concurrency: 2, StatConcurrency: 5}, 5, 2},
		{"fewer stats than transfers", copyutil.ExecuteOptions{Concurrency: 4, StatConcurrency: 1}, 1, 4},
		{"unset uses default", copyutil.ExecuteOptions{Concurrency: 3}, 4, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)
			synctest.Test(t, func(t *testing.T) {
				from := testtools.MakeDefaultInMemoryStorageFolder()
				for i := range objects {
					require.NoError(t, from.PutObject(t.Context(), fmt.Sprintf("object-%02d", i), bytes.NewBufferString("payload")))
				}
				puts := &gatedPutFolder{Folder: testtools.MakeDefaultInMemoryStorageFolder(), release: make(chan struct{})}
				to := &gatedStatFolder{Folder: puts, release: make(chan struct{})}
				defer puts.open()
				defer to.open()
				done := make(chan error, 1)
				go func() { done <- copyAll(t.Context(), from, to, tc.options) }()

				synctest.Wait()
				require.EqualValues(t, tc.wantStats, to.inFlight.Load())
				require.Zero(t, puts.inFlight.Load())
				to.open()
				synctest.Wait()
				require.EqualValues(t, tc.wantTransfers, puts.inFlight.Load())
				puts.open()
				require.NoError(t, <-done)
				require.EqualValues(t, tc.wantStats, to.maxInFlight.Load())
				require.EqualValues(t, tc.wantTransfers, puts.maxInFlight.Load())
				copied, err := storage.ListFolderRecursively(t.Context(), to)
				require.NoError(t, err)
				require.Len(t, copied, objects)
			})
		})
	}
}

func TestExecuteRejectsInvalidStatConcurrency(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	from := testtools.MakeDefaultInMemoryStorageFolder()
	require.NoError(t, from.PutObject(t.Context(), "object", bytes.NewBufferString("payload")))
	to := testtools.MakeDefaultInMemoryStorageFolder()

	err := copyAll(t.Context(), from, to, copyutil.ExecuteOptions{Concurrency: 2, StatConcurrency: -1})
	require.ErrorContains(t, err, conf.CopyStatConcurrency)
	require.ErrorContains(t, err, "-1")
	copied, listErr := storage.ListFolderRecursively(t.Context(), to)
	require.NoError(t, listErr)
	require.Empty(t, copied)
}

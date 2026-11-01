package copy_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	conf "github.com/wal-g/wal-g/internal/config"
	copyutil "github.com/wal-g/wal-g/internal/copy"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/testtools"
)

func TestExecuteDestinationSizeConflicts(t *testing.T) {
	for _, tc := range []struct {
		name, source, destination string
	}{
		{"empty destination", "opaque\x00\xffpayload", ""},
		{"shorter destination", "opaque\x00\xffpayload", "short"},
		{"longer destination", "opaque\x00\xffpayload", "longer destination payload"},
		{"empty source", "", "nonempty destination"},
	} {
		for _, overwrite := range []bool{false, true} {
			t.Run(tc.name+"/"+map[bool]string{false: "normal", true: "disaster"}[overwrite], func(t *testing.T) {
				from := testtools.MakeDefaultInMemoryStorageFolder()
				target := testtools.MakeDefaultInMemoryStorageFolder()
				require.NoError(t, from.PutObject(t.Context(), "object", bytes.NewBufferString(tc.source)))
				require.NoError(t, target.PutObject(t.Context(), "object", bytes.NewBufferString(tc.destination)))
				to := &recordingPutFolder{Folder: target}
				plan, err := copyutil.NewPlan(t.Context(), from, to)
				require.NoError(t, err)
				plan.AddInline("a-missing", []byte("missing"), copyutil.PayloadPhase, false)
				require.NoError(t, plan.AddObject("object", "object", copyutil.PayloadPhase, false))
				plan.AddInline("sentinel", []byte("{}"), copyutil.BackupCommitPhase, false)

				err = copyutil.Execute(t.Context(), plan, copyutil.ExecuteOptions{OverwriteConflicts: overwrite})
				if !overwrite {
					require.ErrorContains(t, err, "conflicts with source")
					require.Empty(t, to.Writes())
					requireObjectContents(t, to, "object", tc.destination)
					return
				}
				require.NoError(t, err)
				requireObjectContents(t, to, "object", tc.source)
				requireObjectContents(t, to, "a-missing", "missing")
				requireObjectContents(t, to, "sentinel", "{}")
				require.Len(t, to.Writes(), 3)
				require.Equal(t, "sentinel", to.Writes()[2])
			})
		}
	}
}

func TestDisasterCopyOverwriteIndependentOfSkipPattern(t *testing.T) {
	for _, pattern := range []string{"unset", "", "^wal_005/.*\\.history$"} {
		t.Run(pattern, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)
			sourceConfig, destinationConfig := viper.New(), viper.New()
			sourceConfig.Set(conf.DisasterCopy, true)
			if pattern != "unset" {
				viper.Set(conf.DisasterCopySkipFiles, "^object$")
				require.NoError(t, destinationConfig.MergeConfigMap(map[string]any{conf.DisasterCopySkipFiles: pattern}))
			}
			options, err := copyutil.OptionsFromConfigs(sourceConfig, destinationConfig)
			require.NoError(t, err)
			options.SkipErrorsFor, err = copyutil.DisasterCopyPattern(sourceConfig, destinationConfig)
			require.NoError(t, err)
			if pattern == "unset" || pattern == "" {
				require.Nil(t, options.SkipErrorsFor)
			} else {
				require.False(t, options.SkipErrorsFor.MatchString("object"))
			}

			from := testtools.MakeDefaultInMemoryStorageFolder()
			to := testtools.MakeDefaultInMemoryStorageFolder()
			require.NoError(t, from.PutObject(t.Context(), "object", bytes.NewBufferString("payload")))
			require.NoError(t, to.PutObject(t.Context(), "object", bytes.NewBufferString("")))
			plan, err := copyutil.NewPlan(t.Context(), from, to)
			require.NoError(t, err)
			require.NoError(t, plan.AddObject("object", "object", copyutil.PayloadPhase, false))
			require.NoError(t, copyutil.Execute(t.Context(), plan, options))
			requireObjectContents(t, to, "object", "payload")
		})
	}
}

func TestExecuteOverwritePreservesSkipAndPublicationOrder(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	target := testtools.MakeDefaultInMemoryStorageFolder()
	for _, name := range []string{"payload", "metadata", "sentinel"} {
		require.NoError(t, target.PutObject(t.Context(), name, bytes.NewBufferString("")))
	}
	for _, name := range []string{"unchanged", "mutable"} {
		require.NoError(t, target.PutObject(t.Context(), name, bytes.NewBufferString("target")))
	}
	to := &recordingPutFolder{Folder: target}
	plan, err := copyutil.NewPlan(t.Context(), from, to)
	require.NoError(t, err)
	plan.AddInline("payload", []byte("payload"), copyutil.PayloadPhase, false)
	plan.AddInline("unchanged", []byte("source"), copyutil.PayloadPhase, false)
	plan.AddInline("metadata", []byte("metadata"), copyutil.MetadataPhase, false)
	plan.AddInline("sentinel", []byte("{}"), copyutil.BackupCommitPhase, false)
	plan.AddInline("mutable", []byte("source"), copyutil.FinalCommitPhase, true)

	require.NoError(t, copyutil.Execute(t.Context(), plan, copyutil.ExecuteOptions{OverwriteConflicts: true}))
	require.Equal(t, []string{"payload", "metadata", "sentinel", "mutable"}, to.Writes())
	requireObjectContents(t, to, "unchanged", "target")
	requireObjectContents(t, to, "mutable", "source")
	requireObjectContents(t, to, "sentinel", "{}")
}

func TestExecuteOverwritesConflictsUsingCrossFolderCopy(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		t.Run(map[bool]string{false: "server copy", true: "streaming fallback"}[fallback], func(t *testing.T) {
			from := testtools.MakeDefaultInMemoryStorageFolder()
			target := testtools.MakeDefaultInMemoryStorageFolder()
			require.NoError(t, from.PutObject(t.Context(), "object", bytes.NewBufferString("payload")))
			require.NoError(t, target.PutObject(t.Context(), "object", bytes.NewBufferString("")))
			to := &recordingCrossCopyFolder{Folder: target}
			if fallback {
				to.copyErr = errors.New("copy API unavailable")
			}
			plan, err := copyutil.NewPlan(t.Context(), from, to)
			require.NoError(t, err)
			require.NoError(t, plan.AddObject("object", "object", copyutil.PayloadPhase, false))
			require.NoError(t, copyutil.Execute(t.Context(), plan, copyutil.ExecuteOptions{
				OverwriteConflicts: true, UseServerSideCopy: true, ServerSideCopyFallback: fallback,
			}))
			require.Equal(t, 1, to.CopyCalls())
			require.Equal(t, map[bool]int{false: 0, true: 1}[fallback], to.PutCalls())
			requireObjectContents(t, to, "object", "payload")
		})
	}
}

func requireObjectContents(t *testing.T, folder storage.Folder, name, want string) {
	t.Helper()
	reader, err := folder.ReadObject(t.Context(), name)
	require.NoError(t, err)
	defer reader.Close()
	content, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, want, string(content))
}

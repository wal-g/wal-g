package pg

import (
	"bytes"
	"context"
	jsonencoding "encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/progress"
)

// Redirecting stderr must leave scripts and JSON consumers free of animation.
func TestProgressSkipsRedirectedStderr(t *testing.T) {
	for _, fileOutput := range []bool{false, true} {
		cmd := &cobra.Command{Use: "backup-push"}
		cmd.Flags().Bool("progress", true, "")
		ctx := context.Background()
		cmd.SetContext(ctx)
		var out bytes.Buffer
		cmd.SetErr(&out)
		var file *os.File
		if fileOutput {
			var err error
			file, err = os.CreateTemp(t.TempDir(), "stderr")
			require.NoError(t, err)
			defer file.Close()
			cmd.SetErr(file)
		}
		stop := startProgress(cmd)
		stop()
		assert.Equal(t, ctx, cmd.Context())
		assert.Zero(t, out.Len())
		if file != nil {
			info, err := file.Stat()
			require.NoError(t, err)
			assert.Zero(t, info.Size())
		}
	}
}

func TestProgressFile(t *testing.T) {
	for _, tc := range []struct {
		name, flag, status string
		panics             bool
	}{
		{name: "terminal disabled", flag: "false", status: "completed"},
		{name: "terminal enabled", flag: "true", status: "completed"},
		{name: "panic", flag: "false", status: "failed", panics: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("TERM", "dumb")
			dir := t.TempDir()
			cmd := &cobra.Command{Use: "backup-fetch"}
			addProgressFlag(cmd)
			require.NoError(t, cmd.Flags().Set(progressFlag, tc.flag))
			require.NoError(t, cmd.Flags().Set(progressDirectoryFlag, dir))
			cmd.SetContext(t.Context())
			var output bytes.Buffer
			cmd.SetErr(&output)
			run := func() {
				stop := startProgress(cmd)
				defer stop()
				progress.SetStage(cmd.Context(), "selecting backup")
				_, err := io.Copy(io.Discard, progress.TrackReader(cmd.Context(), strings.NewReader("payload")))
				require.NoError(t, err)
				if tc.panics {
					panic("bad sentinel")
				}
			}
			if tc.panics {
				require.PanicsWithValue(t, "bad sentinel", run)
			} else {
				run()
			}
			assert.Equal(t, t.Context(), cmd.Context())
			assert.Zero(t, output.Len(), "file reporting must not emit terminal control bytes")
			var snapshot progress.Snapshot
			require.Eventually(t, func() bool {
				paths, err := filepath.Glob(filepath.Join(dir, "walg-progress-*.json"))
				if err != nil || len(paths) != 1 {
					return false
				}
				data, err := os.ReadFile(paths[0])
				return err == nil && jsonencoding.Unmarshal(data, &snapshot) == nil && snapshot.Status == tc.status
			}, 3*time.Second, 10*time.Millisecond)
			assert.Equal(t, "selecting backup", snapshot.Stage)
			assert.Equal(t, int64(7), snapshot.TransferredBytes)
		})
	}
}

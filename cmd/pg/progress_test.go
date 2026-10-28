package pg

import (
	"bytes"
	"context"
	jsonencoding "encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

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

func TestProgressFileWithoutTerminal(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			t.Setenv("TERM", "dumb")
			dir := t.TempDir()
			cmd := &cobra.Command{Use: "backup-fetch"}
			addProgressFlag(cmd)
			require.NoError(t, cmd.Flags().Set("progress", fmt.Sprint(enabled)))
			require.NoError(t, cmd.Flags().Set("progress-dir", dir))
			require.NoError(t, cmd.Flags().Set("progress-retention", "0s"))
			original := context.Background()
			cmd.SetContext(original)
			var output bytes.Buffer
			cmd.SetErr(&output)
			stop := startProgress(cmd)
			progress.SetStage(cmd.Context(), "selecting backup")
			_, err := io.Copy(io.Discard, progress.TrackReader(cmd.Context(), strings.NewReader("payload")))
			require.NoError(t, err)
			stop()
			assert.Equal(t, original, cmd.Context())
			assert.Zero(t, output.Len(), "file reporting must not emit terminal control bytes")
			paths, err := filepath.Glob(filepath.Join(dir, "walg-progress-*.json"))
			require.NoError(t, err)
			require.Len(t, paths, 1)
			data, err := os.ReadFile(paths[0])
			require.NoError(t, err)
			var snapshot progress.Snapshot
			require.NoError(t, jsonencoding.Unmarshal(data, &snapshot))
			assert.Equal(t, "completed", snapshot.Status)
			assert.Equal(t, "selecting backup", snapshot.Stage)
			assert.Equal(t, int64(7), snapshot.TransferredBytes)
		})
	}
}

func TestProgressFileDoesNotReportSuccessOnPanic(t *testing.T) {
	dir := t.TempDir()
	cmd := &cobra.Command{Use: "backup-fetch"}
	addProgressFlag(cmd)
	require.NoError(t, cmd.Flags().Set("progress-dir", dir))
	cmd.SetContext(context.Background())
	cmd.SetErr(io.Discard)
	require.PanicsWithValue(t, "bad sentinel", func() {
		stop := startProgress(cmd)
		defer stop()
		panic("bad sentinel")
	})
	paths, err := filepath.Glob(filepath.Join(dir, "walg-progress-*.json"))
	require.NoError(t, err)
	require.Len(t, paths, 1)
	data, err := os.ReadFile(paths[0])
	require.NoError(t, err)
	var snapshot progress.Snapshot
	require.NoError(t, jsonencoding.Unmarshal(data, &snapshot))
	assert.Equal(t, "failed", snapshot.Status)
}

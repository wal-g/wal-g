package pg

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

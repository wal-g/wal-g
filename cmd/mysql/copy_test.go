package mysql

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal/testutils"
)

func TestCopyCommandsStartWebServer(t *testing.T) {
	for _, command := range []*cobra.Command{uniformCopyCmd, copyCmd} {
		t.Run(command.Use, func(t *testing.T) {
			testutils.RequireServesPprof(t, func() { command.PersistentPreRun(command, nil) })
		})
	}
}

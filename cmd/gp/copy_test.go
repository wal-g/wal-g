package gp

import (
	"testing"

	"github.com/wal-g/wal-g/internal/testutils"
)

func TestCopyStartsWebServer(t *testing.T) {
	testutils.RequireServesPprof(t, func() { backupCopyCmd.PersistentPreRun(backupCopyCmd, nil) })
}

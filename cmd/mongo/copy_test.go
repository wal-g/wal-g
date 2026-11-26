package mongo

import (
	"testing"

	"github.com/wal-g/wal-g/internal/testutils"
)

func TestCopyStartsWebServer(t *testing.T) {
	testutils.RequireServesPprof(t, func() { copyCmd.PersistentPreRun(copyCmd, nil) })
}

package redis_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	redisdb "github.com/wal-g/wal-g/internal/databases/redis"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

func TestDeleteRejectsBrokenOrRedirectedSentinel(t *testing.T) {
	for _, contents := range []string{"broken", `{"BackupName":"stream_other","Permanent":false}`} {
		t.Run(contents, func(t *testing.T) {
			root := testtools.MakeDefaultInMemoryStorageFolder()
			base := root.GetSubFolder(utility.BaseBackupPath)
			require.NoError(t, base.PutObject(t.Context(), "stream_target"+utility.SentinelSuffix, strings.NewReader(contents)))
			require.NoError(t, base.PutObject(t.Context(), "stream_other/payload", strings.NewReader("keep")))
			require.Error(t, redisdb.HandleBackupDelete(t.Context(), root, "stream_target", false))
			exists, err := base.Exists(t.Context(), "stream_other/payload")
			require.NoError(t, err)
			require.True(t, exists)
		})
	}
}

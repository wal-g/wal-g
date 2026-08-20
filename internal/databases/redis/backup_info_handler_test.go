package redis

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

func TestHandleBackupInfoTagFormatsNonStringFields(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	backup := archive.Backup{
		BackupName:  "aof_20260729T144621Z",
		HasTS:       true,
		TSFileCount: 92,
	}
	serialized, err := json.Marshal(backup)
	require.NoError(t, err)
	require.NoError(t, folder.GetSubFolder(utility.BaseBackupPath).PutObject(
		t.Context(), backup.BackupName+utility.SentinelSuffix, bytes.NewReader(serialized),
	))

	for _, test := range []struct {
		tag  string
		want string
	}{
		{tag: "HasTS", want: "true\n"},
		{tag: "TSFileCount", want: "92\n"},
	} {
		t.Run(test.tag, func(t *testing.T) {
			output := new(bytes.Buffer)

			HandleBackupInfo(t.Context(), folder, backup.BackupName, output, test.tag)

			require.Equal(t, test.want, output.String())
		})
	}
}

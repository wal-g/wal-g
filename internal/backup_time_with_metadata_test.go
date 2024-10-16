package internal

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/printlist"
)

func TestBackupTimeWithMetadata_PrintableFields(t *testing.T) {
	btm := BackupTimeWithMetadata{
		BackupTime: BackupTime{
			BackupName:  "my first backup",
			Time:        time.Unix(1692883732, 0).UTC(),
			WalFileName: "my/wal/file/name",
			StorageName: "my failover ssh storage",
		},
		GenericMetadata: GenericMetadata{
			StartTime: time.Unix(1692883722, 0).UTC(),
		},
	}
	got := btm.PrintableFields()
	prettyTime := "Thursday, 24-Aug-23 13:28:42 UTC"
	want := []printlist.TableField{
		{
			Name:        "name",
			PrettyName:  "Name",
			Value:       "my first backup",
			PrettyValue: nil,
		},
		{
			Name:        "created",
			PrettyName:  "Created",
			Value:       "2023-08-24T13:28:42Z",
			PrettyValue: &prettyTime,
		},
		{
			Name:        "wal_segment_backup_start",
			PrettyName:  "WAL segment backup start",
			Value:       "my/wal/file/name",
			PrettyValue: nil,
		},
	}

	assert.Equal(t, want, got)
}

func TestBackupTimeWithMetadata_MarshalJSON(t *testing.T) {
	btm := BackupTimeWithMetadata{
		BackupTime: BackupTime{
			BackupName:  "my first backup",
			Time:        time.Unix(1692883732, 0).UTC(),
			WalFileName: "my/wal/file/name",
			StorageName: "my failover ssh storage",
		},
		GenericMetadata: GenericMetadata{
			StartTime: time.Unix(1692883722, 0).UTC(),
		},
	}

	got, err := json.Marshal(btm)
	require.NoError(t, err)

	wantJSON := `{
	"backup_name": "my first backup",
	"time": "2023-08-24T13:28:52Z",
	"wal_file_name": "my/wal/file/name",
	"storage_name": "my failover ssh storage"
}`
	assert.JSONEq(t, wantJSON, string(got))
}

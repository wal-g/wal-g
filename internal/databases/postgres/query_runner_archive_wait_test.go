package postgres

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBackupHandlerStopBackupArchiveWait(t *testing.T) {
	for _, tc := range []struct {
		version     int
		defaultCall string
		noWaitCall  string
	}{
		{90600, "pg_stop_backup(false)", "pg_stop_backup(false)"},
		{100000, "pg_stop_backup(false)", "pg_stop_backup(false, false)"},
		{140000, "pg_stop_backup(false)", "pg_stop_backup(false, false)"},
		{150000, "pg_backup_stop()", "pg_backup_stop(false)"},
		{180000, "pg_backup_stop()", "pg_backup_stop(false)"},
	} {
		t.Run(fmt.Sprint(tc.version), func(t *testing.T) {
			runner := &PgQueryRunner{Version: tc.version}
			handler := &BackupHandler{Workers: BackupWorkers{QueryRunner: runner}}
			for _, enabled := range []bool{false, true, false} {
				handler.Arguments.waitForWALArchive = enabled
				require.NoError(t, handler.initQueryRunner(t.Context()))
				query, err := runner.BuildStopBackup()
				require.NoError(t, err)
				call := tc.defaultCall
				if enabled {
					call = tc.noWaitCall
				}
				require.Equal(t, "SELECT labelfile, spcmapfile, lsn FROM pg_catalog."+call, query)
			}
		})
	}
}

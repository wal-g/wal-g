package mysql

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustMariadbGTID(t *testing.T, s string) *gomysql.MariadbGTIDSet {
	t.Helper()
	set, err := gomysql.ParseMariadbGTIDSet(s)
	require.NoError(t, err)
	mset, ok := set.(*gomysql.MariadbGTIDSet)
	require.True(t, ok)
	return mset
}

func TestParseMariadbGTIDChecked(t *testing.T) {
	tests := []struct {
		name    string
		gtidStr string
		wantNil bool
	}{
		{"empty string", "", true},
		{"valid mariadb gtid", "0-1-100", false},
		{"mysql-style gtid is not mariadb", "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5", true},
		{"garbage", "not-a-gtid", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseMariadbGTIDChecked(tt.gtidStr)
			if tt.wantNil {
				assert.Nil(t, got)
			} else {
				assert.NotNil(t, got)
			}
		})
	}
}

// newTestReplayHandler builds a replayHandler with GTID-lookup and replay
// stubbed out, so the skip/replay logic can be tested without real binlog
// files or a configured replay command.
//
// binlogEndStates maps a binlog name to its own end-of-file GTID checkpoint
// (read from the *next* file in the real implementation); binlogEndErrors
// makes that lookup fail for a given binlog instead.
func newTestReplayHandler(
	t *testing.T,
	appliedGTID string,
	files []string,
	binlogEndStates map[string]string,
	binlogEndErrors map[string]error,
) (rh *replayHandler, paths []string, replayed *[]string) {
	t.Helper()
	return newTestReplayHandlerWithBoundary(t, appliedGTID, "", files, binlogEndStates, binlogEndErrors)
}

// newTestReplayHandlerWithBoundary adds control over the backup's boundary
// file name, to test the file-number pre-filter alongside a GTID checkpoint.
func newTestReplayHandlerWithBoundary(
	t *testing.T,
	appliedGTID string,
	backupFileName string,
	files []string,
	binlogEndStates map[string]string,
	binlogEndErrors map[string]error,
) (rh *replayHandler, paths []string, replayed *[]string) {
	t.Helper()
	dir := t.TempDir()
	replayed = &[]string{}

	// nextName -> the binlog right before it.
	prevOf := make(map[string]string, len(files))
	for i := 1; i < len(files); i++ {
		prevOf[files[i]] = files[i-1]
	}

	rh = new(replayHandler)
	rh.endTS = "2026-01-01 00:00:00"
	rh.backupBinlogPos.LastGTID = appliedGTID
	rh.backupBinlogPos.FileName = backupFileName
	rh.logCh = make(chan string, binlogFetchAhead)
	rh.errCh = make(chan error, 1)

	rh.getPreviousGTIDs = func(filename, flavor string) (gomysql.GTIDSet, error) {
		nextName := filepath.Base(filename)
		prevName, ok := prevOf[nextName]
		if !ok {
			t.Fatalf("no known predecessor configured for %s", nextName)
		}
		if err, ok := binlogEndErrors[prevName]; ok {
			return nil, err
		}
		gtid, ok := binlogEndStates[prevName]
		if !ok {
			t.Fatalf("no configured end-state GTID for binlog %s", prevName)
		}
		return mustMariadbGTID(t, gtid), nil
	}
	rh.doReplay = func(ctx context.Context, binlogPath string) error {
		*replayed = append(*replayed, filepath.Base(binlogPath))
		return nil
	}

	for _, name := range files {
		p := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(p, nil, 0600))
		paths = append(paths, p)
	}

	go rh.replayLogs(context.Background())
	return rh, paths, replayed
}

func feedAll(t *testing.T, rh *replayHandler, paths []string) {
	t.Helper()
	for _, p := range paths {
		require.NoError(t, rh.handleBinlog(p))
	}
}

func TestReplayLogs_SkipsBinlogsFullyCoveredByBackupGTIDAcrossFailover(t *testing.T) {
	// After a failover, binlogs come from a different server, and some
	// only contain transactions already in the backup (relayed from the
	// old master). Those must be skipped by GTID, not just file order.
	files := []string{"mysql-bin.000005", "mysql-bin.000001", "mysql-bin.000002", "mysql-bin.000003"}
	rh, paths, replayed := newTestReplayHandler(t, "0-1-100", files,
		map[string]string{
			"mysql-bin.000005": "0-1-100", // old master, fully covered
			"mysql-bin.000001": "0-1-100", // new master, still just relayed data
			"mysql-bin.000002": "0-1-150", // new master, genuinely new data
		},
		nil,
	)
	feedAll(t, rh, paths)

	require.NoError(t, rh.wait())

	// Only the file with new GTIDs onward gets replayed -- skipping stops
	// once we've had to replay something.
	assert.Equal(t, []string{"mysql-bin.000002", "mysql-bin.000003"}, *replayed)
}

func TestReplayLogs_NoGTIDCheckpointReplaysEverything(t *testing.T) {
	// No BinLogLastGTID recorded (MySQL, or MariaDB <10.8) -- skipping
	// stays off, everything gets replayed.
	files := []string{"mysql-bin.000005", "mysql-bin.000006"}
	rh, paths, replayed := newTestReplayHandler(t, "", files, nil, nil)
	feedAll(t, rh, paths)

	require.NoError(t, rh.wait())
	assert.Equal(t, files, *replayed)
}

func TestReplayLogs_LastFileWithNoLookaheadIsReplayed(t *testing.T) {
	// Stream ends with a file still pending -- replay it rather than
	// silently drop data.
	files := []string{"mysql-bin.000005"}
	rh, paths, replayed := newTestReplayHandler(t, "0-1-100", files, nil, nil)
	feedAll(t, rh, paths)

	require.NoError(t, rh.wait())
	assert.Equal(t, files, *replayed)
}

func TestReplayLogs_GTIDLookupErrorFallsBackToReplaying(t *testing.T) {
	files := []string{"mysql-bin.000005", "mysql-bin.000006"}
	rh, paths, replayed := newTestReplayHandler(t, "0-1-100", files,
		nil,
		map[string]error{"mysql-bin.000005": assert.AnError},
	)
	feedAll(t, rh, paths)

	require.NoError(t, rh.wait())
	// Both replayed: the failed lookup falls back to replaying
	// mysql-bin.000005 and turns off skipping for the rest of the run.
	assert.Equal(t, files, *replayed)
}

func TestReplayLogs_GTIDOverridesFileNumberPreFilterAfterFailover(t *testing.T) {
	// Lower file number than the boundary, but still genuinely new data.
	files := []string{"mysql-bin.000001", "mysql-bin.000002"}
	rh, paths, replayed := newTestReplayHandlerWithBoundary(t, "0-1-100", "mysql-bin.000005", files,
		map[string]string{
			"mysql-bin.000001": "0-1-150", // genuinely new, despite the low file number
		},
		nil,
	)
	feedAll(t, rh, paths)

	require.NoError(t, rh.wait())
	assert.Equal(t, files, *replayed)
}

func TestReplayLogs_FileNumberPreFilterStillAppliesWithoutGTID(t *testing.T) {
	// No GTID checkpoint -- the old filename-only shortcut still applies.
	files := []string{"mysql-bin.000001", "mysql-bin.000002"}
	rh, paths, replayed := newTestReplayHandlerWithBoundary(t, "", "mysql-bin.000005", files, nil, nil)
	feedAll(t, rh, paths)

	require.NoError(t, rh.wait())
	assert.Empty(t, *replayed)
}

func TestShouldApplyStartPosition(t *testing.T) {
	tests := []struct {
		name           string
		backupFileName string
		backupPosition int64
		backupLastGTID string
		actualServerID uint32
		serverIDErr    error
		candidateName  string
		want           bool
	}{
		{
			name:           "no gtid recorded -- old filename-only behavior",
			backupFileName: "mysql-bin.000003", backupPosition: 385, backupLastGTID: "",
			candidateName: "mysql-bin.000003", want: true,
		},
		{
			name:           "matching filename and matching server id",
			backupFileName: "mysql-bin.000003", backupPosition: 385, backupLastGTID: "0-1-12",
			actualServerID: 1, candidateName: "mysql-bin.000003", want: true,
		},
		{
			name:           "matching filename but different server -- coincidental match after failover",
			backupFileName: "mysql-bin.000003", backupPosition: 385, backupLastGTID: "0-1-12",
			actualServerID: 2, candidateName: "mysql-bin.000003", want: false,
		},
		{
			name:           "different filename entirely",
			backupFileName: "mysql-bin.000003", backupPosition: 385, backupLastGTID: "0-1-12",
			actualServerID: 1, candidateName: "mysql-bin.000004", want: false,
		},
		{
			name:           "no position recorded",
			backupFileName: "mysql-bin.000003", backupPosition: 0, backupLastGTID: "0-1-12",
			actualServerID: 1, candidateName: "mysql-bin.000003", want: false,
		},
		{
			name:           "server id lookup fails -- don't trust the position",
			backupFileName: "mysql-bin.000003", backupPosition: 385, backupLastGTID: "0-1-12",
			serverIDErr: assert.AnError, candidateName: "mysql-bin.000003", want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rh := new(replayHandler)
			rh.backupBinlogPos.FileName = tt.backupFileName
			rh.backupBinlogPos.FilePosition = tt.backupPosition
			rh.backupBinlogPos.LastGTID = tt.backupLastGTID
			rh.getBinlogServerID = func(string) (uint32, error) {
				return tt.actualServerID, tt.serverIDErr
			}

			got := rh.shouldApplyStartPosition(filepath.Join(t.TempDir(), tt.candidateName))
			assert.Equal(t, tt.want, got)
		})
	}
}

package mysql

import (
	"path/filepath"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseGTIDChecked(t *testing.T) {
	tests := []struct {
		name       string
		gtidStr    string
		wantFlavor string
	}{
		{"empty string", "", ""},
		{"mariadb gtid", "0-1-100", gomysql.MariaDBFlavor},
		{"mysql gtid", "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5", gomysql.MySQLFlavor},
		{"garbage", "not-a-gtid", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			set, flavor := parseGTIDChecked(tt.gtidStr)
			assert.Equal(t, tt.wantFlavor, flavor)
			if tt.wantFlavor == "" {
				assert.Nil(t, set)
			} else {
				assert.NotNil(t, set)
			}
		})
	}
}

// fakeHandler records handleBinlog calls in order, standing in for whatever
// gtidSkipFilter wraps (replay, and eventually binlog-server).
type fakeHandler struct {
	handled []string
}

func (h *fakeHandler) handleBinlog(binlogPath string) error {
	h.handled = append(h.handled, filepath.Base(binlogPath))
	return nil
}

// newTestFilter builds a gtidSkipFilter with GTID-lookup stubbed, so the
// skip/forward logic can be tested without real binlog files.
//
// binlogEndStates maps a binlog name to its own end-of-file GTID checkpoint
// (read from the *next* file in the real implementation); binlogEndErrors
// makes that lookup fail for a given binlog instead.
func newTestFilter(
	t *testing.T,
	checkpointStr string,
	files []string,
	binlogEndStates map[string]string,
	binlogEndErrors map[string]error,
) (f *gtidSkipFilter, inner *fakeHandler, paths []string) {
	t.Helper()
	dir := t.TempDir()
	inner = &fakeHandler{}

	// nextName -> the binlog right before it.
	prevOf := make(map[string]string, len(files))
	for i := 1; i < len(files); i++ {
		prevOf[files[i]] = files[i-1]
	}

	checkpoint, flavor := parseGTIDChecked(checkpointStr)
	f = newGTIDSkipFilter(inner, checkpoint, flavor)
	f.getPreviousGTIDs = func(filename, _ string) (gomysql.GTIDSet, error) {
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
		set, err := gomysql.ParseGTIDSet(gomysql.MariaDBFlavor, gtid)
		require.NoError(t, err)
		return set, nil
	}

	for _, name := range files {
		paths = append(paths, filepath.Join(dir, name))
	}
	return f, inner, paths
}

func feedAll(t *testing.T, f *gtidSkipFilter, paths []string) {
	t.Helper()
	for _, p := range paths {
		require.NoError(t, f.handleBinlog(p))
	}
}

func TestGTIDSkipFilter_SkipsBinlogsFullyCoveredByCheckpointAcrossFailover(t *testing.T) {
	// After a failover, binlogs come from a different server, and some
	// only contain transactions already in the backup (relayed from the
	// old master). Those must be skipped by GTID, not just file order.
	files := []string{"mysql-bin.000005", "mysql-bin.000001", "mysql-bin.000002", "mysql-bin.000003"}
	f, inner, paths := newTestFilter(t, "0-1-100", files,
		map[string]string{
			"mysql-bin.000005": "0-1-100", // old master, fully covered
			"mysql-bin.000001": "0-1-100", // new master, still just relayed data
			"mysql-bin.000002": "0-1-150", // new master, genuinely new data
		},
		nil,
	)
	feedAll(t, f, paths)
	require.NoError(t, f.flush())

	// Only the file with new GTIDs onward gets forwarded -- skipping stops
	// once we've had to forward something.
	assert.Equal(t, []string{"mysql-bin.000002", "mysql-bin.000003"}, inner.handled)
}

func TestGTIDSkipFilter_NoCheckpointForwardsEverything(t *testing.T) {
	// No checkpoint recorded (MySQL, or MariaDB <10.8) -- skipping stays
	// off, everything gets forwarded.
	files := []string{"mysql-bin.000005", "mysql-bin.000006"}
	f, inner, paths := newTestFilter(t, "", files, nil, nil)
	feedAll(t, f, paths)
	require.NoError(t, f.flush())

	assert.Equal(t, files, inner.handled)
}

func TestGTIDSkipFilter_LastFileWithNoLookaheadIsForwarded(t *testing.T) {
	// Stream ends with a file still pending -- forward it on flush rather
	// than silently drop data.
	files := []string{"mysql-bin.000005"}
	f, inner, paths := newTestFilter(t, "0-1-100", files, nil, nil)
	feedAll(t, f, paths)
	require.NoError(t, f.flush())

	assert.Equal(t, files, inner.handled)
}

func TestGTIDSkipFilter_LookupErrorFallsBackToForwarding(t *testing.T) {
	files := []string{"mysql-bin.000005", "mysql-bin.000006"}
	f, inner, paths := newTestFilter(t, "0-1-100", files,
		nil,
		map[string]error{"mysql-bin.000005": assert.AnError},
	)
	feedAll(t, f, paths)
	require.NoError(t, f.flush())

	// Both forwarded: the failed lookup falls back to forwarding
	// mysql-bin.000005 and turns off skipping for the rest of the run.
	assert.Equal(t, files, inner.handled)
}

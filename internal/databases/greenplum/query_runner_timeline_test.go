package greenplum

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

func TestBuildReadTimelineBySegmentUsesVersionedWalFunctions(t *testing.T) {
	tests := []struct {
		name     string
		version  int
		function string
	}{
		{name: "Greenplum 6", version: 90400, function: "pg_xlogfile_name"},
		{name: "Greenplum 7", version: 120000, function: "pg_walfile_name"},
		{name: "Cloudberry", version: 140000, function: "pg_walfile_name"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := &GpQueryRunner{PgQueryRunner: &postgres.PgQueryRunner{Version: test.version}}
			query, err := runner.buildReadTimelineBySegment()
			require.NoError(t, err)
			require.Contains(t, query, test.function)
			require.Contains(t, query, "gp_dist_random('gp_id')")
			require.Contains(t, query, "SELECT -1")
		})
	}
}

func TestValidateRestorePointTimelinesRequiresExactTopology(t *testing.T) {
	restoreLSNs := map[int]string{-1: "0/10", 0: "0/20"}
	require.NoError(t, validateRestorePointTimelines(restoreLSNs, map[int]uint32{-1: 1, 0: 2}))
	require.Error(t, validateRestorePointTimelines(restoreLSNs, map[int]uint32{-1: 1}))
	require.Error(t, validateRestorePointTimelines(restoreLSNs, map[int]uint32{-1: 1, 1: 2}))
	require.Error(t, validateRestorePointTimelines(restoreLSNs, map[int]uint32{-1: 1, 0: 0}))
}

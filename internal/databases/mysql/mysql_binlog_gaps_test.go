package mysql

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
)

func TestCalculateGap(t *testing.T) {
	const (
		A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
		B = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	)

	cases := []struct {
		name          string
		gtidArchived  string
		gtidPurged    string
		previousGTIDs string
		existingGaps  *GapTrackingState
		expectedGap   string
	}{
		{
			name:          "the very first uploaded binlog file",
			gtidArchived:  "",
			gtidPurged:    "",
			previousGTIDs: "",
			existingGaps:  nil,
			expectedGap:   "",
		},
		{
			name:          "no missing transactions",
			gtidArchived:  A + ":1-3",
			gtidPurged:    "",
			previousGTIDs: A + ":1-3",
			existingGaps:  &GapTrackingState{},
			expectedGap:   "",
		},
		{
			name:          "gap in previous gtids",
			gtidArchived:  A + ":1",
			gtidPurged:    "",
			previousGTIDs: A + ":1-3",
			existingGaps:  nil,
			expectedGap:   A + ":2-3",
		},
		{
			name:          "gap in gtid_purged",
			gtidArchived:  B + ":1",
			gtidPurged:    B + ":1-3",
			previousGTIDs: "",
			existingGaps:  &GapTrackingState{},
			expectedGap:   B + ":2-3",
		},
		{
			name:          "gap in both",
			gtidArchived:  B + ":4-5",
			gtidPurged:    B + ":1-4",
			previousGTIDs: B + ":5-9",
			existingGaps:  &GapTrackingState{},
			expectedGap:   B + ":1-3:6-9",
		},
		{
			name:          "gap already recorded with location",
			gtidArchived:  A + ":1:3",
			gtidPurged:    "",
			previousGTIDs: A + ":1-3",
			existingGaps: &GapTrackingState{Gaps: []BinlogGap{
				{GTIDSet: A + ":2", ReplayStopBinlog: binlog1},
			}},
			expectedGap: "",
		},
		{
			name:          "gap already recorded without location",
			gtidArchived:  A + ":1",
			gtidPurged:    "",
			previousGTIDs: A + ":1-3",
			existingGaps: &GapTrackingState{
				GapWithoutLocation: A + ":2-3",
			},
			expectedGap: "",
		},
		{
			name:          "only return the new gap",
			gtidArchived:  A + ":1:3",
			gtidPurged:    "",
			previousGTIDs: A + ":1-4",
			existingGaps: &GapTrackingState{Gaps: []BinlogGap{
				{GTIDSet: A + ":2", ReplayStopBinlog: binlog1},
			}},
			expectedGap: A + ":4",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			previousGTIDs, err := mysql.ParseMysqlGTIDSet(tc.previousGTIDs)
			require.NoError(t, err)
			purgedGTIDs, err := mysql.ParseMysqlGTIDSet(tc.gtidPurged)
			require.NoError(t, err)
			sentinel := BinlogSentinelDto{GTIDArchived: tc.gtidArchived, GapTracking: tc.existingGaps}

			gap, err := calculateGap(&sentinel, previousGTIDs, purgedGTIDs)

			require.NoError(t, err)
			require.Equal(t, tc.expectedGap, gap.String())
			require.Equal(t, tc.gtidArchived, sentinel.GTIDArchived)
			require.Equal(t, tc.previousGTIDs, previousGTIDs.String())
			require.Equal(t, tc.gtidPurged, purgedGTIDs.String())
		})
	}
}

func TestAppendGap(t *testing.T) {
	const A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

	cases := []struct {
		name             string
		initialGaps      GapTrackingState
		gap              string
		replayStopBinlog string
		expectedGaps     GapTrackingState
	}{
		{
			name:             "append a located gap",
			initialGaps:      GapTrackingState{},
			gap:              A + ":2-3",
			replayStopBinlog: binlog1,
			expectedGaps: GapTrackingState{Gaps: []BinlogGap{
				{GTIDSet: A + ":2-3", ReplayStopBinlog: binlog1},
			}},
		},
		{
			name: "append after an existing gap",
			initialGaps: GapTrackingState{Gaps: []BinlogGap{
				{GTIDSet: A + ":2", ReplayStopBinlog: binlog1},
			}},
			gap:              A + ":4",
			replayStopBinlog: binlog3,
			expectedGaps: GapTrackingState{Gaps: []BinlogGap{
				{GTIDSet: A + ":2", ReplayStopBinlog: binlog1},
				{GTIDSet: A + ":4", ReplayStopBinlog: binlog3},
			}},
		},
		{
			name: "merge gaps in lastUploadedBinlog",
			initialGaps: GapTrackingState{Gaps: []BinlogGap{
				{GTIDSet: A + ":2", ReplayStopBinlog: binlog1},
			}},
			gap:              A + ":3",
			replayStopBinlog: binlog1,
			expectedGaps: GapTrackingState{Gaps: []BinlogGap{
				{GTIDSet: A + ":2-3", ReplayStopBinlog: binlog1},
			}},
		},
		{
			name:             "append a gap without location",
			initialGaps:      GapTrackingState{},
			gap:              A + ":2-3",
			replayStopBinlog: UnknownGapLocation,
			expectedGaps: GapTrackingState{
				GapWithoutLocation: A + ":2-3",
			},
		},

		{
			name: "append a located gap after the gap without location",
			initialGaps: GapTrackingState{
				GapWithoutLocation: A + ":2",
			},
			gap:              A + ":4",
			replayStopBinlog: binlog3,
			expectedGaps: GapTrackingState{
				GapWithoutLocation: A + ":2",
				Gaps: []BinlogGap{
					{GTIDSet: A + ":4", ReplayStopBinlog: binlog3},
				},
			},
		},
		{
			name: "merge transactions into the gap without location",
			initialGaps: GapTrackingState{
				GapWithoutLocation: A + ":2",
			},
			gap:              A + ":3",
			replayStopBinlog: UnknownGapLocation,
			expectedGaps: GapTrackingState{
				GapWithoutLocation: A + ":2-3",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gaps := tc.initialGaps

			err := appendGap(&gaps, tc.gap, tc.replayStopBinlog)

			require.NoError(t, err)
			require.Equal(t, tc.expectedGaps, gaps)
		})
	}
}

func TestCompactGaps(t *testing.T) {
	const A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

	cases := []struct {
		name         string
		initialGaps  GapTrackingState
		expectedGaps GapTrackingState
	}{
		{
			name: "compact without an unknown-location gap",
			initialGaps: GapTrackingState{Gaps: []BinlogGap{
				{GTIDSet: A + ":2", ReplayStopBinlog: binlog1},
				{GTIDSet: A + ":4", ReplayStopBinlog: binlog3},
				{GTIDSet: A + ":6", ReplayStopBinlog: binlog5},
			}},
			expectedGaps: GapTrackingState{
				GapWithoutLocation: A + ":2",
				Gaps: []BinlogGap{
					{GTIDSet: A + ":4", ReplayStopBinlog: binlog3},
					{GTIDSet: A + ":6", ReplayStopBinlog: binlog5},
				},
			},
		},
		{
			name: "compact with an existing gap without location",
			initialGaps: GapTrackingState{
				GapWithoutLocation: A + ":2",
				Gaps: []BinlogGap{
					{GTIDSet: A + ":4", ReplayStopBinlog: binlog3},
					{GTIDSet: A + ":6", ReplayStopBinlog: binlog5},
				},
			},
			expectedGaps: GapTrackingState{
				GapWithoutLocation: A + ":2:4",
				Gaps: []BinlogGap{
					{GTIDSet: A + ":6", ReplayStopBinlog: binlog5},
				},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gaps := tc.initialGaps

			err := compactGaps(&gaps)

			require.NoError(t, err)
			require.Equal(t, tc.expectedGaps, gaps)
		})
	}
}

func TestUpdateSentinelGaps(t *testing.T) {
	const (
		A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
		B = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	)

	cases := []struct {
		name               string
		previousGTIDs      string
		initialGaps        *GapTrackingState
		lastUploadedBinlog string
		maxGapArraySize    int
		expectedGaps       *GapTrackingState
		expectedChanged    bool
	}{

		{
			name:               "initialize gap metadata without a gap",
			previousGTIDs:      A + ":1",
			initialGaps:        nil,
			lastUploadedBinlog: binlog1,
			maxGapArraySize:    3,
			expectedGaps:       &GapTrackingState{},
			expectedChanged:    true,
		},
		{
			name:               "initialize gap metadata",
			previousGTIDs:      A + ":1-2",
			initialGaps:        nil,
			lastUploadedBinlog: binlog1,
			maxGapArraySize:    3,
			expectedGaps: &GapTrackingState{
				GapWithoutLocation: A + ":2",
			},
			expectedChanged: true,
		},
		{
			name:               "record a located gap",
			previousGTIDs:      A + ":1-2",
			initialGaps:        &GapTrackingState{},
			lastUploadedBinlog: binlog1,
			maxGapArraySize:    3,
			expectedGaps: &GapTrackingState{Gaps: []BinlogGap{
				{GTIDSet: A + ":2", ReplayStopBinlog: binlog1},
			}},
			expectedChanged: true,
		},
		{
			name:               "record a gap without lastUploadedBinlog",
			previousGTIDs:      A + ":1-2",
			initialGaps:        &GapTrackingState{},
			lastUploadedBinlog: "",
			maxGapArraySize:    3,
			expectedGaps: &GapTrackingState{
				GapWithoutLocation: A + ":2",
			},
			expectedChanged: true,
		},
		{
			name:          "already recorded gap does not change metadata",
			previousGTIDs: A + ":1-2",
			initialGaps: &GapTrackingState{
				GapWithoutLocation: A + ":2",
			},
			lastUploadedBinlog: binlog1,
			maxGapArraySize:    3,
			expectedGaps: &GapTrackingState{
				GapWithoutLocation: A + ":2",
			},
			expectedChanged: false,
		},
		{
			name:          "compact when appending exceeds the limit",
			previousGTIDs: A + ":1-2",
			initialGaps: &GapTrackingState{Gaps: []BinlogGap{
				{GTIDSet: B + ":2", ReplayStopBinlog: "previous-primary-bin.000001"},
				{GTIDSet: B + ":4", ReplayStopBinlog: "previous-primary-bin.000003"},
			}},
			lastUploadedBinlog: binlog1,
			maxGapArraySize:    2,
			expectedGaps: &GapTrackingState{
				GapWithoutLocation: B + ":2",
				Gaps: []BinlogGap{
					{GTIDSet: B + ":4", ReplayStopBinlog: "previous-primary-bin.000003"},
					{GTIDSet: A + ":2", ReplayStopBinlog: binlog1},
				},
			},
			expectedChanged: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			previousGTIDs, err := mysql.ParseMysqlGTIDSet(tc.previousGTIDs)
			require.NoError(t, err)
			purgedGTIDs, err := mysql.ParseMysqlGTIDSet("")
			require.NoError(t, err)
			sentinel := BinlogSentinelDto{
				GTIDArchived:       A + ":1",
				LastUploadedBinlog: tc.lastUploadedBinlog,
				GapTracking:        tc.initialGaps,
			}

			changed, err := updateSentinelGaps(&sentinel, previousGTIDs, purgedGTIDs, tc.maxGapArraySize)

			require.NoError(t, err)
			require.Equal(t, tc.expectedChanged, changed)
			require.Equal(t, tc.expectedGaps, sentinel.GapTracking)
			require.Equal(t, tc.lastUploadedBinlog, sentinel.LastUploadedBinlog)
			require.Equal(t, A+":1", sentinel.GTIDArchived)
		})
	}
}

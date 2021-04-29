package postgres_test

import (
	"testing"

	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/stretchr/testify/assert"
)

// TestWalSegmentRunner_AllExists tests the case when all segments exists
func TestWalSegmentRunner_AllExists(t *testing.T) {
	timelineId := uint32(1)
	stopSegmentNo := postgres.WalSegmentNo(300)
	startSegmentNo := postgres.WalSegmentNo(600)

	testWalSegmentRunnerSingleTimeline(t, stopSegmentNo, startSegmentNo, timelineId, make(map[postgres.WalSegmentNo]bool))
}

// TestWalSegmentRunner_MissingSegments tests the case when some segments are missing
func TestWalSegmentRunner_MissingSegments(t *testing.T) {
	timelineId := uint32(1)
	stopSegmentNo := postgres.WalSegmentNo(300)
	startSegmentNo := postgres.WalSegmentNo(400)

	missingSegments := map[postgres.WalSegmentNo]bool{
		301: true,
		305: true,
		310: true,
		311: true,
		312: true,
	}

	testWalSegmentRunnerSingleTimeline(t, stopSegmentNo, startSegmentNo, timelineId, missingSegments)
}

// TestWalSegmentRunner_AllMissing tests the case when all of the segments are missing
func TestWalSegmentRunner_AllMissing(t *testing.T) {
	timelineId := uint32(1)
	stopSegmentNo := postgres.WalSegmentNo(1)
	startSegmentNo := postgres.WalSegmentNo(5)

	missingSegments := map[postgres.WalSegmentNo]bool{
		1: true,
		2: true,
		3: true,
		4: true,
	}

	testWalSegmentRunnerSingleTimeline(t, stopSegmentNo, startSegmentNo, timelineId, missingSegments)
}

// TestWalSegmentRunner_SwitchTimelines tests that WalSegmentRunner correctly switch timelines
func TestWalSegmentRunner_SwitchTimelines(t *testing.T) {
	startTimelineId := uint32(2)
	stopSegmentNo := postgres.WalSegmentNo(1)
	startSegment := postgres.WalSegmentDescription{
		Number:   postgres.WalSegmentNo(8),
		Timeline: startTimelineId,
	}

	storageSegmentsByTimeline := map[uint32][]postgres.WalSegmentNo{
		1: {
			1, 2, 3, 4, 5, 6,
		},
		2: {
			5, 6, 7,
		},
	}

	expectedFoundSegmentsByTimeline := map[uint32][]postgres.WalSegmentNo{
		1: {
			1, 2, 3, 4,
		},
		2: {
			5, 6, 7,
		},
	}

	timelineSwitchMap := map[postgres.WalSegmentNo]*postgres.TimelineHistoryRecord{
		5: postgres.NewTimelineHistoryRecord(1, 5*postgres.WalSegmentSize+1, ""),
	}

	testWalSegmentRunnerMultipleTimelines(t, stopSegmentNo, startSegment, timelineSwitchMap,
		storageSegmentsByTimeline, expectedFoundSegmentsByTimeline)
}

// TestWalSegmentRunner_SwitchTimelines tests that WalSegmentRunner correctly handles timeline switches
// when missing segments exist
func TestWalSegmentRunner_SwitchTimelinesMissingSegments(t *testing.T) {
	startTimelineId := uint32(2)
	stopSegmentNo := postgres.WalSegmentNo(1)
	startSegment := postgres.WalSegmentDescription{
		Number:   postgres.WalSegmentNo(8),
		Timeline: startTimelineId,
	}

	storageSegmentsByTimeline := map[uint32][]postgres.WalSegmentNo{
		1: {
			1, 3, 4, 6,
		},
		2: {
			6, 7,
		},
	}

	expectedFoundSegmentsByTimeline := map[uint32][]postgres.WalSegmentNo{
		1: {
			1, 3, 4,
		},
		2: {
			6, 7,
		},
	}

	timelineSwitchMap := map[postgres.WalSegmentNo]*postgres.TimelineHistoryRecord{
		5: postgres.NewTimelineHistoryRecord(1, 5*postgres.WalSegmentSize+1, ""),
	}

	testWalSegmentRunnerMultipleTimelines(t, stopSegmentNo, startSegment, timelineSwitchMap,
		storageSegmentsByTimeline, expectedFoundSegmentsByTimeline)
}

func testWalSegmentRunnerMultipleTimelines(t *testing.T, stopSegmentNo postgres.WalSegmentNo,
	startSegment postgres.WalSegmentDescription, timelineSwitchMap map[postgres.WalSegmentNo]*postgres.TimelineHistoryRecord,
	storageSegmentsByTimeline, expectedSegmentsByTimeline map[uint32][]postgres.WalSegmentNo) {

	// convert segments grouped by timeline to segments set
	storageSegments := flattenSegmentsByTimelinesToSet(storageSegmentsByTimeline)
	expectedSegments := flattenSegmentsByTimelinesToSet(expectedSegmentsByTimeline)

	walSegmentRunner := postgres.NewWalSegmentRunner(startSegment, storageSegments, stopSegmentNo, timelineSwitchMap)
	testWalSegmentRunner(t, expectedSegments, walSegmentRunner)
}

func testWalSegmentRunnerSingleTimeline(t *testing.T, stopSegmentNo, startSegmentNo postgres.WalSegmentNo,
	timelineId uint32, missingSegments map[postgres.WalSegmentNo]bool) {

	expectedFoundSegments := make(map[postgres.WalSegmentDescription]bool, 0)
	for i := stopSegmentNo; i < startSegmentNo; i++ {
		// do not add wal segment if it in missing segments set
		if _, ok := missingSegments[i]; !ok {
			segment := postgres.WalSegmentDescription{Number: i, Timeline: timelineId}
			expectedFoundSegments[segment] = true
		}
	}

	startSegment := postgres.WalSegmentDescription{Number: startSegmentNo, Timeline: timelineId}
	walSegmentRunner := postgres.NewWalSegmentRunner(startSegment, expectedFoundSegments, stopSegmentNo, nil)

	testWalSegmentRunner(t, expectedFoundSegments, walSegmentRunner)
}

func testWalSegmentRunner(t *testing.T, expectedFoundSegments map[postgres.WalSegmentDescription]bool,
	walSegmentRunner *postgres.WalSegmentRunner) {

	prevSegment := walSegmentRunner.Current()
	outputSegments := make(map[postgres.WalSegmentDescription]bool)

SegmentRunnerLoop:
	for {
		nextSegment, err := walSegmentRunner.Next()
		if err != nil {
			switch err := err.(type) {
			case postgres.WalSegmentNotFoundError:
				walSegmentRunner.ForceMoveNext()
				nextSegment = walSegmentRunner.Current()
				// check that we should not have found this segment
				assert.NotContains(t, expectedFoundSegments, nextSegment)
			case postgres.ReachedStopSegmentError:
				break SegmentRunnerLoop
			default:
				assert.FailNow(t, "testWalSegmentRunner: unexpected error %v ", err)
			}
		} else {
			// check that we received the correct nextSegment
			assert.Contains(t, expectedFoundSegments, nextSegment)
		}

		// make sure we didn't encounter this segment before
		assert.NotContains(t, outputSegments, nextSegment)

		// check that we traverse segments sequentially
		expectedNextNumber := prevSegment.Number - 1
		assert.Equal(t, expectedNextNumber, nextSegment.Number)

		// add the encountered segment into outputSegments
		outputSegments[nextSegment] = true
		prevSegment = nextSegment
	}

	// check that we traversed through all expected segments
	for segment := range expectedFoundSegments {
		assert.Contains(t, outputSegments, segment)
	}
}

func flattenSegmentsByTimelinesToSet(segmentsByTimeline map[uint32][]postgres.WalSegmentNo) map[postgres.WalSegmentDescription]bool {
	segmentsSet := make(map[postgres.WalSegmentDescription]bool, 0)
	for timeline, segmentNumbers := range segmentsByTimeline {
		for _, segmentNo := range segmentNumbers {
			segment := postgres.WalSegmentDescription{Number: segmentNo, Timeline: timeline}
			segmentsSet[segment] = true
		}
	}
	return segmentsSet
}

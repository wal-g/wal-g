package internal_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"testing"
)

func TestWalSegmentRunner_AllExists(t *testing.T) {
	timelineId := uint32(1)
	minSegmentNo := internal.WalSegmentNo(300)
	maxSegmentNo := internal.WalSegmentNo(600)

	testWalSegmentRunner(t, minSegmentNo, maxSegmentNo, timelineId, make(map[internal.WalSegmentNo]bool))
}

func TestWalSegmentRunner_MissingSegments(t *testing.T) {
	timelineId := uint32(1)
	minSegmentNo := internal.WalSegmentNo(300)
	maxSegmentNo := internal.WalSegmentNo(400)

	missingSegments := map[internal.WalSegmentNo]bool{
		301: true,
		305: true,
		310: true,
		311: true,
		312: true,
	}

	testWalSegmentRunner(t, minSegmentNo, maxSegmentNo, timelineId, missingSegments)
}

func TestWalSegmentRunner_AllMissing(t *testing.T) {
	timelineId := uint32(1)
	minSegmentNo := internal.WalSegmentNo(1)
	maxSegmentNo := internal.WalSegmentNo(5)

	missingSegments := map[internal.WalSegmentNo]bool{
		1: true,
		2: true,
		3: true,
		4: true,
		5: true,
	}

	testWalSegmentRunner(t, minSegmentNo, maxSegmentNo, timelineId, missingSegments)
}

func testWalSegmentRunner(t *testing.T, minSegmentNo, maxSegmentNo internal.WalSegmentNo, timelineId uint32,
	missingSegmentsNo map[internal.WalSegmentNo]bool) {
	walSegments := make(map[internal.WalSegmentDescription]bool, 0)
	for i := minSegmentNo; i <= maxSegmentNo; i++ {
		// do not add wal segment if it in missing segments set
		if _, ok := missingSegmentsNo[i]; !ok {
			segment := internal.WalSegmentDescription{Number: i, Timeline: timelineId}
			walSegments[segment] = true
		}
	}
	startSegment := internal.WalSegmentDescription{Number: maxSegmentNo, Timeline: timelineId}
	walSegmentRunner := internal.NewWalSegmentRunner(startSegment, walSegments, minSegmentNo, nil)

	prevSegment := startSegment
	outputSegments := make(map[internal.WalSegmentDescription]bool)
SegmentRunnerLoop:
	for {
		nextSegment, err := walSegmentRunner.Next()
		if err != nil {
			switch err := err.(type) {
			case internal.WalSegmentNotFoundError:
				walSegmentRunner.ForceMoveNext()
				nextSegment = walSegmentRunner.Current()
				// check that missingSegmentsNo set has this number
				assert.Contains(t, missingSegmentsNo, nextSegment.Number)
			case internal.ReachedStopSegmentError:
				break SegmentRunnerLoop
			default:
				assert.FailNow(t, "testWalSegmentRunner: unexpected error %v ", err)
			}
		}
		// make sure we didn't encounter this segment before
		assert.NotContains(t, outputSegments, nextSegment)

		expectedNext := internal.WalSegmentDescription{Number: prevSegment.Number - 1, Timeline: prevSegment.Timeline}
		assert.Equal(t, expectedNext, nextSegment)

		outputSegments[nextSegment] = true
		prevSegment = nextSegment
	}

	expectedEndSegment := internal.WalSegmentDescription{Number: minSegmentNo, Timeline: timelineId}
	assert.Equal(t, expectedEndSegment, prevSegment)
}

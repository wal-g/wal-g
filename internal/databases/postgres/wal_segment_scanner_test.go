package postgres_test

import (
	"testing"

	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/stretchr/testify/assert"
)

// TestWalSegmentScanner_SingleElement works when stopSegment equals startSegment
func TestWalSegmentScanner_SingleElement(t *testing.T) {
	segmentNo := postgres.WalSegmentNo(5000)

	foundMissing, err := testWalSegmentScannerMissingSegments(t, segmentNo, segmentNo, nil)
	assert.NoError(t, err)

	// check that there are no missing segments found
	assert.Len(t, foundMissing, 0)
}

// TestWalSegmentScanner_NoMissingSegments tests the case when there is no missing elements in sequence
func TestWalSegmentScanner_NoMissingSegments(t *testing.T) {
	stopSegmentNo := postgres.WalSegmentNo(5000)
	startSegmentNo := postgres.WalSegmentNo(5200)
	// no missing segments
	missingSegments := make(map[postgres.WalSegmentNo]bool)

	foundMissing, err := testWalSegmentScannerMissingSegments(t, stopSegmentNo, startSegmentNo, missingSegments)
	assert.NoError(t, err)

	// check that there are no missing segments found
	assert.Len(t, foundMissing, 0)
}

// TestWalSegmentScanner_SearchMissing verifies that WalSegmentScanner returns
// missing segments only in range [stopSegmentNo, startSegmentNo)
func TestWalSegmentScanner_SearchMissing(t *testing.T) {
	stopSegmentNo := postgres.WalSegmentNo(5000)
	startSegmentNo := postgres.WalSegmentNo(5050)
	missingSegmentsNo := map[postgres.WalSegmentNo]bool{
		5001: true,
		5003: true,
		5004: true,
		5010: true,
	}

	foundMissing, err := testWalSegmentScannerMissingSegments(t, stopSegmentNo, startSegmentNo, missingSegmentsNo)
	assert.NoError(t, err)

	assert.Equal(t, missingSegmentsNo, foundMissing)
}

// TestWalSegmentScanner_ScanAllRange tests if WalSegmentScanner scans the entire [stopSegmentNo, startSegmentNo) range
func TestWalSegmentScanner_ScanAllRange(t *testing.T) {
	stopSegmentNo := postgres.WalSegmentNo(5000)
	startSegmentNo := postgres.WalSegmentNo(5050)
	timelineId := uint32(1)
	walSegmentScanner := createWalSegmentScanner(timelineId, stopSegmentNo, startSegmentNo, nil)
	err := walSegmentScanner.Scan(postgres.SegmentScanConfig{
		UnlimitedScan: true,
	})
	assert.NoError(t, err)

	assert.Len(t, walSegmentScanner.ScannedSegments, int(startSegmentNo-stopSegmentNo))

	for _, segment := range walSegmentScanner.ScannedSegments {
		// check that all scanned segments are in range [stopSegmentNo, startSegmentNo)
		assert.True(t, segment.Number >= stopSegmentNo && segment.Number <= startSegmentNo)
		// check that timeline is correct for each segment
		assert.Equal(t, timelineId, segment.Timeline)
	}

	// check that all segments in walSegmentScanner.ScannedSegments are unique
	scannedSegmentNumbersSet := make(map[postgres.WalSegmentNo]bool)
	for _, segment := range walSegmentScanner.ScannedSegments {
		scannedSegmentNumbersSet[segment.Number] = true
	}
	assert.Len(t, scannedSegmentNumbersSet, len(walSegmentScanner.ScannedSegments))
}

// testWalSegmentScannerMissingSegments invokes Scan() method and returns found missing segment numbers
func testWalSegmentScannerMissingSegments(t *testing.T, stopSegmentNo, startSegmentNo postgres.WalSegmentNo,
	lostSegmentNumbers map[postgres.WalSegmentNo]bool) (map[postgres.WalSegmentNo]bool, error) {
	timelineId := uint32(1)
	walSegmentScanner := createWalSegmentScanner(timelineId, stopSegmentNo, startSegmentNo, lostSegmentNumbers)

	err := walSegmentScanner.Scan(postgres.SegmentScanConfig{
		UnlimitedScan:        true,
		MissingSegmentStatus: postgres.Lost,
	})
	assert.NoError(t, err)

	missingSegments := walSegmentScanner.GetMissingSegmentsDescriptions()

	// check that missingSegments slice contains only unique elements
	missingNumbersSet := make(map[postgres.WalSegmentNo]bool)
	for _, segment := range missingSegments {
		missingNumbersSet[segment.Number] = true
	}
	assert.Len(t, missingNumbersSet, len(missingSegments))
	return missingNumbersSet, nil
}

func createWalSegmentScanner(timelineId uint32, stopSegmentNo, startSegmentNo postgres.WalSegmentNo,
	lostSegmentNumbers map[postgres.WalSegmentNo]bool) *postgres.WalSegmentScanner {
	startWalSegment := postgres.WalSegmentDescription{Number: startSegmentNo, Timeline: timelineId}

	walSegments := make(map[postgres.WalSegmentDescription]bool)
	for i := stopSegmentNo; i < startSegmentNo; i++ {
		// if this segment number is not in lost set, add it
		if _, exists := lostSegmentNumbers[i]; !exists {
			segment := postgres.WalSegmentDescription{Number: i, Timeline: timelineId}
			walSegments[segment] = true
		}
	}

	walSegmentRunner := postgres.NewWalSegmentRunner(startWalSegment, walSegments, stopSegmentNo, nil)
	return postgres.NewWalSegmentScanner(walSegmentRunner)
}

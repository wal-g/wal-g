package internal_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
)

func TestFindLastCommonPoint_SameTimeline(t *testing.T) {
	target := []*internal.TimelineHistoryRecord{
		internal.NewTimelineHistoryRecord(1, 12, ""),
		internal.NewTimelineHistoryRecord(2, 16, ""),
	}
	source := []*internal.TimelineHistoryRecord{
		internal.NewTimelineHistoryRecord(1, 8, ""),
		internal.NewTimelineHistoryRecord(2, 20, ""),
	}
	_, _, err := internal.FindLastCommonPoint(target, source)
	assert.Error(t, err)
}

func TestFindLastCommonPoint_OneOnTheFirstTimeline(t *testing.T) {
	wantLsn, wantTimeline := uint64(12), uint32(1)
	target := []*internal.TimelineHistoryRecord{
		internal.NewTimelineHistoryRecord(wantTimeline, wantLsn, ""),
	}
	source := []*internal.TimelineHistoryRecord{}
	lastLsn, lastTimeline, err := internal.FindLastCommonPoint(target, source)
	assert.Nil(t, err)
	assert.Equal(t, wantLsn, lastLsn)
	assert.Equal(t, wantTimeline, lastTimeline)
}

func TestFindLastCommonPoint_OneOnTheFirstTimelineMirror(t *testing.T) {
	wantLsn, wantTimeline := uint64(12), uint32(1)
	target := []*internal.TimelineHistoryRecord{}
	source := []*internal.TimelineHistoryRecord{
		internal.NewTimelineHistoryRecord(wantTimeline, wantLsn, ""),
	}
	lastLsn, lastTimeline, err := internal.FindLastCommonPoint(target, source)
	assert.Nil(t, err)
	assert.Equal(t, wantLsn, lastLsn)
	assert.Equal(t, wantTimeline, lastTimeline)
}

func TestFindLastCommonPoint_FirstRandomCase(t *testing.T) {
	wantLsn, wantTimeline := uint64(12), uint32(2)
	target := []*internal.TimelineHistoryRecord{
		internal.NewTimelineHistoryRecord(1, 5, ""),
		internal.NewTimelineHistoryRecord(2, 16, ""),
		internal.NewTimelineHistoryRecord(3, 20, ""),
	}
	source := []*internal.TimelineHistoryRecord{
		internal.NewTimelineHistoryRecord(1, 5, ""),
		internal.NewTimelineHistoryRecord(wantTimeline, wantLsn, ""),
	}
	lastLsn, lastTimeline, err := internal.FindLastCommonPoint(target, source)
	assert.Nil(t, err)
	assert.Equal(t, wantLsn, lastLsn)
	assert.Equal(t, wantTimeline, lastTimeline)
}

func TestFindLastCommonPoint_SecondRandomCase(t *testing.T) {
	wantLsn, wantTimeline := uint64(20), uint32(3)
	target := []*internal.TimelineHistoryRecord{
		internal.NewTimelineHistoryRecord(1, 5, ""),
		internal.NewTimelineHistoryRecord(2, 16, ""),
		internal.NewTimelineHistoryRecord(wantTimeline, wantLsn, ""),
		internal.NewTimelineHistoryRecord(4, 26, ""),
	}
	source := []*internal.TimelineHistoryRecord{
		internal.NewTimelineHistoryRecord(1, 5, ""),
		internal.NewTimelineHistoryRecord(2, 16, ""),
		internal.NewTimelineHistoryRecord(wantTimeline, wantLsn, ""),
	}
	lastLsn, lastTimeline, err := internal.FindLastCommonPoint(target, source)
	assert.Nil(t, err)
	assert.Equal(t, wantLsn, lastLsn)
	assert.Equal(t, wantTimeline, lastTimeline)
}

func TestFindLastCommonPoint_ThirdRandomCase(t *testing.T) {
	wantLsn, wantTimeline := uint64(4), uint32(1)
	target := []*internal.TimelineHistoryRecord{
		internal.NewTimelineHistoryRecord(wantTimeline, wantLsn, ""),
		internal.NewTimelineHistoryRecord(2, 7, ""),
		internal.NewTimelineHistoryRecord(3, 10, ""),
		internal.NewTimelineHistoryRecord(4, 25, ""),
	}
	source := []*internal.TimelineHistoryRecord{
		internal.NewTimelineHistoryRecord(1, 5, ""),
		internal.NewTimelineHistoryRecord(2, 9, ""),
		internal.NewTimelineHistoryRecord(3, 30, ""),
	}
	lastLsn, lastTimeline, err := internal.FindLastCommonPoint(target, source)
	assert.Nil(t, err)
	assert.Equal(t, wantLsn, lastLsn)
	assert.Equal(t, wantTimeline, lastTimeline)
}

func TestFindLastCommonPoint_FourthRandomCase(t *testing.T) {
	wantLsn, wantTimeline := uint64(32), uint32(4)
	target := []*internal.TimelineHistoryRecord{
		internal.NewTimelineHistoryRecord(1, 5, ""),
		internal.NewTimelineHistoryRecord(2, 9, ""),
		internal.NewTimelineHistoryRecord(3, 30, ""),
		internal.NewTimelineHistoryRecord(wantTimeline, wantLsn, ""),
		internal.NewTimelineHistoryRecord(5, 48, ""),
	}
	source := []*internal.TimelineHistoryRecord{
		internal.NewTimelineHistoryRecord(1, 5, ""),
		internal.NewTimelineHistoryRecord(2, 9, ""),
		internal.NewTimelineHistoryRecord(3, 30, ""),
		internal.NewTimelineHistoryRecord(4, 36, ""),
	}
	lastLsn, lastTimeline, err := internal.FindLastCommonPoint(target, source)
	assert.Nil(t, err)
	assert.Equal(t, wantLsn, lastLsn)
	assert.Equal(t, wantTimeline, lastTimeline)
}

func TestFindLastCommonPoint_FifthRandomCase(t *testing.T) {
	wantLsn, wantTimeline := uint64(32), uint32(4)
	target := []*internal.TimelineHistoryRecord{
		internal.NewTimelineHistoryRecord(1, 5, ""),
		internal.NewTimelineHistoryRecord(2, 9, ""),
		internal.NewTimelineHistoryRecord(3, 30, ""),
		internal.NewTimelineHistoryRecord(wantTimeline, wantLsn, ""),
		internal.NewTimelineHistoryRecord(5, 48, ""),
	}
	source := []*internal.TimelineHistoryRecord{
		internal.NewTimelineHistoryRecord(1, 5, ""),
		internal.NewTimelineHistoryRecord(2, 9, ""),
		internal.NewTimelineHistoryRecord(3, 30, ""),
		internal.NewTimelineHistoryRecord(wantTimeline, wantLsn, ""),
	}
	lastLsn, lastTimeline, err := internal.FindLastCommonPoint(target, source)
	assert.Nil(t, err)
	assert.Equal(t, wantLsn, lastLsn)
	assert.Equal(t, wantTimeline, lastTimeline)
}

func TestGetMissingWals_TwoWalsOneMissingInARow(t *testing.T) {
	wals := internal.NewSegmentsSequence(1, internal.WalSegmentNo(2))
	walsToTl := make(map[uint32]*internal.WalSegmentsSequence)
	walsToTl[1] = wals
	tlHistRecs := make(map[uint32]*internal.TimelineHistoryRecord)
	tlHistRecs[1] = internal.NewTimelineHistoryRecord(1, 1, "")
	result, err := internal.GetMissingWals(1, 1, 1, tlHistRecs, walsToTl)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, "000000010000000000000001", result[0])
}

func TestGetMissingWals_ThreeWalsTwoMissingInARow(t *testing.T) {
	wals := internal.NewSegmentsSequence(1, internal.WalSegmentNo(3))
	walsToTl := make(map[uint32]*internal.WalSegmentsSequence)
	walsToTl[1] = wals
	tlHistRecs := make(map[uint32]*internal.TimelineHistoryRecord)
	tlHistRecs[1] = internal.NewTimelineHistoryRecord(1, 1, "")
	result, err := internal.GetMissingWals(1, 1, 1, tlHistRecs, walsToTl)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(result))
	assert.Equal(t, "000000010000000000000002", result[0])
	assert.Equal(t, "000000010000000000000001", result[1])
}

func TestGetMissingWals_FourWalsTwoMissingNotARow(t *testing.T) {
	wals1Tl := internal.NewSegmentsSequence(1, internal.WalSegmentNo(2))
	wals2Tl := internal.NewSegmentsSequence(2, internal.WalSegmentNo(4))
	walsToTl := make(map[uint32]*internal.WalSegmentsSequence)
	walsToTl[1] = wals1Tl
	walsToTl[2] = wals2Tl
	tlHistRecs := make(map[uint32]*internal.TimelineHistoryRecord)
	tlHistRecs[1] = internal.NewTimelineHistoryRecord(1, 1, "")
	tlHistRecs[2] = internal.NewTimelineHistoryRecord(2, 3, "")
	result, err := internal.GetMissingWals(1, 1, 2, tlHistRecs, walsToTl)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(result))
	assert.Equal(t, "000000020000000000000003", result[0])
	assert.Equal(t, "000000010000000000000001", result[1])
}

func TestGetMissingWals_ThreeWalsTwoMissingOnPrevTl(t *testing.T) {
	wals := internal.NewSegmentsSequence(2, internal.WalSegmentNo(3))
	walsToTl := make(map[uint32]*internal.WalSegmentsSequence)
	walsToTl[2] = wals
	tlHistRecs := make(map[uint32]*internal.TimelineHistoryRecord)
	tlHistRecs[1] = internal.NewTimelineHistoryRecord(1, 1, "")
	tlHistRecs[2] = internal.NewTimelineHistoryRecord(2, 3, "")
	result, err := internal.GetMissingWals(1, 1, 2, tlHistRecs, walsToTl)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(result))
	assert.Equal(t, "000000010000000000000002", result[0])
	assert.Equal(t, "000000010000000000000001", result[1])
}

func TestGetMissingWals_FiveWalsThreeMissingOnThreeTl(t *testing.T) {
	wals2Tl := internal.NewSegmentsSequence(2, internal.WalSegmentNo(3))
	wals3Tl := internal.NewSegmentsSequence(3, internal.WalSegmentNo(5))
	walsToTl := make(map[uint32]*internal.WalSegmentsSequence)
	walsToTl[2] = wals2Tl
	walsToTl[3] = wals3Tl
	tlHistRecs := make(map[uint32]*internal.TimelineHistoryRecord)
	tlHistRecs[1] = internal.NewTimelineHistoryRecord(1, 1, "")
	tlHistRecs[2] = internal.NewTimelineHistoryRecord(2, 2, "")
	tlHistRecs[3] = internal.NewTimelineHistoryRecord(3, 4, "")
	result, err := internal.GetMissingWals(1, 1, 3, tlHistRecs, walsToTl)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(result))
	assert.Equal(t, "000000030000000000000004", result[0])
	assert.Equal(t, "000000020000000000000002", result[1])
	assert.Equal(t, "000000010000000000000001", result[2])
}

func TestGetMissingWals_TwoWalsZeroMissing(t *testing.T) {
	wals := internal.NewSegmentsSequence(1, internal.WalSegmentNo(1))
	wals.AddWalSegmentNo(internal.WalSegmentNo(2))
	walsToTl := make(map[uint32]*internal.WalSegmentsSequence)
	walsToTl[1] = wals
	tlHistRecs := make(map[uint32]*internal.TimelineHistoryRecord)
	tlHistRecs[1] = internal.NewTimelineHistoryRecord(1, 1, "")
	result, err := internal.GetMissingWals(1, 1, 1, tlHistRecs, walsToTl)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(result))
}

func TestGetMissingWals_FourWalsZeroMissingInTwoTls(t *testing.T) {
	wals1Tl := internal.NewSegmentsSequence(1, internal.WalSegmentNo(1))
	wals1Tl.AddWalSegmentNo(internal.WalSegmentNo(2))
	wals2Tl := internal.NewSegmentsSequence(2, internal.WalSegmentNo(3))
	wals2Tl.AddWalSegmentNo(internal.WalSegmentNo(4))
	walsToTl := make(map[uint32]*internal.WalSegmentsSequence)
	walsToTl[1] = wals1Tl
	walsToTl[2] = wals2Tl
	tlHistRecs := make(map[uint32]*internal.TimelineHistoryRecord)
	tlHistRecs[1] = internal.NewTimelineHistoryRecord(1, 1, "")
	tlHistRecs[2] = internal.NewTimelineHistoryRecord(2, 3, "")
	result, err := internal.GetMissingWals(1, 1, 2, tlHistRecs, walsToTl)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(result))
}

func TestGetMissingWals_FirstRandomCase(t *testing.T) {
	wals4Tl := internal.NewSegmentsSequence(4, internal.WalSegmentNo(9))
	wals4Tl.AddWalSegmentNo(internal.WalSegmentNo(10))
	wals4Tl.AddWalSegmentNo(internal.WalSegmentNo(11))
	wals3Tl := internal.NewSegmentsSequence(3, internal.WalSegmentNo(8))
	walsToTl := make(map[uint32]*internal.WalSegmentsSequence)
	walsToTl[4] = wals4Tl
	walsToTl[3] = wals3Tl
	tlHistRecs := make(map[uint32]*internal.TimelineHistoryRecord)
	tlHistRecs[1] = internal.NewTimelineHistoryRecord(1, 1, "")
	tlHistRecs[2] = internal.NewTimelineHistoryRecord(2, 3, "")
	tlHistRecs[3] = internal.NewTimelineHistoryRecord(3, 6, "")
	tlHistRecs[4] = internal.NewTimelineHistoryRecord(4, 9, "")
	result, err := internal.GetMissingWals(5, 2, 4, tlHistRecs, walsToTl)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(result))
	assert.Equal(t, "000000030000000000000007", result[0])
	assert.Equal(t, "000000030000000000000006", result[1])
	assert.Equal(t, "000000020000000000000005", result[2])
}

func TestGetMissingWals_SecondRandomCase(t *testing.T) {
	wals3Tl := internal.NewSegmentsSequence(3, internal.WalSegmentNo(7))
	wals3Tl.AddWalSegmentNo(internal.WalSegmentNo(10))
	wals3Tl.AddWalSegmentNo(internal.WalSegmentNo(11))
	wals2Tl := internal.NewSegmentsSequence(2, internal.WalSegmentNo(3))
	wals2Tl.AddWalSegmentNo(internal.WalSegmentNo(6))
	walsToTl := make(map[uint32]*internal.WalSegmentsSequence)
	walsToTl[2] = wals2Tl
	walsToTl[3] = wals3Tl
	tlHistRecs := make(map[uint32]*internal.TimelineHistoryRecord)
	tlHistRecs[1] = internal.NewTimelineHistoryRecord(1, 1, "")
	tlHistRecs[2] = internal.NewTimelineHistoryRecord(2, 3, "")
	tlHistRecs[3] = internal.NewTimelineHistoryRecord(3, 7, "")
	result, err := internal.GetMissingWals(3, 2, 3, tlHistRecs, walsToTl)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(result))
	assert.Equal(t, "000000030000000000000009", result[0])
	assert.Equal(t, "000000030000000000000008", result[1])
	assert.Equal(t, "000000020000000000000005", result[2])
	assert.Equal(t, "000000020000000000000004", result[3])
}

func TestGetMissingWals_ThirdRandomCase(t *testing.T) {
	wals := internal.NewSegmentsSequence(3, internal.WalSegmentNo(7))
	wals.AddWalSegmentNo(internal.WalSegmentNo(8))
	wals.AddWalSegmentNo(internal.WalSegmentNo(9))
	wals.AddWalSegmentNo(internal.WalSegmentNo(10))
	wals.AddWalSegmentNo(internal.WalSegmentNo(11))
	walsToTl := make(map[uint32]*internal.WalSegmentsSequence)
	walsToTl[3] = wals
	tlHistRecs := make(map[uint32]*internal.TimelineHistoryRecord)
	tlHistRecs[1] = internal.NewTimelineHistoryRecord(1, 1, "")
	tlHistRecs[2] = internal.NewTimelineHistoryRecord(2, 3, "")
	tlHistRecs[3] = internal.NewTimelineHistoryRecord(3, 7, "")
	result, err := internal.GetMissingWals(3, 2, 3, tlHistRecs, walsToTl)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(result))
	assert.Equal(t, "000000020000000000000006", result[0])
	assert.Equal(t, "000000020000000000000005", result[1])
	assert.Equal(t, "000000020000000000000004", result[2])
	assert.Equal(t, "000000020000000000000003", result[3])
}

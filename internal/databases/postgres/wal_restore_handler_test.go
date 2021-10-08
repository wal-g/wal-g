package postgres_test

import (
	"testing"

	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/stretchr/testify/assert"
)

func TestFindLastCommonPoint_SameTimeline(t *testing.T) {
	target := []*postgres.TimelineHistoryRecord{
		postgres.NewTimelineHistoryRecord(1, 12, ""),
		postgres.NewTimelineHistoryRecord(2, 16, ""),
	}
	source := []*postgres.TimelineHistoryRecord{
		postgres.NewTimelineHistoryRecord(1, 8, ""),
		postgres.NewTimelineHistoryRecord(2, 20, ""),
	}
	_, _, err := postgres.FindLastCommonPoint(source, target)
	assert.Error(t, err)
}

func TestFindLastCommonPoint_OneOnTheFirstTimeline(t *testing.T) {
	wantLsn, wantTimeline := uint64(12), uint32(1)
	target := []*postgres.TimelineHistoryRecord{
		postgres.NewTimelineHistoryRecord(wantTimeline, wantLsn, ""),
	}
	source := []*postgres.TimelineHistoryRecord{}
	lastLsn, lastTimeline, err := postgres.FindLastCommonPoint(source, target)
	assert.Nil(t, err)
	assert.Equal(t, wantLsn, lastLsn)
	assert.Equal(t, wantTimeline, lastTimeline)
}

func TestFindLastCommonPoint_OneOnTheFirstTimelineMirror(t *testing.T) {
	wantLsn, wantTimeline := uint64(12), uint32(1)
	target := []*postgres.TimelineHistoryRecord{}
	source := []*postgres.TimelineHistoryRecord{
		postgres.NewTimelineHistoryRecord(wantTimeline, wantLsn, ""),
	}
	lastLsn, lastTimeline, err := postgres.FindLastCommonPoint(source, target)
	assert.Nil(t, err)
	assert.Equal(t, wantLsn, lastLsn)
	assert.Equal(t, wantTimeline, lastTimeline)
}

func TestFindLastCommonPoint_FirstRandomCase(t *testing.T) {
	wantLsn, wantTimeline := uint64(12), uint32(2)
	target := []*postgres.TimelineHistoryRecord{
		postgres.NewTimelineHistoryRecord(1, 5, ""),
		postgres.NewTimelineHistoryRecord(2, 16, ""),
		postgres.NewTimelineHistoryRecord(3, 20, ""),
	}
	source := []*postgres.TimelineHistoryRecord{
		postgres.NewTimelineHistoryRecord(1, 5, ""),
		postgres.NewTimelineHistoryRecord(wantTimeline, wantLsn, ""),
	}
	lastLsn, lastTimeline, err := postgres.FindLastCommonPoint(source, target)
	assert.Nil(t, err)
	assert.Equal(t, wantLsn, lastLsn)
	assert.Equal(t, wantTimeline, lastTimeline)
}

func TestFindLastCommonPoint_SecondRandomCase(t *testing.T) {
	wantLsn, wantTimeline := uint64(20), uint32(3)
	target := []*postgres.TimelineHistoryRecord{
		postgres.NewTimelineHistoryRecord(1, 5, ""),
		postgres.NewTimelineHistoryRecord(2, 16, ""),
		postgres.NewTimelineHistoryRecord(wantTimeline, wantLsn, ""),
		postgres.NewTimelineHistoryRecord(4, 26, ""),
	}
	source := []*postgres.TimelineHistoryRecord{
		postgres.NewTimelineHistoryRecord(1, 5, ""),
		postgres.NewTimelineHistoryRecord(2, 16, ""),
		postgres.NewTimelineHistoryRecord(wantTimeline, wantLsn, ""),
	}
	lastLsn, lastTimeline, err := postgres.FindLastCommonPoint(source, target)
	assert.Nil(t, err)
	assert.Equal(t, wantLsn, lastLsn)
	assert.Equal(t, wantTimeline, lastTimeline)
}

func TestFindLastCommonPoint_ThirdRandomCase(t *testing.T) {
	wantLsn, wantTimeline := uint64(4), uint32(1)
	target := []*postgres.TimelineHistoryRecord{
		postgres.NewTimelineHistoryRecord(wantTimeline, wantLsn, ""),
		postgres.NewTimelineHistoryRecord(2, 7, ""),
		postgres.NewTimelineHistoryRecord(3, 10, ""),
		postgres.NewTimelineHistoryRecord(4, 25, ""),
	}
	source := []*postgres.TimelineHistoryRecord{
		postgres.NewTimelineHistoryRecord(1, 5, ""),
		postgres.NewTimelineHistoryRecord(2, 9, ""),
		postgres.NewTimelineHistoryRecord(3, 30, ""),
	}
	lastLsn, lastTimeline, err := postgres.FindLastCommonPoint(source, target)
	assert.Nil(t, err)
	assert.Equal(t, wantLsn, lastLsn)
	assert.Equal(t, wantTimeline, lastTimeline)
}

func TestFindLastCommonPoint_FourthRandomCase(t *testing.T) {
	wantLsn, wantTimeline := uint64(32), uint32(4)
	target := []*postgres.TimelineHistoryRecord{
		postgres.NewTimelineHistoryRecord(1, 5, ""),
		postgres.NewTimelineHistoryRecord(2, 9, ""),
		postgres.NewTimelineHistoryRecord(3, 30, ""),
		postgres.NewTimelineHistoryRecord(wantTimeline, wantLsn, ""),
		postgres.NewTimelineHistoryRecord(5, 48, ""),
	}
	source := []*postgres.TimelineHistoryRecord{
		postgres.NewTimelineHistoryRecord(1, 5, ""),
		postgres.NewTimelineHistoryRecord(2, 9, ""),
		postgres.NewTimelineHistoryRecord(3, 30, ""),
		postgres.NewTimelineHistoryRecord(4, 36, ""),
	}
	lastLsn, lastTimeline, err := postgres.FindLastCommonPoint(source, target)
	assert.Nil(t, err)
	assert.Equal(t, wantLsn, lastLsn)
	assert.Equal(t, wantTimeline, lastTimeline)
}

func TestFindLastCommonPoint_FifthRandomCase(t *testing.T) {
	wantLsn, wantTimeline := uint64(32), uint32(4)
	target := []*postgres.TimelineHistoryRecord{
		postgres.NewTimelineHistoryRecord(1, 5, ""),
		postgres.NewTimelineHistoryRecord(2, 9, ""),
		postgres.NewTimelineHistoryRecord(3, 30, ""),
		postgres.NewTimelineHistoryRecord(wantTimeline, wantLsn, ""),
		postgres.NewTimelineHistoryRecord(5, 48, ""),
	}
	source := []*postgres.TimelineHistoryRecord{
		postgres.NewTimelineHistoryRecord(1, 5, ""),
		postgres.NewTimelineHistoryRecord(2, 9, ""),
		postgres.NewTimelineHistoryRecord(3, 30, ""),
		postgres.NewTimelineHistoryRecord(wantTimeline, wantLsn, ""),
	}
	lastLsn, lastTimeline, err := postgres.FindLastCommonPoint(source, target)
	assert.Nil(t, err)
	assert.Equal(t, wantLsn, lastLsn)
	assert.Equal(t, wantTimeline, lastTimeline)
}

func TestGetMissingWals_TwoWalsOneMissingInARow(t *testing.T) {
	wals := postgres.NewSegmentsSequence(1, postgres.WalSegmentNo(2))
	walsToTl := make(map[uint32]*postgres.WalSegmentsSequence)
	walsToTl[1] = wals
	tlHistRecs := make(map[uint32]*postgres.TimelineWithSegmentNo)
	tlHistRecs[1] = postgres.NewTimelineWithSegmentNo(0, 1)
	result, err := postgres.GetMissingWals(1, 1, 1, tlHistRecs, walsToTl)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(result))
	if len(result) == 1 {
		assert.Equal(t, "000000010000000000000001", result[0])
	}
}

func TestGetMissingWals_ThreeWalsTwoMissingInARow(t *testing.T) {
	wals := postgres.NewSegmentsSequence(1, postgres.WalSegmentNo(3))
	walsToTl := make(map[uint32]*postgres.WalSegmentsSequence)
	walsToTl[1] = wals
	tlHistRecs := make(map[uint32]*postgres.TimelineWithSegmentNo)
	tlHistRecs[1] = postgres.NewTimelineWithSegmentNo(0, 1)
	result, err := postgres.GetMissingWals(1, 1, 1, tlHistRecs, walsToTl)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(result))
	if len(result) == 2 {
		assert.Equal(t, "000000010000000000000002", result[0])
		assert.Equal(t, "000000010000000000000001", result[1])
	}
}

func TestGetMissingWals_FourWalsTwoMissingNotARow(t *testing.T) {
	wals1Tl := postgres.NewSegmentsSequence(1, postgres.WalSegmentNo(2))
	wals2Tl := postgres.NewSegmentsSequence(2, postgres.WalSegmentNo(4))
	walsToTl := make(map[uint32]*postgres.WalSegmentsSequence)
	walsToTl[1] = wals1Tl
	walsToTl[2] = wals2Tl
	tlHistRecs := make(map[uint32]*postgres.TimelineWithSegmentNo)
	tlHistRecs[1] = postgres.NewTimelineWithSegmentNo(0, 1)
	tlHistRecs[2] = postgres.NewTimelineWithSegmentNo(1, 3)
	result, err := postgres.GetMissingWals(1, 1, 2, tlHistRecs, walsToTl)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(result))
	if len(result) == 2 {
		assert.Equal(t, "000000020000000000000003", result[0])
		assert.Equal(t, "000000010000000000000001", result[1])
	}
}

func TestGetMissingWals_ThreeWalsTwoMissingOnPrevTl(t *testing.T) {
	wals := postgres.NewSegmentsSequence(2, postgres.WalSegmentNo(3))
	walsToTl := make(map[uint32]*postgres.WalSegmentsSequence)
	walsToTl[2] = wals
	tlHistRecs := make(map[uint32]*postgres.TimelineWithSegmentNo)
	tlHistRecs[1] = postgres.NewTimelineWithSegmentNo(0, 1)
	tlHistRecs[2] = postgres.NewTimelineWithSegmentNo(1, 3)
	result, err := postgres.GetMissingWals(1, 1, 2, tlHistRecs, walsToTl)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(result))
	if len(result) == 2 {
		assert.Equal(t, "000000010000000000000002", result[0])
		assert.Equal(t, "000000010000000000000001", result[1])
	}
}

func TestGetMissingWals_FiveWalsThreeMissingOnThreeTl(t *testing.T) {
	wals2Tl := postgres.NewSegmentsSequence(2, postgres.WalSegmentNo(3))
	wals3Tl := postgres.NewSegmentsSequence(3, postgres.WalSegmentNo(5))
	walsToTl := make(map[uint32]*postgres.WalSegmentsSequence)
	walsToTl[2] = wals2Tl
	walsToTl[3] = wals3Tl
	tlHistRecs := make(map[uint32]*postgres.TimelineWithSegmentNo)
	tlHistRecs[1] = postgres.NewTimelineWithSegmentNo(0, 1)
	tlHistRecs[2] = postgres.NewTimelineWithSegmentNo(1, 2)
	tlHistRecs[3] = postgres.NewTimelineWithSegmentNo(2, 4)
	result, err := postgres.GetMissingWals(1, 1, 3, tlHistRecs, walsToTl)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(result))
	if len(result) == 3 {
		assert.Equal(t, "000000030000000000000004", result[0])
		assert.Equal(t, "000000020000000000000002", result[1])
		assert.Equal(t, "000000010000000000000001", result[2])
	}
}

func TestGetMissingWals_TwoWalsZeroMissing(t *testing.T) {
	wals := postgres.NewSegmentsSequence(1, postgres.WalSegmentNo(1))
	wals.AddWalSegmentNo(postgres.WalSegmentNo(2))
	walsToTl := make(map[uint32]*postgres.WalSegmentsSequence)
	walsToTl[1] = wals
	tlHistRecs := make(map[uint32]*postgres.TimelineWithSegmentNo)
	tlHistRecs[1] = postgres.NewTimelineWithSegmentNo(0, 1)
	result, err := postgres.GetMissingWals(1, 1, 1, tlHistRecs, walsToTl)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(result))
}

func TestGetMissingWals_FourWalsZeroMissingInTwoTls(t *testing.T) {
	wals1Tl := postgres.NewSegmentsSequence(1, postgres.WalSegmentNo(1))
	wals1Tl.AddWalSegmentNo(postgres.WalSegmentNo(2))
	wals2Tl := postgres.NewSegmentsSequence(2, postgres.WalSegmentNo(3))
	wals2Tl.AddWalSegmentNo(postgres.WalSegmentNo(4))
	walsToTl := make(map[uint32]*postgres.WalSegmentsSequence)
	walsToTl[1] = wals1Tl
	walsToTl[2] = wals2Tl
	tlHistRecs := make(map[uint32]*postgres.TimelineWithSegmentNo)
	tlHistRecs[1] = postgres.NewTimelineWithSegmentNo(0, 1)
	tlHistRecs[2] = postgres.NewTimelineWithSegmentNo(1, 3)
	result, err := postgres.GetMissingWals(1, 1, 2, tlHistRecs, walsToTl)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(result))
}

func TestGetMissingWals_FirstRandomCase(t *testing.T) {
	wals4Tl := postgres.NewSegmentsSequence(4, postgres.WalSegmentNo(9))
	wals4Tl.AddWalSegmentNo(postgres.WalSegmentNo(10))
	wals4Tl.AddWalSegmentNo(postgres.WalSegmentNo(11))
	wals3Tl := postgres.NewSegmentsSequence(3, postgres.WalSegmentNo(8))
	walsToTl := make(map[uint32]*postgres.WalSegmentsSequence)
	walsToTl[4] = wals4Tl
	walsToTl[3] = wals3Tl
	tlHistRecs := make(map[uint32]*postgres.TimelineWithSegmentNo)
	tlHistRecs[1] = postgres.NewTimelineWithSegmentNo(0, 1)
	tlHistRecs[2] = postgres.NewTimelineWithSegmentNo(1, 3)
	tlHistRecs[3] = postgres.NewTimelineWithSegmentNo(2, 6)
	tlHistRecs[4] = postgres.NewTimelineWithSegmentNo(3, 9)
	result, err := postgres.GetMissingWals(5, 2, 4, tlHistRecs, walsToTl)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(result))
	if len(result) == 3 {
		assert.Equal(t, "000000030000000000000007", result[0])
		assert.Equal(t, "000000030000000000000006", result[1])
		assert.Equal(t, "000000020000000000000005", result[2])
	}
}

func TestGetMissingWals_SecondRandomCase(t *testing.T) {
	wals3Tl := postgres.NewSegmentsSequence(3, postgres.WalSegmentNo(7))
	wals3Tl.AddWalSegmentNo(postgres.WalSegmentNo(10))
	wals3Tl.AddWalSegmentNo(postgres.WalSegmentNo(11))
	wals2Tl := postgres.NewSegmentsSequence(2, postgres.WalSegmentNo(3))
	wals2Tl.AddWalSegmentNo(postgres.WalSegmentNo(6))
	walsToTl := make(map[uint32]*postgres.WalSegmentsSequence)
	walsToTl[2] = wals2Tl
	walsToTl[3] = wals3Tl
	tlHistRecs := make(map[uint32]*postgres.TimelineWithSegmentNo)
	tlHistRecs[1] = postgres.NewTimelineWithSegmentNo(0, 1)
	tlHistRecs[2] = postgres.NewTimelineWithSegmentNo(1, 3)
	tlHistRecs[3] = postgres.NewTimelineWithSegmentNo(2, 7)
	result, err := postgres.GetMissingWals(3, 2, 3, tlHistRecs, walsToTl)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(result))
	if len(result) == 4 {
		assert.Equal(t, "000000030000000000000009", result[0])
		assert.Equal(t, "000000030000000000000008", result[1])
		assert.Equal(t, "000000020000000000000005", result[2])
		assert.Equal(t, "000000020000000000000004", result[3])
	}
}

func TestGetMissingWals_ThirdRandomCase(t *testing.T) {
	wals := postgres.NewSegmentsSequence(3, postgres.WalSegmentNo(7))
	wals.AddWalSegmentNo(postgres.WalSegmentNo(8))
	wals.AddWalSegmentNo(postgres.WalSegmentNo(9))
	wals.AddWalSegmentNo(postgres.WalSegmentNo(10))
	wals.AddWalSegmentNo(postgres.WalSegmentNo(11))
	walsToTl := make(map[uint32]*postgres.WalSegmentsSequence)
	walsToTl[3] = wals
	tlHistRecs := make(map[uint32]*postgres.TimelineWithSegmentNo)
	tlHistRecs[1] = postgres.NewTimelineWithSegmentNo(0, 1)
	tlHistRecs[2] = postgres.NewTimelineWithSegmentNo(1, 3)
	tlHistRecs[3] = postgres.NewTimelineWithSegmentNo(2, 7)
	result, err := postgres.GetMissingWals(3, 2, 3, tlHistRecs, walsToTl)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(result))
	if len(result) == 4 {
		assert.Equal(t, "000000020000000000000006", result[0])
		assert.Equal(t, "000000020000000000000005", result[1])
		assert.Equal(t, "000000020000000000000004", result[2])
		assert.Equal(t, "000000020000000000000003", result[3])
	}
}

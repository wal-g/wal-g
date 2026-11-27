package postgres

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/memory"
)

// Scenario from wal-g#1390: two timeline switches (5/590000A0 and 5/592A0E88)
// fall into the same WAL segment 0x59.
const sameSegmentHistory = "5\t5/590000A0\tno recovery target specified\n\n" +
	"6\t5/592A0E88\tno recovery target specified\n"

func newSameSegmentHistoryFolder(t *testing.T) *memory.Folder {
	folder := memory.NewFolder("", memory.NewKVS())
	require.NoError(t, folder.PutObject(context.Background(), "00000007.history", strings.NewReader(sameSegmentHistory)))
	return folder
}

func TestCreateTimelineSwitchMap_TwoSwitchesInSameSegment(t *testing.T) {
	folder := newSameSegmentHistoryFolder(t)

	switchMap, err := createTimelineSwitchMap(context.Background(), 7, folder)
	require.NoError(t, err)

	records, ok := switchMap[NewWalSegmentNo(LSN(0x5590000A0))]
	require.True(t, ok)
	require.Len(t, records, 2)
	assert.Equal(t, uint32(5), records[0].timeline)
	assert.Equal(t, uint32(6), records[1].timeline)
}

func TestWalSegmentRunner_TwoSwitchesInSameSegment(t *testing.T) {
	folder := newSameSegmentHistoryFolder(t)
	switchMap, err := createTimelineSwitchMap(context.Background(), 7, folder)
	require.NoError(t, err)

	seg59 := NewWalSegmentNo(LSN(0x5590000A0))
	seg58 := seg59.previous()
	segments := map[WalSegmentDescription]bool{
		{Timeline: 5, Number: seg58}: true,
		{Timeline: 5, Number: seg59}: true,
		{Timeline: 6, Number: seg59}: true,
		{Timeline: 7, Number: seg59}: true,
	}
	runner := NewWalSegmentRunner(WalSegmentDescription{Timeline: 7, Number: seg59}, segments, seg58, switchMap)

	next, err := runner.Next()
	require.NoError(t, err)
	assert.Equal(t, WalSegmentDescription{Timeline: 5, Number: seg58}, next)
}

// The wal-verify code must still see every timeline from .history.
func TestSwitchSegNoByTimeline_TwoSwitchesInSameSegment(t *testing.T) {
	folder := memory.NewFolder("", memory.NewKVS())
	history := "4\t5/28000000\tno recovery target specified\n\n" +
		"5\t5/590000A0\tno recovery target specified\n\n" +
		"6\t5/592A0E88\tno recovery target specified\n"
	require.NoError(t, folder.PutObject(context.Background(), "00000007.history", strings.NewReader(history)))
	switchMap, err := createTimelineSwitchMap(context.Background(), 7, folder)
	require.NoError(t, err)

	byTimeline := switchSegNoByTimeline(switchMap)
	seg59 := NewWalSegmentNo(LSN(0x5590000A0))
	assert.Equal(t, map[uint32]WalSegmentNo{4: NewWalSegmentNo(LSN(0x528000000)), 5: seg59, 6: seg59}, byTimeline)

	// a backup made on timeline 5 is still accepted, and timeline 6 is known
	detail := &BackupDetail{}
	assert.True(t, checkBackupIsCorrect(7, detail, 5, WalSegmentNo(0x550), byTimeline, true))
	_, known := byTimeline[6]
	assert.True(t, known)
}

func TestWalSegmentRunner_ThreeSwitchesInSameSegment(t *testing.T) {
	folder := memory.NewFolder("", memory.NewKVS())
	history := "5\t5/590000A0\tno recovery target specified\n\n" +
		"6\t5/592A0E88\tno recovery target specified\n\n" +
		"7\t5/592B0000\tno recovery target specified\n"
	require.NoError(t, folder.PutObject(context.Background(), "00000008.history", strings.NewReader(history)))
	switchMap, err := createTimelineSwitchMap(context.Background(), 8, folder)
	require.NoError(t, err)

	seg59 := NewWalSegmentNo(LSN(0x5590000A0))
	seg58 := seg59.previous()
	segments := map[WalSegmentDescription]bool{
		{Timeline: 5, Number: seg58}: true,
		{Timeline: 5, Number: seg59}: true,
		{Timeline: 6, Number: seg59}: true,
		{Timeline: 7, Number: seg59}: true,
		{Timeline: 8, Number: seg59}: true,
	}
	for _, startTimeline := range []uint32{8, 7, 6} {
		runner := NewWalSegmentRunner(WalSegmentDescription{Timeline: startTimeline, Number: seg59}, segments, seg58, switchMap)
		next, err := runner.Next()
		require.NoError(t, err, "start timeline %d", startTimeline)
		assert.Equal(t, WalSegmentDescription{Timeline: 5, Number: seg58}, next, "start timeline %d", startTimeline)
	}
}

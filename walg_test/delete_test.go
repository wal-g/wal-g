package walg_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g"
	"github.com/wal-g/wal-g/testtools"
	"sort"
	"testing"
	"time"
)

//NB: order will reverse after sorting
var backup_times1 = []walg.BackupTime{
	{
		Name:        "base_00000001000000000000007C",
		Time:        time.Date(2007, 2, 2, 30, 48, 39, 651387233, time.UTC),
		WalFileName: "00000001000000000000007C",
	},
	{
		Name:        "base_00000001000000000000008C",
		Time:        time.Date(2008, 2, 27, 20, 8, 33, 651387235, time.UTC),
		WalFileName: "00000001000000000000008C",
	},
	{
		Name:        "base_00000001000000000000009C",
		Time:        time.Date(2009, 11, 20, 16, 34, 58, 651387232, time.UTC),
		WalFileName: "00000001000000000000009C",
	},
	{
		Name:        "base_0000000100000000000000AC",
		Time:        time.Date(2010, 11, 31, 20, 3, 58, 651387237, time.UTC),
		WalFileName: "0000000100000000000000AC",
	},
	{
		Name:        "base_0000000100000000000000BC",
		Time:        time.Date(2011, 3, 13, 4, 2, 42, 651387234, time.UTC),
		WalFileName: "0000000100000000000000BC",
	},
}

func TestSkiplineComputation(t *testing.T) {
	folder := testtools.NewMockS3Folder(false, false)

	sort.Sort(walg.TimeSlice(backup_times1))

	skipLine, walSkipFileName := walg.ComputeDeletionSkipline(backup_times1, walg.NewBackup(folder, "base_00000001000000000000008C"))

	assert.Equal(t, "00000001000000000000008C", walSkipFileName)
	assert.Equal(t, 3, skipLine) // we will skip 3 backups
}

//NB: order will reverse after sorting
var backup_times2 = []walg.BackupTime{
	{
		Name:        "base_00000004000000000000007C",
		Time:        time.Date(2007, 2, 2, 30, 48, 39, 651387233, time.UTC),
		WalFileName: "00000004000000000000007C",
	},
	{
		Name:        "base_00000004000000000000008C",
		Time:        time.Date(2008, 2, 27, 20, 8, 33, 651387235, time.UTC),
		WalFileName: "00000004000000000000008C",
	},
	{
		Name:        "base_00000001000000000000009C",
		Time:        time.Date(2009, 11, 20, 16, 34, 58, 651387232, time.UTC),
		WalFileName: "00000001000000000000009C",
	},
	{
		Name:        "base_0000000100000000000000AC",
		Time:        time.Date(2010, 11, 31, 20, 3, 58, 651387237, time.UTC),
		WalFileName: "0000000100000000000000AC",
	},
	{
		Name:        "base_0000000100000000000000BC",
		Time:        time.Date(2011, 3, 13, 4, 2, 42, 651387234, time.UTC),
		WalFileName: "0000000100000000000000BC",
	},
}

func TestSkiplineComputationAfterUpgrade(t *testing.T) {
	folder := testtools.NewMockS3Folder(false, false)

	sort.Sort(walg.TimeSlice(backup_times2))

	skipLine, walSkipFileName := walg.ComputeDeletionSkipline(backup_times2, walg.NewBackup(folder, "base_00000004000000000000008C"))

	assert.Equal(t, "00000001000000000000009C", walSkipFileName)
	assert.Equal(t, 3, skipLine)
}

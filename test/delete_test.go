package test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/testtools"
	"sort"
	"testing"
	"time"
)

// NB: order will reverse after sorting
var backup_times1 = []internal.BackupTime{
	{
		BackupName:  "base_00000001000000000000007C",
		Time:        time.Date(2007, 2, 2, 30, 48, 39, 651387233, time.UTC),
		WalFileName: "00000001000000000000007C",
	},
	{
		BackupName:  "base_00000001000000000000008C",
		Time:        time.Date(2008, 2, 27, 20, 8, 33, 651387235, time.UTC),
		WalFileName: "00000001000000000000008C",
	},
	{
		BackupName:  "base_00000001000000000000009C",
		Time:        time.Date(2009, 11, 20, 16, 34, 58, 651387232, time.UTC),
		WalFileName: "00000001000000000000009C",
	},
	{
		BackupName:  "base_0000000100000000000000AC",
		Time:        time.Date(2010, 11, 31, 20, 3, 58, 651387237, time.UTC),
		WalFileName: "0000000100000000000000AC",
	},
	{
		BackupName:  "base_0000000100000000000000BC",
		Time:        time.Date(2011, 3, 13, 4, 2, 42, 651387234, time.UTC),
		WalFileName: "0000000100000000000000BC",
	},
}

func TestSkiplineComputation(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()

	sort.Sort(internal.TimeSlice(backup_times1))

	skipLine, walSkipFileName := internal.ComputeDeletionSkipline(backup_times1, internal.NewBackup(folder, "base_00000001000000000000008C"))

	assert.Equal(t, "00000001000000000000008C", walSkipFileName)
	assert.Equal(t, 3, skipLine) // we will skip 3 backups
}

// NB: order will reverse after sorting
var backup_times2 = []internal.BackupTime{
	{
		BackupName:  "base_00000004000000000000007C",
		Time:        time.Date(2007, 2, 2, 30, 48, 39, 651387233, time.UTC),
		WalFileName: "00000004000000000000007C",
	},
	{
		BackupName:  "base_00000004000000000000008C",
		Time:        time.Date(2008, 2, 27, 20, 8, 33, 651387235, time.UTC),
		WalFileName: "00000004000000000000008C",
	},
	{
		BackupName:  "base_00000001000000000000009C",
		Time:        time.Date(2009, 11, 20, 16, 34, 58, 651387232, time.UTC),
		WalFileName: "00000001000000000000009C",
	},
	{
		BackupName:  "base_0000000100000000000000AC",
		Time:        time.Date(2010, 11, 31, 20, 3, 58, 651387237, time.UTC),
		WalFileName: "0000000100000000000000AC",
	},
	{
		BackupName:  "base_0000000100000000000000BC",
		Time:        time.Date(2011, 3, 13, 4, 2, 42, 651387234, time.UTC),
		WalFileName: "0000000100000000000000BC",
	},
}

func TestSkiplineComputationAfterUpgrade(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()

	sort.Sort(internal.TimeSlice(backup_times2))

	skipLine, walSkipFileName := internal.ComputeDeletionSkipline(backup_times2, internal.NewBackup(folder, "base_00000004000000000000008C"))

	assert.Equal(t, "00000001000000000000009C", walSkipFileName)
	assert.Equal(t, 3, skipLine)
}

package main

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

const (
	walFile5 = "000000010000000000000005"
	walFile6 = "000000010000000000000006"
	walFile7 = "000000010000000000000007"
)

func newBackup(name, walFile string, startLSN, finishLSN uint64) BackupInfo {
	return BackupInfo{BackupName: name, WalFileName: walFile, StartLSN: startLSN, FinishLSN: finishLSN}
}

func TestParentIndices(t *testing.T) {
	tests := []struct {
		name     string
		backups  []BackupInfo
		expected []int
	}{
		{
			name: "full backup has no parent",
			backups: []BackupInfo{
				newBackup("base_"+walFile5, walFile5, 0x5000028, 0x5000100),
			},
			expected: []int{-1},
		},
		{
			name: "chain of delta backups",
			backups: []BackupInfo{
				newBackup("base_"+walFile5, walFile5, 0x5000028, 0x5000100),
				newBackup("base_"+walFile6+"_D_"+walFile5, walFile6, 0x6000028, 0x6000100),
				newBackup("base_"+walFile7+"_D_"+walFile6, walFile7, 0x7000028, 0x7000100),
			},
			expected: []int{-1, 0, 1},
		},
		{
			name: "delta backups made from the full backup",
			backups: []BackupInfo{
				newBackup("base_"+walFile5, walFile5, 0x5000028, 0x5000100),
				newBackup("base_"+walFile6+"_D_"+walFile5, walFile6, 0x6000028, 0x6000100),
				newBackup("base_"+walFile7+"_D_"+walFile5, walFile7, 0x7000028, 0x7000100),
			},
			expected: []int{-1, 0, 0},
		},
		{
			name: "parent is not in the list",
			backups: []BackupInfo{
				newBackup("base_"+walFile6+"_D_"+walFile5, walFile6, 0x6000028, 0x6000100),
			},
			expected: []int{-1},
		},
		{
			// A standby does not switch WAL at the start and the end of a backup,
			// so several backups can start in the WAL file of the parent
			name: "two backups start in the WAL file of the parent",
			backups: []BackupInfo{
				newBackup("base_"+walFile5, walFile5, 0x5000100, 0x5200000),
				newBackup("base_"+walFile5+"_D_"+walFile5, walFile5, 0x5300000, 0x5400000),
				newBackup("base_"+walFile6+"_D_"+walFile5, walFile6, 0x6100000, 0x6200000),
			},
			expected: []int{-1, 0, -1},
		},
		{
			name: "backup finished after the delta backup started is not its parent",
			backups: []BackupInfo{
				newBackup("base_"+walFile5, walFile5, 0x5000028, 0x6100000),
				newBackup("base_"+walFile6+"_D_"+walFile5, walFile6, 0x6000028, 0x6000100),
			},
			expected: []int{-1, -1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, parentIndices(tt.backups))
		})
	}
}

func TestIsRestorable(t *testing.T) {
	tests := []struct {
		name     string
		backups  []BackupInfo
		expected []bool
	}{
		{
			name: "full backup",
			backups: []BackupInfo{
				newBackup("base_"+walFile5, walFile5, 0x5000028, 0x5000100),
			},
			expected: []bool{true},
		},
		{
			name: "chain of delta backups reaches the full backup",
			backups: []BackupInfo{
				newBackup("base_"+walFile5, walFile5, 0x5000028, 0x5000100),
				newBackup("base_"+walFile6+"_D_"+walFile5, walFile6, 0x6000028, 0x6000100),
				newBackup("base_"+walFile7+"_D_"+walFile6, walFile7, 0x7000028, 0x7000100),
			},
			expected: []bool{true, true, true},
		},
		{
			name: "full backup is deleted",
			backups: []BackupInfo{
				newBackup("base_"+walFile6+"_D_"+walFile5, walFile6, 0x6000028, 0x6000100),
				newBackup("base_"+walFile7+"_D_"+walFile6, walFile7, 0x7000028, 0x7000100),
			},
			expected: []bool{false, false},
		},
		{
			name: "delta backup in the middle of the chain is deleted",
			backups: []BackupInfo{
				newBackup("base_"+walFile5, walFile5, 0x5000028, 0x5000100),
				newBackup("base_"+walFile7+"_D_"+walFile6, walFile7, 0x7000028, 0x7000100),
			},
			expected: []bool{true, false},
		},
		{
			name: "delta backup made from the full backup is deleted",
			backups: []BackupInfo{
				newBackup("base_"+walFile5, walFile5, 0x5000028, 0x5000100),
				newBackup("base_"+walFile7+"_D_"+walFile5, walFile7, 0x7000028, 0x7000100),
			},
			expected: []bool{true, true},
		},
		{
			name: "parent of the delta backup cannot be determined",
			backups: []BackupInfo{
				newBackup("base_"+walFile5, walFile5, 0x5000100, 0x5200000),
				newBackup("base_"+walFile5+"_D_"+walFile5, walFile5, 0x5300000, 0x5400000),
				newBackup("base_"+walFile6+"_D_"+walFile5, walFile6, 0x6100000, 0x6200000),
			},
			expected: []bool{true, true, false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parents := parentIndices(tt.backups)
			restorable := make([]bool, len(tt.backups))
			for i := range tt.backups {
				restorable[i] = isRestorable(tt.backups, parents, i)
			}
			require.Equal(t, tt.expected, restorable)
		})
	}
}

func gaugeValue(t *testing.T, gauge prometheus.Gauge) float64 {
	registry := prometheus.NewRegistry()
	require.NoError(t, registry.Register(gauge))
	families, err := registry.Gather()
	require.NoError(t, err)
	return families[0].GetMetric()[0].GetGauge().GetValue()
}

func TestUpdatePitrWindow(t *testing.T) {
	now := time.Now()
	withTime := func(backup BackupInfo, age time.Duration, isPermanent bool) BackupInfo {
		backup.Time = now.Add(-age)
		backup.IsPermanent = isPermanent
		return backup
	}

	tests := []struct {
		name     string
		backups  []BackupInfo
		expected time.Duration
	}{
		{
			name:     "no backups",
			backups:  nil,
			expected: 0,
		},
		{
			// The modification time changes when the backup is copied or marked
			name: "window starts at the finish time of the backup",
			backups: []BackupInfo{
				{BackupName: "base_" + walFile5, WalFileName: walFile5, Time: now, FinishTime: now.Add(-3 * time.Hour)},
			},
			expected: 3 * time.Hour,
		},
		{
			name: "earliest backup is full",
			backups: []BackupInfo{
				withTime(newBackup("base_"+walFile5, walFile5, 0x5000028, 0x5000100), 3*time.Hour, false),
				withTime(newBackup("base_"+walFile6+"_D_"+walFile5, walFile6, 0x6000028, 0x6000100), 2*time.Hour, false),
			},
			expected: 3 * time.Hour,
		},
		{
			name: "full backup is deleted",
			backups: []BackupInfo{
				withTime(newBackup("base_"+walFile6+"_D_"+walFile5, walFile6, 0x6000028, 0x6000100), 3*time.Hour, false),
				withTime(newBackup("base_"+walFile7, walFile7, 0x7000028, 0x7000100), 2*time.Hour, false),
			},
			expected: 2 * time.Hour,
		},
		{
			name: "permanent parent does not break the chain",
			backups: []BackupInfo{
				withTime(newBackup("base_"+walFile5, walFile5, 0x5000028, 0x5000100), 3*time.Hour, true),
				withTime(newBackup("base_"+walFile6+"_D_"+walFile5, walFile6, 0x6000028, 0x6000100), 2*time.Hour, false),
			},
			expected: 2 * time.Hour,
		},
		{
			name: "no backup can be restored",
			backups: []BackupInfo{
				withTime(newBackup("base_"+walFile6+"_D_"+walFile5, walFile6, 0x6000028, 0x6000100), 3*time.Hour, false),
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewWalgExporter(slog.New(slog.NewTextHandler(io.Discard, nil)), "", time.Second, time.Second, time.Second, "")
			e.updatePitrWindow(tt.backups, parentIndices(tt.backups))
			require.InDelta(t, tt.expected.Seconds(), gaugeValue(t, e.pitrWindow), 60)
		})
	}
}

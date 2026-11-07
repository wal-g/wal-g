package internal_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/internal/databases/mongo"
	"github.com/wal-g/wal-g/internal/databases/mysql"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/internal/databases/redis"
	"github.com/wal-g/wal-g/pkg/storages/fs"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func recoveryPut(t *testing.T, root storage.Folder, key, value string) {
	t.Helper()
	require.NoError(t, root.PutObject(t.Context(), key, strings.NewReader(value)))
}

func recoveryExists(t *testing.T, root storage.Folder, key string, expected bool) {
	t.Helper()
	exists, err := root.Exists(t.Context(), key)
	require.NoError(t, err)
	require.Equal(t, expected, exists, key)
}

func TestRecoveryDeletesDamagedOwnedFilesAcrossDatabases(t *testing.T) {
	planners := map[string]internal.RecoveryPlanner{"redis": redis.PlanRecoveryDeletion, "mongo": mongo.PlanRecoveryDeletion,
		"mysql": mysql.PlanRecoveryDeletion, "postgres": postgres.PlanRecoveryDeletion, "greenplum": greenplum.PlanRecoveryDeletion}
	for db, build := range planners {
		for _, sentinel := range []string{"absent", "", "{", `{}`, `{"BackupName":"other","Permanent":false}`} {
			t.Run(db+"/"+sentinel, func(t *testing.T) {
				root := fs.NewFolder(t.TempDir(), "")
				base := root.GetSubFolder(utility.BaseBackupPath)
				if sentinel != "absent" {
					recoveryPut(t, base, "B1"+utility.SentinelSuffix, sentinel)
				}
				for _, key := range []string{"B1/metadata.json", "B1/files_metadata.json", "B1/stream_metadata.json", "B1/slots.json", "B1/ts_data/data", "B1/ts_data_backup_stop_sentinel.json", "B1/zero", "journal_B1"} {
					recoveryPut(t, base, key, "")
				}
				recoveryPut(t, base, "B10/keep", "unreadable")
				recoveryPut(t, base, "other/keep", "unreadable")
				p, err := build(t.Context(), root, "default", []string{"B1"}, false)
				require.NoError(t, err)
				defer p.Close()
				err = p.Execute(t.Context(), false)
				if db == "greenplum" {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
				recoveryExists(t, base, "B1/zero", true)
				err = p.Execute(t.Context(), true)
				if db == "greenplum" {
					require.ErrorContains(t, err, "segment mapping")
				} else {
					require.NoError(t, err)
				}
				recoveryExists(t, base, "B1/zero", false)
				recoveryExists(t, base, "journal_B1", false)
				recoveryExists(t, base, "B10/keep", true)
				recoveryExists(t, base, "other/keep", true)
				err = p.Execute(t.Context(), true)
				if db != "greenplum" {
					require.NoError(t, err)
				}
			})
		}
	}
}

func putPGRecovery(t *testing.T, root storage.Folder, number int, permanent bool) string {
	name := fmt.Sprintf("base_0000000100000000000000%02X", number)
	start := uint64(number)*postgres.WalSegmentSize + 1
	end := start + 5
	base := root.GetSubFolder(utility.BaseBackupPath)
	wal := postgres.WalSegmentNo(number).GetFilename(1)
	recoveryPut(t, base, name+utility.SentinelSuffix, fmt.Sprintf(`{"LSN":%d,"FinishLSN":%d,"PgVersion":150000,"StartWalFileName":%q,"EndWalFileName":%q}`, start, end, wal, wal))
	recoveryPut(t, base, name+"/metadata.json", fmt.Sprintf(`{"start_lsn":%d,"finish_lsn":%d,"is_permanent":%t}`, start, end, permanent))
	return name
}

func TestRecoveryPostgresRetainedBoundaryAndPermanentIntervals(t *testing.T) {
	for _, deleteFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(deleteFirst), func(t *testing.T) {
			root := fs.NewFolder(t.TempDir(), "")
			var names []string
			for i := 1; i <= 8; i++ {
				names = append(names, putPGRecovery(t, root, i, false))
			}
			wal := root.GetSubFolder(utility.WalPath)
			for i := 0; i <= 8; i++ {
				recoveryPut(t, wal, postgres.WalSegmentNo(i).GetFilename(1)+".br", "wal")
			}
			recoveryPut(t, wal, "00000002.history.br", "history")
			recoveryPut(t, wal, postgres.WalSegmentNo(0).GetFilename(9)+".br", "unknown lineage")
			selected := []string{names[1]}
			if deleteFirst {
				selected = append(selected, names[0])
			}
			for _, name := range selected {
				recoveryPut(t, root.GetSubFolder(utility.BaseBackupPath), name+utility.SentinelSuffix, "broken")
			}
			p, err := postgres.PlanRecoveryDeletion(t.Context(), root, "default", selected, true)
			require.NoError(t, err)
			defer p.Close()
			require.NoError(t, p.Execute(t.Context(), true))
			recoveryExists(t, wal, postgres.WalSegmentNo(2).GetFilename(1)+".br", !deleteFirst)
			recoveryExists(t, wal, postgres.WalSegmentNo(3).GetFilename(1)+".br", true)
			recoveryExists(t, wal, "00000002.history.br", true)
			recoveryExists(t, wal, postgres.WalSegmentNo(0).GetFilename(9)+".br", true)
		})
	}
	t.Run("permanent interval", func(t *testing.T) {
		root := fs.NewFolder(t.TempDir(), "")
		putPGRecovery(t, root, 1, true)
		target := putPGRecovery(t, root, 2, false)
		putPGRecovery(t, root, 3, false)
		wal := root.GetSubFolder(utility.WalPath)
		for i := 1; i <= 3; i++ {
			recoveryPut(t, wal, postgres.WalSegmentNo(i).GetFilename(1), "wal")
		}
		p, err := postgres.PlanRecoveryDeletion(t.Context(), root, "default", []string{target}, true)
		require.NoError(t, err)
		defer p.Close()
		require.NoError(t, p.Execute(t.Context(), true))
		recoveryExists(t, wal, postgres.WalSegmentNo(1).GetFilename(1), true)
		recoveryExists(t, wal, postgres.WalSegmentNo(2).GetFilename(1), false)
	})
}

func TestRecoveryDependenciesAndProtectionPreflight(t *testing.T) {
	for _, scenario := range []string{"cascade", "permanent", "unknown", "cycle"} {
		t.Run(scenario, func(t *testing.T) {
			root := fs.NewFolder(t.TempDir(), "")
			base := root.GetSubFolder(utility.BaseBackupPath)
			recoveryPut(t, base, "stream_B1/payload", "payload")
			recoveryPut(t, base, "stream_B1"+utility.SentinelSuffix, "broken")
			s := `{"IsPermanent":false,"IsIncremental":true,"DeltaFrom":"stream_B1","DeltaFullName":"stream_B1","DeltaCount":1,"DeltaLSN":1}`
			if scenario == "permanent" {
				s = strings.Replace(s, `"IsPermanent":false`, `"IsPermanent":true`, 1)
			}
			if scenario == "unknown" {
				s = "{}"
			}
			if scenario == "cycle" {
				s = strings.ReplaceAll(s, "stream_B1", "stream_B2")
			}
			recoveryPut(t, base, "stream_B2"+utility.SentinelSuffix, s)
			recoveryPut(t, base, "journal_stream_B2", "broken")
			p, err := mysql.PlanRecoveryDeletion(t.Context(), root, "default", []string{"stream_B1"}, false)
			if p != nil {
				defer p.Close()
			}
			if scenario != "cascade" {
				require.Error(t, err)
				recoveryExists(t, base, "stream_B1/payload", true)
				return
			}
			require.NoError(t, err)
			require.Equal(t, []string{"stream_B1", "stream_B2"}, p.TargetNames())
			require.NoError(t, p.Execute(t.Context(), true))
			recoveryExists(t, base, "journal_stream_B2", false)
		})
	}
}

type deletionFaultFolder struct {
	storage.Folder
	ambiguous bool
	readsFail bool
}

func (f deletionFaultFolder) GetSubFolder(key string) storage.Folder {
	f.Folder = f.Folder.GetSubFolder(key)
	return f
}
func (f deletionFaultFolder) DeleteObjects(ctx context.Context, objects []storage.Object) error {
	for _, obj := range objects {
		if strings.HasSuffix(obj.GetName(), utility.SentinelSuffix) {
			if f.ambiguous {
				return nil
			}
			return errors.New("delete failed")
		}
	}
	return f.Folder.DeleteObjects(ctx, objects)
}

func TestRecoverySentinelFailureStopsPayloadAndLogs(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		t.Run(fmt.Sprint(ambiguous), func(t *testing.T) {
			root := deletionFaultFolder{Folder: fs.NewFolder(t.TempDir(), ""), ambiguous: ambiguous}
			base := root.GetSubFolder(utility.BaseBackupPath)
			recoveryPut(t, base, "B1"+utility.SentinelSuffix, "bad")
			recoveryPut(t, base, "B1/payload", "keep")
			p, err := redis.PlanRecoveryDeletion(t.Context(), root, "default", []string{"B1"}, false)
			require.NoError(t, err)
			defer p.Close()
			require.Error(t, p.Execute(t.Context(), true))
			recoveryExists(t, base, "B1/payload", true)
		})
	}
}

func TestRecoveryJournalDoesNotReadDamagedTarget(t *testing.T) {
	root := fs.NewFolder(t.TempDir(), "")
	base := root.GetSubFolder(utility.BaseBackupPath)
	for i := 1; i <= 3; i++ {
		name := fmt.Sprintf("stream_B%d", i)
		recoveryPut(t, base, name+utility.SentinelSuffix, `{"IsPermanent":false,"IsIncremental":false}`)
		j := internal.NewEmptyJournalInfo(name, time.Unix(int64(i-1), 0), time.Unix(int64(i), 0), mysql.BinlogPath)
		require.NoError(t, j.Upload(t.Context(), root))
	}
	recoveryPut(t, base, "journal_stream_B2", "broken")
	p, err := mysql.PlanRecoveryDeletion(t.Context(), root, "default", []string{"stream_B2"}, false)
	require.NoError(t, err)
	defer p.Close()
	require.NoError(t, p.Execute(t.Context(), true))
	j, err := internal.NewJournalInfo(t.Context(), "stream_B3", root, mysql.BinlogPath)
	require.NoError(t, err)
	require.True(t, time.Unix(1, 0).Equal(j.PriorBackupEnd))
	recoveryExists(t, base, "journal_stream_B2", false)
}

func TestRecoveryPostgresV2FallbackAndConflict(t *testing.T) {
	root := fs.NewFolder(t.TempDir(), "")
	name := "base_000000010000000000000001"
	base := root.GetSubFolder(utility.BaseBackupPath)
	recoveryPut(t, base, name+utility.SentinelSuffix, `{"LSN":1,"FinishLSN":2,"Version":2,"IsPermanent":true}`)
	b, err := postgres.LoadDeletionBackup(t.Context(), root, name)
	require.NoError(t, err)
	require.True(t, b.ProtectionKnown)
	require.True(t, b.Permanent)
	recoveryPut(t, base, name+"/metadata.json", `{"start_lsn":1,"finish_lsn":2,"is_permanent":false}`)
	b, err = postgres.LoadDeletionBackup(t.Context(), root, name)
	require.NoError(t, err)
	require.True(t, b.Permanent)
	require.ErrorContains(t, b.Problem, "conflicting")
	_, err = postgres.PlanRecoveryDeletion(t.Context(), root, "default", []string{name}, false)
	require.ErrorContains(t, err, "permanent")
}

func TestRecoveryGreenplumDirectMappingAndUnknownReferences(t *testing.T) {
	root := fs.NewFolder(t.TempDir(), "")
	seg := root.GetSubFolder(greenplum.FormatSegmentStoragePrefix(0))
	first, second := putPGRecovery(t, seg, 1, false), putPGRecovery(t, seg, 2, false)
	marker := "backup_reservations/" + utility.BaseBackupPath + first + ".json"
	recoveryPut(t, seg, marker, "{}")
	base := root.GetSubFolder(utility.BaseBackupPath)
	for i, name := range []string{first, second} {
		s := greenplum.BackupSentinelDto{Segments: []greenplum.SegmentMetadata{{ContentID: 0, BackupName: name}}, FinishTime: time.Unix(int64(i+1), 0)}
		data, err := json.Marshal(s)
		require.NoError(t, err)
		recoveryPut(t, base, fmt.Sprintf("backup_B%d", i+1)+utility.SentinelSuffix, string(data))
	}
	segBase := seg.GetSubFolder(utility.BaseBackupPath)
	recoveryPut(t, segBase, first+"/ao_files_metadata.json", "broken")
	recoveryPut(t, segBase, "aosegments/garbage_aoseg", "shared")
	recoveryPut(t, base, "backup_B1_restore_point.json", "broken")
	p, err := greenplum.PlanRecoveryDeletion(t.Context(), root, "default", []string{"backup_B1"}, false)
	require.NoError(t, err)
	defer p.Close()
	require.ErrorContains(t, p.Execute(t.Context(), false), "unknown references")
	recoveryExists(t, segBase, first+utility.SentinelSuffix, true)
	require.ErrorContains(t, p.Execute(t.Context(), true), "unknown references")
	recoveryExists(t, segBase, first+utility.SentinelSuffix, false)
	listed, err := greenplum.ListStorageBackups(t.Context(), root)
	require.NoError(t, err)
	require.Len(t, listed, 1, "a partially deleted cluster backup must not remain visible")
	recoveryExists(t, segBase, second+utility.SentinelSuffix, true)
	recoveryExists(t, segBase, "aosegments/garbage_aoseg", true)
	recoveryExists(t, seg, marker, true)
	recoveryExists(t, base, "backup_B1_restore_point.json", false)
	for _, manifest := range []string{"ao_files_metadata.json", "pax_files_metadata.json"} {
		recoveryPut(t, segBase, second+"/"+manifest, `{"Files":{}}`)
	}
	retry, err := greenplum.PlanRecoveryDeletion(t.Context(), root, "default", []string{"backup_B1"}, false)
	require.NoError(t, err)
	defer retry.Close()
	require.NoError(t, retry.Execute(t.Context(), true))
	recoveryExists(t, segBase, "aosegments/garbage_aoseg", false)
	recoveryExists(t, root, "delete_recovery/greenplum/backup_B1.json", false)
}

func TestRecoveryPostgresWALSegmentSize(t *testing.T) {
	original := postgres.WalSegmentSize
	t.Cleanup(func() { postgres.SetWalSize(original / (1024 * 1024)) })
	for _, configured := range []uint64{16, 64} {
		t.Run(fmt.Sprint(configured), func(t *testing.T) {
			postgres.SetWalSize(configured)
			root := fs.NewFolder(t.TempDir(), "")
			base := root.GetSubFolder(utility.BaseBackupPath)
			const retained = "base_00000001000000000000000A"
			const target = "base_000000010000000000000001"
			const start = 10*64*1024*1024 + 1
			recoveryPut(t, base, retained+utility.SentinelSuffix, fmt.Sprintf(`{"LSN":%d,"FinishLSN":%d,"StartWalFileName":"00000001000000000000000A","EndWalFileName":"00000001000000000000000A"}`, start, start+5))
			recoveryPut(t, base, retained+"/metadata.json", fmt.Sprintf(`{"start_lsn":%d,"finish_lsn":%d,"is_permanent":false}`, start, start+5))
			recoveryPut(t, base, target+"/payload", "broken")
			wal := root.GetSubFolder(utility.WalPath)
			for _, key := range []string{"000000010000000000000009", "00000001000000000000000A"} {
				recoveryPut(t, wal, key, "wal")
			}
			p, err := postgres.PlanRecoveryDeletion(t.Context(), root, "default", []string{target}, true)
			require.NoError(t, err)
			defer p.Close()
			err = p.Execute(t.Context(), true)
			recoveryExists(t, wal, "00000001000000000000000A", true)
			recoveryExists(t, base, retained+utility.SentinelSuffix, true)
			recoveryExists(t, base, target+"/payload", false)
			if configured == 64 {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "WAL segment size")
			}
			recoveryExists(t, wal, "000000010000000000000009", configured != 64)
		})
	}
}

func TestRecoveryJournalExcludesPlannedLogDeletion(t *testing.T) {
	root := fs.NewFolder(t.TempDir(), "")
	archive := root.GetSubFolder(utility.WalPath)
	now := time.Now()
	for name, end := range map[string]time.Time{"B1": now.Add(-time.Hour), "B3": now.Add(time.Hour)} {
		journal := internal.NewEmptyJournalInfo(name, time.Time{}, end, utility.WalPath)
		require.NoError(t, journal.Upload(t.Context(), root))
	}
	recoveryPut(t, archive, "reclaim", "deleted log")
	recoveryPut(t, archive, "retain", "kept")
	p := &internal.DeletionPlan{Root: root, Backups: map[string]internal.DeletionBackup{"B1": {Name: "B1"}, "B3": {Name: "B3"}}}
	defer p.Close()
	require.NoError(t, p.PlanObjects(t.Context(), "unneeded WALs", archive, func(obj storage.Object) bool { return obj.GetName() == "reclaim" }))
	require.NoError(t, internal.PlanDeletionJournals(t.Context(), p, utility.WalPath))
	require.NoError(t, p.Execute(t.Context(), true))
	journal, err := internal.NewJournalInfo(t.Context(), "B1", root, utility.WalPath)
	require.NoError(t, err)
	require.EqualValues(t, len("kept"), journal.SizeToNextBackup)
}

package archive

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
)

func shuffledArchives(s []models.Archive) []models.Archive {
	a := copyArchives(s)
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(a), func(i, j int) {
		a[i], a[j] = a[j], a[i]
	})
	return a
}

func copyArchives(a []models.Archive) []models.Archive {
	b := make([]models.Archive, len(a))
	copy(b, a)
	return b
}

var (
	continuousArchives = []models.Archive{
		{Start: models.Timestamp{TS: 1579000001, Inc: 1}, End: models.Timestamp{TS: 1579001001, Inc: 2}, Ext: "br", Type: "oplog"},
		{Start: models.Timestamp{TS: 1579001001, Inc: 2}, End: models.Timestamp{TS: 1579002001, Inc: 1}, Ext: "br", Type: "oplog"},
		{Start: models.Timestamp{TS: 1579002001, Inc: 1}, End: models.Timestamp{TS: 1579002001, Inc: 99}, Ext: "br", Type: "oplog"},
		{Start: models.Timestamp{TS: 1579002001, Inc: 99}, End: models.Timestamp{TS: 1579003001, Inc: 3}, Ext: "br", Type: "oplog"},
		{Start: models.Timestamp{TS: 1579003001, Inc: 3}, End: models.Timestamp{TS: 1579004001, Inc: 2}, Ext: "br", Type: "oplog"},
	}
	continuousArchivesOverlappedFirst = []models.Archive{
		{Start: models.Timestamp{TS: 1579000001, Inc: 1}, End: models.Timestamp{TS: 1579001001, Inc: 2}, Ext: "br", Type: "oplog"},
		{Start: models.Timestamp{TS: 1579000001, Inc: 1}, End: models.Timestamp{TS: 1579002001, Inc: 3}, Ext: "br", Type: "oplog"},
		{Start: models.Timestamp{TS: 1579001001, Inc: 2}, End: models.Timestamp{TS: 1579002001, Inc: 1}, Ext: "br", Type: "oplog"},
		{Start: models.Timestamp{TS: 1579002001, Inc: 1}, End: models.Timestamp{TS: 1579002001, Inc: 99}, Ext: "br", Type: "oplog"},
		{Start: models.Timestamp{TS: 1579002001, Inc: 99}, End: models.Timestamp{TS: 1579003001, Inc: 3}, Ext: "br", Type: "oplog"},
		{Start: models.Timestamp{TS: 1579003001, Inc: 3}, End: models.Timestamp{TS: 1579004001, Inc: 2}, Ext: "br", Type: "oplog"},
	}
	continuousArchivesOverlappedMiddle = []models.Archive{
		{Start: models.Timestamp{TS: 1579000001, Inc: 1}, End: models.Timestamp{TS: 1579001001, Inc: 2}, Ext: "br", Type: "oplog"},
		{Start: models.Timestamp{TS: 1579001001, Inc: 2}, End: models.Timestamp{TS: 1579002001, Inc: 1}, Ext: "br", Type: "oplog"},
		{Start: models.Timestamp{TS: 1579002001, Inc: 1}, End: models.Timestamp{TS: 1579002001, Inc: 99}, Ext: "br", Type: "oplog"},
		{Start: models.Timestamp{TS: 1579002001, Inc: 99}, End: models.Timestamp{TS: 1579003001, Inc: 3}, Ext: "br", Type: "oplog"},
		{Start: models.Timestamp{TS: 1579002001, Inc: 1}, End: models.Timestamp{TS: 1579002010, Inc: 1}, Ext: "br", Type: "oplog"},
		{Start: models.Timestamp{TS: 1579003001, Inc: 3}, End: models.Timestamp{TS: 1579004001, Inc: 2}, Ext: "br", Type: "oplog"},
	}
	gapArchives = []models.Archive{
		{Start: models.Timestamp{TS: 1579000001, Inc: 1}, End: models.Timestamp{TS: 1579001001, Inc: 2}, Ext: "br", Type: "oplog"},
		{Start: models.Timestamp{TS: 1579001001, Inc: 2}, End: models.Timestamp{TS: 1579002001, Inc: 1}, Ext: "br", Type: "oplog"},
		{Start: models.Timestamp{TS: 1579002001, Inc: 99}, End: models.Timestamp{TS: 1579003001, Inc: 3}, Ext: "br", Type: "oplog"},
		{Start: models.Timestamp{TS: 1579003001, Inc: 3}, End: models.Timestamp{TS: 1579004001, Inc: 2}, Ext: "br", Type: "oplog"},
	}
	gapArchivesWithMarks = []models.Archive{
		{Start: models.Timestamp{TS: 1579000001, Inc: 1}, End: models.Timestamp{TS: 1579001001, Inc: 2}, Ext: "br", Type: "oplog"},
		{Start: models.Timestamp{TS: 1579001001, Inc: 2}, End: models.Timestamp{TS: 1579002001, Inc: 1}, Ext: "br", Type: "oplog"},
		{Start: models.Timestamp{TS: 1579002001, Inc: 1}, End: models.Timestamp{TS: 1579002001, Inc: 98}, Ext: "br", Type: "gap"},
		{Start: models.Timestamp{TS: 1579002001, Inc: 99}, End: models.Timestamp{TS: 1579003001, Inc: 3}, Ext: "br", Type: "oplog"},
		{Start: models.Timestamp{TS: 1579003001, Inc: 3}, End: models.Timestamp{TS: 1579004001, Inc: 2}, Ext: "br", Type: "oplog"},
	}
)

func TestSequenceBetweenTS(t *testing.T) {
	type args struct {
		since models.Timestamp
		until models.Timestamp
	}

	archivesCases := map[string][]models.Archive{
		"continuous archives":                             shuffledArchives(continuousArchives),
		"continuous archives, first archives overlapped":  continuousArchivesOverlappedFirst,
		"continuous archives, middle archives overlapped": continuousArchivesOverlappedMiddle,
	}

	tests := []struct {
		name string
		args args
		want Sequence
		err  error
	}{
		{
			name: "ts are borders of archives",
			args: args{
				since: models.Timestamp{TS: 1579000001, Inc: 2},
				until: models.Timestamp{TS: 1579004001, Inc: 2},
			},
			want: continuousArchives,
			err:  nil,
		},
		{
			name: "since ts is in second archive, until is in last",
			args: args{
				since: models.Timestamp{TS: 1579000011, Inc: 2},
				until: models.Timestamp{TS: 1579004001, Inc: 1},
			},
			want: continuousArchives,
			err:  nil,
		},
		{
			name: "since ts is in second archive, until is in last",
			args: args{
				since: models.Timestamp{TS: 1579001011, Inc: 2},
				until: models.Timestamp{TS: 1579004001, Inc: 1},
			},
			want: continuousArchives[1:],
			err:  nil,
		},
		{
			name: "since ts is in first archive, until is in pre-last",
			args: args{
				since: models.Timestamp{TS: 1579000011, Inc: 2},
				until: models.Timestamp{TS: 1579002001, Inc: 100},
			},
			want: continuousArchives[:len(continuousArchives)-1],
			err:  nil,
		},
		{
			name: "ts are in one (first) archive",
			args: args{
				since: models.Timestamp{TS: 1579000001, Inc: 2},
				until: models.Timestamp{TS: 1579001001, Inc: 1},
			},
			want: continuousArchives[0:1],
			err:  nil,
		},
		{
			name: "ts are in one (last) archive",
			args: args{
				since: models.Timestamp{TS: 1579003011, Inc: 3},
				until: models.Timestamp{TS: 1579004001, Inc: 2},
			},
			want: continuousArchives[len(continuousArchives)-1:],
			err:  nil,
		},
		{
			name: "ts are in one (middle) archive",
			args: args{
				since: models.Timestamp{TS: 1579002001, Inc: 2},
				until: models.Timestamp{TS: 1579002001, Inc: 4},
			},
			want: continuousArchives[2:3],
			err:  nil,
		},

		// Failures test
		{
			name: "error: ts are out of bounds",
			args: args{
				since: models.Timestamp{TS: 1579000000, Inc: 1},
				until: models.Timestamp{TS: 1579005001, Inc: 1},
			},
			want: nil,
			err:  fmt.Errorf("can not find archive with until timestamp '1579005001.1'"),
		},
		{
			name: "error: since ts is out of bounds",
			args: args{
				since: models.Timestamp{TS: 1579000000, Inc: 1},
				until: models.Timestamp{TS: 1579003001, Inc: 2},
			},
			want: nil,
			err:  fmt.Errorf("previous archive in sequence with last ts '1579000001.1' does not exist"),
		},
		{
			name: "error: until ts is out of bounds",
			args: args{
				since: models.Timestamp{TS: 1579000001, Inc: 2},
				until: models.Timestamp{TS: 1579005001, Inc: 1},
			},
			want: nil,
			err:  fmt.Errorf("can not find archive with until timestamp '1579005001.1'"),
		},
		{
			name: "error: since ts is out of bounds (start ts is not included in archive)",
			args: args{
				since: models.Timestamp{TS: 1579000001, Inc: 1},
				until: models.Timestamp{TS: 1579004001, Inc: 2},
			},
			want: nil,
			err:  fmt.Errorf("previous archive in sequence with last ts '1579000001.1' does not exist"),
		},
		{
			name: "error: since ts is in last archive, until is out of bounds",
			args: args{
				since: models.Timestamp{TS: 1579003011, Inc: 1},
				until: models.Timestamp{TS: 1579005001, Inc: 2},
			},
			want: nil,
			err:  fmt.Errorf("can not find archive with until timestamp '1579005001.2'"),
		},
		{
			name: "error: since ts is greater than until ts",
			args: args{
				since: models.Timestamp{TS: 1579002001, Inc: 1},
				until: models.Timestamp{TS: 1579001001, Inc: 2},
			},
			want: nil,
			err:  fmt.Errorf("until ts must be greater or equal to since ts"),
		},
	}

	for _, tt := range tests {
		for archivesDesc, archives := range archivesCases {
			t.Run(fmt.Sprintf("%s; %s", tt.name, archivesDesc), func(t *testing.T) {
				got, err := SequenceBetweenTS(archives, tt.args.since, tt.args.until)
				if tt.err != nil {
					assert.EqualError(t, err, tt.err.Error())
				} else {
					assert.Nil(t, err)
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("SequenceBetweenTS() got = %v, want %v", got, tt.want)
				}
			})
		}
	}
}

func TestSequenceBetweenTSGaps(t *testing.T) {
	type args struct {
		since models.Timestamp
		until models.Timestamp
	}

	archivesCases := map[string][]models.Archive{
		"gap archives": shuffledArchives(gapArchives),
	}

	tests := []struct {
		name string
		args args
		err  error
	}{
		{
			name: "error: ts are borders of archives",
			args: args{
				since: models.Timestamp{TS: 1579000001, Inc: 2},
				until: models.Timestamp{TS: 1579004001, Inc: 2},
			},
			err: fmt.Errorf("previous archive in sequence with last ts '1579002001.99' does not exist"),
		},
	}

	for _, tt := range tests {
		for archivesDesc, archives := range archivesCases {
			t.Run(fmt.Sprintf("%s; %s", tt.name, archivesDesc), func(t *testing.T) {
				_, err := SequenceBetweenTS(archives, tt.args.since, tt.args.until)
				assert.EqualError(t, err, tt.err.Error())
			})
		}
	}
}

func TestSequenceBetweenTSMarkedGaps(t *testing.T) {
	type args struct {
		since models.Timestamp
		until models.Timestamp
	}

	archivesCases := map[string][]models.Archive{
		"gap archives": shuffledArchives(gapArchivesWithMarks),
	}

	tests := []struct {
		name string
		args args
		err  error
	}{
		{
			name: "error: ts are borders of archives",
			args: args{
				since: models.Timestamp{TS: 1579000001, Inc: 2},
				until: models.Timestamp{TS: 1579004001, Inc: 2},
			},
			err: fmt.Errorf("previous archive in sequence with last ts '1579002001.99' does not exist"),
		},
	}

	for _, tt := range tests {
		for archivesDesc, archives := range archivesCases {
			t.Run(fmt.Sprintf("%s; %s", tt.name, archivesDesc), func(t *testing.T) {
				_, err := SequenceBetweenTS(archives, tt.args.since, tt.args.until)
				assert.EqualError(t, err, tt.err.Error())
			})
		}
	}
}

var (
	arch1 = models.Archive{Start: models.Timestamp{TS: 1579881975, Inc: 1}, End: models.Timestamp{TS: 1579881985, Inc: 2}, Ext: "br", Type: "oplog"}
	arch2 = models.Archive{Start: models.Timestamp{TS: 1579881985, Inc: 2}, End: models.Timestamp{TS: 1579882985, Inc: 1}, Ext: "br", Type: "oplog"}
	arch3 = models.Archive{Start: models.Timestamp{TS: 1579882985, Inc: 1}, End: models.Timestamp{TS: 1579882985, Inc: 99}, Ext: "br", Type: "oplog"}
)

func TestSequence_Reverse(t *testing.T) {
	tests := []struct {
		name   string
		before Sequence
		after  Sequence
	}{
		{
			name:   "3 archive seq",
			before: Sequence{arch1, arch2, arch3},
			after:  Sequence{arch3, arch2, arch1},
		},
		{
			name:   "2 archive seq",
			before: Sequence{arch1, arch2},
			after:  Sequence{arch2, arch1},
		},
		{
			name:   "1 archive seq",
			before: Sequence{arch1},
			after:  Sequence{arch1},
		},
		{
			name:   "empty archive seq",
			before: Sequence{},
			after:  Sequence{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.before.Reverse()
			assert.Equal(t, tt.before, tt.after)
		})
	}
}

var (
	LastTSBackups = []models.Backup{
		{MongoMeta: models.MongoMeta{Before: models.NodeMeta{LastMajTS: models.Timestamp{TS: 1579000001, Inc: 2}}, After: models.NodeMeta{LastMajTS: models.Timestamp{TS: 1579000001, Inc: 9}}}},
		{MongoMeta: models.MongoMeta{Before: models.NodeMeta{LastMajTS: models.Timestamp{TS: 1579000010, Inc: 1}}, After: models.NodeMeta{LastMajTS: models.Timestamp{TS: 1579000011, Inc: 9}}}},
		{MongoMeta: models.MongoMeta{Before: models.NodeMeta{LastMajTS: models.Timestamp{TS: 1579000019, Inc: 11}}, After: models.NodeMeta{LastMajTS: models.Timestamp{TS: 1579000031, Inc: 12}}}},
	}
)

func TestLastKnownInBackupTS(t *testing.T) {
	type args struct {
		backups []models.Backup
	}
	tests := []struct {
		name string
		args args
		want models.Timestamp
		err  error
	}{
		{
			name: "empty backups error",
			args: args{
				backups: []models.Backup{},
			},
			want: models.Timestamp{},
			err:  fmt.Errorf("empty backups list given"),
		},
		{
			name: "it works",
			args: args{
				backups: LastTSBackups,
			},
			want: models.Timestamp{TS: 1579000001, Inc: 2},
			err:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := LastKnownInBackupTS(tt.args.backups)
			if tt.err != nil {
				assert.EqualError(t, err, tt.err.Error())
			} else {
				assert.Nil(t, err)
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

var (
	MockBackup1 = models.Backup{BackupName: "back1", StartLocalTime: time.Unix(1579000500, 0), FinishLocalTime: time.Unix(1579000550, 0)}
	MockBackup2 = models.Backup{BackupName: "back2", StartLocalTime: time.Unix(1579000300, 0), FinishLocalTime: time.Unix(1579000400, 0)}
	MockBackup3 = models.Backup{BackupName: "back3", StartLocalTime: time.Unix(1579000300, 0), FinishLocalTime: time.Unix(1579000400, 0)}
	MockBackup4 = models.Backup{BackupName: "back4", StartLocalTime: time.Unix(1579000200, 0), FinishLocalTime: time.Unix(1579000250, 0)}
	MockBackup5 = models.Backup{BackupName: "back5", StartLocalTime: time.Unix(1579000100, 0), FinishLocalTime: time.Unix(1579000101, 0)}
	MockBackup6 = models.Backup{BackupName: "back6", StartLocalTime: time.Unix(1579000001, 0), FinishLocalTime: time.Unix(1579000001, 0)}
	MockBackup7 = models.Backup{BackupName: "back7", StartLocalTime: time.Unix(1579000001, 0), FinishLocalTime: time.Unix(1579000001, 0)}

	MockBackup5Perm = models.Backup{BackupName: "perm5", StartLocalTime: time.Unix(1579000100, 0), FinishLocalTime: time.Unix(1579000101, 0), Permanent: true}
	MockBackup8Perm = models.Backup{BackupName: "perm8", StartLocalTime: time.Unix(1579000000, 0), FinishLocalTime: time.Unix(1579000001, 0), Permanent: true}

	SplitBackups = []models.Backup{
		MockBackup1,
		MockBackup2,
		MockBackup3,
		MockBackup4,
		MockBackup5,
		MockBackup6,
	}
	SplitBackupsPermanent = []models.Backup{
		MockBackup1,
		MockBackup2,
		MockBackup3,
		MockBackup4,
		MockBackup5Perm,
		MockBackup6,
		MockBackup7,
		MockBackup8Perm,
	}
)

func IntPtr(i int) *int {
	return &i
}

func TimePtr(t time.Time) *time.Time {
	return &t
}

func TimestampPtr(ts models.Timestamp) *models.Timestamp {
	return &ts
}

func TestSplitPurgingBackups(t *testing.T) {
	type args struct {
		backups     []models.Backup
		retainCount *int
		retainAfter *time.Time
	}
	tests := []struct {
		name       string
		args       args
		wantPurge  []models.Backup
		wantRetain []models.Backup
		err        error
	}{
		{
			name: "Purge_all,count=nil,after=nil",
			args: args{
				backups:     SplitBackups,
				retainCount: nil,
				retainAfter: nil,
			},
			wantPurge:  SplitBackups,
			wantRetain: nil,
			err:        nil,
		},
		{
			name: "Purge_all,count=0,after=nil",
			args: args{
				backups:     SplitBackups,
				retainCount: IntPtr(0),
				retainAfter: nil,
			},
			wantPurge:  SplitBackups,
			wantRetain: nil,
			err:        nil,
		},
		{
			name: "Purge_all,time_in_future",
			args: args{
				backups:     SplitBackups,
				retainCount: nil,
				retainAfter: TimePtr(SplitBackups[0].StartLocalTime.Add(time.Second)),
			},
			wantPurge:  SplitBackups,
			wantRetain: nil,
			err:        nil,
		},
		{
			name: "Purge_none,count=100",
			args: args{
				backups:     SplitBackups,
				retainCount: IntPtr(100),
				retainAfter: nil,
			},
			wantPurge:  nil,
			wantRetain: SplitBackups,
			err:        nil,
		},
		{
			name: "Purge_none,count=6",
			args: args{
				backups:     SplitBackups,
				retainCount: IntPtr(6),
				retainAfter: nil,
			},
			wantPurge:  nil,
			wantRetain: SplitBackups,
			err:        nil,
		},
		{
			name: "Purge_none,time_in_past",
			args: args{
				backups:     SplitBackups,
				retainCount: nil,
				retainAfter: TimePtr(SplitBackups[5].StartLocalTime.Add(-1 * time.Second)),
			},
			wantPurge:  nil,
			wantRetain: SplitBackups,
			err:        nil,
		},
		{
			name: "Purge_1,count=5",
			args: args{
				backups:     SplitBackups,
				retainCount: IntPtr(5),
				retainAfter: nil,
			},
			wantPurge:  SplitBackups[5:],
			wantRetain: SplitBackups[:5],
			err:        nil,
		},
		{
			name: "Purge_2,count=4",
			args: args{
				backups:     SplitBackups,
				retainCount: IntPtr(4),
				retainAfter: nil,
			},
			wantPurge:  SplitBackups[4:],
			wantRetain: SplitBackups[:4],
			err:        nil,
		},
		{
			name: "Purge_2,count=4,time_in_future",
			args: args{
				backups:     SplitBackups,
				retainCount: IntPtr(4),
				retainAfter: TimePtr(SplitBackups[0].StartLocalTime.Add(time.Second)),
			},
			wantPurge:  SplitBackups[4:],
			wantRetain: SplitBackups[:4],
			err:        nil,
		},
		{
			name: "Purge_2,count=4_eq_time_after_4",
			args: args{
				backups:     SplitBackups,
				retainCount: IntPtr(4),
				retainAfter: TimePtr(SplitBackups[4].StartLocalTime.Add(time.Second)),
			},
			wantPurge:  SplitBackups[4:],
			wantRetain: SplitBackups[:4],
			err:        nil,
		},
		{
			name: "Purge_1,count=4_gt_time_after_5",
			args: args{
				backups:     SplitBackups,
				retainCount: IntPtr(3),
				retainAfter: TimePtr(SplitBackups[5].StartLocalTime.Add(time.Second)),
			},
			wantPurge:  SplitBackups[5:],
			wantRetain: SplitBackups[:5],
			err:        nil,
		},
		{
			name: "Purge_3,count=2_gt_time_after_3",
			args: args{
				backups:     SplitBackups,
				retainCount: IntPtr(2),
				retainAfter: TimePtr(SplitBackups[3].StartLocalTime.Add(time.Second)),
			},
			wantPurge:  SplitBackups[3:],
			wantRetain: SplitBackups[:3],
			err:        nil,
		},
		{
			name: "Purge_2,count=4_lt_time_after_2",
			args: args{
				backups:     SplitBackups,
				retainCount: IntPtr(4),
				retainAfter: TimePtr(SplitBackups[2].StartLocalTime.Add(time.Second)),
			},
			wantPurge:  SplitBackups[4:],
			wantRetain: SplitBackups[:4],
			err:        nil,
		},
		{
			name: "Purge_3,count=2_gt_time_after_3",
			args: args{
				backups:     SplitBackupsPermanent,
				retainCount: IntPtr(2),
				retainAfter: TimePtr(SplitBackups[3].StartLocalTime.Add(time.Second)),
			},
			wantPurge: []models.Backup{
				MockBackup4,
				MockBackup6,
				MockBackup7,
			},
			wantRetain: []models.Backup{
				MockBackup1,
				MockBackup2,
				MockBackup3,
				MockBackup5Perm,
				MockBackup8Perm,
			},
			err: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backups := MongoModelToTimedBackup(tt.args.backups)
			internal.SortTimedBackup(backups)
			purgeList, retainList, err := internal.SplitPurgingBackups(backups, tt.args.retainCount, tt.args.retainAfter)
			if tt.err != nil {
				assert.EqualError(t, err, tt.err.Error())
			} else {
				assert.Nil(t, err)
			}

			gotPurge, gotRetain := SplitMongoBackups(tt.args.backups, purgeList, retainList)

			assert.Equal(t, tt.wantPurge, gotPurge, "wrong purge list")
			assert.Equal(t, tt.wantRetain, gotRetain, "wrong retain list")
		})
	}
}

func TestSplitPurgingOplogArchivesByTS(t *testing.T) {
	type args struct {
		archives      []models.Archive
		purgeBeforeTS models.Timestamp
	}
	tests := []struct {
		name string
		args args
		want []models.Archive
	}{
		{
			name: "purge_none,empty_archive_list",
			args: args{
				archives:      []models.Archive{},
				purgeBeforeTS: models.Timestamp{TS: 1, Inc: 1},
			},
			want: []models.Archive{},
		},
		{
			name: "purge_none,before_ts_in_past",
			args: args{
				archives:      continuousArchives,
				purgeBeforeTS: models.Timestamp{TS: 1578000001, Inc: 1},
			},
			want: []models.Archive{},
		},
		{
			name: "purge_none,before_ts_is_start_of_oldest_archive",
			args: args{
				archives:      continuousArchives,
				purgeBeforeTS: continuousArchives[0].Start,
			},
			want: []models.Archive{},
		},
		{
			name: "purge_none,before_ts_is_end_of_oldest_archive",
			args: args{
				archives:      continuousArchives,
				purgeBeforeTS: continuousArchives[0].End,
			},
			want: []models.Archive{},
		},
		{
			name: "purge_all_except_newest,before_ts_is_end_of_newest_archive",
			args: args{
				archives:      continuousArchives,
				purgeBeforeTS: continuousArchives[len(continuousArchives)-1].End,
			},
			want: continuousArchives[:len(continuousArchives)-1],
		},
		{
			name: "purge_all,before_ts_is_end_of_newest_archive",
			args: args{
				archives:      continuousArchives,
				purgeBeforeTS: continuousArchives[len(continuousArchives)-1].End,
			},
			want: continuousArchives[:len(continuousArchives)-1],
		},
		{
			name: "purge_half",
			args: args{
				archives:      continuousArchives,
				purgeBeforeTS: continuousArchives[3].End,
			},
			want: continuousArchives[:3],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SplitPurgingOplogArchivesByTS(tt.args.archives, tt.args.purgeBeforeTS)
			assert.Equal(t, tt.want, got, "wrong oplog archives list")
		})
	}
}

func TestOldestBackupAfterTime(t *testing.T) {
	type args struct {
		backups     []models.Backup
		retainAfter time.Time
	}
	tests := []struct {
		name    string
		args    args
		want    models.Backup
		wantErr error
	}{
		{
			name: "retain_point_in_the_middle",
			want: SplitBackups[3],
			args: args{SplitBackups, time.Unix(1579000201, 0)},
		},
		{
			name: "retain_point_between_backups",
			want: SplitBackups[3],
			args: args{SplitBackups, time.Unix(1579000150, 0)},
		},
		{
			name:    "retain_point_in_the_future",
			wantErr: fmt.Errorf("no backups newer than retain point"),
			args:    args{SplitBackups, time.Unix(1579900201, 0)},
		},
		{
			name: "retain_point_in_the_past",
			want: SplitBackups[len(SplitBackups)-1],
			args: args{SplitBackups, time.Unix(157800001, 0)},
		},
		{
			name: "backups_wrong_sorting",
			want: models.Backup{},
			args: args{[]models.Backup{
				{BackupName: "wrong1", StartLocalTime: time.Unix(1579000200, 0), FinishLocalTime: time.Unix(1579000300, 0)},
				MockBackup2,
				MockBackup1,
			}, time.Unix(157800001, 0)},
			wantErr: fmt.Errorf("backups are not sorted by finish time"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldestBackup, err := OldestBackupAfterTime(tt.args.backups, tt.args.retainAfter)
			if tt.wantErr != nil {
				assert.EqualError(t, err, tt.wantErr.Error())
				return
			}
			assert.Nil(t, err)

			assert.Equal(t, oldestBackup, tt.want)
		})
	}
}

var (
	Backups = []models.Backup{
		{MongoMeta: models.MongoMeta{Before: models.NodeMeta{LastMajTS: models.Timestamp{TS: 800}}, After: models.NodeMeta{LastMajTS: models.Timestamp{TS: 900}}}},
		{MongoMeta: models.MongoMeta{Before: models.NodeMeta{LastMajTS: models.Timestamp{TS: 600}}, After: models.NodeMeta{LastMajTS: models.Timestamp{TS: 700}}}},
		{MongoMeta: models.MongoMeta{Before: models.NodeMeta{LastMajTS: models.Timestamp{TS: 350}}, After: models.NodeMeta{LastMajTS: models.Timestamp{TS: 500}}}},
		{MongoMeta: models.MongoMeta{Before: models.NodeMeta{LastMajTS: models.Timestamp{TS: 300}}, After: models.NodeMeta{LastMajTS: models.Timestamp{TS: 400}}}},
	}
	Archives = []models.Archive{
		{Start: models.Timestamp{TS: 100}, End: models.Timestamp{TS: 200}},
		{Start: models.Timestamp{TS: 200}, End: models.Timestamp{TS: 300}},
		{Start: models.Timestamp{TS: 300}, End: models.Timestamp{TS: 400}},
		{Start: models.Timestamp{TS: 400}, End: models.Timestamp{TS: 550}},
		{Start: models.Timestamp{TS: 550}, End: models.Timestamp{TS: 560}},
		{Start: models.Timestamp{TS: 560}, End: models.Timestamp{TS: 570}},
		{Start: models.Timestamp{TS: 570}, End: models.Timestamp{TS: 750}},
		{Start: models.Timestamp{TS: 750}, End: models.Timestamp{TS: 760}},
		{Start: models.Timestamp{TS: 760}, End: models.Timestamp{TS: 770}},
		{Start: models.Timestamp{TS: 770}, End: models.Timestamp{TS: 800}},
		{Start: models.Timestamp{TS: 800}, End: models.Timestamp{TS: 900}},
		{Start: models.Timestamp{TS: 900}, End: models.Timestamp{TS: 950}},
		{Start: models.Timestamp{TS: 950}, End: models.Timestamp{TS: 960}},
	}
)

func TestSelectPurgingOplogArchives(t *testing.T) {
	type args struct {
		archives      []models.Archive
		backups       []models.Backup
		retainAfterTS *models.Timestamp
	}
	tests := []struct {
		name string
		args args
		want []models.Archive
	}{
		{
			name: "retain_after_in_the_middle",
			args: args{
				archives:      Archives,
				backups:       Backups,
				retainAfterTS: TimestampPtr(models.Timestamp{TS: 600}),
			},
			want: []models.Archive{
				{Start: models.Timestamp{TS: 100}, End: models.Timestamp{TS: 200}},
				{Start: models.Timestamp{TS: 550}, End: models.Timestamp{TS: 560}},
				{Start: models.Timestamp{TS: 560}, End: models.Timestamp{TS: 570}},
			},
		},
		{
			name: "retain_after_in_the_middle_of_overlapped_backups",
			args: args{
				archives:      Archives,
				backups:       Backups,
				retainAfterTS: TimestampPtr(models.Timestamp{TS: 360}),
			},
			want: []models.Archive{
				{Start: models.Timestamp{TS: 100}, End: models.Timestamp{TS: 200}},
			},
		},
		{
			name: "retain_after_in_the_past",
			args: args{
				archives:      Archives,
				backups:       Backups,
				retainAfterTS: TimestampPtr(models.Timestamp{TS: 50}),
			},
			want: nil,
		},
		{
			name: "retain_after_in_the_future",
			args: args{
				archives:      Archives,
				backups:       Backups,
				retainAfterTS: TimestampPtr(models.Timestamp{TS: 1000}),
			},
			want: []models.Archive{
				{Start: models.Timestamp{TS: 100}, End: models.Timestamp{TS: 200}},
				{Start: models.Timestamp{TS: 550}, End: models.Timestamp{TS: 560}},
				{Start: models.Timestamp{TS: 560}, End: models.Timestamp{TS: 570}},
				{Start: models.Timestamp{TS: 750}, End: models.Timestamp{TS: 760}},
				{Start: models.Timestamp{TS: 760}, End: models.Timestamp{TS: 770}},
				{Start: models.Timestamp{TS: 950}, End: models.Timestamp{TS: 960}},
			},
		},
		{
			name: "retain_after_is_nil",
			args: args{
				archives:      Archives,
				backups:       Backups,
				retainAfterTS: nil,
			},
			want: []models.Archive{
				{Start: models.Timestamp{TS: 100}, End: models.Timestamp{TS: 200}},
				{Start: models.Timestamp{TS: 550}, End: models.Timestamp{TS: 560}},
				{Start: models.Timestamp{TS: 560}, End: models.Timestamp{TS: 570}},
				{Start: models.Timestamp{TS: 750}, End: models.Timestamp{TS: 760}},
				{Start: models.Timestamp{TS: 760}, End: models.Timestamp{TS: 770}},
				{Start: models.Timestamp{TS: 950}, End: models.Timestamp{TS: 960}},
			},
		},
		{
			name: "retain_after_in_the_middle,_empty_backups",
			args: args{
				archives:      Archives,
				backups:       []models.Backup{},
				retainAfterTS: TimestampPtr(models.Timestamp{TS: 600}),
			},
			want: Archives[:6],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SelectPurgingOplogArchives(tt.args.archives, tt.args.backups, tt.args.retainAfterTS)
			assert.Equal(t, tt.want, got)
		})
	}
}

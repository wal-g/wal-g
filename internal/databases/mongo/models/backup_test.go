package models

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/printlist"
)

func TestTimestampInInterval(t *testing.T) {
	type args struct {
		ts    Timestamp
		begin Timestamp
		end   Timestamp
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "ts_to_the_left",
			args: args{
				ts:    Timestamp{TS: 100, Inc: 0},
				begin: Timestamp{TS: 150, Inc: 0},
				end:   Timestamp{TS: 200, Inc: 0},
			},
			want: false,
		},
		{
			name: "ts_to_the_right",
			args: args{
				ts:    Timestamp{TS: 250, Inc: 0},
				begin: Timestamp{TS: 150, Inc: 0},
				end:   Timestamp{TS: 200, Inc: 0},
			},
			want: false,
		},
		{
			name: "ts_middle",
			args: args{
				ts:    Timestamp{TS: 170, Inc: 0},
				begin: Timestamp{TS: 150, Inc: 0},
				end:   Timestamp{TS: 200, Inc: 0},
			},
			want: true,
		},
		{
			name: "ts_at_begin",
			args: args{
				ts:    Timestamp{TS: 150, Inc: 0},
				begin: Timestamp{TS: 150, Inc: 0},
				end:   Timestamp{TS: 200, Inc: 0},
			},
			want: true,
		},
		{
			name: "ts_at_end",
			args: args{
				ts:    Timestamp{TS: 200, Inc: 10},
				begin: Timestamp{TS: 150, Inc: 0},
				end:   Timestamp{TS: 200, Inc: 10},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TimestampInInterval(tt.args.ts, tt.args.begin, tt.args.end)
			assert.Equal(t, got, tt.want)
		})
	}
}

func TestArchInBackup(t *testing.T) {
	type args struct {
		arch   Archive
		backup Backup
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "to_the_left",
			args: args{
				arch: Archive{Start: Timestamp{TS: 100}, End: Timestamp{TS: 200}},
				backup: Backup{
					MongoMeta: MongoMeta{
						Before: NodeMeta{LastMajTS: Timestamp{TS: 300}},
						After:  NodeMeta{LastMajTS: Timestamp{TS: 400}},
					},
				},
			},
			want: false,
		},
		{
			name: "to_the_right",
			args: args{
				arch: Archive{Start: Timestamp{TS: 500}, End: Timestamp{TS: 600}},
				backup: Backup{
					MongoMeta: MongoMeta{
						Before: NodeMeta{LastMajTS: Timestamp{TS: 300}},
						After:  NodeMeta{LastMajTS: Timestamp{TS: 400}},
					},
				},
			},
			want: false,
		},
		{
			name: "all_in_backup",
			args: args{
				arch: Archive{Start: Timestamp{TS: 350}, End: Timestamp{TS: 370}},
				backup: Backup{
					MongoMeta: MongoMeta{
						Before: NodeMeta{LastMajTS: Timestamp{TS: 300}},
						After:  NodeMeta{LastMajTS: Timestamp{TS: 400}},
					},
				},
			},
			want: true,
		},
		{
			name: "overlaps_backup_to_the_left",
			args: args{
				arch: Archive{Start: Timestamp{TS: 250}, End: Timestamp{TS: 310}},
				backup: Backup{
					MongoMeta: MongoMeta{
						Before: NodeMeta{LastMajTS: Timestamp{TS: 300}},
						After:  NodeMeta{LastMajTS: Timestamp{TS: 400}},
					},
				},
			},
			want: true,
		},
		{
			name: "overlaps_backup_to_the_right",
			args: args{
				arch: Archive{Start: Timestamp{TS: 390}, End: Timestamp{TS: 450}},
				backup: Backup{
					MongoMeta: MongoMeta{
						Before: NodeMeta{LastMajTS: Timestamp{TS: 300}},
						After:  NodeMeta{LastMajTS: Timestamp{TS: 400}},
					},
				},
			},
			want: true,
		},
		{
			name: "overlaps_all_backup",
			args: args{
				arch: Archive{Start: Timestamp{TS: 150}, End: Timestamp{TS: 500}},
				backup: Backup{
					MongoMeta: MongoMeta{
						Before: NodeMeta{LastMajTS: Timestamp{TS: 300}},
						After:  NodeMeta{LastMajTS: Timestamp{TS: 400}},
					},
				},
			},
			want: true,
		},
		{
			name: "same_as_backup",
			args: args{
				arch: Archive{Start: Timestamp{TS: 300}, End: Timestamp{TS: 400}},
				backup: Backup{
					MongoMeta: MongoMeta{
						Before: NodeMeta{LastMajTS: Timestamp{TS: 300}},
						After:  NodeMeta{LastMajTS: Timestamp{TS: 400}},
					},
				},
			},
			want: true,
		},
		{
			name: "same_start_ts,_end_arch_ts_bigger",
			args: args{
				arch: Archive{Start: Timestamp{TS: 300}, End: Timestamp{TS: 500}},
				backup: Backup{
					MongoMeta: MongoMeta{
						Before: NodeMeta{LastMajTS: Timestamp{TS: 300}},
						After:  NodeMeta{LastMajTS: Timestamp{TS: 400}},
					},
				},
			},
			want: true,
		},
		{
			name: "same_start_ts,_end_arch_ts_smaller",
			args: args{
				arch: Archive{Start: Timestamp{TS: 300}, End: Timestamp{TS: 350}},
				backup: Backup{
					MongoMeta: MongoMeta{
						Before: NodeMeta{LastMajTS: Timestamp{TS: 300}},
						After:  NodeMeta{LastMajTS: Timestamp{TS: 400}},
					},
				},
			},
			want: true,
		},
		{
			name: "same_end_ts,_start_arch_ts_bigger",
			args: args{
				arch: Archive{Start: Timestamp{TS: 350}, End: Timestamp{TS: 400}},
				backup: Backup{
					MongoMeta: MongoMeta{
						Before: NodeMeta{LastMajTS: Timestamp{TS: 300}},
						After:  NodeMeta{LastMajTS: Timestamp{TS: 400}},
					},
				},
			},
			want: true,
		},
		{
			name: "same_end_ts,_start_arch_ts_smaller",
			args: args{
				arch: Archive{Start: Timestamp{TS: 250}, End: Timestamp{TS: 400}},
				backup: Backup{
					MongoMeta: MongoMeta{
						Before: NodeMeta{LastMajTS: Timestamp{TS: 300}},
						After:  NodeMeta{LastMajTS: Timestamp{TS: 400}},
					},
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ArchInBackup(tt.args.arch, &tt.args.backup)
			assert.Equal(t, got, tt.want)
		})
	}
}

var (
	Backups = []*Backup{
		{MongoMeta: MongoMeta{Before: NodeMeta{LastMajTS: Timestamp{TS: 800}}, After: NodeMeta{LastMajTS: Timestamp{TS: 900}}}},
		{MongoMeta: MongoMeta{Before: NodeMeta{LastMajTS: Timestamp{TS: 600}}, After: NodeMeta{LastMajTS: Timestamp{TS: 700}}}},
		{MongoMeta: MongoMeta{Before: NodeMeta{LastMajTS: Timestamp{TS: 350}}, After: NodeMeta{LastMajTS: Timestamp{TS: 500}}}},
		{MongoMeta: MongoMeta{Before: NodeMeta{LastMajTS: Timestamp{TS: 300}}, After: NodeMeta{LastMajTS: Timestamp{TS: 400}}}},
	}
)

func TestFirstOverlappingBackupForArch(t *testing.T) {
	type args struct {
		arch    Archive
		backups []*Backup
	}
	tests := []struct {
		name string
		args args
		want *Backup
	}{
		{
			name: "to_the_left",
			args: args{
				arch:    Archive{Start: Timestamp{TS: 100}, End: Timestamp{TS: 200}},
				backups: Backups,
			},
			want: nil,
		},
		{
			name: "to_the_right",
			args: args{
				arch:    Archive{Start: Timestamp{TS: 950}, End: Timestamp{TS: 1000}},
				backups: Backups,
			},
			want: nil,
		},
		{
			name: "between_backups",
			args: args{
				arch:    Archive{Start: Timestamp{TS: 550}, End: Timestamp{TS: 590}},
				backups: Backups,
			},
			want: nil,
		},
		{
			name: "in_newest_backup",
			args: args{
				arch:    Archive{Start: Timestamp{TS: 800}, End: Timestamp{TS: 850}},
				backups: Backups,
			},
			want: Backups[0],
		},
		{
			name: "in_oldest_backup",
			args: args{
				arch:    Archive{Start: Timestamp{TS: 350}, End: Timestamp{TS: 350}},
				backups: Backups,
			},
			want: Backups[2],
		},
		{
			name: "overlaps_all_backups",
			args: args{
				arch:    Archive{Start: Timestamp{TS: 500}, End: Timestamp{TS: 600}},
				backups: Backups,
			},
			want: Backups[1],
		},
		{
			name: "overlaps_two_backups",
			args: args{
				arch:    Archive{Start: Timestamp{TS: 50}, End: Timestamp{TS: 1000}},
				backups: Backups,
			},
			want: Backups[0],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FirstOverlappingBackupForArch(tt.args.arch, tt.args.backups)
			assert.Equal(t, got, tt.want)
		})
	}
}

func TestBackup_PrintableFields(t *testing.T) {
	b := Backup{
		BackupName:      "my first backup",
		BackupType:      "type1",
		StartLocalTime:  time.Unix(1692811111, 0).UTC(),
		FinishLocalTime: time.Unix(1692822222, 0).UTC(),
		Hostname:        "my-favourite-host",
		MongoMeta: MongoMeta{
			Version: "123",
			Before:  NodeMeta{LastMajTS: Timestamp{TS: 800}},
			After:   NodeMeta{LastMajTS: Timestamp{TS: 900}},
		},
		UncompressedSize: 200000,
		CompressedSize:   100000,
		Permanent:        true,
		UserData:         []string{"a", "b", "c"},
	}
	got := b.PrintableFields()
	prettyStartTime := "Wednesday, 23-Aug-23 17:18:31 UTC"
	prettyFinishTime := "Wednesday, 23-Aug-23 20:23:42 UTC"
	want := []printlist.TableField{
		{
			Name:        "name",
			PrettyName:  "Name",
			Value:       "my first backup",
			PrettyValue: nil,
		},
		{
			Name:        "type",
			PrettyName:  "Type",
			Value:       "type1",
			PrettyValue: nil,
		},
		{
			Name:        "version",
			PrettyName:  "Version",
			Value:       "123",
			PrettyValue: nil,
		},
		{
			Name:        "start_time",
			PrettyName:  "Start time",
			Value:       "2023-08-23T17:18:31Z",
			PrettyValue: &prettyStartTime,
		},
		{
			Name:        "finish_time",
			PrettyName:  "Finish time",
			Value:       "2023-08-23T20:23:42Z",
			PrettyValue: &prettyFinishTime,
		},
		{
			Name:        "hostname",
			PrettyName:  "Hostname",
			Value:       "my-favourite-host",
			PrettyValue: nil,
		},
		{
			Name:        "start_ts",
			PrettyName:  "Start Ts",
			Value:       "800.0",
			PrettyValue: nil,
		},
		{
			Name:        "end_ts",
			PrettyName:  "End Ts",
			Value:       "900.0",
			PrettyValue: nil,
		},
		{
			Name:        "uncompressed_size",
			PrettyName:  "Uncompressed size",
			Value:       "200000",
			PrettyValue: nil,
		},
		{
			Name:        "compressed_size",
			PrettyName:  "Compressed size",
			Value:       "100000",
			PrettyValue: nil,
		},
		{
			Name:        "permanent",
			PrettyName:  "Permanent",
			Value:       "true",
			PrettyValue: nil,
		},
		{
			Name:        "user_data",
			PrettyName:  "User data",
			Value:       "[\"a\",\"b\",\"c\"]",
			PrettyValue: nil,
		},
	}
	assert.Equal(t, want, got)
}

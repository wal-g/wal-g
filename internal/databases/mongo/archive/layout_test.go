package archive

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/wal-g/wal-g/internal/databases/mongo/models"

	"github.com/stretchr/testify/assert"
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

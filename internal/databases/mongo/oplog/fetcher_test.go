package oplog

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	archiveMocks "github.com/wal-g/wal-g/internal/databases/mongo/archive/mocks"
	mongoMocks "github.com/wal-g/wal-g/internal/databases/mongo/client/mocks"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.mongodb.org/mongo-driver/bson"
)

const (
	archiveExt = "br"
)

var (
	ops = []models.Oplog{
		{TS: models.Timestamp{TS: 1579002001, Inc: 1}, OP: "i", NS: "testdb.testc"},
		{TS: models.Timestamp{TS: 1579002002, Inc: 1}, OP: "i", NS: "testdb.testc1"},
		{TS: models.Timestamp{TS: 1579002003, Inc: 1}, OP: "i", NS: "testdb.testc1"},
		{TS: models.Timestamp{TS: 1579002004, Inc: 1}, OP: "i", NS: "testdb.testc2"},
		{TS: models.Timestamp{TS: 1579002005, Inc: 1}, OP: "i", NS: "testdb.testc2"},
		{TS: models.Timestamp{TS: 1579002006, Inc: 1}, OP: "i", NS: "testdb.testc3"},
	}
)

func TestMain(m *testing.M) {
	fillOpsRawData()
	os.Exit(m.Run())
}

func fillOpsRawData() {
	for i := range ops {
		opMeta := models.OplogMeta{
			TS: models.BsonTimestampFromOplogTS(ops[i].TS),
			NS: ops[i].NS,
			Op: ops[i].OP,
		}
		raw, err := bson.Marshal(opMeta)
		if err != nil {
			panic(err)
		}
		ops[i].Data = raw
	}
}

func ArchRawMocks(batches ...[]models.Oplog) ([]models.Archive, [][]byte) {
	archives := make([]models.Archive, 0, len(batches))
	raws := make([][]byte, 0, len(batches))
	startTS := models.Timestamp{}
	for _, ops := range batches {
		buf := bytes.Buffer{}
		for _, op := range ops {
			buf.Write(op.Data)
		}
		arch, err := models.NewArchive(startTS, ops[len(ops)-1].TS, archiveExt)
		if err != nil {
			panic(err)
		}
		startTS = ops[len(ops)-1].TS
		archives = append(archives, arch)
		raws = append(raws, buf.Bytes())
	}
	return archives, raws
}

type DownloaderFields struct {
	downloader *archiveMocks.Downloader
	path       archive.Sequence
}

func SetupDownloaderMocks(ops ...[]models.Oplog) DownloaderFields {
	dl := archiveMocks.Downloader{}
	archives, raws := ArchRawMocks(ops...)
	dl.On("DownloadOplogArchive", mock.Anything, mock.Anything).
		Return(nil).
		Run(func(args mock.Arguments) {
			writer := args.Get(1).(io.WriteCloser)
			arch := args.Get(0).(models.Archive)
			for i, a := range archives {
				if a == arch {
					if _, err := writer.Write(raws[i]); err != nil {
						panic(err)
					}
					return
				}
			}
			panic("bad mock data")
		}).Times(len(archives))

	return DownloaderFields{downloader: &dl, path: archives}
}

func TestStorageFetcher_OplogBetween(t *testing.T) {
	type args struct {
		ctx   context.Context
		from  models.Timestamp
		until models.Timestamp
		wg    *sync.WaitGroup
	}
	tests := []struct {
		name     string
		fields   DownloaderFields
		args     args
		wantOps  []models.Oplog
		wantErr  error
		wantErrc error
	}{
		{
			name:   "from first until last, one archive",
			fields: SetupDownloaderMocks(ops),
			args: args{
				ctx:   context.TODO(),
				from:  ops[0].TS,
				until: ops[len(ops)-1].TS,
				wg:    &sync.WaitGroup{},
			},
			wantOps:  ops[:len(ops)-1],
			wantErrc: nil,
		},
		{
			name:   "from first until last, three archives",
			fields: SetupDownloaderMocks(ops[0:2], ops[2:3], ops[3:]),
			args: args{
				ctx:   context.TODO(),
				from:  ops[0].TS,
				until: ops[len(ops)-1].TS,
				wg:    &sync.WaitGroup{},
			},
			wantOps:  ops[:len(ops)-1],
			wantErrc: nil,
		},
		{
			name:   "from second until pre-last, three archives",
			fields: SetupDownloaderMocks(ops[0:3], ops[3:4], ops[4:]),
			args: args{
				ctx:   context.TODO(),
				from:  ops[1].TS,
				until: ops[len(ops)-2].TS,
				wg:    &sync.WaitGroup{},
			},
			wantOps:  ops[1 : len(ops)-2],
			wantErrc: nil,
		},
		{
			name:   "error: first > until",
			fields: SetupDownloaderMocks(ops[0:2], ops[2:3], ops[3:]),
			args: args{
				ctx:   context.TODO(),
				from:  ops[0].TS,
				until: models.Timestamp{TS: 1579002000, Inc: 1},
				wg:    &sync.WaitGroup{},
			},
			wantOps: []models.Oplog{},
			wantErr: fmt.Errorf("fromTS '1579002001.1' must be less than untilTS '1579002000.1'"),
		},
		{
			name:   "error: first was not found in first archive",
			fields: SetupDownloaderMocks(ops),
			args: args{
				ctx:   context.TODO(),
				from:  models.Timestamp{TS: 1579002000, Inc: 1},
				until: ops[len(ops)-1].TS,
				wg:    &sync.WaitGroup{},
			},
			wantOps:  []models.Oplog{},
			wantErrc: fmt.Errorf("'from' timestamp '1579002000.1' was not found in first archive: oplog_0.0_1579002006.1.br"),
		},
		{
			name:   "error: until is not reached",
			fields: SetupDownloaderMocks(ops[0:2], ops[2:3], ops[3:]),
			args: args{
				ctx:   context.TODO(),
				from:  ops[0].TS,
				until: models.Timestamp{TS: 1579002099, Inc: 1},
				wg:    &sync.WaitGroup{},
			},
			wantOps:  ops,
			wantErrc: fmt.Errorf("restore sequence was fetched, but restore point '1579002099.1' is not reached"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sf := &StorageFetcher{
				downloader: tt.fields.downloader,
				path:       tt.fields.path,
			}

			outc, errc, err := sf.OplogBetween(tt.args.ctx, tt.args.from, tt.args.until, tt.args.wg)
			if tt.wantErr != nil {
				assert.EqualError(t, err, tt.wantErr.Error())
				return
			}
			assert.Nil(t, err)

			outOpsCh := gatherOps(outc)
			err, _ = <-errc
			outOps := <-outOpsCh

			assert.Equal(t, tt.wantOps, outOps)
			if tt.wantErrc != nil {
				assert.EqualError(t, err, tt.wantErrc.Error())
			} else {
				assert.Nil(t, err)
			}

			// check if error chan is closed
			_, ok := <-errc
			assert.False(t, ok)

			tt.fields.downloader.AssertExpectations(t)
		})
	}
}

type MongoDriverFields struct {
	mongo  *mongoMocks.MongoDriver
	cursor *mongoMocks.OplogCursor
}

func SetupMongoDriverMocks(ops []models.Oplog, driverErr, curErr error, badOp bool) MongoDriverFields {
	md := &mongoMocks.MongoDriver{}
	cur := &mongoMocks.OplogCursor{}

	if curErr == nil {
		for i := range ops {
			cur.On("Data").Return(ops[i].Data).Once().
				On("Next", mock.Anything).Return(true).Once()
		}
	}

	cur.On("Close", mock.Anything).Return(nil).Once()

	if !badOp {
		cur.On("Next", mock.Anything).Return(false).Once().
			On("Err").Return(curErr).Once()
	}

	md.On("TailOplogFrom", mock.Anything, mock.Anything).Return(cur, driverErr).Once()

	return MongoDriverFields{mongo: md, cursor: cur}
}

func TestDBFetcher_OplogFrom(t *testing.T) {
	type args struct {
		ctx  context.Context
		from models.Timestamp
		wg   *sync.WaitGroup
	}
	tests := []struct {
		name     string
		fields   MongoDriverFields
		args     args
		wantOps  []models.Oplog
		wantErr  error
		wantErrc error
	}{
		{
			name:   "from first until last, until cursor exhausted",
			fields: SetupMongoDriverMocks(ops, nil, nil, false),
			args: args{
				ctx:  context.TODO(),
				from: ops[0].TS,
				wg:   &sync.WaitGroup{},
			},
			wantOps:  ops,
			wantErrc: fmt.Errorf("oplog cursor exhausted"),
		},
		{
			name:   "error: cursor error",
			fields: SetupMongoDriverMocks(ops, nil, fmt.Errorf("cursor error"), false),
			args: args{
				ctx:  context.TODO(),
				from: ops[0].TS,
				wg:   &sync.WaitGroup{},
			},
			wantOps:  []models.Oplog{},
			wantErrc: fmt.Errorf("oplog cursor error: cursor error"),
		},
		{
			name:   "error: from ts is not found",
			fields: SetupMongoDriverMocks(ops[1:2], nil, nil, true),
			args: args{
				ctx:  context.TODO(),
				from: ops[0].TS,
				wg:   &sync.WaitGroup{},
			},
			wantOps:  []models.Oplog{},
			wantErrc: fmt.Errorf("'from' timestamp '1579002001.1' was not found"),
		},
		{
			name:   "error: driver error",
			fields: SetupMongoDriverMocks(ops, fmt.Errorf("driver error"), nil, false),
			args: args{
				ctx:  context.TODO(),
				from: ops[0].TS,
				wg:   &sync.WaitGroup{},
			},
			wantOps: ops,
			wantErr: fmt.Errorf("driver error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbf := &DBFetcher{
				db: tt.fields.mongo,
			}

			outc, errc, err := dbf.OplogFrom(tt.args.ctx, tt.args.from, tt.args.wg)
			if tt.wantErr != nil {
				assert.EqualError(t, err, tt.wantErr.Error())
				return
			}
			assert.Nil(t, err)

			outOpsCh := gatherOps(outc)
			err, _ = <-errc
			outOps := <-outOpsCh

			assert.Equal(t, tt.wantOps, outOps)
			if tt.wantErrc != nil {
				assert.EqualError(t, err, tt.wantErrc.Error())
			} else {
				assert.Nil(t, err)
			}

			// check if error chan is closed
			_, ok := <-errc
			assert.False(t, ok)

			tt.fields.mongo.AssertExpectations(t)
			tt.fields.cursor.AssertExpectations(t)
		})
	}
}

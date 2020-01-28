package oplog

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/wal-g/wal-g/internal/databases/mongo/models"

	"github.com/stretchr/testify/assert"
)

func gatherOps(in chan models.Oplog) chan []models.Oplog {
	ch := make(chan []models.Oplog)
	go func() {
		outOps := make([]models.Oplog, 0, 0)
		for op := range in {
			outOps = append(outOps, op)
		}
		ch <- outOps
		close(ch)
	}()
	return ch
}

func TestDBValidator_Validate(t *testing.T) {
	type fields struct {
		since models.Timestamp
	}
	type args struct {
		ctx context.Context
		in  chan models.Oplog
		wg  *sync.WaitGroup
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		wantOut  chan models.Oplog
		wantErrc error
		ops      []models.Oplog
	}{
		{
			name: "3 docs, no error",
			fields: fields{
				since: models.Timestamp{},
			},
			args: args{
				ctx: context.TODO(),
				in:  make(chan models.Oplog),
				wg:  &sync.WaitGroup{},
			},
			ops: []models.Oplog{
				{
					TS: models.Timestamp{TS: 1579002001, Inc: 1},
					OP: "i",
					NS: "testdb.testc",
				},
				{
					TS: models.Timestamp{TS: 1579002002, Inc: 1},
					OP: "i",
					NS: "testdb.testc1",
				},
				{
					TS: models.Timestamp{TS: 1579002003, Inc: 1},
					OP: "i",
					NS: "testdb.testc2",
				},
			},
			wantErrc: nil,
		},
		{
			name: "1 doc, gap error",
			fields: fields{
				since: models.Timestamp{TS: 1579002001, Inc: 2},
			},
			args: args{
				ctx: context.TODO(),
				in:  make(chan models.Oplog),
				wg:  &sync.WaitGroup{},
			},
			ops: []models.Oplog{
				{
					TS: models.Timestamp{TS: 1579002001, Inc: 1},
					OP: "i",
					NS: "testdb.testc",
				},
			},
			wantErrc: fmt.Errorf("oplog validate error: last known document was not found - expected first ts is 1579002001.2, but 1579002001.1 is given"),
		},
		{
			name: "2 docs, validation error: renameCollection",
			fields: fields{
				since: models.Timestamp{TS: 1579002001, Inc: 1},
			},
			args: args{
				ctx: context.TODO(),
				in:  make(chan models.Oplog),
				wg:  &sync.WaitGroup{},
			},
			ops: []models.Oplog{
				{
					TS: models.Timestamp{TS: 1579002001, Inc: 1},
					OP: "i",
					NS: "testdb.testc",
				},
				{
					TS: models.Timestamp{TS: 1579002003, Inc: 1},
					OP: "renameCollections",
					NS: "testdb.testc",
				},
			},
			wantErrc: fmt.Errorf("oplog validate error: collection renamed - testdb.testc"),
		},
		{
			name: "2 docs, validation error: auth schema",
			fields: fields{
				since: models.Timestamp{TS: 1579002001, Inc: 1},
			},
			args: args{
				ctx: context.TODO(),
				in:  make(chan models.Oplog),
				wg:  &sync.WaitGroup{},
			},
			ops: []models.Oplog{
				{
					TS: models.Timestamp{TS: 1579002001, Inc: 1},
					OP: "u",
					NS: "admin.system.version",
				},
			},
			wantErrc: fmt.Errorf("oplog validate error: schema version of the user credential documents changed - operation 'u'"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbv := &DBValidator{
				since: tt.fields.since,
			}
			outc, errc, err := dbv.Validate(tt.args.ctx, tt.args.in, tt.args.wg)
			assert.Nil(t, err)

			outOpsCh := gatherOps(outc)

			for _, op := range tt.ops {
				tt.args.in <- op
			}
			close(tt.args.in)

			err = <-errc
			outOps := <-outOpsCh
			if tt.wantErrc != nil {
				assert.Equal(t, outOps, tt.ops[:len(tt.ops)-1])
				assert.EqualError(t, err, tt.wantErrc.Error())
			} else {
				assert.Equal(t, outOps, tt.ops)
				assert.Nil(t, err)
			}

			// check if error chan is closed
			_, ok := <-errc
			assert.False(t, ok)
		})
	}
}

package oplog

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	archiveMocks "github.com/wal-g/wal-g/internal/databases/mongo/archive/mocks"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"

	"github.com/stretchr/testify/assert"
)

// TODO: test archive timeout
func TestStorageApplier_Apply(t *testing.T) {
	type fields struct {
		uploader *archiveMocks.Uploader
		size     int
		timeout  time.Duration
	}
	type args struct {
		ctx    context.Context
		oplogc chan models.Oplog
		wg     *sync.WaitGroup
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		ops     []models.Oplog
		want    chan error
		wantErr error
	}{
		{
			name: "3 docs, closed input channel initiates upload",
			fields: fields{
				uploader: func() *archiveMocks.Uploader {
					upl := archiveMocks.Uploader{}

					buf27 := bytes.Buffer{}
					buf27.Write(make([]byte, 27))

					upl.On("UploadOplogArchive",
						&buf27,
						models.Timestamp{TS: 1579002001, Inc: 1},
						models.Timestamp{TS: 1579002003, Inc: 1}).
						Return(nil).Once()
					return &upl
				}(),
				size:    256,
				timeout: time.Second * 50,
			},
			args: args{
				ctx:    context.TODO(),
				oplogc: make(chan models.Oplog),
				wg:     &sync.WaitGroup{},
			},
			ops: []models.Oplog{
				{
					TS:   models.Timestamp{TS: 1579002001, Inc: 1},
					Data: make([]byte, 8),
				},
				{
					TS:   models.Timestamp{TS: 1579002002, Inc: 1},
					Data: make([]byte, 9),
				},
				{
					TS:   models.Timestamp{TS: 1579002003, Inc: 1},
					Data: make([]byte, 10),
				},
			},
			want:    nil,
			wantErr: nil,
		},
		{
			name: "3 docs, batch size initiates upload",
			fields: fields{
				uploader: func() *archiveMocks.Uploader {
					upl := archiveMocks.Uploader{}

					buf17 := bytes.Buffer{}
					buf17.Write(make([]byte, 17))

					buf16 := bytes.Buffer{}
					buf16.Write(make([]byte, 16))

					upl.On("UploadOplogArchive",
						&buf17,
						models.Timestamp{TS: 1579002001, Inc: 1},
						models.Timestamp{TS: 1579002002, Inc: 1}).
						Return(nil).Once().
						On("UploadOplogArchive",
							&buf16,
							models.Timestamp{TS: 1579002002, Inc: 1},
							models.Timestamp{TS: 1579002009, Inc: 1}).
						Return(nil).Once()

					return &upl
				}(),
				size:    16,
				timeout: 1024000000,
			},
			args: args{
				ctx:    context.TODO(),
				oplogc: make(chan models.Oplog),
				wg:     &sync.WaitGroup{},
			},
			ops: []models.Oplog{
				{
					TS:   models.Timestamp{TS: 1579002001, Inc: 1},
					Data: make([]byte, 8),
				},
				{
					TS:   models.Timestamp{TS: 1579002002, Inc: 1},
					Data: make([]byte, 9),
				},
				{
					TS:   models.Timestamp{TS: 1579002009, Inc: 1},
					Data: make([]byte, 16),
				},
			},
			want:    nil,
			wantErr: nil,
		},
		{
			name: "1 doc, upload error",
			fields: fields{
				uploader: func() *archiveMocks.Uploader {
					upl := archiveMocks.Uploader{}

					buf8 := bytes.Buffer{}
					buf8.Write(make([]byte, 8))

					upl.On("UploadOplogArchive",
						&buf8,
						models.Timestamp{TS: 1579002001, Inc: 1},
						models.Timestamp{TS: 1579002001, Inc: 1}).
						Return(fmt.Errorf("error while uploading stream: X")).Once()
					return &upl
				}(),
				size:    16,
				timeout: 1024000000,
			},
			args: args{
				ctx:    context.TODO(),
				oplogc: make(chan models.Oplog),
				wg:     &sync.WaitGroup{},
			},
			ops: []models.Oplog{
				{
					TS:   models.Timestamp{TS: 1579002001, Inc: 1},
					Data: make([]byte, 8),
				},
			},
			want:    nil,
			wantErr: fmt.Errorf("can not upload oplog archive: error while uploading stream: X"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sa := &StorageApplier{
				uploader: tt.fields.uploader,
				size:     tt.fields.size,
				timeout:  tt.fields.timeout,
			}

			errc, err := sa.Apply(tt.args.ctx, tt.args.oplogc, tt.args.wg)
			assert.Nil(t, err)

			for _, op := range tt.ops {
				tt.args.oplogc <- op
			}
			close(tt.args.oplogc)

			err = <-errc
			if tt.wantErr != nil {
				assert.EqualError(t, err, tt.wantErr.Error())
			} else {
				assert.Nil(t, err)
			}

			// check if error chan is closed
			_, ok := <-errc
			assert.False(t, ok)

			tt.fields.uploader.AssertExpectations(t)
		})
	}
}

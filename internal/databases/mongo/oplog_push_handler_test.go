package mongo

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/internal/databases/mongo/stages"
	mocks "github.com/wal-g/wal-g/internal/databases/mongo/stages/mocks"
	"github.com/wal-g/wal-g/internal/databases/mongo/stats"
	"go.mongodb.org/mongo-driver/bson"
)

type fetcherReturn struct {
	outChan chan *models.Oplog
	errChan chan error
	err     error
}

type applierReturn struct {
	errChan chan error
	err     error
}

type oplogPushArgs struct {
	ctx context.Context

	fetcherReturn *fetcherReturn
	applierReturn *applierReturn
}

type oplogPushMocks struct {
	fetcher *mocks.Fetcher
	applier *mocks.Applier
}

func (tm *oplogPushMocks) AssertExpectations(t *testing.T) {
	if tm.fetcher != nil {
		tm.fetcher.AssertExpectations(t)
	}
	if tm.applier != nil {
		tm.applier.AssertExpectations(t)
	}
}

func buildTestArgs() oplogPushArgs {
	return oplogPushArgs{
		ctx: context.TODO(),

		fetcherReturn: &fetcherReturn{make(chan *models.Oplog), make(chan error), nil},
		applierReturn: &applierReturn{make(chan error), nil},
	}
}

func prepareOplogPushMocks(args oplogPushArgs, mocks oplogPushMocks) {
	if mocks.fetcher != nil {
		mocks.fetcher.On("Fetch", mock.Anything).
			Return(args.fetcherReturn.outChan, args.fetcherReturn.errChan, args.fetcherReturn.err).
			Once()
	}

	if mocks.applier != nil {
		mocks.applier.On("Apply", mock.Anything, args.fetcherReturn.outChan).
			Return(args.applierReturn.errChan, args.applierReturn.err).
			Once()
	}
}

func TestHandleOplogPush(t *testing.T) {
	tests := []struct {
		name        string
		args        oplogPushArgs
		mocks       oplogPushMocks
		failErrRet  func(args oplogPushArgs)
		failErrChan func(args oplogPushArgs)
		expectedErr error
	}{
		{
			name:        "fetcher call returns error",
			args:        buildTestArgs(),
			mocks:       oplogPushMocks{&mocks.Fetcher{}, nil},
			failErrRet:  func(args oplogPushArgs) { args.fetcherReturn.err = fmt.Errorf("fetcher ret err") },
			expectedErr: fmt.Errorf("fetcher ret err"),
		},
		{
			name:        "applier call returns error",
			args:        buildTestArgs(),
			mocks:       oplogPushMocks{&mocks.Fetcher{}, &mocks.Applier{}},
			failErrRet:  func(args oplogPushArgs) { args.applierReturn.err = fmt.Errorf("applier ret err") },
			expectedErr: fmt.Errorf("applier ret err"),
		},
		{
			name:  "fetcher returns error via error channel",
			args:  buildTestArgs(),
			mocks: oplogPushMocks{&mocks.Fetcher{}, &mocks.Applier{}},
			failErrChan: func(args oplogPushArgs) {
				args.fetcherReturn.errChan <- fmt.Errorf("fetcher chan err")
				close(args.fetcherReturn.errChan)
				close(args.applierReturn.errChan)
			},
			expectedErr: fmt.Errorf("fetcher chan err"),
		},
		{
			name:  "applier returns error via error channel",
			args:  buildTestArgs(),
			mocks: oplogPushMocks{&mocks.Fetcher{}, &mocks.Applier{}},
			failErrChan: func(args oplogPushArgs) {
				args.applierReturn.errChan <- fmt.Errorf("applier chan err")
				close(args.applierReturn.errChan)
				close(args.fetcherReturn.errChan)
			},
			expectedErr: fmt.Errorf("applier chan err"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {

			if tc.failErrRet != nil {
				tc.failErrRet(tc.args)
			}
			if tc.failErrChan != nil {
				go tc.failErrChan(tc.args)
			}

			prepareOplogPushMocks(tc.args, tc.mocks)
			err := HandleOplogPush(tc.args.ctx, tc.mocks.fetcher, tc.mocks.applier)
			if tc.expectedErr != nil {
				assert.EqualError(t, err, tc.expectedErr.Error())
			} else {
				assert.Nil(t, err)
			}

			tc.mocks.AssertExpectations(t)
		})
	}
}

func TestHandleOplogPush_CancelLongUpload(t *testing.T) {
	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()

	fetcher := &mocks.Fetcher{}
	oplogc := make(chan *models.Oplog)
	errc := make(chan error)
	fetcher.On("Fetch", mock.Anything).
		Return(oplogc, errc, nil).
		Once()

	nextOplog := func() *models.Oplog {
		ts := models.Timestamp{TS: uint32(time.Now().Unix()), Inc: uint32(time.Now().Nanosecond())}
		data, err := bson.Marshal(map[string]interface{}{
			"ts": ts.ToBsonTS(),
		})
		require.NoError(t, err)
		return &models.Oplog{TS: ts, Data: data}
	}
	go func() {
		for i := 0; i < 5; i++ {
			oplogc <- nextOplog()
		}
		oplogc <- nextOplog()
		t.Log("Cancel context")
		cancelCtx()
		close(oplogc)
		close(errc)
	}()

	uploaderMock := &uploaderMock{
		t:                  t,
		waitForCancelAfter: 5,
	}
	buff := stages.NewMemoryBuffer()
	statsUpdater := stats.NewOplogUploadStats(models.Timestamp{TS: uint32(time.Now().Unix()), Inc: uint32(time.Now().Nanosecond())})
	applier := stages.NewStorageApplier(
		uploaderMock,
		buff,
		1,
		time.Hour,
		statsUpdater,
	)

	err := HandleOplogPush(ctx, fetcher, applier)
	require.Error(t, err)
	require.Contains(t, err.Error(), "can not upload oplog archive: stop uploading oplog: context canceled")
}

var _ archive.Uploader = &uploaderMock{}

type uploaderMock struct {
	t                  *testing.T
	waitForCancelAfter int
}

func (u *uploaderMock) UploadOplogArchive(ctx context.Context, stream io.Reader, firstTS, lastTS models.Timestamp) error {
	if u.waitForCancelAfter <= 0 {
		u.t.Log("Wait for context is cancelled")
		select {
		case <-ctx.Done():
			return fmt.Errorf("stop uploading oplog: %w", ctx.Err())
		}
	}
	u.waitForCancelAfter--
	u.t.Logf("Uploaded oplog: firstTS %v, lastTS %v", firstTS, lastTS)
	return nil
}

func (u *uploaderMock) UploadGapArchive(err error, firstTS, lastTS models.Timestamp) error {
	u.t.Fatal("unexpected call")
	return nil
}

func (u *uploaderMock) UploadBackup(stream io.Reader, cmd internal.ErrWaiter, metaConstructor internal.MetaConstructor) error {
	u.t.Fatal("unexpected call")
	return nil
}

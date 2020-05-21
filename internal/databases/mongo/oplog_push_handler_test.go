package mongo

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/internal/databases/mongo/stages/mocks"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type fromFetcherReturn struct {
	outChan chan models.Oplog
	errChan chan error
	err     error
}

type validatorReturn struct {
	outChan chan models.Oplog
	errChan chan error
	err     error
}

type applierReturn struct {
	errChan chan error
	err     error
}

type oplogPushArgs struct {
	ctx   context.Context
	since models.Timestamp
	wg    *sync.WaitGroup

	fromFetcherReturn *fromFetcherReturn
	applierReturn     *applierReturn
}

type oplogPushMocks struct {
	fetcher *mocks.FromFetcher
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
		ctx:   context.TODO(),
		since: models.Timestamp{TS: 1579021614, Inc: 15},
		wg:    &sync.WaitGroup{},

		fromFetcherReturn: &fromFetcherReturn{make(chan models.Oplog), make(chan error), nil},
		applierReturn:     &applierReturn{make(chan error), nil},
	}
}

func prepareOplogPushMocks(args oplogPushArgs, mocks oplogPushMocks) {
	if mocks.fetcher != nil {
		mocks.fetcher.On("OplogFrom", mock.Anything, args.since, args.wg).
			Return(args.fromFetcherReturn.outChan, args.fromFetcherReturn.errChan, args.fromFetcherReturn.err).
			Once()
	}

	if mocks.applier != nil {
		mocks.applier.On("Apply", mock.Anything, args.fromFetcherReturn.outChan, args.wg).
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
			mocks:       oplogPushMocks{&mocks.FromFetcher{}, nil},
			failErrRet:  func(args oplogPushArgs) { args.fromFetcherReturn.err = fmt.Errorf("fetcher ret err") },
			expectedErr: fmt.Errorf("fetcher ret err"),
		},
		{
			name:        "applier call returns error",
			args:        buildTestArgs(),
			mocks:       oplogPushMocks{&mocks.FromFetcher{}, &mocks.Applier{}},
			failErrRet:  func(args oplogPushArgs) { args.applierReturn.err = fmt.Errorf("applier ret err") },
			expectedErr: fmt.Errorf("applier ret err"),
		},
		{
			name:        "fetcher returns error via error channel",
			args:        buildTestArgs(),
			mocks:       oplogPushMocks{&mocks.FromFetcher{}, &mocks.Applier{}},
			failErrChan: func(args oplogPushArgs) { args.fromFetcherReturn.errChan <- fmt.Errorf("fetcher chan err") },
			expectedErr: fmt.Errorf("fetcher chan err"),
		},
		{
			name:        "applier returns error via error channel",
			args:        buildTestArgs(),
			mocks:       oplogPushMocks{&mocks.FromFetcher{}, &mocks.Applier{}},
			failErrChan: func(args oplogPushArgs) { args.fromFetcherReturn.errChan <- fmt.Errorf("applier chan err") },
			expectedErr: fmt.Errorf("applier chan err"),
		},
	}

	for _, tc := range tests {
		if tc.failErrRet != nil {
			tc.failErrRet(tc.args)
		}
		if tc.failErrChan != nil {
			go tc.failErrChan(tc.args)
		}

		prepareOplogPushMocks(tc.args, tc.mocks)
		err := HandleOplogPush(tc.args.ctx, tc.args.since, tc.mocks.fetcher, tc.mocks.applier)
		if tc.expectedErr != nil {
			assert.EqualError(t, err, tc.expectedErr.Error())
		} else {
			assert.Nil(t, err)
		}

		tc.mocks.AssertExpectations(t)
	}
}

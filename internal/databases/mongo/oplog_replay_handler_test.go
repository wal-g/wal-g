package mongo

import (
	"context"
	"fmt"
	"testing"

	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	mocks "github.com/wal-g/wal-g/internal/databases/mongo/stages/mocks"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type betweenFetcherReturn struct {
	outChan chan *models.Oplog
	errChan chan error
	err     error
}

type oplogReplayTestArgs struct {
	ctx   context.Context
	since models.Timestamp
	until models.Timestamp

	betweenFetcherReturn *betweenFetcherReturn
	applierReturn        *applierReturn
}

type oplogReplayTestMocks struct {
	fetcher *mocks.BetweenFetcher
	applier *mocks.Applier
}

func (tm *oplogReplayTestMocks) AssertExpectations(t *testing.T) {
	if tm.fetcher != nil {
		tm.fetcher.AssertExpectations(t)
	}

	if tm.applier != nil {
		tm.applier.AssertExpectations(t)
	}
}

func buildOplogReplayTestArgs() oplogReplayTestArgs {
	return oplogReplayTestArgs{
		ctx:   context.TODO(),
		since: models.Timestamp{TS: 1579021614, Inc: 15},
		until: models.Timestamp{TS: 1579023614, Inc: 11},

		betweenFetcherReturn: &betweenFetcherReturn{make(chan *models.Oplog), make(chan error), nil},
		applierReturn:        &applierReturn{make(chan error), nil},
	}
}

func prepareOplogReplayMocks(args oplogReplayTestArgs, mocks oplogReplayTestMocks) {
	if mocks.fetcher != nil {
		mocks.fetcher.On("FetchBetween", mock.Anything, args.since, args.until).
			Return(args.betweenFetcherReturn.outChan, args.betweenFetcherReturn.errChan, args.betweenFetcherReturn.err)
	}
	if mocks.applier != nil {
		mocks.applier.On("Apply", mock.Anything, args.betweenFetcherReturn.outChan).
			Return(args.applierReturn.errChan, args.applierReturn.err)
	}
}

func TestHandleOplogReplay(t *testing.T) {
	tests := []struct {
		name        string
		args        oplogReplayTestArgs
		mocks       oplogReplayTestMocks
		failErrRet  func(args oplogReplayTestArgs)
		failErrChan func(args oplogReplayTestArgs)
		expectedErr error
	}{
		{
			name:        "fetcher call returns error",
			args:        buildOplogReplayTestArgs(),
			mocks:       oplogReplayTestMocks{&mocks.BetweenFetcher{}, nil},
			failErrRet:  func(args oplogReplayTestArgs) { args.betweenFetcherReturn.err = fmt.Errorf("fetcher ret err") },
			expectedErr: fmt.Errorf("fetcher ret err"),
		},
		{
			name:        "applier call returns error",
			args:        buildOplogReplayTestArgs(),
			mocks:       oplogReplayTestMocks{&mocks.BetweenFetcher{}, &mocks.Applier{}},
			failErrRet:  func(args oplogReplayTestArgs) { args.applierReturn.err = fmt.Errorf("applier ret err") },
			expectedErr: fmt.Errorf("applier ret err"),
		},
		{
			name:  "fetcher returns error via error channel",
			args:  buildOplogReplayTestArgs(),
			mocks: oplogReplayTestMocks{&mocks.BetweenFetcher{}, &mocks.Applier{}},
			failErrChan: func(args oplogReplayTestArgs) {
				args.betweenFetcherReturn.errChan <- fmt.Errorf("fetcher chan err")
				close(args.betweenFetcherReturn.errChan)
				close(args.applierReturn.errChan)
			},
			expectedErr: fmt.Errorf("fetcher chan err"),
		},
		{
			name:  "applier returns error via error channel",
			args:  buildOplogReplayTestArgs(),
			mocks: oplogReplayTestMocks{&mocks.BetweenFetcher{}, &mocks.Applier{}},
			failErrChan: func(args oplogReplayTestArgs) {
				args.applierReturn.errChan <- fmt.Errorf("applier chan err")
				close(args.applierReturn.errChan)
				close(args.betweenFetcherReturn.errChan)
			},
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

		prepareOplogReplayMocks(tc.args, tc.mocks)
		err := HandleOplogReplay(tc.args.ctx, tc.args.since, tc.args.until, tc.mocks.fetcher, tc.mocks.applier)
		if tc.expectedErr != nil {
			assert.EqualError(t, err, tc.expectedErr.Error())
		} else {
			assert.Nil(t, err)
		}

		tc.mocks.AssertExpectations(t)
	}
}

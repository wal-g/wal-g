package internal

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type OpInfo struct {
	command   bson.D
	id        int
	opName    string
	status    bool
	err       error
	timeStart time.Time
	timeEnd   time.Time
	subcmds   []OpInfo
	res       bson.M
}

func NewOpInfo(opName string, id int, timeStart time.Time, timeEnd time.Time, err error) OpInfo {
	return OpInfo{
		command:   bson.D{},
		id:        id,
		opName:    opName,
		status:    err == nil,
		err:       err,
		timeStart: timeStart,
		timeEnd:   timeEnd,
		subcmds:   nil,
		res:       nil,
	}
}

func BuildStage(ctx context.Context,
	cli *mongo.Client,
	roc <-chan RawMongoOp,
	wg *sync.WaitGroup) (<-chan ExecFunc, <-chan error) {
	cmds := make(chan ExecFunc, cap(roc))
	errc := make(chan error, 1)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(cmds)
		defer close(errc)

		for cmd := range roc {
			opCmd, err := NewExecFunc(cli, cmd)
			if err != nil {
				errc <- fmt.Errorf("cannot parse command: %v", err)
				return
			}
			select {
			case cmds <- opCmd:
			case <-ctx.Done():
				return
			}
		}
	}()

	return cmds, errc
}

func NewExecFunc(client *mongo.Client, opdata RawMongoOp) (ExecFunc, error) {
	switch opdata.OP {
	case `c`:
		op, err := NewCommandOp(opdata)
		if err != nil {
			return nil, err
		}
		return NewOpExec(client, op), nil

		// TODO: refactor
	case `t`:
		transactionDoc, err := NewTxnOp(opdata)
		if err != nil {
			return nil, err
		}
		return NewTxnExec(client, transactionDoc), nil
	case `sleep`:
		sleepDoc, err := NewSleepOp(opdata)
		if err != nil {
			return nil, err
		}
		return NewSleepExec(client, sleepDoc), nil
	case `abort`:
		abortDoc, err := NewAbortOp(opdata)
		if err != nil {
			return nil, err
		}
		return NewAbortExec(client, abortDoc), nil
	default:
		return nil, fmt.Errorf("unknown command: %v", opdata.OP)
	}
}

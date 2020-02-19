package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type CommandOp struct {
	DB       string `json:"db"`
	ID       int    `json:"id"`
	callback func(ctx context.Context, client *mongo.Client) OpInfo
}

func NewCommandOp(rawOp RawMongoOp) (*CommandOp, error) {
	op := CommandOp{
		DB: rawOp.DB,
		ID: rawOp.ID,
	}
	var command bson.D
	if err := bson.UnmarshalExtJSON(rawOp.Cmd, true, &command); err != nil {
		return nil, err
	}

	op.callback = func(ctx context.Context, client *mongo.Client) OpInfo {
		var result bson.M
		tm := time.Now()
		err := client.Database(op.DB).RunCommand(ctx, command).Decode(&result)
		info := NewOpInfo(command[0].Key, op.ID, tm, time.Now(), err)
		info.command = command
		info.res = result
		return info
	}
	return &op, nil
}

func NewOpExec(client *mongo.Client, op *CommandOp) ExecFunc {
	return func(ctx context.Context) OpInfo {
		return op.callback(ctx, client)
	}
}

type TxnOp struct {
	Cmds     []RawMongoOp `json:"dc"`
	ID       int          `json:"id"`
	callback func(ctx context.Context, client *mongo.Client) (interface{}, error)
}

func NewTxnOp(rawOp RawMongoOp) (*TxnOp, error) {
	// Unmarshal txn ops array
	var cmds []RawMongoOp
	if err := json.Unmarshal(rawOp.Cmd, &cmds); err != nil {
		return nil, fmt.Errorf("cannot unmarshal txn commands: %+v", err)
	}

	op := TxnOp{
		Cmds: cmds,
		ID:   rawOp.ID,
	}

	// Build exec functions for all transaction's operations
	var fns []func(ctx context.Context, client *mongo.Client) OpInfo
	for _, opCmd := range op.Cmds {
		cmd := opCmd
		command := func(ctx context.Context, client *mongo.Client) OpInfo {
			f, err := NewExecFunc(client, cmd)
			if err != nil {
				return NewOpInfo("transaction", op.ID, time.Time{}, time.Time{}, err)
			}
			opLog := f(ctx)
			return opLog
		}
		fns = append(fns, func(ctx context.Context, client *mongo.Client) OpInfo {
			return command(ctx, client)
		})
	}
	op.callback = func(ctx context.Context, client *mongo.Client) (interface{}, error) {
		var res []OpInfo
		for _, f := range fns {
			res = append(res, f(ctx, client))
		}
		return res, nil
	}
	return &op, nil
}

func NewTxnExec(client *mongo.Client, op *TxnOp) ExecFunc {
	// TODO: clean && refactor
	// TODO: do we need these tmp namespace
	return func(ctx context.Context) OpInfo {
		tm := time.Now()

		session, err := client.StartSession()
		if err != nil {
			return NewOpInfo("transaction", op.ID, tm, time.Now(), err)
		}
		defer session.EndSession(context.Background())
		tres, err := session.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
			return op.callback(sessCtx, client)
		})

		info := NewOpInfo("transaction", op.ID, tm, time.Now(), err)
		info.subcmds = tres.([]OpInfo)
		return info
	}
}

type SleepOp struct {
	DB       string  `json:"db"`
	Coll     string  `json:"cl"`
	ID       int     `json:"id"`
	Duration float32 `json:"time"`
}

func NewSleepOp(rawOp RawMongoOp) (*SleepOp, error) {
	// TODO: refactor, move coll, id, duration to "cd" part and remove marshal-unmarshal

	var res SleepOp
	bmrco, err := json.Marshal(rawOp)
	if err != nil {
		return nil, fmt.Errorf("cannot parse opts for commad: %+v", err)
	}
	if err = json.Unmarshal(bmrco, &res); err != nil {
		return nil, fmt.Errorf("cannot parse opts for commad: %+v", err)
	}
	return &res, nil
}

func NewSleepExec(client *mongo.Client, op *SleepOp) ExecFunc {
	// TODO: refactor
	return func(ctx context.Context) OpInfo {
		sleepCmd := CommandOp{
			DB: op.DB,
			ID: op.ID,
			callback: func(ctx context.Context, client *mongo.Client) OpInfo {
				db := client.Database("sleep_db_temp")
				var result bson.M
				tm := time.Now()
				doc := bson.D{
					primitive.E{Key: "find", Value: "sleep_temp"},
					primitive.E{Key: "filter", Value: bson.D{
						primitive.E{Key: "$where", Value: fmt.Sprintf("sleep(%f)", op.Duration)}}},
				}
				err := db.RunCommand(ctx, doc).Decode(&result)
				info := NewOpInfo(doc[0].Key, op.ID, tm, time.Now(), err)
				info.command = doc
				info.res = result
				return info
			},
		}
		opLog := sleepCmd.callback(ctx, client)
		opLog.opName = "sleep"
		return opLog
	}
}

type AbortOp struct {
	ID int `json:"id"`
}

func NewAbortOp(cd RawMongoOp) (*AbortOp, error) {
	return &AbortOp{cd.ID}, nil
}

func NewAbortExec(_ *mongo.Client, op *AbortOp) ExecFunc {
	return func(ctx context.Context) OpInfo {
		t := time.Time{}
		sessCtx, ok := ctx.(mongo.SessionContext)
		if !ok {
			return OpInfo{err: fmt.Errorf("expected sessioinContext instance")}
		}

		err := sessCtx.AbortTransaction(sessCtx)
		return NewOpInfo("abort", op.ID, t, time.Time{}, err)
	}
}

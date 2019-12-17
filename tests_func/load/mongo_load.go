package mongoload

import (
	"context"
	"encoding/json"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"io"
	"sync"
)

type RawMongoOp = map[string]json.RawMessage

func ReadRawMongoOps(ctx context.Context, reader io.Reader, mxCmdsSize int) (<-chan RawMongoOp, <-chan error, error) {
	cmds := make(chan RawMongoOp, mxCmdsSize)
	errc := make(chan error, 1)
	dec := json.NewDecoder(reader)

	t, err := dec.Token()
	if err != nil {
		return nil, nil, fmt.Errorf("cannot parse patron json: %v", err)
	}

	if t != json.Delim('[') {
		return nil, nil, fmt.Errorf("expected the begging of the array of commands")
	}

	go func() {

		defer close(cmds)
		defer close(errc)

		for dec.More() {
			var cmd map[string]json.RawMessage
			err := dec.Decode(&cmd)
			if err != nil {
				errc <- fmt.Errorf("cannot parse command from patron: %v", err)
				return
			}
			select {
			case cmds <- cmd:
			case <-ctx.Done():
				return
			}
		}

		t, err = dec.Token()
		if err != nil {
			errc <- fmt.Errorf("expected the end of the array of commands")
			return
		}
	}()

	return cmds, errc, nil
}

func MakeMongoOps(ctx context.Context, cli *mongo.Client, roc <-chan RawMongoOp) (<-chan func(ctx context.Context) string, <-chan error) {
	cmds := make(chan func(ctx context.Context) string, cap(roc))
	errc := make(chan error, 1)

	go func() {

		defer close(cmds)
		defer close(errc)

		for cmd := range roc {
			opCmd, err := makeMongoOp(cli, cmd)
			if err != nil {
				errc <- fmt.Errorf("cannot parse command from patron: %v", err)
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

func makeMongoOp(client *mongo.Client, opdata map[string]json.RawMessage) (func(ctx context.Context) string, error) {
	switch string(opdata["op"]) {
	case `"c"`:
		commandDoc, err := parseCommandOp(opdata)
		if err != nil {
			return nil, err
		}
		return makeFuncCommand(client, commandDoc), nil
	case `"t"`:
		transactionDoc, err := parseTransactionOp(opdata)
		if err != nil {
			return nil, err
		}
		return makeFuncTransaction(client, transactionDoc), nil
	default:
		return nil, fmt.Errorf("unknown command %v", string(opdata["op"]))
	}
}

type MongoRunCommandOp struct {
	DbName string `json:"db"`
	Id     int    `json:"id"`
	Doc    bson.D
}

func parseCommandOp(cd map[string]json.RawMessage) (*MongoRunCommandOp, error) {
	var res MongoRunCommandOp
	bmrco, err := json.Marshal(cd)
	if err != nil {
		return nil, fmt.Errorf("cannot parse opts for commad: %+v", err)
	}
	err = json.Unmarshal(bmrco, &res)
	if err != nil {
		return nil, fmt.Errorf("cannot parse opts for commad: %+v", err)
	}
	err = bson.UnmarshalExtJSON(cd["dc"], true, &res.Doc)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func mongoRunCommandOp(ctx context.Context, client *mongo.Client, op *MongoRunCommandOp) string {
	db := client.Database(op.DbName)
	var result bson.M
	err := db.RunCommand(ctx, op.Doc).Decode(&result)
	if err != nil {
		return fmt.Sprintf("Failed execution of runCommand with id %d: %+v", op.Id, err)
	} else {
		return fmt.Sprintf("Successful execution of runCommand with id %d", op.Id)
	}
}

func makeFuncCommand(client *mongo.Client, op *MongoRunCommandOp) func(ctx context.Context) string {
	return func(ctx context.Context) string {
		return mongoRunCommandOp(ctx, client, op)
	}
}

type MongoRunTransactionOp struct {
	Cmds     []map[string]json.RawMessage `json:"cmds"`
	Id       int                          `json:"id"`
	callback func(ctx context.Context, client *mongo.Client) (interface{}, error)
}

func parseTransactionOp(cd map[string]json.RawMessage) (*MongoRunTransactionOp, error) {
	var res MongoRunTransactionOp
	bmrco, err := json.Marshal(cd)
	if err != nil {
		return nil, fmt.Errorf("cannot parse opts for commad: %+v", err)
	}
	err = json.Unmarshal(bmrco, &res)
	if err != nil {
		return nil, fmt.Errorf("cannot parse opts for commad: %+v", err)
	}
	var fns []func(ctx context.Context, client *mongo.Client) string
	for _, cmd := range res.Cmds {
		commandDoc, err := parseCommandOp(cmd)
		if err != nil {
			return nil, err
		}
		fns = append(fns, func(ctx context.Context, client *mongo.Client) string {
			return mongoRunCommandOp(ctx, client, commandDoc)
		})
	}
	res.callback = func(ctx context.Context, client *mongo.Client) (interface{}, error) {
		var res string
		for _, f := range fns {
			res = res + "\n\t|\t" + f(ctx, client)
		}
		return res, nil
	}
	return &res, nil
}

func makeFuncTransaction(client *mongo.Client, op *MongoRunTransactionOp) func(ctx context.Context) string {
	return func(ctx context.Context) string {
		session, err := client.StartSession()
		if err != nil {
			return fmt.Sprintf("Cannot start session: %+v", err)
		}
		defer session.EndSession(context.Background())
		tres, err := session.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
			return op.callback(ctx, client)
		})
		if err != nil {
			return fmt.Sprintf("Failed execution of transaction with id %d: %+v", op.Id, err)
		} else {
			return fmt.Sprintf("Successful execution of transaction with id %d%s", op.Id, tres)
		}
	}
}

func RunMongoOpFuncs(ctx context.Context, fc <-chan func(ctx context.Context) string, mxWorkCnt, resSize int) <-chan string {
	resc := make(chan string, resSize)
	wg := sync.WaitGroup{}

	wg.Add(mxWorkCnt)
	for i := 0; i < mxWorkCnt; i++ {
		go func() {
			defer wg.Done()
			for f := range fc {
				select {
				case resc <- f(ctx):
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resc)
	}()

	return resc
}

func PrintMongoOpRes(ctx context.Context, resc <-chan string) {
	go func() {
		for res := range resc {
			fmt.Println(res)
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()
}

func WaitForPipeline(errs ...<-chan error) error {
	errc := MergeErrors(errs...)
	for err := range errc {
		if err != nil {
			return err
		}
	}
	return nil
}

func MergeErrors(cs ...<-chan error) <-chan error {
	var wg sync.WaitGroup
	out := make(chan error, len(cs))
	output := func(c <-chan error) {
		for n := range c {
			out <- n
		}
		wg.Done()
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

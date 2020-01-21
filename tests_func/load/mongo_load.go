package mongoload

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/olekukonko/tablewriter"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type RawMongoOp = map[string]json.RawMessage

type MongoOpInfo struct {
	command   bson.D
	id        int
	opName    string
	status    bool
	err       error
	timeStart time.Time
	timeEnd   time.Time
	subcmds   []MongoOpInfo
	res       bson.M
}

func NewMongoOpInfo(opName string, id int, timeStart time.Time, timeEnd time.Time, err error) MongoOpInfo {
	return MongoOpInfo{
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
			if err := dec.Decode(&cmd); err != nil {
				errc <- fmt.Errorf("cannot parse command from patron: %v", err)
				return
			}
			select {
			case cmds <- cmd:
			case <-ctx.Done():
				return
			}
		}

		if _, err = dec.Token(); err != nil {
			errc <- fmt.Errorf("expected the end of the array of commands")
			return
		}
	}()

	return cmds, errc, nil
}

func MakeMongoOps(ctx context.Context, cli *mongo.Client, roc <-chan RawMongoOp) (<-chan func(ctx context.Context) MongoOpInfo, <-chan error) {
	cmds := make(chan func(ctx context.Context) MongoOpInfo, cap(roc))
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

func makeMongoOp(client *mongo.Client, opdata map[string]json.RawMessage) (func(ctx context.Context) MongoOpInfo, error) {
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
	case `"sleep"`:
		sleepDoc, err := parseSleepOp(opdata)
		if err != nil {
			return nil, err
		}
		return makeFuncSleep(client, sleepDoc), nil
	case `"abort"`:
		abortDoc, err := parseAbortOp(opdata)
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context) MongoOpInfo {
			t := time.Time{}
			sessCtx, ok := ctx.(mongo.SessionContext)
			if !ok {
				return MongoOpInfo{err: fmt.Errorf("expeted sessioinContext instance")}
			}

			err := sessCtx.AbortTransaction(sessCtx)
			return NewMongoOpInfo("abort", abortDoc.Id, t, time.Time{}, err)
		}, nil
	default:
		return nil, fmt.Errorf("unknown command %v", string(opdata["op"]))
	}
}

type MongoRunCommandOp struct {
	DbName   string `json:"db"`
	Id       int    `json:"id"`
	callback func(ctx context.Context, client *mongo.Client) MongoOpInfo
}

func parseCommandOp(cd map[string]json.RawMessage) (*MongoRunCommandOp, error) {
	var res MongoRunCommandOp
	bmrco, err := json.Marshal(cd)
	if err != nil {
		return nil, fmt.Errorf("cannot parse opts for commad: %+v", err)
	}
	if err = json.Unmarshal(bmrco, &res); err != nil {
		return nil, fmt.Errorf("cannot parse opts for commad: %+v", err)
	}
	var doc bson.D
	if err = bson.UnmarshalExtJSON(cd["dc"], true, &doc); err != nil {
		return nil, err
	}
	res.callback = func(ctx context.Context, client *mongo.Client) MongoOpInfo {
		db := client.Database(res.DbName)
		var result bson.M
		tm := time.Now()
		err := db.RunCommand(ctx, doc).Decode(&result)
		info := NewMongoOpInfo(doc[0].Key, res.Id, tm, time.Now(), err)
		info.command = doc
		info.res = result
		return info
	}
	return &res, nil
}

func mongoRunCommandOp(ctx context.Context, client *mongo.Client, op *MongoRunCommandOp) MongoOpInfo {
	return op.callback(ctx, client)
}

func makeFuncCommand(client *mongo.Client, op *MongoRunCommandOp) func(ctx context.Context) MongoOpInfo {
	return func(ctx context.Context) MongoOpInfo {
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
	if err = json.Unmarshal(bmrco, &res); err != nil {
		return nil, fmt.Errorf("cannot parse opts for commad: %+v", err)
	}
	var fns []func(ctx context.Context, client *mongo.Client) MongoOpInfo
	for _, cmd := range res.Cmds {
		curCmd := cmd
		command := func(ctx context.Context, client *mongo.Client) MongoOpInfo {
			f, err := makeMongoOp(client, curCmd)
			if err != nil {
				return NewMongoOpInfo("transaction", res.Id, time.Time{}, time.Time{}, err)
			}
			opLog := f(ctx)
			return opLog
		}
		fns = append(fns, func(ctx context.Context, client *mongo.Client) MongoOpInfo {
			return command(ctx, client)
		})
	}
	res.callback = func(ctx context.Context, client *mongo.Client) (interface{}, error) {
		var res []MongoOpInfo
		for _, f := range fns {
			res = append(res, f(ctx, client))
		}
		return res, nil
	}
	return &res, nil
}

func makeFuncTransaction(client *mongo.Client, op *MongoRunTransactionOp) func(ctx context.Context) MongoOpInfo {
	return func(ctx context.Context) MongoOpInfo {
		tm := time.Now()
		var tempres bson.D
		doc := bson.D{
			primitive.E{Key: "insert", Value: "sleep_temp"},
			primitive.E{Key: "documents", Value: bson.A{bson.D{primitive.E{Key: "aa", Value: "b"}}}}}
		db := client.Database("sleep_db_temp")
		if err := db.RunCommand(ctx, doc).Decode(&tempres); err != nil {
			return NewMongoOpInfo("transaction", op.Id, time.Time{}, time.Time{}, fmt.Errorf("cannot start transaction because temp collection cannot be created: %+v", err))
		}

		session, err := client.StartSession()
		if err != nil {
			return NewMongoOpInfo("transaction", op.Id, tm, time.Now(), err)
		}
		defer session.EndSession(context.Background())
		tres, err := session.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
			return op.callback(sessCtx, client)
		})

		doc = bson.D{
			primitive.E{Key: "delete", Value: "sleep_temp"},
			primitive.E{Key: "deletes", Value: bson.A{bson.D{
				primitive.E{Key: "q", Value: bson.D{primitive.E{Key: "aa", Value: "b"}}},
				primitive.E{Key: "limit", Value: 1}}}}}
		_ = db.RunCommand(ctx, doc).Decode(&tempres)

		info := NewMongoOpInfo("transaction", op.Id, tm, time.Now(), err)
		info.subcmds = tres.([]MongoOpInfo)
		return info
	}
}

type MongoRunSleepOp struct {
	DbName  string  `json:"db"`
	ColName string  `json:"cl"`
	Id      int     `json:"id"`
	Dur     float32 `json:"time"`
}

func parseSleepOp(cd map[string]json.RawMessage) (*MongoRunSleepOp, error) {
	var res MongoRunSleepOp
	bmrco, err := json.Marshal(cd)
	if err != nil {
		return nil, fmt.Errorf("cannot parse opts for commad: %+v", err)
	}
	if err = json.Unmarshal(bmrco, &res); err != nil {
		return nil, fmt.Errorf("cannot parse opts for commad: %+v", err)
	}
	return &res, nil
}

func makeFuncSleep(client *mongo.Client, op *MongoRunSleepOp) func(ctx context.Context) MongoOpInfo {
	return func(ctx context.Context) MongoOpInfo {
		sleepCmd := MongoRunCommandOp{
			DbName: op.DbName,
			Id:     op.Id,
			callback: func(ctx context.Context, client *mongo.Client) MongoOpInfo {
				db := client.Database("sleep_db_temp")
				var result bson.M
				tm := time.Now()
				doc := bson.D{
					primitive.E{Key: "find", Value: "sleep_temp"},
					primitive.E{Key: "filter", Value: bson.D{
						primitive.E{Key: "$where", Value: fmt.Sprintf("sleep(%f)", op.Dur)}}},
				}
				err := db.RunCommand(ctx, doc).Decode(&result)
				info := NewMongoOpInfo(doc[0].Key, op.Id, tm, time.Now(), err)
				info.command = doc
				info.res = result
				return info
			},
		}
		opLog := mongoRunCommandOp(ctx, client, &sleepCmd)
		opLog.opName = "sleep"
		return opLog
	}
}

type MongoRunAbortOp struct {
	Id int `json:"id"`
}

func parseAbortOp(cd map[string]json.RawMessage) (*MongoRunAbortOp, error) {
	var res MongoRunAbortOp
	bmrco, err := json.Marshal(cd)
	if err != nil {
		return nil, fmt.Errorf("cannot parse opts for commad: %+v", err)
	}
	if err = json.Unmarshal(bmrco, &res); err != nil {
		return nil, fmt.Errorf("cannot parse opts for commad: %+v", err)
	}
	return &res, nil
}

func RunMongoOpFuncs(ctx context.Context, fc <-chan func(ctx context.Context) MongoOpInfo, mxWorkCnt, resSize int) <-chan MongoOpInfo {
	resc := make(chan MongoOpInfo, resSize)
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

type MongoOpsStat struct {
	Succ       map[string]int
	Fail       map[string]int
	InsDocsCnt int
	UpdDocsCnt int
	DelDocsCnt int
}

type MongoOpStat struct {
	CmdStat MongoOpsStat
	TxnStat MongoOpsStat
}

func updateStatWithMongoOpLog(stat *MongoOpsStat, log MongoOpInfo) {
	if stat.Succ == nil {
		stat.Succ = make(map[string]int)
	}
	if stat.Fail == nil {
		stat.Fail = make(map[string]int)
	}
	if log.status {
		stat.Succ[log.opName]++
	} else {
		stat.Fail[log.opName]++
		return
	}
	var docs int
	if log.res != nil && log.res["n"] != nil {
		docs = int(log.res["n"].(int32))
	}
	if log.opName == "insert" {
		stat.InsDocsCnt += docs
	} else if log.opName == "update" {
		stat.UpdDocsCnt += docs
	} else if log.opName == "delete" {
		stat.DelDocsCnt += docs
	}
}

func CollectStat(ctx context.Context, logs <-chan MongoOpInfo) MongoOpStat {
	var stat MongoOpStat
	for log := range logs {
		updateStatWithMongoOpLog(&stat.CmdStat, log)
		if log.opName == "transaction" {
			for _, cmdLog := range log.subcmds {
				updateStatWithMongoOpLog(&stat.TxnStat, cmdLog)
			}
		}
		select {
		case <-ctx.Done():
			return stat
		default:
		}
	}
	return stat
}

func mergeKeys(mp1 map[string]int, mp2 map[string]int) []string {
	var res []string
	for k, _ := range mp1 {
		if _, ok := mp2[k]; !ok {
			res = append(res, k)
		}
	}
	for k, _ := range mp2 {
		if _, ok := mp1[k]; !ok {
			res = append(res, k)
		}
	}
	for k, _ := range mp1 {
		if _, ok := mp2[k]; ok {
			res = append(res, k)
		}
	}
	return res
}

func PrintStat(stat MongoOpStat, writer io.Writer, format string) error {
	if format == "table" {
		var data [][]string
		var cmds []string
		cmdTable := tablewriter.NewWriter(writer)
		_, err := fmt.Fprintln(writer, "Command stat")
		if err != nil {
			return err
		}
		cmdTable.SetHeader([]string{"Txn", "Command", "Total", "Successful", "Failed"})
		cmds = mergeKeys(stat.CmdStat.Succ, stat.CmdStat.Fail)
		for _, cmd := range cmds {
			data = append(data, []string{"-", cmd, strconv.Itoa(stat.CmdStat.Succ[cmd] + stat.CmdStat.Fail[cmd]),
				strconv.Itoa(stat.CmdStat.Succ[cmd]), strconv.Itoa(stat.CmdStat.Fail[cmd])})
		}
		cmds = mergeKeys(stat.TxnStat.Succ, stat.TxnStat.Fail)
		for _, cmd := range cmds {
			data = append(data, []string{"+", cmd, strconv.Itoa(stat.TxnStat.Succ[cmd] + stat.TxnStat.Fail[cmd]),
				strconv.Itoa(stat.TxnStat.Succ[cmd]), strconv.Itoa(stat.TxnStat.Fail[cmd])})
		}
		cmdTable.AppendBulk(data)
		cmdTable.Render()
		_, err = fmt.Fprintln(writer, "Documents stat")
		if err != nil {
			return err
		}
		docTable := tablewriter.NewWriter(writer)
		docTable.SetHeader([]string{"Txn", "inserted", "updated", "deleted"})
		docTable.Append([]string{"-", strconv.Itoa(stat.CmdStat.InsDocsCnt), strconv.Itoa(stat.CmdStat.UpdDocsCnt),
			strconv.Itoa(stat.CmdStat.DelDocsCnt)})
		docTable.Append([]string{"+", strconv.Itoa(stat.TxnStat.InsDocsCnt), strconv.Itoa(stat.TxnStat.UpdDocsCnt),
			strconv.Itoa(stat.TxnStat.DelDocsCnt)})
		docTable.Render()
	} else if format == "json" {
		bstat, err := json.MarshalIndent(stat, "", "    ")
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(writer, string(bstat))
		if err != nil {
			return err
		}
	}
	return fmt.Errorf("no such format for printing mongo load operations stat")
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

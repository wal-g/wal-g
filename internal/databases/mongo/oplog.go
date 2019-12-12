package mongo

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/wal-g/wal-g/internal"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// TODO: unit tests

const OplogTimestampDelimiter = "."

var OplogTimestampRegexp = fmt.Sprintf(`[0-9]+\%s[0-9]+`, OplogTimestampDelimiter)

type OplogTimestamp struct {
	TS  uint32
	Inc uint32
}

func (ots OplogTimestamp) String() string {
	return fmt.Sprintf("%d%s%d", ots.TS, OplogTimestampDelimiter, ots.Inc)
}

func (ots OplogTimestamp) Regexp() string {
	return fmt.Sprintf("[0-9]+%s[0-9]+", OplogTimestampDelimiter)
}

func OplogTimestampFromStr(s string) (OplogTimestamp, error) {
	strs := strings.Split(s, OplogTimestampDelimiter)
	if len(strs) != 2 {
		return OplogTimestamp{}, fmt.Errorf("can not split oplog ts string '%s': two parts expected", s)
	}

	ts, err := strconv.ParseUint(strs[0], 10, 32)
	if err != nil {
		return OplogTimestamp{}, fmt.Errorf("can not convert ts string '%v': %w", ts, err)
	}
	inc, err := strconv.ParseUint(strs[1], 10, 32)
	if err != nil {
		return OplogTimestamp{}, fmt.Errorf("can not convert inc string '%v': %w", inc, err)
	}

	return OplogTimestamp{TS: uint32(ts), Inc: uint32(inc)}, nil
}

func OplogTimestampFromBson(bts primitive.Timestamp) OplogTimestamp {
	return OplogTimestamp{TS: bts.T, Inc: bts.I}
}

func BsonTimestampFromOplogTS(ots OplogTimestamp) primitive.Timestamp {
	return primitive.Timestamp{T: ots.TS, I: ots.Inc}
}

type OplogRecord struct {
	TS   OplogTimestamp
	OP   string
	NS   string
	Data []byte
	Err  error
}

type OplogMeta struct {
	TS primitive.Timestamp `bson:"ts"`
	NS string              `bson:"ns"`
	Op string              `bson:"op"`
}

type OplogFetcher interface {
	GetOplogFrom(context.Context, OplogTimestamp) (chan OplogRecord, error)
}

type DBOplogFetcher struct {
	uri string
	mc  *internal.MongoClient
	wg  *sync.WaitGroup
}

func NewOplogFetcherDB(uri string, wg *sync.WaitGroup) *DBOplogFetcher {
	return &DBOplogFetcher{uri: uri, wg: wg}
}

func (db *DBOplogFetcher) GetOplogFrom(ctx context.Context, fromTS OplogTimestamp) (chan OplogRecord, error) {
	// TODO: handle disconnects && stepdown
	// TODO: use sessions
	mc, err := internal.NewMongoClient(ctx, db.uri)
	if err != nil {
		return nil, err
	}

	coll, err := mc.GetOplogCollection(ctx)
	if err != nil {
		return nil, err
	}

	ch := make(chan OplogRecord)
	db.wg.Add(1)
	go func(ctx context.Context, mc *internal.MongoClient, ch chan OplogRecord, wg *sync.WaitGroup) {
		defer wg.Done()
		defer close(ch)
		defer mc.Close(ctx)

		bsonTS := BsonTimestampFromOplogTS(fromTS)
		filter := bson.M{"ts": bson.M{"$gte": bsonTS}}

		cur, err := coll.Find(ctx, filter, options.Find().SetCursorType(options.TailableAwait))
		if err == nil && cur.ID() == 0 {
			err = fmt.Errorf("dead cursor from oplog find")
		}
		if err != nil {
			ch <- OplogRecord{Err: fmt.Errorf("oplog lookup failed: %w", err)}
			return
		}

		defer func() { _ = cur.Close(ctx) }()
		for cur.Next(ctx) {
			// TODO: benchmark decode vs. bson.Reader vs. bson.Raw.LookupErr
			opMeta := OplogMeta{}
			if err := cur.Decode(&opMeta); err != nil {
				ch <- OplogRecord{Err: fmt.Errorf("oplog record decoding failed: %w", err)}
				return
			}
			ch <- OplogRecord{
				TS:   OplogTimestampFromBson(opMeta.TS),
				OP:   opMeta.Op,
				NS:   opMeta.NS,
				Data: cur.Current,
			}
		}

		if err := cur.Err(); err != nil {
			if err == ctx.Err() {
				return
			}
			ch <- OplogRecord{Err: fmt.Errorf("oplog cursor error: %w", err)}
			return
		}
		ch <- OplogRecord{Err: fmt.Errorf("oplog cursor exhausted")}

	}(ctx, mc, ch, db.wg)

	return ch, nil
}

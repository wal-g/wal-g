package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/mongodb/mongo-tools-common/db"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/utility"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	_ = []MongoDriver{&MongoClient{}}
	_ = []OplogCursor{&MongoOplogCursor{}}
	_ = []OplogCursor{&MongoOplogCursor{}, &BsonCursor{}}
)

const (
	driverAppName       = "wal-g-mongo"
	oplogDatabaseName   = "local"
	oplogCollectionName = "oplog.rs"
)

type OplogAppMode string

const (
	OplogAppModeInitSync   OplogAppMode = "InitialSync"
	OplogAppModeRecovering OplogAppMode = "Recovering"
	OplogAppModeSecondary  OplogAppMode = "Secondary"
)

var (
	OplogAppModes = map[OplogAppMode]struct{}{
		OplogAppModeInitSync:   {},
		OplogAppModeRecovering: {},
		OplogAppModeSecondary:  {},
	}
)

// CmdResponse is used to unmarshal mongodb cmd responses
type CmdResponse struct {
	Ok     int    `bson:"ok"`
	ErrMsg string `bson:"errmsg, omitempty"`
}

// Optime ...
type OpTime struct {
	TS   primitive.Timestamp `bson:"ts" json:"ts"`
	Term int64               `bson:"t" json:"t"`
}

// IsMasterLastWrite ...
type IsMasterLastWrite struct {
	OpTime         OpTime `bson:"opTime"`
	MajorityOpTime OpTime `bson:"majorityOpTime"`
}

// IsMaster is used to unmarshal results of IsMaster command
type IsMaster struct {
	IsMaster  bool              `bson:"ismaster"`
	LastWrite IsMasterLastWrite `bson:"lastWrite"`
}

// MongoDriver defines methods to work with mongodb.
type MongoDriver interface {
	CreateIndexes(ctx context.Context, dbName, collName string, indexes []IndexDocument) error
	DropIndexes(ctx context.Context, dbName string, rawCommand bson.D) error
	EnsureIsMaster(ctx context.Context) error
	IsMaster(ctx context.Context) (models.IsMaster, error)
	LastWriteTS(ctx context.Context) (lastTS, lastMajTS models.Timestamp, err error)
	TailOplogFrom(ctx context.Context, from models.Timestamp) (OplogCursor, error)
	ApplyOp(ctx context.Context, op db.Oplog) error
	Close(ctx context.Context) error
}

// OplogCursor defines methods to work with mongodb cursor.
type OplogCursor interface {
	Close(ctx context.Context) error
	Data() []byte
	Err() error
	Next(context.Context) bool
	Push([]byte) error
}

// MongoOplogCursor implements OplogCursor.
type MongoOplogCursor struct {
	*mongo.Cursor
	pushed []byte
}

// NewMongoOplogCursor builds MongoOplogCursor.
func NewMongoOplogCursor(c *mongo.Cursor) *MongoOplogCursor {
	return &MongoOplogCursor{c, nil}
}

// Data returns current cursor document
func (m *MongoOplogCursor) Data() []byte {
	return m.Current
}

// Push returns document back to cursor
func (m *MongoOplogCursor) Push(data []byte) error {
	if m.pushed != nil {
		return fmt.Errorf("cursor already has one unread pushed document")
	}
	m.pushed = data
	return nil
}

// Next fills Current by next document, returns true if there were no errors and the cursor has not been exhausted.
func (m *MongoOplogCursor) Next(ctx context.Context) bool {
	if m.pushed != nil {
		m.Current = m.pushed
		m.pushed = nil
		return true
	}
	return m.Cursor.Next(ctx)
}

// ApplyOplog is used to replay oplog entry.
type ApplyOplog struct {
	Operation  string            `bson:"op"`
	Namespace  string            `bson:"ns"`
	Object     bson.D            `bson:"o"`
	Query      bson.D            `bson:"o2,omitempty"`
	UI         *primitive.Binary `bson:"ui,omitempty"`
	LSID       bson.Raw          `bson:"lsid,omitempty"`
	TxnNumber  *int64            `bson:"txnNumber,omitempty"`
	PrevOpTime bson.Raw          `bson:"prevOpTime,omitempty"`
}

// MongoClient implements MongoDriver
type MongoClient struct {
	c           *mongo.Client
	applyOpsCmd bson.D
}

// Options defines mongo client options
type Options struct {
	OplogApplicationMode *OplogAppMode
	OplogAlwaysUpsert    *bool
}

type Option func(*Options)

// OplogAlwaysUpsert sets applyOps argument oplogApplicationMode
func OplogApplicationMode(mode OplogAppMode) Option {
	return func(args *Options) {
		args.OplogApplicationMode = &mode
	}
}

// OplogAlwaysUpsert sets applyOps argument alwaysUpsert
func OplogAlwaysUpsert(alwaysUpsert bool) Option {
	return func(args *Options) {
		args.OplogAlwaysUpsert = &alwaysUpsert
	}
}

// NewMongoClient builds MongoClient
func NewMongoClient(ctx context.Context, uri string, setters ...Option) (*MongoClient, error) {
	// Default Options
	args := &Options{}
	for _, setter := range setters {
		setter(args)
	}

	applyOpsCmd := bson.D{
		{Key: "applyOps"},
	}
	if args.OplogApplicationMode != nil {
		oplogApplicationMode := *args.OplogApplicationMode
		if _, ok := OplogAppModes[oplogApplicationMode]; !ok {
			return nil, fmt.Errorf("unsupported oplogApplicationMode: %s", oplogApplicationMode)
		}
		applyOpsCmd = append(applyOpsCmd, bson.E{Key: "oplogApplicationMode", Value: oplogApplicationMode})
	}
	if args.OplogAlwaysUpsert != nil {
		applyOpsCmd = append(applyOpsCmd, bson.E{Key: "alwaysUpsert", Value: *args.OplogAlwaysUpsert})
	}

	client, err := mongo.Connect(ctx,
		options.Client().ApplyURI(uri).
			SetAppName(driverAppName).
			SetDirect(true).
			SetRetryReads(false))
	if err != nil {
		return nil, err
	}

	return &MongoClient{
		c:           client,
		applyOpsCmd: applyOpsCmd,
	}, client.Ping(ctx, nil)
}

// IndexDocument holds information about a collection's index.
type IndexDocument struct {
	Options                 bson.M `bson:",inline"`
	Key                     bson.D `bson:"key"`
	PartialFilterExpression bson.D `bson:"partialFilterExpression,omitempty"`
}

func (mc *MongoClient) CreateIndexes(ctx context.Context, dbName, collName string, indexes []IndexDocument) error {
	rawCommand := bson.D{
		{Key: "createIndexes", Value: collName},
		{Key: "indexes", Value: indexes},
	}

	if err := mc.c.Database(dbName).RunCommand(ctx, rawCommand).Err(); err != nil {
		return fmt.Errorf("createIndexes command %q failed: %w", rawCommand, err)
	}

	return nil
}

func (mc *MongoClient) DropIndexes(ctx context.Context, dbName string, rawCommand bson.D) error {
	if err := mc.c.Database(dbName).RunCommand(ctx, rawCommand).Err(); err != nil {
		var mongoErr mongo.CommandError
		isMongoErr := errors.As(err, &mongoErr)

		if isMongoErr && mongoErr.Name == "BackgroundOperationInProgressForNamespace" {
			// In Mongo versions Prior to 5.2, an attempt to drop an index during an in-progress build of another index
			// on the same collection results in an error:
			// https://www.mongodb.com/docs/manual/reference/command/dropIndexes/#behavior

			// We just ignore these error and continue a replay
			tracelog.WarningLogger.Printf("Unable to drop index, skipped. Error is: %w\n", err)
			return nil
		}

		return fmt.Errorf("dropIndexes command %q failed: %w", rawCommand, err)
	}

	return nil
}

func (mc *MongoClient) EnsureIsMaster(ctx context.Context) error {
	im, err := mc.IsMaster(ctx)
	if err != nil {
		return err
	}

	if !im.IsMaster {
		return fmt.Errorf("current node is not a primary")
	}
	return nil
}

func (mc *MongoClient) IsMaster(ctx context.Context) (models.IsMaster, error) {
	im := IsMaster{}
	err := mc.c.Database("test").RunCommand(ctx, bson.D{{Key: "isMaster", Value: 1}}).Decode(&im)
	if err != nil {
		return models.IsMaster{}, fmt.Errorf("isMaster command failed: %w", err)
	}

	return models.IsMaster{
		IsMaster: im.IsMaster,
		LastWrite: models.IsMasterLastWrite{
			OpTime: models.OpTime{
				TS: models.TimestampFromBson(im.LastWrite.OpTime.TS),
			},
			MajorityOpTime: models.OpTime{
				TS: models.TimestampFromBson(im.LastWrite.MajorityOpTime.TS),
			},
		},
	}, nil
}

// LastWriteTS fetches timestamps with last write
// TODO: support non-replset setups
func (mc *MongoClient) LastWriteTS(ctx context.Context) (lastTS, lastMajTS models.Timestamp, err error) {
	im, err := mc.IsMaster(ctx)
	if err != nil {
		return models.Timestamp{}, models.Timestamp{}, fmt.Errorf("isMaster command failed: %+v", err)
	}
	return im.LastWrite.OpTime.TS, im.LastWrite.MajorityOpTime.TS, nil
}

// Close disconnects from mongodb
func (mc *MongoClient) Close(ctx context.Context) error {
	return mc.c.Disconnect(ctx)
}

// TailOplogFrom gives OplogCursor to tail oplog from
func (mc *MongoClient) TailOplogFrom(ctx context.Context, from models.Timestamp) (OplogCursor, error) {
	coll, err := mc.getOplogCollection(ctx)
	if err != nil {
		return nil, err
	}

	bsonTS := models.BsonTimestampFromOplogTS(from)
	filter := bson.M{"ts": bson.M{"$gte": bsonTS}}
	cur, err := coll.Find(ctx, filter, options.Find().SetCursorType(options.TailableAwait))

	if err == nil && cur.ID() == 0 {
		err = fmt.Errorf("dead cursor from oplog find")
	}
	if err != nil {
		return nil, fmt.Errorf("oplog lookup failed: %w", err)
	}

	return NewMongoOplogCursor(cur), nil
}

func (mc *MongoClient) getOplogCollection(ctx context.Context) (*mongo.Collection, error) {
	odb := mc.c.Database(oplogDatabaseName)
	colls, err := odb.ListCollectionNames(ctx, bson.M{"name": oplogCollectionName})
	if err != nil {
		return nil, fmt.Errorf("can not list collections in 'local' database: %w", err)
	}
	if len(colls) != 1 {
		return nil, fmt.Errorf("collection '%s' was not found in database '%s'",
			oplogCollectionName, oplogDatabaseName)
	}

	return odb.Collection(oplogCollectionName), nil
}

func (mc *MongoClient) getApplyOpsCmd() bson.D {
	return mc.applyOpsCmd
}

// ApplyOp calls applyOps and check response
func (mc *MongoClient) ApplyOp(ctx context.Context, dbop db.Oplog) error {
	// mongod complains if 'ts' or 'history' are passed to applyOps
	op := ApplyOplog{
		Operation:  dbop.Operation,
		Namespace:  dbop.Namespace,
		Object:     dbop.Object,
		Query:      dbop.Query,
		UI:         dbop.UI,
		LSID:       dbop.LSID,
		TxnNumber:  dbop.TxnNumber,
		PrevOpTime: dbop.PrevOpTime,
	}

	// TODO: fix ugly interface after switch to passing pointers
	cmd := mc.getApplyOpsCmd()
	cmd[0] = bson.E{Key: "applyOps", Value: []interface{}{op}}
	apply := mc.c.Database("admin").RunCommand(ctx, cmd)
	if err := apply.Err(); err != nil {
		return err
	}
	resp := CmdResponse{}
	if err := apply.Decode(&resp); err != nil {
		return fmt.Errorf("can not unmarshall command execution response: %+v\ncommand was:%+v", err, cmd)
	}
	if resp.Ok != 1 {
		return fmt.Errorf("command execution failed with: %s\ncommand was: %+v", resp.ErrMsg, cmd)
	}

	return nil
}

// BsonCursor implements OplogCursor with source io.reader
type BsonCursor struct {
	r      io.Reader
	raw    []byte
	pushed []byte
	err    error
}

// NewMongoOplogCursor builds MongoOplogCursor.
func NewBsonCursor(r io.Reader) *BsonCursor {
	return &BsonCursor{r: r}
}

// Close closes this cursor.
func (b *BsonCursor) Close(ctx context.Context) error {
	b.raw = nil
	return nil
}

// Data returns current cursor document.
func (b *BsonCursor) Data() []byte {
	return b.raw
}

// Err returns the last error.
func (b *BsonCursor) Err() error {
	return b.err
}

// Next gets the next document for this cursor, returns true if there were no errors.
func (b *BsonCursor) Next(ctx context.Context) bool {
	if b.err != nil {
		return false
	}
	if b.pushed != nil {
		b.raw = b.pushed
		b.pushed = nil
		return true
	}

	b.raw, b.err = bson.NewFromIOReader(b.r)
	return b.err == nil
}

// Push returns document back to cursor
func (b *BsonCursor) Push(data []byte) error {
	if b.pushed != nil {
		return fmt.Errorf("cursor already has one unread pushed document")
	}
	b.pushed = data
	return nil
}

// WaitForBecomePrimary waits until mongo client connection node becomes primary
func WaitForBecomePrimary(ctx context.Context, mc *MongoClient, checkTimeout time.Duration) error {
	reconnect := time.NewTimer(checkTimeout)
	for {
		select {
		case <-reconnect.C:
			err := mc.EnsureIsMaster(ctx)
			if err == nil {
				return nil
			}
			tracelog.InfoLogger.Printf("Waiting: %v", err)
			utility.ResetTimer(reconnect, checkTimeout)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

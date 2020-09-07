package oplog

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/wal-g/wal-g/internal/databases/mongo/client"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"

	"github.com/mongodb/mongo-tools-common/db"
	"github.com/mongodb/mongo-tools-common/txn"
	"github.com/mongodb/mongo-tools-common/util"
	"github.com/wal-g/tracelog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type TypeAssertionError struct {
	etype string
	key   string
	value interface{}
}

func NewTypeAssertionError(etype, key string, value interface{}) *TypeAssertionError {
	return &TypeAssertionError{etype, key, value}
}

func (e *TypeAssertionError) Error() string {
	return fmt.Sprintf("assertion to %s failed, field %s: %+v", e.etype, e.key, e.value)
}

type OpHandleError struct {
	op  interface{}
	err error
}

func NewOpHandleError(op interface{}, err error) *OpHandleError {
	return &OpHandleError{op, err}
}

func (e *OpHandleError) Error() string {
	return fmt.Sprintf("failed to apply with error: %+v\nop:\n%+v", e.err, e.op)
}

var (
	jsonBegin     = []byte("[\n")
	jsonDelimiter = []byte(",\n")
	jsonEnd       = []byte("\n]\n")

	_ = []Applier{&DBApplier{}, &JSONApplier{}, &BSONApplier{}, &BSONRawApplier{}}
)

// Applier defines interface to apply given oplog records.
type Applier interface {
	Apply(ctx context.Context, opr models.Oplog) error
	Close(ctx context.Context) error
}

// NewWriteApplier builds one of write appliers
func NewWriteApplier(format string, wc io.WriteCloser) (Applier, error) {
	switch format {
	case "json":
		return NewJSONApplier(wc), nil
	case "bson":
		return NewBSONApplier(wc), nil
	case "bson-raw":
		return NewBSONRawApplier(wc), nil
	}

	return nil, fmt.Errorf("wrong write applier format: %s", format)
}

// DBApplier implements Applier interface for mongodb.
type DBApplier struct {
	db                    client.MongoDriver
	txnBuffer             *txn.Buffer
	preserveUUID          bool
	applyIgnoreErrorCodes map[string][]int32
}

// NewDBApplier builds DBApplier with given args.
func NewDBApplier(m client.MongoDriver, preserveUUID bool, ignoreErrCodes map[string][]int32) *DBApplier {
	return &DBApplier{db: m, txnBuffer: txn.NewBuffer(), preserveUUID: preserveUUID, applyIgnoreErrorCodes: ignoreErrCodes}
}

func (ap *DBApplier) Apply(ctx context.Context, opr models.Oplog) error {
	op := db.Oplog{}
	if err := bson.Unmarshal(opr.Data, &op); err != nil {
		return fmt.Errorf("can not unmarshal oplog entry: %w", err)
	}

	if err := ap.shouldSkip(op.Operation, op.Namespace); err != nil {
		tracelog.DebugLogger.Printf("skipping op %+v due to: %+v", op, err)
		return nil
	}

	meta, err := txn.NewMeta(op)
	if err != nil {
		return fmt.Errorf("can not extract op metadata: %w", err)
	}

	if meta.IsTxn() {
		err = ap.handleTxnOp(ctx, meta, op)
	} else {
		err = ap.handleNonTxnOp(ctx, op)
	}

	if err != nil {
		return err
	}

	return nil
}

func (ap *DBApplier) Close(ctx context.Context) error {
	if err := ap.db.Close(ctx); err != nil {
		return err
	}
	if err := ap.txnBuffer.Stop(); err != nil {
		return err
	}
	return nil
}

func (ap *DBApplier) shouldSkip(op, ns string) error {
	if op == "n" {
		return fmt.Errorf("noop op")
	}

	// sharded clusters are not supported yet
	if strings.HasPrefix(ns, "config.") {
		return fmt.Errorf("config database op")
	}

	// temporary skip view creation due to mongodb bug
	if strings.HasSuffix(ns, "system.views") {
		return fmt.Errorf("view op")
	}

	return nil
}

// shouldIgnore checks if error should be ignored
func (ap *DBApplier) shouldIgnore(op string, err error) bool {
	ce, ok := err.(mongo.CommandError)
	if !ok {
		return false
	}

	ignoreErrorCodes, ok := ap.applyIgnoreErrorCodes[op]
	if !ok {
		return false
	}

	for i := range ignoreErrorCodes {
		if ce.Code == ignoreErrorCodes[i] {
			return true
		}
	}
	return false
}

// handleNonTxnOp tries to apply given oplog record.
func (ap *DBApplier) handleNonTxnOp(ctx context.Context, op db.Oplog) error {
	if !ap.preserveUUID {
		var err error
		op, err = filterUUIDs(op)
		if err != nil {
			return fmt.Errorf("can not filter UUIDs from op '%+v', error: %+v", op, err)
		}
	}

	// TODO: wait for index building
	if op.Operation == "c" && op.Object[0].Key == "commitIndexBuild" {
		collName, indexes, err := indexSpecFromCommitIndexBuilds(op)
		if err != nil {
			return NewOpHandleError(op, err)
		}
		dbName, _ := util.SplitNamespace(op.Namespace)
		return ap.db.CreateIndexes(ctx, dbName, collName, indexes)
	}
	if op.Operation == "c" && op.Object[0].Key == "createIndexes" {
		collName, indexes, err := indexSpecsFromCreateIndexes(op)
		if err != nil {
			return NewOpHandleError(op, err)
		}
		dbName, _ := util.SplitNamespace(op.Namespace)
		return ap.db.CreateIndexes(ctx, dbName, collName,
			[]client.IndexDocument{indexes})
	}

	//tracelog.DebugLogger.Printf("applying op: %+v", op)
	if err := ap.db.ApplyOp(ctx, op); err != nil {
		// we ignore some errors (for example 'duplicate key error')
		// TODO: check after TOOLS-2041
		if !ap.shouldIgnore(op.Operation, err) {
			return NewOpHandleError(op, err)
		}
		tracelog.WarningLogger.Printf("apply error is skipped: %+v\nop:\n%+v", err, op)
	}
	return nil
}

func indexSpecsFromCreateIndexes(op db.Oplog) (string, client.IndexDocument, error) {
	index := client.IndexDocument{Options: bson.M{}}
	var collName string
	var elem bson.E
	var ok bool
	for i := range op.Object {
		elem = op.Object[i]
		switch elem.Key {
		case "createIndexes":
			if collName, ok = elem.Value.(string); !ok {
				return "", client.IndexDocument{}, NewTypeAssertionError("string", "createIndexes", elem.Value)
			}
		case "key":
			if index.Key, ok = elem.Value.(bson.D); !ok {
				return "", client.IndexDocument{}, NewTypeAssertionError("bson.D", "key", elem.Value)
			}
		default:
			index.Options[elem.Key] = elem.Value
		}
	}

	return collName, index, nil
}

func indexSpecFromCommitIndexBuilds(op db.Oplog) (string, []client.IndexDocument, error) {
	var collName string
	var ok bool
	var elemE bson.E
	for i := range op.Object {
		elemE = op.Object[i]
		if elemE.Key == "commitIndexBuild" {
			if collName, ok = elemE.Value.(string); !ok {
				return "", nil, NewTypeAssertionError("string", "commitIndexBuild", elemE.Value)
			}
		}
	}

	for i := range op.Object {
		elemE = op.Object[i]
		if elemE.Key == "indexes" {
			indexes, ok := elemE.Value.(bson.A)
			if !ok {
				return "", nil, NewTypeAssertionError("bson.A", "indexes", elemE.Value)
			}

			indexSpecs := make([]client.IndexDocument, len(indexes))
			for i := range indexes {
				indexSpecs[i].Options = bson.M{}
				elements, ok := indexes[i].(bson.D)
				if !ok {
					return "", nil, NewTypeAssertionError("bson.D", fmt.Sprintf("indexes[%d]", i), elemE.Value)
				}
				for i := range elements {
					elemE = elements[i]
					if elemE.Key == "key" {
						if indexSpecs[i].Key, ok = elemE.Value.(bson.D); !ok {
							return "", nil, NewTypeAssertionError("bson.D", "key", elemE.Value)
						}
					} else {
						indexSpecs[i].Options[elemE.Key] = elemE.Value
					}
				}
			}
			return collName, indexSpecs, nil
		}
	}

	return "", nil, fmt.Errorf("can not find indexes key in op.Object: %+v", op)
}

// handleTxnOp handles oplog record with transaction attributes.
// TODO: unit test
func (ap *DBApplier) handleTxnOp(ctx context.Context, meta txn.Meta, op db.Oplog) error {
	if meta.IsAbort() {
		if err := ap.txnBuffer.PurgeTxn(meta); err != nil {
			return fmt.Errorf("can not clean txn buffer after rollback cmd: %w", err)
		}
	}
	if err := ap.txnBuffer.AddOp(meta, op); err != nil {
		return fmt.Errorf("can not append command to txn buffer: %w", err)
	}

	if !meta.IsCommit() {
		return nil
	}

	if err := ap.applyTxn(ctx, meta); err != nil {
		return err
	}

	if err := ap.txnBuffer.PurgeTxn(meta); err != nil {
		return fmt.Errorf("txn buffer failed to purge: %w", err)
	}

	return nil
}

func (ap *DBApplier) applyTxn(ctx context.Context, meta txn.Meta) error {
	opc, errc := ap.txnBuffer.GetTxnStream(meta)
	for {
		select {
		case op, ok := <-opc:
			if !ok {
				return nil
			}
			if err := ap.handleNonTxnOp(ctx, op); err != nil {
				return err
			}
		case err, ok := <-errc:
			if ok {
				return err
			}
		case <-ctx.Done():
			// opc and errc channels will be closed in PurgeTxn or Stop calls
			return nil
		}
	}
}

// JSONApplier implements Applier interface for debugging.
type JSONApplier struct {
	writer  io.WriteCloser
	started bool
}

// NewJSONApplier builds JSONApplier with given args.
func NewJSONApplier(w io.WriteCloser) *JSONApplier {
	return &JSONApplier{writer: w, started: false}
}

func (ap *JSONApplier) Apply(ctx context.Context, opr models.Oplog) error {
	op := db.Oplog{}
	if err := bson.Unmarshal(opr.Data, &op); err != nil {
		return fmt.Errorf("can not unmarshal oplog entry: %w", err)
	}

	jsonData, err := bson.MarshalExtJSON(op, true, true)
	if err != nil {
		return fmt.Errorf("can not convert to json: %w", err)
	}

	if !ap.started {
		if _, err := ap.writer.Write(jsonBegin); err != nil {
			return fmt.Errorf("can not write begin mark: %w", err)
		}
		ap.started = true
	} else if _, err := ap.writer.Write(jsonDelimiter); err != nil {
		return fmt.Errorf("can not write delimiter: %w", err)
	}

	if _, err := ap.writer.Write(jsonData); err != nil {
		return fmt.Errorf("can not write json data: %w", err)
	}

	return nil
}

func (ap *JSONApplier) Close(ctx context.Context) error {
	if ap.started {
		if _, err := ap.writer.Write(jsonEnd); err != nil {
			return fmt.Errorf("can not write end mark: %w", err)
		}
	}
	return ap.writer.Close()
}

// BSONApplier implements Applier interface for debugging.
type BSONApplier struct {
	writer io.WriteCloser
}

// NewBSONApplier builds BSONApplier with given args.
func NewBSONApplier(w io.WriteCloser) *BSONApplier {
	return &BSONApplier{writer: w}
}

func (ap *BSONApplier) Apply(ctx context.Context, opr models.Oplog) error {
	op := db.Oplog{}
	if err := bson.Unmarshal(opr.Data, &op); err != nil {
		return fmt.Errorf("can not unmarshal oplog entry: %w", err)
	}

	bsonBytes, err := bson.Marshal(op)
	if err != nil {
		return fmt.Errorf("can not marshal oplog entry: %w", err)
	}

	if _, err := ap.writer.Write(bsonBytes); err != nil {
		return fmt.Errorf("can not write bson data: %w", err)
	}

	return nil
}

func (ap *BSONApplier) Close(ctx context.Context) error {
	return ap.writer.Close()
}

// BSONRawApplier implements Applier interface for debugging.
type BSONRawApplier struct {
	writer io.WriteCloser
}

// NewBSONRawApplier builds BSONRawApplier with given args.
func NewBSONRawApplier(w io.WriteCloser) *BSONRawApplier {
	return &BSONRawApplier{writer: w}
}

func (ap *BSONRawApplier) Apply(ctx context.Context, opr models.Oplog) error {
	if _, err := ap.writer.Write(opr.Data); err != nil {
		return fmt.Errorf("can not write raw bson data: %w", err)
	}
	return nil
}

func (ap *BSONRawApplier) Close(ctx context.Context) error {
	return ap.writer.Close()
}

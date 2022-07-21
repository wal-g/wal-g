package models

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	timestampDelimiter = "."
	timestampsCount    = 2
	MaxDocumentSize    = 16 * 1024 * 1024
)

// Timestamp represents oplog record uniq id.
type Timestamp struct {
	TS  uint32 `json:"TS"`
	Inc uint32 `json:"Inc"`
}

func (ots Timestamp) ToBsonTS() primitive.Timestamp {
	return primitive.Timestamp{
		T: ots.TS,
		I: ots.Inc,
	}
}

// String returns text representation of Timestamp struct
func (ots Timestamp) String() string {
	return fmt.Sprintf("%d%s%d", ots.TS, timestampDelimiter, ots.Inc)
}

// TimestampFromStr builds Timestamp from string
func TimestampFromStr(s string) (Timestamp, error) {
	strs := strings.Split(s, timestampDelimiter)
	if len(strs) != timestampsCount {
		return Timestamp{}, fmt.Errorf("can not split oplog ts string '%s': two parts expected", s)
	}

	ts, err := strconv.ParseUint(strs[0], 10, 32)
	if err != nil {
		return Timestamp{}, fmt.Errorf("can not convert ts string '%v': %w", ts, err)
	}
	inc, err := strconv.ParseUint(strs[1], 10, 32)
	if err != nil {
		return Timestamp{}, fmt.Errorf("can not convert inc string '%v': %w", inc, err)
	}

	return Timestamp{TS: uint32(ts), Inc: uint32(inc)}, nil
}

// MaxTS returns maximum of two timestamps.
func MaxTS(ots1, ots2 Timestamp) Timestamp {
	if LessTS(ots1, ots2) {
		return ots2
	}
	return ots1
}

// LessTS returns if first timestamp less than second
func LessTS(ots1, ots2 Timestamp) bool {
	if ots1.TS < ots2.TS {
		return true
	}
	if ots1.TS > ots2.TS {
		return false
	}
	return ots1.Inc < ots2.Inc
}

// TimestampFromBson builds Timestamp from BSON primitive
func TimestampFromBson(bts primitive.Timestamp) Timestamp {
	return Timestamp{TS: bts.T, Inc: bts.I}
}

// BsonTimestampFromOplogTS builds BSON primitive from Timestamp
func BsonTimestampFromOplogTS(ots Timestamp) primitive.Timestamp {
	return primitive.Timestamp{T: ots.TS, I: ots.Inc}
}

// Oplog represents oplog raw and parsed metadata.
type Oplog struct {
	TS   Timestamp `bson:"ts"`
	Data []byte
}

var oplogPool = sync.Pool{New: func() interface{} {
	return &Oplog{}
}}

// GetOplogEntry fetches empty Oplog struct from pool
func GetOplogEntry() *Oplog {
	return oplogPool.Get().(*Oplog)
}

// PutOplogEntry zeroes fields of Oplog struct and it to pool
func PutOplogEntry(oplog *Oplog) {
	oplog.TS.TS = 0
	oplog.TS.Inc = 0
	oplog.Data = nil
	oplogPool.Put(oplog)
}

// OplogFromRaw tries to decode bytes to Oplog model
func OplogFromRaw(raw bson.Raw) (*Oplog, error) {
	tsT, tsI, ok := raw.Lookup("ts").TimestampOK()
	if !ok {
		return nil, fmt.Errorf("can not cast oplog 'ts' field to timestamp: %v", raw.Lookup("ts"))
	}

	oplog := GetOplogEntry()
	oplog.TS = Timestamp{TS: tsT, Inc: tsI}
	oplog.Data = raw
	return oplog, nil
}

// Optime ...
type OpTime struct {
	TS   Timestamp
	Term int64
}

// IsMasterLastWrite ...
type IsMasterLastWrite struct {
	OpTime         OpTime
	MajorityOpTime OpTime
}

// IsMaster ...
type IsMaster struct {
	IsMaster  bool
	LastWrite IsMasterLastWrite
}

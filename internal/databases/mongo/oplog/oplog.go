package oplog

import (
	"fmt"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	TimestampDelimiter = "."
	TimestampRegexp    = `[0-9]+\` + TimestampDelimiter + `[0-9]+`
)

// Timestamp represents oplog record uniq id.
type Timestamp struct {
	TS  uint32
	Inc uint32
}

func (ots Timestamp) String() string {
	return fmt.Sprintf("%d%s%d", ots.TS, TimestampDelimiter, ots.Inc)
}

// TODO: unit tests
func TimestampFromStr(s string) (Timestamp, error) {
	strs := strings.Split(s, TimestampDelimiter)
	if len(strs) != 2 {
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

// Returns max of two timestamps.
// TODO: unit tests
func Max(ots1, ots2 Timestamp) Timestamp {
	if ots1.TS > ots2.TS {
		return ots1
	}
	if ots1.TS < ots2.TS {
		return ots2
	}
	if ots1.Inc > ots2.Inc {
		return ots1
	}
	return ots2
}

func TimestampFromBson(bts primitive.Timestamp) Timestamp {
	return Timestamp{TS: bts.T, Inc: bts.I}
}

func BsonTimestampFromOplogTS(ots Timestamp) primitive.Timestamp {
	return primitive.Timestamp{T: ots.TS, I: ots.Inc}
}

// Record represents oplog raw and parsed metadata.
type Record struct {
	TS   Timestamp
	OP   string
	NS   string
	Data []byte
	Err  error
}

// Meta is used to decode raw bson record.
type Meta struct {
	TS primitive.Timestamp `bson:"ts"`
	NS string              `bson:"ns"`
	Op string              `bson:"op"`
}

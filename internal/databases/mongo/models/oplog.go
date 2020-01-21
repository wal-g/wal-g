package models

import (
	"fmt"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	timestampDelimiter = "."
	timestampRegexp    = `[0-9]+\` + timestampDelimiter + `[0-9]+`
)

// Timestamp represents oplog record uniq id.
type Timestamp struct {
	TS  uint32
	Inc uint32
}

// String returns text representation of Timestamp struct
func (ots Timestamp) String() string {
	return fmt.Sprintf("%d%s%d", ots.TS, timestampDelimiter, ots.Inc)
}

// TimestampFromStr builds Timestamp from string
// TODO: unit tests
func TimestampFromStr(s string) (Timestamp, error) {
	strs := strings.Split(s, timestampDelimiter)
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

// Max returns maximum of two timestamps.
// TODO: unit tests
func Max(ots1, ots2 Timestamp) Timestamp {
	if Less(ots1, ots2) {
		return ots2
	}
	return ots1
}

// Less returns if first timestamp less than second
func Less(ots1, ots2 Timestamp) bool {
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
	TS   Timestamp
	OP   string
	NS   string
	Data []byte
	Err  error
}

// OplogMeta is used to decode raw bson record.
type OplogMeta struct {
	TS primitive.Timestamp `bson:"ts"`
	NS string              `bson:"ns"`
	Op string              `bson:"op"`
}

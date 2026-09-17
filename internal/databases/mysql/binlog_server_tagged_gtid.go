package mysql

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

func decodeTaggedTransactionGTID(body []byte) (mysql.GTIDSet, error) {
	// Decode only the identity; scheduling metadata (including the optional
	// commit_group_ticket) is forwarded verbatim in RawData. go-mysql v1.16.0
	// cannot decode that ticket and its FieldIntVar corrupts nine-byte GNOs.
	r := taggedGTIDReader{data: body}
	version := r.readUint()
	size := r.readUint()
	if r.err != nil {
		return nil, r.err
	}
	if version != 1 {
		return nil, fmt.Errorf("unsupported tagged GTID serialization version %d", version)
	}
	consumed := len(body) - len(r.data)
	if size > uint64(len(body)) || size < uint64(consumed) {
		return nil, errors.New("invalid tagged GTID message size")
	}
	r.data = body[consumed:int(size)]
	r.readUint() // last non-ignorable field; only identity fields are needed here
	r.field(0)
	r.readByte() // flags
	r.field(1)
	sid := make([]byte, 16)
	for i := range sid {
		sid[i] = r.readByte()
	}
	r.field(2)
	encodedGNO := r.readUint()
	// MySQL signed varints use the low bit for the sign (zigzag encoding).
	gno := int64(encodedGNO>>1) ^ -int64(encodedGNO&1)
	r.field(3)
	tag := r.readString()
	if r.err != nil {
		return nil, r.err
	}
	if gno <= 0 || tag == "" {
		return nil, errors.New("invalid tagged GTID identity")
	}
	ge := replication.GTIDEvent{SID: sid, GNO: gno, Tag: mysql.NewTag(tag)}
	return ge.GTIDNext()
}

// A bounded reader for the mysql::serialization identity prefix. Retain the
// first error so a truncated/missing field cannot become a different GTID.
type taggedGTIDReader struct {
	data []byte
	err  error
}

func (r *taggedGTIDReader) readUint() uint64 {
	if r.err != nil {
		return 0
	}
	if len(r.data) == 0 {
		r.err = io.ErrUnexpectedEOF
		return 0
	}
	n := bits.TrailingZeros8(^r.data[0]) + 1
	if len(r.data) < n {
		r.err = io.ErrUnexpectedEOF
		return 0
	}
	encoded := r.data[:n]
	r.data = r.data[n:]
	if n == 9 {
		// 0xff is only a length prefix. All following eight bytes are value
		// bits; including the prefix or shifting here truncates large GNOs.
		return binary.LittleEndian.Uint64(encoded[1:])
	}
	var buf [8]byte
	copy(buf[:], encoded)
	return binary.LittleEndian.Uint64(buf[:]) >> n
}

func (r *taggedGTIDReader) field(want uint64) {
	if got := r.readUint(); r.err == nil && got != want {
		r.err = fmt.Errorf("missing tagged GTID field %d (got %d)", want, got)
	}
}

func (r *taggedGTIDReader) readByte() byte {
	value := r.readUint()
	if value > math.MaxUint8 {
		r.err = errors.New("tagged GTID byte overflows uint8")
		return 0
	}
	return byte(value)
}

func (r *taggedGTIDReader) readString() string {
	n := r.readUint()
	if r.err != nil {
		return ""
	}
	if n > uint64(len(r.data)) {
		r.err = io.ErrUnexpectedEOF
		return ""
	}
	value := string(r.data[:int(n)])
	r.data = r.data[int(n):]
	return value
}

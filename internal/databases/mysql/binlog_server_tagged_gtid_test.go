package mysql

import (
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
)

var taggedGNOBoundaries = []struct {
	gno  int64
	wire []byte
}{
	{1, []byte{0x04}},
	{63, []byte{0xfc}},
	{64, []byte{0x01, 0x02}},
	{1<<55 - 1, []byte{0x7f, 0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	{1 << 55, []byte{0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}},
	{1<<55 + 2, []byte{0xff, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}},
	{math.MaxInt64 - 1, []byte{0xff, 0xfc, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
}

// A real MySQL 9.2 tagged event from go-mysql's serialization
// tagged-event fixture. Replace only GNO and the affected lengths.
// The boundary wire values above follow MySQL's variable_length_integers.h,
// independently of both WAL-G's decoder and go-mysql's broken FieldIntVar.
func taggedGTIDFixture(gnoWire []byte) *replication.BinlogEvent {
	body := []byte{
		0x02, 0x76, 0x00, 0x00, 0x02, 0x02, 0x25, 0x02, 0xdc, 0xf0, 0x09, 0x02,
		0x30, 0xf9, 0x03, 0x22, 0xbd, 0x03, 0xad, 0x02, 0x21, 0x02, 0x44, 0x44,
		0x5a, 0x68, 0x51, 0x03, 0x22, 0x04,
	}
	body = append(body, gnoWire...)
	body = append(body,
		0x06, 0x0c, 'f', 'o', 'o', 'b', 'a', 'z',
		0x08, 0x00, 0x0a, 0x04, 0x0c, 0x7f, 0x15, 0x83, 0x22, 0x2d, 0x5c, 0x2e,
		0x06, 0x10,
	)
	// transaction_length = 210 + the additional GNO bytes (two-byte varint).
	txnLength := uint16(210 + len(gnoWire) - 1)
	body = append(body, byte(txnLength<<2)|1, byte(txnLength>>6), 0x12, 0xc3, 0x02, 0x0b)
	body[1] = byte(len(body) << 1)
	e := rawEvent(replication.GTID_TAGGED_LOG_EVENT, "2026-01-01 00:00:01", body)
	e.Event = &replication.GenericEvent{Data: body}
	e.RawData = append(e.RawData, 0, 0, 0, 0)
	return e
}

func TestTaggedGTIDGNOBoundaries(t *testing.T) {
	const ts = "2026-01-01 00:00:01"
	const tsid = "896e7882-18fe-11ef-ab88-22222d34d411:foobaz:"
	for _, tc := range taggedGNOBoundaries {
		t.Run(fmt.Sprint(tc.gno), func(t *testing.T) {
			e := taggedGTIDFixture(tc.wire)
			rawBefore := append([]byte(nil), e.RawData...)
			want := tsid + fmt.Sprint(tc.gno)
			got, err := decodeTransactionGTID(e)
			require.NoError(t, err)
			require.Equal(t, want, got.String())
			require.Equal(t, rawBefore, e.RawData)

			t.Run("already applied transaction and rows are skipped", func(t *testing.T) {
				p, sink := newTestProcessor(t, nil, requireGTIDSet(t, want), at(ts))
				require.NoError(t, p.handleEvent(e))
				require.NoError(t, p.handleEvent(writeRowsEvent(ts)))
				require.Empty(t, sink.recorded())
				require.True(t, p.sentGTIDs.IsEmpty())
			})

			for _, required := range []string{"", tsid + "1"} {
				if tc.gno == 1 && required != "" {
					continue
				}
				t.Run("new transaction with required="+required, func(t *testing.T) {
					p, sink := newTestProcessor(t, nil, nil, at(ts))
					if required != "" {
						p.requiredGTIDs = requireGTIDSet(t, required)
					}
					require.NoError(t, p.handleEvent(e))
					rows := writeRowsEvent(ts)
					require.NoError(t, p.handleEvent(rows))
					require.Equal(t, []*replication.BinlogEvent{e, rows}, sink.recorded())
					require.Equal(t, want, p.sentGTIDs.String())
					require.False(t, p.sentGTIDs.IsEmpty())

					// These are the completion conditions used by waitForReplica:
					// it must wait until the actual, not a truncated, GNO is applied.
					executed := requireGTIDSet(t, required)
					require.False(t, executed.Contain(p.sentGTIDs))
					require.NoError(t, executed.Update(want))
					require.True(t, executed.Contain(p.sentGTIDs))
				})
			}
		})
	}
}

func TestTaggedGTIDTruncatedIdentity(t *testing.T) {
	for _, tc := range taggedGNOBoundaries {
		body := taggedGTIDFixture(tc.wire).Event.(*replication.GenericEvent).Data
		// Identity ends after field id, string length and the six-byte tag.
		identityEnd := 30 + len(tc.wire) + 8
		for end := 0; end < identityEnd; end++ {
			t.Run(fmt.Sprintf("gno=%d/length=%d", tc.gno, end), func(t *testing.T) {
				truncated := append([]byte(nil), body[:end]...)
				if end > 1 {
					truncated[1] = byte(end << 1)
				}
				_, err := decodeTaggedTransactionGTID(truncated)
				require.Error(t, err)
			})
		}
	}
}

func TestTaggedGTIDVarintWidths(t *testing.T) {
	for n := 1; n <= 9; n++ {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			// Maximum unsigned value in each wire width. The first n-1 bits
			// are ones, followed by a zero (except the nine-byte 0xff prefix).
			wire := make([]byte, n)
			for i := range wire {
				wire[i] = 0xff
			}
			var want uint64 = math.MaxUint64
			if n < 9 {
				wire[0] &^= 1 << (n - 1)
				want = 1<<(7*n) - 1
			}
			r := taggedGTIDReader{data: append(wire, 0x04)}
			require.Equal(t, want, r.readUint())
			require.NoError(t, r.err)
			require.Equal(t, uint64(2), r.readUint(), "consume exactly the encoded width")
			require.NoError(t, r.err)
			require.Empty(t, r.data)
			for end := 0; end < n; end++ {
				r := taggedGTIDReader{data: wire[:end]}
				require.Zero(t, r.readUint())
				require.ErrorIs(t, r.err, io.ErrUnexpectedEOF)
			}
		})
	}
}

func TestTaggedGTIDInvalidGNO(t *testing.T) {
	for _, wire := range [][]byte{
		{0},    // 0
		{0x02}, // -1
		{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, // int64 min
	} {
		_, err := decodeTransactionGTID(taggedGTIDFixture(wire))
		require.ErrorContains(t, err, "invalid tagged GTID identity")
	}
}

func FuzzDecodeTaggedTransactionGTID(f *testing.F) {
	f.Add([]byte(nil))
	for _, tc := range taggedGNOBoundaries {
		f.Add(taggedGTIDFixture(tc.wire).Event.(*replication.GenericEvent).Data)
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		// Call the prefix decoder directly, without decodeTransactionGTID's
		// recover: malformed input must return an error, not panic.
		gtid, err := decodeTaggedTransactionGTID(body)
		if err == nil {
			require.NotNil(t, gtid)
			require.False(t, gtid.IsEmpty())
		}
	})
}

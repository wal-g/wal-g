package mysql

import (
	"context"
	"encoding/binary"
	"fmt"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Dump binlog processor test implementations ---

type binlogFile struct {
	name   string
	events []*replication.BinlogEvent
}

type memFetcher struct {
	files []binlogFile
}

func (f *memFetcher) fetchBinlogFiles(ctx context.Context, fileCh chan<- string) error {
	defer close(fileCh)
	for _, file := range f.files {
		select {
		case fileCh <- file.name:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (f *memFetcher) cleanupFile(string) {}

type memEventParser struct {
	eventsByName map[string][]*replication.BinlogEvent
}

func (p *memEventParser) parse(file string, _ int64, emit func(*replication.BinlogEvent) error) error {
	for _, e := range p.eventsByName[path.Base(file)] {
		if err := emit(e); err != nil {
			return err
		}
	}
	return nil
}

type recordingSink struct {
	mu     sync.Mutex
	events []*replication.BinlogEvent
}

func (s *recordingSink) addEvent(e *replication.BinlogEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
	return nil
}

func (s *recordingSink) recorded() []*replication.BinlogEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*replication.BinlogEvent, len(s.events))
	copy(out, s.events)
	return out
}

// --- Test helpers ---

func rawEvent(eventType replication.EventType, ts string, body []byte) *replication.BinlogEvent {
	t := at(ts)
	raw := append(make([]byte, replication.EventHeaderSize), body...)
	binary.LittleEndian.PutUint32(raw[0:], uint32(t.Unix()))
	raw[binlogFileHeaderSize] = byte(eventType)
	return &replication.BinlogEvent{
		Header:  &replication.EventHeader{EventType: eventType, Timestamp: uint32(t.Unix())},
		RawData: raw,
	}
}

func eventType(e *replication.BinlogEvent) replication.EventType {
	return replication.EventType(e.RawData[binlogFileHeaderSize])
}

func rotateName(e *replication.BinlogEvent) string {
	body := e.RawData[replication.EventHeaderSize:]
	nameEnd := 8
	for nameEnd < len(body) && body[nameEnd] != 0 {
		nameEnd++
	}
	return string(body[8:nameEnd])
}

func gtidNext(e *replication.BinlogEvent) string {
	ge := &replication.GTIDEvent{}
	if err := ge.Decode(e.RawData[replication.EventHeaderSize:]); err != nil {
		return "?"
	}
	one, err := ge.GTIDNext()
	if err != nil {
		return "?"
	}
	return one.String()
}

func gtidEvent(ts string, sid uuid.UUID, gno int64) *replication.BinlogEvent {
	body := make([]byte, 25)
	copy(body[1:17], sid[:])
	binary.LittleEndian.PutUint64(body[17:25], uint64(gno))
	return rawEvent(replication.GTID_EVENT, ts, body)
}

// Encode the mysql::serialization fields used by tagged GTIDs. Small positive
// GNOs fit into one byte; UUID bytes use the fixed-integer wire encoding.
func taggedGTIDEvent(ts string, sid uuid.UUID, tag string, gno byte) *replication.BinlogEvent {
	body := []byte{2, 0, 18, 0, 0, 2} // version, size, last field, flags, UUID field
	for _, b := range sid {
		if b < 128 {
			body = append(body, b<<1)
		} else {
			body = append(body, b<<2|1, b>>6)
		}
	}
	body = append(body, 4, gno<<2, 6, byte(len(tag)<<1))
	body = append(body, tag...)
	// last_committed, sequence_number, immediate_commit_timestamp,
	// transaction_length, immediate_server_version (optional fields omitted).
	body = append(body, 8, 0, 10, 4, 12, 0, 16, 0, 18, 0)
	body[1] = byte(len(body) << 1)
	e := rawEvent(replication.GTID_TAGGED_LOG_EVENT, ts, body)
	e.Event = &replication.GenericEvent{Data: body}
	// Production RawData retains the checksum, unlike GenericEvent.Data.
	e.RawData = append(e.RawData, 0, 0, 0, 0)
	return e
}

func TestHandleEventTaggedGTIDs(t *testing.T) {
	const ts = "2026-01-01 00:00:01"
	for _, tc := range []struct {
		name     string
		required string
		events   []*replication.BinlogEvent
		forward  []int
		sent     string
	}{
		{
			name:    "tagged-only stream is tracked",
			events:  []*replication.BinlogEvent{taggedGTIDEvent(ts, uuid1, "review", 1), writeRowsEvent(ts)},
			forward: []int{0, 1}, sent: uuid1.String() + ":review:1",
		},
		{
			name:     "already-applied tagged transaction and payload are skipped",
			required: uuid1.String() + ":review:1",
			events:   []*replication.BinlogEvent{taggedGTIDEvent(ts, uuid1, "review", 1), queryEvent(ts), writeRowsEvent(ts)},
		},
		{
			name:     "mixed stream distinguishes tags and clears skip state",
			required: uuid1.String() + ":1:review:1",
			events: []*replication.BinlogEvent{
				taggedGTIDEvent(ts, uuid1, "review", 1), writeRowsEvent(ts),
				gtidEvent(ts, uuid1, 2), writeRowsEvent(ts),
				taggedGTIDEvent(ts, uuid1, "review", 2), writeRowsEvent(ts),
				taggedGTIDEvent(ts, uuid1, "other", 1), writeRowsEvent(ts),
				gtidEvent(ts, uuid1, 1), writeRowsEvent(ts),
				taggedGTIDEvent(ts, uuid2, "review", 1), writeRowsEvent(ts),
			},
			forward: []int{2, 3, 4, 5, 6, 7, 10, 11},
			sent:    uuid1.String() + ":2:review:2:other:1," + uuid2.String() + ":review:1",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, sink := newTestProcessor(t, nil, requireGTIDSet(t, tc.required), at(ts))
			for _, e := range tc.events {
				require.NoError(t, p.handleEvent(e))
			}
			want := make([]*replication.BinlogEvent, 0, len(tc.forward))
			for _, i := range tc.forward {
				want = append(want, tc.events[i])
			}
			require.Equal(t, want, sink.recorded())
			require.True(t, p.sentGTIDs.Equal(requireGTIDSet(t, tc.sent)), "sent: %s", p.sentGTIDs)
		})
	}
}

func TestHandleEventMalformedGTID(t *testing.T) {
	for _, kind := range []replication.EventType{replication.GTID_EVENT, replication.GTID_TAGGED_LOG_EVENT} {
		for _, body := range [][]byte{nil, {2}, {2, 6, 18}, {2, 10, 18, 0, 1}} {
			p, sink := newTestProcessor(t, nil, nil, at("2026-01-01 00:00:01"))
			err := p.handleEvent(rawEvent(kind, "2026-01-01 00:00:01", body))
			require.Error(t, err, "%s %x", kind, body)
			require.Empty(t, sink.recorded())
			require.True(t, p.sentGTIDs.IsEmpty())
		}
	}
}

func rotateEvent(ts string, name string, pos uint64) *replication.BinlogEvent {
	body := make([]byte, 8+len(name)+1)
	binary.LittleEndian.PutUint64(body, pos)
	copy(body[8:], name)
	return rawEvent(replication.ROTATE_EVENT, ts, body)
}

func queryEvent(ts string) *replication.BinlogEvent {
	return rawEvent(replication.QUERY_EVENT, ts, []byte("BEGIN"))
}

func tableMapEvent(ts string) *replication.BinlogEvent {
	return rawEvent(replication.TABLE_MAP_EVENT, ts, []byte{0})
}

func writeRowsEvent(ts string) *replication.BinlogEvent {
	return rawEvent(replication.WRITE_ROWS_EVENTv2, ts, []byte{0})
}

func updateRowsEvent(ts string) *replication.BinlogEvent {
	return rawEvent(replication.UPDATE_ROWS_EVENTv2, ts, []byte{0})
}

func deleteRowsEvent(ts string) *replication.BinlogEvent {
	return rawEvent(replication.DELETE_ROWS_EVENTv2, ts, []byte{0})
}

func newTestProcessor(
	t *testing.T,
	files []binlogFile,
	requiredGTIDs *mysql.MysqlGTIDSet,
	untilTS time.Time,
) (*BinlogDumpProcessor, *recordingSink) {
	t.Helper()

	eventsByName := make(map[string][]*replication.BinlogEvent, len(files))
	for _, f := range files {
		eventsByName[f.name] = f.events
	}

	sink := &recordingSink{}
	sent, _ := mysql.ParseGTIDSet(mysql.MySQLFlavor, "")
	p := &BinlogDumpProcessor{
		ctx:           context.Background(),
		untilTS:       untilTS,
		serverID:      1,
		fetcher:       &memFetcher{files: files},
		parser:        &memEventParser{eventsByName: eventsByName},
		sink:          sink,
		sentGTIDs:     sent,
		requiredGTIDs: requiredGTIDs,
	}
	return p, sink
}

var uuid1 = uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeea")
var uuid2 = uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeeb")

func requireGTIDSet(t *testing.T, s string) *mysql.MysqlGTIDSet {
	t.Helper()
	set, err := mysql.ParseMysqlGTIDSet(s)
	require.NoError(t, err)
	return set.(*mysql.MysqlGTIDSet)
}

func describeEvent(e *replication.BinlogEvent) string {
	switch t := eventType(e); t {
	case replication.ROTATE_EVENT:
		return fmt.Sprintf("ROTATE(%s)", rotateName(e))
	case replication.GTID_EVENT:
		return fmt.Sprintf("GTID(%s)", gtidNext(e))
	case replication.QUERY_EVENT:
		return "QUERY"
	default:
		return t.String()
	}
}

func describeEvents(events []*replication.BinlogEvent) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = describeEvent(e)
	}
	return out
}

func at(s string) time.Time {
	t, err := time.Parse("2006-01-02 15:04:05", s)
	if err != nil {
		panic(err)
	}
	return t
}

// --- Tests ---

type processTestCase struct {
	name              string
	files             []binlogFile
	requiredGTIDs     *mysql.MysqlGTIDSet
	untilTS           time.Time
	expected          []*replication.BinlogEvent
	expectedSentGTIDs string
}

func TestProcess(t *testing.T) {
	cases := []processTestCase{
		{
			name:              "no files produces no output",
			files:             nil,
			expected:          []*replication.BinlogEvent{},
			expectedSentGTIDs: "",
		},
		{
			// A real binlog file typically ends with a ROTATE_EVENT
			// pointing at the next file on the host that produced it.
			// That file may not be the one we actually stream next (e.g.
			// after a primary switchover/failover gave us a different
			// continuation file), so the real rotate must be ignored and
			// replaced by our own artificial rotate naming the file we
			// are really about to stream.
			name: "trailing real rotate to a mismatched file is ignored; artificial rotate targets the real next file",
			files: []binlogFile{
				{
					name: "a.000001",
					events: []*replication.BinlogEvent{
						gtidEvent("2026-01-01 00:00:01", uuid1, 1),
						tableMapEvent("2026-01-01 00:00:01"),
						writeRowsEvent("2026-01-01 00:00:01"),
						rotateEvent("2026-01-01 00:00:02", "a.000002", 4),
					},
				},
				{
					name: "b.000001",
					events: []*replication.BinlogEvent{
						gtidEvent("2026-01-01 00:00:03", uuid2, 1),
						tableMapEvent("2026-01-01 00:00:03"),
						writeRowsEvent("2026-01-01 00:00:03"),
						rotateEvent("2026-01-01 00:00:04", "b.000002", 4),
					},
				},
			},
			expected: []*replication.BinlogEvent{
				rotateEvent("1970-01-01 00:00:00", "a.000001", 4),
				gtidEvent("2026-01-01 00:00:01", uuid1, 1),
				tableMapEvent("2026-01-01 00:00:01"),
				writeRowsEvent("2026-01-01 00:00:01"),

				rotateEvent("1970-01-01 00:00:00", "b.000001", 4),
				gtidEvent("2026-01-01 00:00:03", uuid2, 1),
				tableMapEvent("2026-01-01 00:00:03"),
				writeRowsEvent("2026-01-01 00:00:03"),
			},
			expectedSentGTIDs: uuid1.String() + ":1" + "," + uuid2.String() + ":1",
		},
		{
			name: "transaction already in requiredGTIDs is skipped",
			files: []binlogFile{
				{
					name: "a.000001",
					events: []*replication.BinlogEvent{
						gtidEvent("2026-01-01 00:00:01", uuid1, 10),
						tableMapEvent("2026-01-01 00:00:01"),
						writeRowsEvent("2026-01-01 00:00:01"),
						gtidEvent("2026-01-01 00:00:02", uuid1, 11),
						tableMapEvent("2026-01-01 00:00:02"),
						writeRowsEvent("2026-01-01 00:00:02"),
					},
				},
			},
			requiredGTIDs: requireGTIDSet(t, uuid1.String()+":1-10"),
			expected: []*replication.BinlogEvent{
				rotateEvent("1970-01-01 00:00:00", "a.000001", 4),
				gtidEvent("1970-01-01 00:00:00", uuid1, 11),
				tableMapEvent("1970-01-01 00:00:00"),
				writeRowsEvent("1970-01-01 00:00:00"),
			},
			expectedSentGTIDs: uuid1.String() + ":11",
		},
		{
			// GTID events carry the transaction's commit timestamp, which
			// is the latest timestamp in the transaction; the row events
			// that make up the transaction body are timestamped earlier,
			// at their individual execution time. Once an event past
			// untilTS is encountered, streaming stops for good (there is
			// nothing further worth sending), so the transaction whose
			// commit time exceeds untilTS is dropped along with
			// everything after it, including any later files - only the
			// earlier, fully-committed-before-the-cutoff transaction is
			// forwarded.
			name: "untilTS stops streaming for good once a transaction commits after the cutoff",
			files: []binlogFile{
				{
					name: "a.000001",
					events: []*replication.BinlogEvent{
						gtidEvent("2026-01-01 00:00:09", uuid1, 1),
						tableMapEvent("2026-01-01 00:00:09"),
						writeRowsEvent("2026-01-01 00:00:09"),
						gtidEvent("2026-01-01 00:00:12", uuid1, 2),
						tableMapEvent("2026-01-01 00:00:10"),
						writeRowsEvent("2026-01-01 00:00:10"),
					},
				},
				{
					name: "a.000002",
					events: []*replication.BinlogEvent{
						gtidEvent("2026-01-01 00:00:13", uuid1, 3),
						tableMapEvent("2026-01-01 00:00:13"),
						writeRowsEvent("2026-01-01 00:00:13"),
						gtidEvent("2026-01-01 00:00:14", uuid1, 4),
						tableMapEvent("2026-01-01 00:00:14"),
						writeRowsEvent("2026-01-01 00:00:14"),
					},
				},
			},
			untilTS: at("2026-01-01 00:00:11"),
			expected: []*replication.BinlogEvent{
				rotateEvent("1970-01-01 00:00:00", "a.000001", 4),
				gtidEvent("2026-01-01 00:00:09", uuid1, 1),
				tableMapEvent("2026-01-01 00:00:09"),
				writeRowsEvent("2026-01-01 00:00:09"),
			},
			expectedSentGTIDs: uuid1.String() + ":1",
		},
		{
			// Binlog files coming from different replicas after a
			// switchover/failover may overlap: the same already-applied
			// transaction can appear at the tail of one file and again
			// at the head of the next.
			name: "binlog files transaction overlap",
			files: []binlogFile{
				{
					name: "a.000001",
					events: []*replication.BinlogEvent{
						gtidEvent("2026-01-01 00:00:00", uuid1, 9),
						tableMapEvent("2026-01-01 00:00:00"),
						writeRowsEvent("2026-01-01 00:00:00"),
						gtidEvent("2026-01-01 00:00:01", uuid1, 10),
						tableMapEvent("2026-01-01 00:00:01"),
						writeRowsEvent("2026-01-01 00:00:01"),
					},
				},
				{
					name: "a.000002",
					events: []*replication.BinlogEvent{
						gtidEvent("2026-01-01 00:00:01", uuid1, 10),
						tableMapEvent("2026-01-01 00:00:01"),
						writeRowsEvent("2026-01-01 00:00:01"),
						gtidEvent("2026-01-01 00:00:02", uuid2, 1),
						tableMapEvent("2026-01-01 00:00:02"),
						deleteRowsEvent("2026-01-01 00:00:02"),
					},
				},
			},
			requiredGTIDs: requireGTIDSet(t, uuid1.String()+":1-9"),
			expected: []*replication.BinlogEvent{
				rotateEvent("1970-01-01 00:00:00", "a.000001", 4),
				gtidEvent("2026-01-01 00:00:01", uuid1, 10),
				tableMapEvent("2026-01-01 00:00:01"),
				writeRowsEvent("2026-01-01 00:00:01"),

				rotateEvent("1970-01-01 00:00:00", "a.000002", 4),
				gtidEvent("2026-01-01 00:00:01", uuid1, 10),
				tableMapEvent("2026-01-01 00:00:01"),
				writeRowsEvent("2026-01-01 00:00:01"),
				gtidEvent("2026-01-01 00:00:02", uuid2, 1),
				tableMapEvent("2026-01-01 00:00:02"),
				deleteRowsEvent("2026-01-01 00:00:02"),
			},
			expectedSentGTIDs: uuid1.String() + ":10" + "," + uuid2.String() + ":1",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			untilTS := tc.untilTS
			if untilTS.IsZero() {
				untilTS = time.Unix(1<<62, 0)
			}
			p, sink := newTestProcessor(t, tc.files, tc.requiredGTIDs, untilTS)
			require.NoError(t, p.process())

			assert.Equal(t, describeEvents(tc.expected), describeEvents(sink.recorded()))
			assert.Equal(t, tc.expectedSentGTIDs, p.sentGTIDs.String())
		})
	}
}

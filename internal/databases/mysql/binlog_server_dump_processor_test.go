package mysql

import (
	"context"
	"encoding/binary"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	conf "github.com/wal-g/wal-g/internal/config"
)

// --- Test doubles for the three streaming abstractions ---

// binlogFile is a fixture: a named binlog file carrying a caller-defined
// sequence of events that the parser will replay.
type binlogFile struct {
	name   string
	events []*replication.BinlogEvent
}

// memFetcher streams the names of the given binlog files in order onto
// fileCh, then closes it. It performs no download and touches no disk; it is
// the test binlogFetcher.
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

// cleanupFile is a no-op for the in-memory fetcher (nothing to remove).
func (f *memFetcher) cleanupFile(string) {}

// memEventParser replays the caller-defined events for each file name. It
// looks the file up by name in its backing map and invokes emit for every
// event in order. It is the test binlogEventParser.
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

// recordingSink captures every event pushed to it in order. It is the test
// eventSink: the "output stream you can validate".
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

// --- Event builders ---

// rawEvent builds a BinlogEvent with the given type and a header-sized
// RawData (enough for handleEvent's switch on EventType and for
// decideSkipForGTID's GTIDEvent.Decode to read the body). The event type
// is encoded both in the Header (for handleEvent's switch) and in the
// raw bytes at offset binlogFileHeaderSize (for the test eventType helper
// and for the replica's wire parser).
func rawEvent(eventType replication.EventType, body []byte) *replication.BinlogEvent {
	raw := append(make([]byte, replication.EventHeaderSize), body...)
	raw[binlogFileHeaderSize] = byte(eventType)
	return &replication.BinlogEvent{
		Header:  &replication.EventHeader{EventType: eventType},
		RawData: raw,
	}
}

// eventType reads the event type from the raw binlog bytes, mirroring
// what a replica sees on the wire. The type byte sits at offset
// binlogFileHeaderSize (right after the 4-byte timestamp).
func eventType(e *replication.BinlogEvent) replication.EventType {
	return replication.EventType(e.RawData[binlogFileHeaderSize])
}

// buildGTIDEvent builds a GTID_EVENT whose RawData decodes to (sid, gno).
func buildGTIDEvent(sid uuid.UUID, gno int64) *replication.BinlogEvent {
	body := make([]byte, 25) // CommitFlag(1) + SID(16) + GNO(8)
	copy(body[1:17], sid[:])
	binary.LittleEndian.PutUint64(body[17:25], uint64(gno))
	return rawEvent(replication.GTID_EVENT, body)
}

// buildRotateEvent builds a real ROTATE_EVENT (as it would appear inside a
// binlog file) pointing at the given next file name. The body layout mirrors
// the on-disk rotate event: 8-byte position + zero-terminated name.
func buildRotateEvent(name string, pos uint64) *replication.BinlogEvent {
	body := make([]byte, 8+len(name)+1)
	binary.LittleEndian.PutUint64(body, pos)
	copy(body[8:], name)
	// body[8+len(name)] is already 0 (zero terminator)
	return rawEvent(replication.ROTATE_EVENT, body)
}

// buildQueryEvent builds a generic QUERY_EVENT placeholder so tests have a
// "regular" event that is neither a txn boundary nor a GTID event.
func buildQueryEvent() *replication.BinlogEvent {
	return rawEvent(replication.QUERY_EVENT, []byte("BEGIN"))
}

// --- Test server constructor ---

// newTestProcessor wires a BinlogDumpRequestProcessor with the in-memory
// test doubles (memFetcher + memEventParser) and the recordingSink, so a
// test can drive the pipeline with fixture files and assert on the recorded
// output. The serverID config is set so addRotateEvent does not fatal.
func newTestProcessor(t *testing.T, files []binlogFile) (*BinlogDumpRequestProcessor, *recordingSink) {
	t.Helper()

	// addRotateEvent reads the server id from global config; set it for
	// this test (restored on cleanup by viperSet).
	viperSet(t, conf.MysqlBinlogServerID, "1")

	eventsByName := make(map[string][]*replication.BinlogEvent, len(files))
	for _, f := range files {
		eventsByName[f.name] = f.events
	}

	sink := &recordingSink{}
	sent, _ := mysql.ParseGTIDSet(mysql.MySQLFlavor, "")
	p := &BinlogDumpRequestProcessor{
		ctx:       context.Background(),
		untilTS:   time.Unix(1<<62, 0), // far future: never filter by timestamp
		fetcher:   &memFetcher{files: files},
		parser:    &memEventParser{eventsByName: eventsByName},
		sink:      sink,
		sentGTIDs: sent,
	}
	return p, sink
}

// viperSet sets a viper key for the duration of the test. The previous
// value is restored on cleanup so tests do not leak config into each other.
func viperSet(t *testing.T, key, value string) {
	t.Helper()
	prev, hadPrev := conf.GetSetting(key)
	viper.Set(key, value)
	t.Cleanup(func() {
		if hadPrev {
			viper.Set(key, prev)
		} else {
			viper.Set(key, nil)
		}
	})
}

// --- Tests ---

// TestProcess_EmitsArtificialRotatePerFile asserts that the pipeline emits
// an artificial ROTATE_EVENT naming each file (basename, 4) before that
// file's own events, and that real ROTATE_EVENTs embedded in the files are
// dropped.
func TestProcess_EmitsArtificialRotatePerFile(t *testing.T) {
	files := []binlogFile{
		{
			name: "mysql-bin.000001",
			events: []*replication.BinlogEvent{
				buildRotateEvent("mysql-bin.000002", 4), // real rotate -> must be dropped
				buildQueryEvent(),
			},
		},
		{
			name: "mysql-bin.000002",
			events: []*replication.BinlogEvent{
				buildQueryEvent(),
			},
		},
	}

	p, sink := newTestProcessor(t, files)
	require.NoError(t, p.process())

	out := sink.recorded()

	// Expected: rotate(000001), query, rotate(000002), query.
	require.Len(t, out, 4)

	assert.Equal(t, replication.ROTATE_EVENT, eventType(out[0]))
	assert.Equal(t, replication.ROTATE_EVENT, eventType(out[2]))
	assert.Equal(t, replication.QUERY_EVENT, eventType(out[1]))
	assert.Equal(t, replication.QUERY_EVENT, eventType(out[3]))

	// The artificial rotates must name the file we are about to stream.
	assertRotateName(t, out[0], "mysql-bin.000001")
	assertRotateName(t, out[2], "mysql-bin.000002")
}

// TestProcess_DropsRealRotateEvents asserts that a real ROTATE_EVENT inside
// a binlog file is never forwarded to the replica.
func TestProcess_DropsRealRotateEvents(t *testing.T) {
	files := []binlogFile{
		{
			name: "mysql-bin.000010",
			events: []*replication.BinlogEvent{
				buildQueryEvent(),
				buildRotateEvent("mysql-bin.000011", 4), // real rotate -> dropped
				buildQueryEvent(),
			},
		},
	}

	p, sink := newTestProcessor(t, files)
	require.NoError(t, p.process())

	out := sink.recorded()

	// Expected: rotate(000010), query, query. The real rotate is gone.
	require.Len(t, out, 3)
	assert.Equal(t, replication.ROTATE_EVENT, eventType(out[0]))
	assertRotateName(t, out[0], "mysql-bin.000010")
	assert.Equal(t, replication.QUERY_EVENT, eventType(out[1]))
	assert.Equal(t, replication.QUERY_EVENT, eventType(out[2]))
}

// TestProcess_FilesStreamedInOrder asserts that two files are streamed in
// the order the fetcher produced them, with their events interleaved by file
// boundary (rotate + events per file).
func TestProcess_FilesStreamedInOrder(t *testing.T) {
	files := []binlogFile{
		{name: "a.000001", events: []*replication.BinlogEvent{buildQueryEvent(), buildQueryEvent()}},
		{name: "b.000002", events: []*replication.BinlogEvent{buildQueryEvent()}},
	}

	p, sink := newTestProcessor(t, files)
	require.NoError(t, p.process())

	out := sink.recorded()

	// rotate(a), q, q, rotate(b), q
	require.Len(t, out, 5)
	assertRotateName(t, out[0], "a.000001")
	assert.Equal(t, replication.QUERY_EVENT, eventType(out[1]))
	assert.Equal(t, replication.QUERY_EVENT, eventType(out[2]))
	assertRotateName(t, out[3], "b.000002")
	assert.Equal(t, replication.QUERY_EVENT, eventType(out[4]))
}

// assertRotateName decodes the artificial rotate event's body and asserts
// the embedded next-file name matches want.
func assertRotateName(t *testing.T, e *replication.BinlogEvent, want string) {
	t.Helper()
	// The rotate event body (after the 19-byte header) is:
	//   8-byte position + zero-terminated name.
	body := e.RawData[replication.EventHeaderSize:]
	if len(body) < 8 {
		t.Fatalf("rotate event body too short: %d bytes", len(body))
		return
	}
	nameEnd := 8
	for nameEnd < len(body) && body[nameEnd] != 0 {
		nameEnd++
	}
	got := string(body[8:nameEnd])
	assert.Equal(t, want, got, "artificial rotate event name")
}

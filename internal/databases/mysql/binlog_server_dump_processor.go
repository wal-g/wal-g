package mysql

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/wal-g/tracelog"
	"golang.org/x/sync/errgroup"
)

// errUntilTSReached signals that an event's timestamp has exceeded the
// target PITR time (untilTS). Since binlog events are ordered by
// timestamp within and across files, no further events can be relevant,
// so streaming stops immediately.
var errUntilTSReached = errors.New("event timestamp is after untilTS")

const binlogFileHeaderSize = 4

// binlogFetcher streams binlog files in order into fileCh. binlogFetcher is
// responsible for closing fileCh before returning (success, error, or ctx
// cancellation). The consumer is responsible for calling cleanupFile once
// it is done with the file.
type binlogFetcher interface {
	fetchBinlogFiles(ctx context.Context, fileCh chan<- string) error
	cleanupFile(file string)
}

// binlogEventParser turns one binlog file into a stream of events, invoking
// emit for each event.
type binlogEventParser interface {
	parse(file string, offset int64, emit func(*replication.BinlogEvent) error) error
}

// eventSink is the output side of the streaming pipeline: every event produced
// by binlogEventParser goes through it.
type eventSink interface {
	addEvent(event *replication.BinlogEvent) error
}

// S3 binlogFetcher implementation. This is the production binlogFetcher.
type s3BinlogFetcher struct {
	params binlogSourceParams
}

func (f *s3BinlogFetcher) fetchBinlogFiles(ctx context.Context, fileCh chan<- string) error {
	defer close(fileCh)

	if err := os.MkdirAll(f.params.dstDir, 0777); err != nil {
		return err
	}

	handleBinlog := binlogHandlerFunc(func(binlogPath string) error {
		select {
		case fileCh <- binlogPath:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	return fetchLogs(ctx, f.params.rootFolder, f.params.dstDir, f.params.startTS, f.params.untilTS, f.params.endBinlogTS, handleBinlog)
}

// fetchLogs helpers
type binlogHandlerFunc func(binlogPath string) error

func (f binlogHandlerFunc) handleBinlog(binlogPath string) error {
	return f(binlogPath)
}

func (f *s3BinlogFetcher) cleanupFile(file string) {
	if err := os.Remove(file); err != nil {
		tracelog.WarningLogger.Printf("Failed to clean up %s: %v", file, err)
	}
}

// Real binlog file parser. This is the production binlogEventParser.
type fileBinlogEventParser struct {
	parser *replication.BinlogParser
}

func newFileEventParser() *fileBinlogEventParser {
	parser := replication.NewBinlogParser()
	parser.SetRawMode(true)
	parser.SetFlavor(mysql.MySQLFlavor)
	parser.SetVerifyChecksum(true)
	return &fileBinlogEventParser{parser: parser}
}

func (p *fileBinlogEventParser) parse(file string, offset int64, emit func(*replication.BinlogEvent) error) error {
	return p.parser.ParseFile(file, offset, emit)
}

// go-mysql replicaStreamer eventSink implementation. This is the production eventSink.
type replicaStreamerSink struct {
	replicaStreamer *replication.BinlogStreamer
}

func (s *replicaStreamerSink) addEvent(e *replication.BinlogEvent) error {
	logEventDebug(e, "Sending event to replica")
	return s.replicaStreamer.AddEventToStreamer(e)
}

// BinlogDumpProcessor is the main workhorse for the COM_BINLOG_DUMP[_GTID] command.
// It fetches binlog files, parses them, and forwards events to the replica. Fetching and
// parsing are delegated to the injectable binlogFetcher/binlogEventParser/eventSink
// abstractions so the core can be exercised in tests without storage, disk,
// or a real go-mysql streamer.
type BinlogDumpProcessor struct {
	ctx      context.Context //nolint:containedctx // connection-scoped cancellation, derived from Handler.ctx
	untilTS  time.Time
	serverID int

	fetcher binlogFetcher
	parser  binlogEventParser
	sink    eventSink

	heartbeatPeriod   time.Duration
	heartbeatDisabled bool

	// currentFile is the basename of the source file being streamed.
	// logPos is the source reader position: the first byte after all consumed
	// source events, including suppressed source rotate events.
	currentFile string
	logPos      uint64

	// Track every streamed transaction so catch-up waits for all of them.
	sentGTIDs mysql.GTIDSet

	// GTID set supplied by the replica in COM_BINLOG_DUMP_GTID.
	requiredGTIDs *mysql.MysqlGTIDSet
}

func newBinlogDumpRequestProcessor(ctx context.Context, params binlogSourceParams, serverID int, sink eventSink) *BinlogDumpProcessor {
	sent, _ := mysql.ParseGTIDSet(mysql.MySQLFlavor, "")
	return &BinlogDumpProcessor{
		ctx:               ctx,
		untilTS:           params.untilTS,
		serverID:          serverID,
		fetcher:           &s3BinlogFetcher{params: params},
		parser:            newFileEventParser(),
		sink:              sink,
		sentGTIDs:         sent,
		heartbeatDisabled: params.heartbeatDisabled,
	}
}

// https://github.com/percona/percona-server/blob/8.0/libbinlogevents/include/control_events.h#L53-L108
func buildRotateEvent(pos mysql.Position, serverID int) *replication.BinlogEvent {
	rotateBinlogEvent := replication.BinlogEvent{}

	messageBodySize := 8 + len(pos.Name) + 1
	eventLength := replication.EventHeaderSize + messageBodySize + replication.BinlogChecksumLength

	rotateBinlogEvent.RawData = make([]byte, eventLength)
	// generate header:
	// timestamp - 4 bytes (default)
	binlogEventPos := binlogFileHeaderSize
	// type - 1 byte
	rotateBinlogEvent.RawData[binlogEventPos] = byte(replication.ROTATE_EVENT)
	binlogEventPos++
	// server_id - 4 bytes
	binary.LittleEndian.PutUint32(rotateBinlogEvent.RawData[binlogEventPos:], uint32(serverID))
	binlogEventPos += 4
	// event_length - 4 bytes
	binary.LittleEndian.PutUint32(rotateBinlogEvent.RawData[binlogEventPos:], uint32(eventLength))
	binlogEventPos += 4
	// end_log_pos - 4 bytes
	binary.LittleEndian.PutUint32(rotateBinlogEvent.RawData[binlogEventPos:], 0)
	binlogEventPos += 4
	// flags - 2 bytes
	binary.LittleEndian.PutUint16(rotateBinlogEvent.RawData[binlogEventPos:], 0)
	binlogEventPos += 2

	// set binlog event data:
	// position - 8 bytes
	binary.LittleEndian.PutUint64(rotateBinlogEvent.RawData[binlogEventPos:], uint64(pos.Pos))
	binlogEventPos += 8
	// new binlog name - zero-terminated string
	copy(rotateBinlogEvent.RawData[binlogEventPos:], pos.Name)
	binlogEventPos += len(pos.Name)
	rotateBinlogEvent.RawData[binlogEventPos] = 0
	binlogEventPos++

	checksum := crc32.ChecksumIEEE(rotateBinlogEvent.RawData[0 : replication.EventHeaderSize+messageBodySize])
	binary.LittleEndian.PutUint32(rotateBinlogEvent.RawData[binlogEventPos:], checksum)

	return &rotateBinlogEvent
}

// https://github.com/percona/percona-server/blob/8.0/libbinlogevents/include/control_events.h#L1513-L1593
const (
	heartbeatLogFilenameField uint64 = 1 // OTW_HB_LOG_FILENAME_FIELD
	heartbeatLogPositionField uint64 = 2 // OTW_HB_LOG_POSITION_FIELD
	heartbeatHeaderEndMark    uint64 = 0 // OTW_HB_HEADER_END_MARK
)

// buildHeartbeatV2Event builds a raw HEARTBEAT_LOG_EVENT_V2 (event type 41)
func buildHeartbeatV2Event(filename string, position uint64, serverID int) *replication.BinlogEvent {
	// The position is stored as a length-encoded integer, so the position
	// field length is the byte length of that encoding.
	positionValue := mysql.PutLengthEncodedInt(position)
	body := make([]byte, 0, len(filename)+len(positionValue)+8)
	body = append(body, mysql.PutLengthEncodedInt(heartbeatLogFilenameField)...)
	body = append(body, mysql.PutLengthEncodedInt(uint64(len(filename)))...)
	body = append(body, filename...)
	body = append(body, mysql.PutLengthEncodedInt(heartbeatLogPositionField)...)
	body = append(body, mysql.PutLengthEncodedInt(uint64(len(positionValue)))...)
	body = append(body, positionValue...)
	body = append(body, mysql.PutLengthEncodedInt(heartbeatHeaderEndMark)...)

	eventLength := replication.EventHeaderSize + len(body) + replication.BinlogChecksumLength
	rawData := make([]byte, eventLength)

	// common header: timestamp 0, event type 41, server id, event length,
	// low 32 bits of the position in log_pos, flags 0
	rawData[binlogFileHeaderSize] = byte(replication.HEARTBEAT_LOG_EVENT_V2)
	binary.LittleEndian.PutUint32(rawData[5:], uint32(serverID))
	binary.LittleEndian.PutUint32(rawData[9:], uint32(eventLength))
	binary.LittleEndian.PutUint32(rawData[13:], uint32(position))
	copy(rawData[replication.EventHeaderSize:], body)

	checksum := crc32.ChecksumIEEE(rawData[0 : eventLength-replication.BinlogChecksumLength])
	binary.LittleEndian.PutUint32(rawData[eventLength-replication.BinlogChecksumLength:], checksum)

	return &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.HEARTBEAT_LOG_EVENT_V2,
			ServerID:  uint32(serverID),
			EventSize: uint32(eventLength),
			LogPos:    uint32(position),
		},
		RawData: rawData,
	}
}

// sendHeartbeat emits an idle heartbeat at the current source position.
func (p *BinlogDumpProcessor) sendHeartbeat() error {
	if p.heartbeatDisabled || p.currentFile == "" {
		return nil
	}
	heartbeatEvent := buildHeartbeatV2Event(p.currentFile, p.logPos, p.serverID)
	tracelog.DebugLogger.Printf("Sending heartbeat V2: file=%s position=%d", p.currentFile, p.logPos)
	return p.sink.addEvent(heartbeatEvent)
}

func logEventDebug(e *replication.BinlogEvent, msg string) {
	eventType := replication.EventType(e.RawData[binlogFileHeaderSize])
	timestamp := binary.LittleEndian.Uint32(e.RawData[0:])
	tracelog.DebugLogger.Printf("%s: type=%s timestamp=%s",
		msg, eventType, time.Unix(int64(timestamp), 0).Format("2006-01-02 15:04:05 UTC"))
}

func (p *BinlogDumpProcessor) handleEvent(e *replication.BinlogEvent) error {
	if p.ctx.Err() != nil {
		return p.ctx.Err()
	}

	// Account for every consumed source event, forwarded or suppressed.
	p.logPos += uint64(len(e.RawData))

	if int64(e.Header.Timestamp) > p.untilTS.Unix() {
		logEventDebug(e, "Stopping stream (reason=after_untilTS)")
		return errUntilTSReached
	}
	switch e.Header.EventType {
	case replication.GTID_EVENT, replication.GTID_TAGGED_LOG_EVENT:
		gtid, err := decodeTransactionGTID(e)
		if err != nil {
			return fmt.Errorf("decode %s: %w", e.Header.EventType, err)
		}
		if err := p.sentGTIDs.Update(gtid.String()); err != nil {
			return fmt.Errorf("record sent GTID %s: %w", gtid, err)
		}
	case replication.ROTATE_EVENT:
		// Real rotate events point at the next file on the host that
		// produced them, which may not match what we stream next (e.g.
		// after a primary switchover/failover). We own file boundaries
		// ourselves via an artificial rotate emitted before each file
		// (see ProcessBinlogFile), so real rotates are dropped here.
		logEventDebug(e, "Dropping event (reason=real_rotate_suppressed)")
		return nil
	}

	return p.sink.addEvent(e)
}

func decodeTransactionGTID(e *replication.BinlogEvent) (mysql.GTIDSet, error) {
	if len(e.RawData) < replication.EventHeaderSize {
		return nil, errors.New("truncated event header")
	}
	body := e.RawData[replication.EventHeaderSize:]
	// Raw-mode parsing exposes a checksum-free body in GenericEvent.
	if generic, ok := e.Event.(*replication.GenericEvent); ok {
		body = generic.Data
	}
	if e.Header.EventType == replication.GTID_TAGGED_LOG_EVENT {
		return decodeTaggedTransactionGTID(body)
	}
	if len(body) < 25 {
		return nil, errors.New("truncated GTID event")
	}
	ge := &replication.GTIDEvent{}
	// Only flags, SID and GNO are needed; do not decode optional metadata.
	if err := ge.Decode(body[:25]); err != nil {
		return nil, err
	}
	return ge.GTIDNext()
}

// processBinlogFiles consumes fetched binlog file identifiers from fileCh
// and processes each in order.
func (p *BinlogDumpProcessor) processBinlogFiles(ctx context.Context, fileCh <-chan string) error {
	for {
		file, ok, err := p.waitForNextFile(ctx, fileCh)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if err := p.ProcessBinlogFile(file); err != nil {
			return err
		}
	}
}

// waitForNextFile receives the next fetched binlog file identifier. While
// the fetch of the next file stalls (e.g. a storage download), an idle
// heartbeat is sent whenever the wait outlasts the requested heartbeat
// period;
func (p *BinlogDumpProcessor) waitForNextFile(ctx context.Context, fileCh <-chan string) (string, bool, error) {
	var tickerC <-chan time.Time
	if p.heartbeatPeriod > 0 {
		ticker := time.NewTicker(p.heartbeatPeriod)
		tickerC = ticker.C
		defer ticker.Stop()
	}

	for {
		select {
		case file, ok := <-fileCh:
			return file, ok, nil
		case <-tickerC:
			if err := p.sendHeartbeat(); err != nil {
				return "", false, err
			}
		case <-ctx.Done():
			return "", false, ctx.Err()
		}
	}
}

// ProcessBinlogFile parses one fetched binlog file and pushes its events to
// the sink. Before parsing, it emits an artificial rotate event naming this
// file so the replica's expected filename always tracks what we control.
// Every file starts right after the binlog magic header.
func (p *BinlogDumpProcessor) ProcessBinlogFile(file string) error {
	defer p.fetcher.cleanupFile(file)

	if p.ctx.Err() != nil {
		return p.ctx.Err()
	}

	tracelog.InfoLogger.Printf("Streaming %s to replica", file)

	basename := path.Base(file)
	// The artificial rotate sets the replica's filename and starting position.
	rotateEvent := buildRotateEvent(mysql.Position{Name: basename, Pos: binlogFileHeaderSize}, p.serverID)
	if err := p.sink.addEvent(rotateEvent); err != nil {
		return err
	}
	// Start from the beginning of the new binlog file.
	p.currentFile = basename
	p.logPos = binlogFileHeaderSize

	return p.parser.parse(file, binlogFileHeaderSize, p.handleEvent)
}

// process runs the fetcher and parser as two goroutines under an errgroup:
// the fetcher lists/downloads binlogs and streams file identifiers over
// fileCh (closing it when done); the main goroutine parses each file and
// pushes its events to the sink. Either goroutine failing cancels the
// group's context and stops the other.
func (p *BinlogDumpProcessor) process() error {
	g, ctx := errgroup.WithContext(p.ctx)
	fileCh := make(chan string, binlogFetchAhead)

	g.Go(func() error {
		return p.fetcher.fetchBinlogFiles(ctx, fileCh)
	})
	g.Go(func() error {
		return p.processBinlogFiles(ctx, fileCh)
	})

	err := g.Wait()

	// drain and clean up any files left buffered in fileCh.
	for file := range fileCh {
		p.fetcher.cleanupFile(file)
	}

	// Reaching untilTS is a normal, successful stop condition, not a
	// failure: there is nothing left worth streaming, so any files not
	// yet processed are simply skipped.
	if errors.Is(err, errUntilTSReached) {
		return nil
	}
	return err
}

// runIdleHeartbeats keeps sending idle heartbeats at the requested period once
// all binlog files have been streamed, while WAL-G waits for the replica to
// apply them. It exits when the phase context is canceled.
func (p *BinlogDumpProcessor) runIdleHeartbeats(ctx context.Context) error {
	if p.heartbeatDisabled || p.heartbeatPeriod <= 0 {
		return nil
	}

	ticker := time.NewTicker(p.heartbeatPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := p.sendHeartbeat(); err != nil {
				return err
			}
		}
	}
}

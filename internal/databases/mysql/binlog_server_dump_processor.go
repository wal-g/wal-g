package mysql

import (
	"context"
	"encoding/binary"
	"errors"
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

	// requiredGTIDs is the replica's already-executed set from
	// COM_BINLOG_DUMP_GTID; transactions it contains are skipped. This
	// command is MySQL-only; MariaDB replicas negotiate GTID state via
	// session variables and COM_BINLOG_DUMP, which is not wired up here.
	sentGTIDs      mysql.GTIDSet
	requiredGTIDs  *mysql.MysqlGTIDSet
	skipCurrentTxn bool
}

func newBinlogDumpRequestProcessor(ctx context.Context, params binlogSourceParams, serverID int, sink eventSink) *BinlogDumpProcessor {
	sent, _ := mysql.ParseGTIDSet(mysql.MySQLFlavor, "")
	return &BinlogDumpProcessor{
		ctx:       ctx,
		untilTS:   params.untilTS,
		serverID:  serverID,
		fetcher:   &s3BinlogFetcher{params: params},
		parser:    newFileEventParser(),
		sink:      sink,
		sentGTIDs: sent,
	}
}

// https://github.com/percona/percona-server/blob/8.0/libbinlogevents/include/control_events.h#L53-L108
func (p *BinlogDumpProcessor) addRotateEvent(pos mysql.Position) error {
	// create rotate event
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
	binary.LittleEndian.PutUint32(rotateBinlogEvent.RawData[binlogEventPos:], uint32(p.serverID))
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

	return p.sink.addEvent(&rotateBinlogEvent)
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
	if int64(e.Header.Timestamp) > p.untilTS.Unix() {
		logEventDebug(e, "Stopping stream (reason=after_untilTS)")
		return errUntilTSReached
	}
	switch e.Header.EventType {
	case replication.GTID_EVENT:
		if p.decideSkipForGTID(e) {
			logEventDebug(e, "Dropping event (reason=gtid_skipped)")
			return nil
		}
	case replication.ROTATE_EVENT:
		// Real rotate events point at the next file on the host that
		// produced them, which may not match what we stream next (e.g.
		// after a primary switchover/failover). We own file boundaries
		// ourselves via an artificial rotate emitted before each file
		// (see ProcessBinlogFile), so real rotates are dropped here.
		p.skipCurrentTxn = false
		logEventDebug(e, "Dropping event (reason=real_rotate_suppressed)")
		return nil
	case replication.ANONYMOUS_GTID_EVENT, replication.GTID_TAGGED_LOG_EVENT,
		replication.FORMAT_DESCRIPTION_EVENT, replication.PREVIOUS_GTIDS_EVENT,
		replication.STOP_EVENT, replication.INCIDENT_EVENT:
		// txn boundary or file-boundary marker; never appears inside a txn
		p.skipCurrentTxn = false
	default:
		if p.skipCurrentTxn {
			logEventDebug(e, "Dropping event (reason=skip_current_txn)")
			return nil
		}
	}
	return p.sink.addEvent(e)
}

// decideSkipForGTID updates skip state from a GTID_EVENT; returns true if
// the caller should drop the event because the replica already applied it.
func (p *BinlogDumpProcessor) decideSkipForGTID(e *replication.BinlogEvent) bool {
	p.skipCurrentTxn = false
	ge := &replication.GTIDEvent{}
	if ge.Decode(e.RawData[replication.EventHeaderSize:]) != nil {
		return false
	}
	one, err := ge.GTIDNext()
	if err != nil {
		return false
	}
	if p.requiredGTIDs != nil && p.requiredGTIDs.Contain(one) {
		tracelog.DebugLogger.Printf("Skipping already-applied transaction %s", one)
		p.skipCurrentTxn = true
		return true
	}
	if err := p.sentGTIDs.Update(one.String()); err != nil {
		tracelog.WarningLogger.Printf("Failed to record sent GTID %s: %v", one, err)
	}
	return false
}

// processBinlogFiles consumes fetched binlog file identifiers from fileCh
// and processes each in order.
func (p *BinlogDumpProcessor) processBinlogFiles(ctx context.Context, fileCh <-chan string) error {
	for {
		select {
		case file, ok := <-fileCh:
			if !ok {
				return nil
			}
			if err := p.ProcessBinlogFile(file); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
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
	if err := p.addRotateEvent(mysql.Position{Name: basename, Pos: binlogFileHeaderSize}); err != nil {
		return err
	}

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

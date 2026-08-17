package mysql

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"strconv"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/wal-g/tracelog"
	conf "github.com/wal-g/wal-g/internal/config"
	"golang.org/x/sync/errgroup"
)

// binlogFetcher streams binlog file identifiers in order onto fileCh. It runs
// in the background fetch goroutine and must close fileCh before returning
// (success, error, or ctx cancellation) so the streaming core's consumer
// loop terminates. cleanupFile releases any resources held for a file once
// the streaming core is done with it (e.g. removing it from local disk);
// implementations are responsible for logging their own cleanup failures.
type binlogFetcher interface {
	fetchBinlogFiles(ctx context.Context, fileCh chan<- string) error
	cleanupFile(file string)
}

// binlogEventParser turns one binlog file into a stream of events, invoking
// emit for each event (mirrors replication.BinlogParser.ParseFile's callback
// contract). It runs in the main streaming goroutine.
type binlogEventParser interface {
	parse(file string, offset int64, emit func(*replication.BinlogEvent) error) error
}

// eventSink is the output side of the streaming pipeline: every event or
// error the core produces goes through it.
type eventSink interface {
	addEvent(event *replication.BinlogEvent) error
}

// expected by fetchLogs.
type binlogHandlerFunc func(binlogPath string) error

func (f binlogHandlerFunc) handleBinlog(binlogPath string) error {
	return f(binlogPath)
}

// storageFetcher lists and downloads binlogs from storage, pushing each
// downloaded path to fileCh. This is the production binlogFetcher.
type storageFetcher struct {
	params binlogSourceParams
}

func (f *storageFetcher) fetchBinlogFiles(ctx context.Context, fileCh chan<- string) error {
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

// cleanupFile removes the downloaded file from local disk once the streaming
// core is done with it, logging a warning if removal fails.
func (f *storageFetcher) cleanupFile(file string) {
	if err := os.Remove(file); err != nil {
		tracelog.WarningLogger.Printf("Failed to clean up %s: %v", file, err)
	}
}

// fileEventParser parses a real binlog file from disk using go-mysql's raw
// binlog parser. This is the production binlogEventParser.
type fileEventParser struct {
	parser *replication.BinlogParser
}

func newFileEventParser() *fileEventParser {
	parser := replication.NewBinlogParser()
	parser.SetRawMode(true)
	parser.SetFlavor(mysql.MySQLFlavor)
	parser.SetVerifyChecksum(true)
	return &fileEventParser{parser: parser}
}

func (p *fileEventParser) parse(file string, offset int64, emit func(*replication.BinlogEvent) error) error {
	return p.parser.ParseFile(file, offset, emit)
}

// replicaStreamerSink wraps the go-mysql *replication.BinlogStreamer event
// queue returned to the replica connection. This is the production
// eventSink.
type replicaStreamerSink struct {
	replicaStreamer *replication.BinlogStreamer
}

func (s *replicaStreamerSink) addEvent(e *replication.BinlogEvent) error {
	return s.replicaStreamer.AddEventToStreamer(e)
}

// BinlogDumpRequestProcessor owns the output sink and the pipeline that
// fetches binlog files, parses them, and forwards events to the replica for
// a single COM_BINLOG_DUMP / COM_BINLOG_DUMP_GTID request. Fetching and
// parsing are delegated to the injectable binlogFetcher/binlogEventParser
// abstractions so the core can be exercised in tests without storage, disk,
// or a real go-mysql streamer.
type BinlogDumpRequestProcessor struct {
	ctx     context.Context //nolint:containedctx // connection-scoped cancellation, derived from Handler.ctx
	untilTS time.Time

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

	// startPos is the replica's initial COM_BINLOG_DUMP position; honored as
	// the parse offset only on the first file, subsequent files start at 4.
	startPos mysql.Position
	// firstFile gates the first-file offset (startPos.Pos vs 4).
	firstFile bool
}

func newBinlogDumpRequestProcessor(ctx context.Context, params binlogSourceParams, sink eventSink) *BinlogDumpRequestProcessor {
	sent, _ := mysql.ParseGTIDSet(mysql.MySQLFlavor, "")
	return &BinlogDumpRequestProcessor{
		ctx:       ctx,
		untilTS:   params.untilTS,
		fetcher:   &storageFetcher{params: params},
		parser:    newFileEventParser(),
		sink:      sink,
		sentGTIDs: sent,
	}
}

// https://github.com/percona/percona-server/blob/8.0/libbinlogevents/include/control_events.h#L53-L108
func (p *BinlogDumpRequestProcessor) addRotateEvent(pos mysql.Position) error {
	serverID, err := conf.GetRequiredSetting(conf.MysqlBinlogServerID)
	tracelog.ErrorLogger.FatalOnError(err)

	serverIDNum, err := strconv.Atoi(serverID)
	tracelog.ErrorLogger.FatalOnError(err)

	// create rotate event
	rotateBinlogEvent := replication.BinlogEvent{}

	messageBodySize := 8 + len(pos.Name) + 1
	eventLength := replication.EventHeaderSize + messageBodySize + replication.BinlogChecksumLength

	rotateBinlogEvent.RawData = make([]byte, eventLength)
	// generate header:
	// timestamp default 4 bytes
	binlogEventPos := 4
	// type - 1 byte
	rotateBinlogEvent.RawData[binlogEventPos] = byte(replication.ROTATE_EVENT)
	binlogEventPos++
	// server_id- 4 bytes
	binary.LittleEndian.PutUint32(rotateBinlogEvent.RawData[binlogEventPos:], uint32(serverIDNum))
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

// handleEvent is the per-event callback passed to the binlogEventParser.
func (p *BinlogDumpRequestProcessor) handleEvent(e *replication.BinlogEvent) error {
	if p.ctx.Err() != nil {
		return p.ctx.Err()
	}
	if int64(e.Header.Timestamp) > p.untilTS.Unix() {
		return nil
	}
	switch e.Header.EventType {
	case replication.GTID_EVENT:
		if p.decideSkipForGTID(e) {
			return nil
		}
	case replication.ANONYMOUS_GTID_EVENT, replication.GTID_TAGGED_LOG_EVENT,
		replication.FORMAT_DESCRIPTION_EVENT, replication.PREVIOUS_GTIDS_EVENT,
		replication.ROTATE_EVENT, replication.STOP_EVENT, replication.INCIDENT_EVENT:
		// txn boundary or file-boundary marker; never appears inside a txn
		p.skipCurrentTxn = false
	default:
		if p.skipCurrentTxn {
			return nil
		}
	}
	return p.sink.addEvent(e)
}

// decideSkipForGTID updates skip state from a GTID_EVENT; returns true if
// the caller should drop the event because the replica already applied it.
func (p *BinlogDumpRequestProcessor) decideSkipForGTID(e *replication.BinlogEvent) bool {
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
// and processes each in order, until fileCh is closed or ctx is cancelled.
func (p *BinlogDumpRequestProcessor) processBinlogFiles(ctx context.Context, fileCh <-chan string) error {
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
// the sink. The first file uses the replica's requested position; later
// files start at offset 4 (the binlog magic header). It runs on the main
// goroutine. The file is always released via the fetcher's cleanupFile,
// regardless of parse outcome.
func (p *BinlogDumpRequestProcessor) ProcessBinlogFile(file string) error {
	defer p.fetcher.cleanupFile(file)

	if p.ctx.Err() != nil {
		return p.ctx.Err()
	}

	offset := int64(4)
	if p.firstFile {
		offset = int64(p.startPos.Pos)
		p.firstFile = false
	}

	tracelog.InfoLogger.Printf("Streaming %s to replica", file)
	return p.parser.parse(file, offset, p.handleEvent)
}

// process runs the fetcher and parser as two goroutines under an errgroup:
// the fetcher lists/downloads binlogs and streams file identifiers over
// fileCh (closing it when done); the main goroutine parses each file and
// pushes its events to the sink. Either goroutine failing cancels the
// group's context and stops the other.
func (p *BinlogDumpRequestProcessor) process(startPos mysql.Position) error {
	if err := p.addRotateEvent(startPos); err != nil {
		return err
	}

	p.startPos = startPos
	p.firstFile = true

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

	return err
}

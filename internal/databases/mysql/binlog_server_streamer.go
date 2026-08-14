package mysql

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/wal-g/tracelog"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"golang.org/x/sync/errgroup"
)

// binlogSourceParams groups the storage location and time window that the
// streaming pipeline fetches binlogs from.
type binlogSourceParams struct {
	rootFolder  storage.Folder
	dstDir      string
	startTS     time.Time
	untilTS     time.Time
	endBinlogTS time.Time
}

// binlogServerStreamer owns the go-mysql event queue and the pipeline that
// fetches binlogs from storage, parses them, and forwards events to the
// replica for a single replication connection.
type binlogServerStreamer struct {
	ctx    context.Context //nolint:containedctx // connection-scoped cancellation, derived from Handler.ctx
	params binlogSourceParams

	// replicaStreamer is the go-mysql event replicaStreamer returned to the replica connection.
	replicaStreamer *replication.BinlogStreamer

	// requiredGTIDs is the replica's already-executed set from
	// COM_BINLOG_DUMP_GTID; transactions it contains are skipped. This
	// command is MySQL-only; MariaDB replicas negotiate GTID state via
	// session variables and COM_BINLOG_DUMP, which is not wired up here.
	sentGTIDs      mysql.GTIDSet
	requiredGTIDs  *mysql.MysqlGTIDSet
	skipCurrentTxn bool

	// parser parses raw binlog files and forwards events to handleEvent.
	parser *replication.BinlogParser
	// startPos is the replica's initial COM_BINLOG_DUMP position; honored as
	// the parse offset only on the first file, subsequent files start at 4.
	startPos mysql.Position
	// firstFile gates the first-file offset (startPos.Pos vs 4).
	firstFile bool
}

func newBinlogServerStreamer(ctx context.Context, params binlogSourceParams) *binlogServerStreamer {
	sent, _ := mysql.ParseGTIDSet(mysql.MySQLFlavor, "")
	return &binlogServerStreamer{
		ctx:             ctx,
		params:          params,
		replicaStreamer: replication.NewBinlogStreamer(),
		sentGTIDs:       sent,
	}
}

func (s *binlogServerStreamer) handleEventError(err error) {
	if err == nil {
		return
	}
	tracelog.ErrorLogger.Println("Error during replication", err)
	ok := s.replicaStreamer.AddErrorToStreamer(err)
	for !ok {
		ok = s.replicaStreamer.AddErrorToStreamer(err)
	}
}

// https://github.com/percona/percona-server/blob/8.0/libbinlogevents/include/control_events.h#L53-L108
func (s *binlogServerStreamer) addRotateEvent(pos mysql.Position) error {
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

	return s.replicaStreamer.AddEventToStreamer(&rotateBinlogEvent)
}

// handleEvent is the per-event callback passed to BinlogParser.ParseFile.
func (s *binlogServerStreamer) handleEvent(e *replication.BinlogEvent) error {
	if s.ctx.Err() != nil {
		return s.ctx.Err()
	}
	if int64(e.Header.Timestamp) > s.params.untilTS.Unix() {
		return nil
	}
	switch e.Header.EventType {
	case replication.GTID_EVENT:
		if s.decideSkipForGTID(e) {
			return nil
		}
	case replication.ANONYMOUS_GTID_EVENT, replication.GTID_TAGGED_LOG_EVENT,
		replication.FORMAT_DESCRIPTION_EVENT, replication.PREVIOUS_GTIDS_EVENT,
		replication.ROTATE_EVENT, replication.STOP_EVENT, replication.INCIDENT_EVENT:
		// txn boundary or file-boundary marker; never appears inside a txn
		s.skipCurrentTxn = false
	default:
		if s.skipCurrentTxn {
			return nil
		}
	}
	return s.replicaStreamer.AddEventToStreamer(e)
}

// decideSkipForGTID updates skip state from a GTID_EVENT; returns true if
// the caller should drop the event because the replica already applied it.
func (s *binlogServerStreamer) decideSkipForGTID(e *replication.BinlogEvent) bool {
	s.skipCurrentTxn = false
	ge := &replication.GTIDEvent{}
	if ge.Decode(e.RawData[replication.EventHeaderSize:]) != nil {
		return false
	}
	one, err := ge.GTIDNext()
	if err != nil {
		return false
	}
	if s.requiredGTIDs != nil && s.requiredGTIDs.Contain(one) {
		tracelog.DebugLogger.Printf("Skipping already-applied transaction %s", one)
		s.skipCurrentTxn = true
		return true
	}
	if err := s.sentGTIDs.Update(one.String()); err != nil {
		tracelog.WarningLogger.Printf("Failed to record sent GTID %s: %v", one, err)
	}
	return false
}

// newRawBinlogParser builds a parser configured to feed handleEvent.
func newRawBinlogParser() *replication.BinlogParser {
	p := replication.NewBinlogParser()
	p.SetRawMode(true)
	p.SetFlavor(mysql.MySQLFlavor)
	p.SetVerifyChecksum(true)
	return p
}

// runBinlogFetcher lists and downloads binlogs, pushing each downloaded path to
// fileCh.
func (s *binlogServerStreamer) runBinlogFetcher(ctx context.Context, fileCh chan<- string) error {
	defer close(fileCh)
	if err := os.MkdirAll(s.params.dstDir, 0777); err != nil {
		return err
	}
	emitter := &binlogFileEmitter{ctx: ctx, fileCh: fileCh}
	return fetchLogs(ctx, s.params.rootFolder, s.params.dstDir, s.params.startTS, s.params.untilTS, s.params.endBinlogTS, emitter)
}

type binlogFileEmitter struct {
	ctx    context.Context //nolint:containedctx // errgroup-derived cancellation context
	fileCh chan<- string
}

func (e *binlogFileEmitter) handleBinlog(binlogPath string) error {
	select {
	case e.fileCh <- binlogPath:
		return nil
	case <-e.ctx.Done():
		return e.ctx.Err()
	}
}

// runEventStreamer consumes downloaded binlog paths from fileCh and streams each
// to the replica in order, until fileCh is closed or ctx is cancelled.
func (s *binlogServerStreamer) runEventStreamer(ctx context.Context, fileCh <-chan string) error {
	for {
		select {
		case binlogPath, ok := <-fileCh:
			if !ok {
				return nil
			}
			if err := s.streamLog(binlogPath); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// streamLog parses one downloaded binlog and pushes its events to the replica.
// The first file uses the replica's requested position; later files start at
// offset 4 (the binlog magic header). It runs on the streamer goroutine.
func (s *binlogServerStreamer) streamLog(binlogPath string) error {
	defer os.Remove(binlogPath)

	if s.ctx.Err() != nil {
		return s.ctx.Err()
	}

	offset := int64(4)
	if s.firstFile {
		offset = int64(s.startPos.Pos)
		s.firstFile = false
	}

	tracelog.InfoLogger.Printf("Streaming %s to replica", path.Base(binlogPath))
	return s.parser.ParseFile(binlogPath, offset, s.handleEvent)
}

// the fetcher lists/downloads binlogs and streams file paths over fileCh;
// the streamer parses each file and pushes its events to the replica.
func (s *binlogServerStreamer) stream(startPos mysql.Position) error {
	if err := s.addRotateEvent(startPos); err != nil {
		return err
	}

	s.parser = newRawBinlogParser()
	s.startPos = startPos
	s.firstFile = true

	g, ctx := errgroup.WithContext(s.ctx)
	fileCh := make(chan string, binlogFetchAhead)

	g.Go(func() error {
		return s.runBinlogFetcher(ctx, fileCh)
	})
	g.Go(func() error {
		return s.runEventStreamer(ctx, fileCh)
	})

	err := g.Wait()

	// drain and clean up any files left buffered in fileCh.
	for binlogPath := range fileCh {
		os.Remove(binlogPath)
	}

	return err
}

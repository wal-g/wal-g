package postgres

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

/*
wal-send is the counterpart of wal-receive: instead of connecting to a PostgreSQL
instance as a standby and pushing the received WAL to the storage, it listens for
incoming replication connections (a standby, pg_receivewal, etc.) and serves
them the WAL it reads from the storage. This effectively turns WAL-G into a
read-only "primary" fed by the WAL archive.

The implementation follows the PostgreSQL Streaming Replication Protocol
(https://www.postgresql.org/docs/current/protocol-replication.html):
  - trust authentication, no TLS;
  - IDENTIFY_SYSTEM and TIMELINE_HISTORY replication commands;
  - CREATE_REPLICATION_SLOT / DROP_REPLICATION_SLOT are accepted and ignored:
    slot state is not tracked, the WAL is served from the storage as-is;
  - START_REPLICATION [SLOT <slot>] PHYSICAL <LSN> [TIMELINE <n>] streams WAL
    segments stored in the storage, starting at the requested LSN, until
    the client disconnects or the storage runs out of segments;
  - keepalive messages and standby status updates are handled.

Timeline switches end COPY at the switch LSN from a validated history file.
After the client acknowledges CopyDone, the server reports the immediate next
timeline and its switch LSN. The client can request that timeline on the same
connection. Missing history is retried; unreadable or malformed history never
authorizes a transition.
*/

const (
	// The server exposes archived WAL with trust authentication and no TLS,
	// so it binds to the loopback interface by default. Pass an explicit
	// address to expose it further.
	walSendDefaultAddress      = "127.0.0.1:8000"
	walSendDefaultPollInterval = 5 * time.Second
	MaxWalSendChunkSize        = 64 * 1024 // max XLogData payload per message

	// walSendMaxMessageLen bounds the body of a regular protocol message,
	// before allocating from an untrusted length. The largest expected
	// message is a standby status update (34 bytes).
	walSendMaxMessageLen = 1 << 20

	sslRequestCode    = 80877103
	cancelRequestCode = 80877102
)

// pgUnixEpoch is the PostgreSQL epoch used in protocol timestamps (2000-01-01)
var pgUnixEpoch = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

// WalSendOptions configures the wal-send server
type WalSendOptions struct {
	// Address to listen on, e.g. "0.0.0.0:8000"
	Address string
	// PollInterval is how often the storage is re-checked for new WAL segments
	// and how often keepalive messages are sent to idle clients
	PollInterval time.Duration
}

// HandleWALSend starts a Streaming Replication Protocol server
// that serves WAL from the storage folder
func HandleWALSend(ctx context.Context, folder storage.Folder, options WalSendOptions) error {
	if options.Address == "" {
		options.Address = walSendDefaultAddress
	}
	if options.PollInterval <= 0 {
		options.PollInterval = walSendDefaultPollInterval
	}

	server := &walSendServer{
		rootFolder:   folder,
		walFolder:    folder.GetSubFolder(utility.WalPath),
		reader:       internal.NewFolderReader(folder.GetSubFolder(utility.WalPath)),
		pollInterval: options.PollInterval,
	}

	listener, err := net.Listen("tcp", options.Address)
	if err != nil {
		return fmt.Errorf("wal-send: failed to listen on %s: %w", options.Address, err)
	}
	tracelog.InfoLogger.Printf("wal-send: listening on %s", options.Address)

	// warm up the IDENTIFY_SYSTEM cache, so that the first client
	// does not pay the storage lookups
	go func() {
		if _, err := server.identifySystem(ctx); err != nil {
			tracelog.WarningLogger.Printf("wal-send: initialize archive identity: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()
		utility.LoggedClose(listener, "wal-send: close listener")
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("wal-send: failed to accept connection: %w", err)
		}
		go server.handleConnection(ctx, conn)
	}
}

// walSendServer holds state shared between replication client connections
type walSendServer struct {
	rootFolder   storage.Folder
	walFolder    storage.Folder
	reader       internal.StorageFolderReader
	pollInterval time.Duration

	mu              sync.Mutex
	identifyMu      sync.Mutex
	timelines       map[uint32]walSendTimeline
	listRefreshedAt time.Time

	identify            walSendIdentify
	identifyValid       bool
	identifyRefreshedAt time.Time
	pgVersionMajor      int
}

// walSendIdentify is the reply for the IDENTIFY_SYSTEM command
type walSendIdentify struct {
	systemIdentifier string
	timeline         uint32
	xlogpos          LSN
}

// identifySystem combines cluster identity from a backup with the current
// archived timeline and its own end position. A backup is not the archive head.
func (s *walSendServer) identifySystem(ctx context.Context) (walSendIdentify, error) {
	s.identifyMu.Lock()
	defer s.identifyMu.Unlock()
	s.mu.Lock()
	if s.identifyValid && time.Since(s.identifyRefreshedAt) < s.pollInterval {
		identify := s.identify
		s.mu.Unlock()
		return identify, nil
	}
	s.mu.Unlock()
	identify, haveBackup := s.identifyFromBackupSentinel(ctx)
	if !haveBackup {
		identify = walSendIdentify{timeline: 1, systemIdentifier: "0"}
	}
	if err := s.refreshWalSegments(ctx); err != nil {
		return walSendIdentify{}, err
	}
	// Prefer the newest branch that can contain the selected base backup.
	for _, timeline := range s.newerTimelines(0) {
		if haveBackup && timeline < identify.timeline {
			continue
		}
		var history []walSendHistoryEntry
		if timeline > 1 {
			var err error
			history, err = s.readTimelineHistory(ctx, timeline)
			if _, missing := err.(internal.ArchiveNonExistenceError); missing {
				continue // history may be uploaded after the segment
			}
			if err != nil {
				return walSendIdentify{}, err
			}
		}
		if haveBackup && timeline != identify.timeline {
			next := walSendHistorySwitch(history, timeline, identify.timeline)
			if next == nil || next.lsn < identify.xlogpos {
				continue
			}
		}
		s.mu.Lock()
		end := s.timelines[timeline].end
		s.mu.Unlock()
		if len(history) > 0 {
			end = max(end, history[len(history)-1].end)
		}
		if haveBackup && timeline == identify.timeline {
			end = max(end, identify.xlogpos)
		}
		identify.timeline, identify.xlogpos = timeline, end
		break
	}
	s.mu.Lock()
	s.identify, s.identifyValid, s.identifyRefreshedAt = identify, true, time.Now()
	s.mu.Unlock()
	return identify, nil
}

// identifyFromBackupSentinel provides cluster identity and the minimum branch
// position compatible with the latest completed backup.
func (s *walSendServer) identifyFromBackupSentinel(ctx context.Context) (walSendIdentify, bool) {
	backup, err := internal.GetLatestBackup(ctx, s.rootFolder.GetSubFolder(utility.BaseBackupPath))
	if err != nil {
		tracelog.WarningLogger.Printf("wal-send: failed to find the latest backup: %v", err)
		return walSendIdentify{}, false
	}
	pgBackup := ToPgBackup(backup)
	sentinel, err := pgBackup.GetSentinel(ctx)
	if err != nil {
		tracelog.WarningLogger.Printf("wal-send: failed to read the latest backup sentinel: %v", err)
		return walSendIdentify{}, false
	}

	identify := walSendIdentify{timeline: 1}
	if sentinel.SystemIdentifier != nil {
		identify.systemIdentifier = strconv.FormatUint(*sentinel.SystemIdentifier, 10)
	}

	if timeline, err := ParseTimelineFromBackupName(backup.Name); err == nil {
		identify.timeline = timeline
	}

	// Require a successor branch to include the completed backup.
	switch {
	case sentinel.BackupFinishLSN != nil:
		identify.xlogpos = *sentinel.BackupFinishLSN
	case sentinel.BackupStartLSN != nil:
		identify.xlogpos = *sentinel.BackupStartLSN
	}

	s.mu.Lock()
	if sentinel.PgVersion >= 10000 {
		s.pgVersionMajor = sentinel.PgVersion / 10000
	}
	s.mu.Unlock()

	return identify, true
}

// pgVersion returns the major PostgreSQL version of the latest backup
func (s *walSendServer) pgVersion() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pgVersionMajor
}

// handleConnection serves a single replication client
func (s *walSendServer) handleConnection(ctx context.Context, conn net.Conn) {
	start := time.Now()
	stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopClose()
	tracelog.InfoLogger.Printf("wal-send: %s connected to %s", conn.RemoteAddr(), conn.LocalAddr())
	defer func() {
		utility.LoggedClose(conn, fmt.Sprintf("wal-send: close connection with %s", conn.RemoteAddr()))
		tracelog.InfoLogger.Printf("wal-send: %s disconnected after %s",
			conn.RemoteAddr(), time.Since(start).Round(time.Millisecond))
	}()

	client := &walSendClient{
		server: s,
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
	}
	client.Serve(ctx)
}

// walSendClient is a single replication protocol client connection
type walSendClient struct {
	server  *walSendServer
	conn    net.Conn
	reader  *bufio.Reader
	writer  *bufio.Writer
	writeMu sync.Mutex
}

func (c *walSendClient) Serve(ctx context.Context) {
	err := c.handshake()
	if err != nil {
		tracelog.ErrorLogger.Printf("wal-send: handshake with %s failed: %v", c.conn.RemoteAddr(), err)
		return
	}

	for {
		msgType, payload, err := readBackendMessage(c.reader)
		if err != nil {
			if ctx.Err() == nil && err != io.EOF {
				tracelog.ErrorLogger.Printf("wal-send: read message from %s: %v", c.conn.RemoteAddr(), err)
			}
			return
		}

		switch msgType {
		case 'Q': // simple query
			c.handleQuery(ctx, payload)
		case 'X': // terminate
			tracelog.DebugLogger.Printf("wal-send: client %s terminated the connection", c.conn.RemoteAddr())
			return
		default:
			tracelog.ErrorLogger.Printf("wal-send: unexpected message type '%c' from %s",
				msgType, c.conn.RemoteAddr())
			return
		}
	}
}

// handshake reads the startup packet and performs trust authentication
func (c *walSendClient) handshake() error {
	for {
		params, version, err := readStartupMessage(c.reader)
		if err != nil {
			return err
		}
		switch version {
		case sslRequestCode:
			// we do not support TLS, clients fall back to plain connections
			_, err := c.conn.Write([]byte{'N'})
			if err != nil {
				return fmt.Errorf("respond to SSL request: %w", err)
			}
			continue
		case cancelRequestCode:
			// nothing to cancel
			return nil
		}

		if replication, ok := params["replication"]; !ok || replication == "false" {
			return fmt.Errorf("client did not request replication mode")
		}

		return c.sendPostStartupMessages()
	}
}

func (c *walSendClient) pgVersionString() string {
	if major := c.server.pgVersion(); major > 0 {
		return fmt.Sprintf("%d.0", major)
	}
	return "16.0"
}

func (c *walSendClient) sendPostStartupMessages() error {
	var msgs [][]byte
	msgs = append(msgs,
		pgMessage('R', int32(0)), // AuthenticationOk
		pgParameterStatus("server_version", c.pgVersionString()),
		pgParameterStatus("integer_datetimes", "on"),
		pgParameterStatus("server_encoding", "UTF8"),
		pgParameterStatus("client_encoding", "UTF8"),
		pgParameterStatus("DateStyle", "ISO, MDY"),
		pgParameterStatus("TimeZone", "UTC"),
		pgParameterStatus("standard_conforming_strings", "on"),
		pgParameterStatus("in_hot_standby", "off"))

	// BackendKeyData: pid and secret key, both are unused by us
	key := make([]byte, 4)
	_, _ = rand.Read(key)
	msgs = append(msgs,
		pgMessage('K', int32(os.Getpid()), int32(binary.BigEndian.Uint32(key))),
		pgMessage('Z', byte('I'))) // ReadyForQuery(idle)

	return c.writeMessages(msgs...)
}

// handleQuery dispatches a replication command
func (c *walSendClient) handleQuery(ctx context.Context, payload []byte) {
	// real clients often terminate replication commands with a semicolon
	query := strings.TrimRight(string(payload), "\x00 \t\n\r;")
	tracelog.DebugLogger.Printf("wal-send: received command from %s: %s", c.conn.RemoteAddr(), query)

	fields := strings.Fields(query)
	if len(fields) == 0 {
		_ = c.writeQueryError("XX000", "empty command")
		return
	}

	switch strings.ToUpper(fields[0]) {
	case "SHOW":
		c.handleShow(fields)
	case "IDENTIFY_SYSTEM":
		c.handleIdentifySystem(ctx)
	case "TIMELINE_HISTORY":
		c.handleTimelineHistory(ctx, fields)
	case "CREATE_REPLICATION_SLOT":
		slotName := ""
		if len(fields) >= 2 {
			slotName = fields[1]
		}
		// accepted and ignored: slot state is not tracked, the WAL is served from the storage as-is
		_ = c.writeMessages(
			pgRowDescription("slot_name", "consistent_point", "snapshot_name", "output_plugin"),
			pgDataRow(slotName, "0/0", nil, nil),
			pgMessage('C', "CREATE_REPLICATION_SLOT"),
			pgMessage('Z', byte('I')))
	case "DROP_REPLICATION_SLOT":
		_ = c.writeMessages(
			pgMessage('C', "DROP_REPLICATION_SLOT"),
			pgMessage('Z', byte('I')))
	case "START_REPLICATION":
		c.handleStartReplication(ctx, fields)
	default:
		_ = c.writeQueryError("0A000",
			fmt.Sprintf("wal-send does not support the %q command", fields[0]))
	}
}

func (c *walSendClient) handleShow(fields []string) {
	if len(fields) != 2 {
		_ = c.writeQueryError("42601", "SHOW requires one parameter")
		return
	}
	name := strings.ToLower(fields[1])
	var value string
	switch name {
	case "data_directory_mode":
		value = "0700"
	case "wal_segment_size":
		value = fmt.Sprintf("%dMB", WalSegmentSize/(1024*1024))
	default:
		_ = c.writeQueryError("42704", fmt.Sprintf("unrecognized configuration parameter %q", name))
		return
	}
	_ = c.writeMessages(pgRowDescription(name), pgDataRow(value),
		pgMessage('C', "SHOW"), pgMessage('Z', byte('I')))
}

// handleIdentifySystem reports the cluster identity and the archive head.
func (c *walSendClient) handleIdentifySystem(ctx context.Context) {
	identify, err := c.server.identifySystem(ctx)
	if err != nil {
		_ = c.writeQueryError("XX000", err.Error())
		return
	}
	_ = c.writeMessages(
		pgRowDescription("systemid", "timeline", "xlogpos", "dbname"),
		pgDataRow(identify.systemIdentifier, strconv.FormatUint(uint64(identify.timeline), 10),
			identify.xlogpos.String(), nil),
		pgMessage('C', "IDENTIFY_SYSTEM"),
		pgMessage('Z', byte('I')))
}

// handleTimelineHistory serves the requested .history file from the storage,
// padding the content the same way a real primary does
func (c *walSendClient) handleTimelineHistory(ctx context.Context, fields []string) {
	if len(fields) != 2 {
		_ = c.writeQueryError("XX000", "invalid TIMELINE_HISTORY command")
		return
	}
	timeline, err := strconv.Atoi(fields[1])
	if err != nil || timeline < 1 {
		_ = c.writeQueryError("XX000", "invalid timeline in TIMELINE_HISTORY command")
		return
	}
	historyName := fmt.Sprintf("%08X.history", timeline)

	content, err := internal.DownloadAndDecompressStorageFile(ctx, c.server.reader, historyName)
	if err != nil {
		_ = c.writeQueryError("58P01",
			fmt.Sprintf("could not receive the timeline history file %s from the storage: %v", historyName, err))
		return
	}
	defer utility.LoggedClose(content, "wal-send: close history file reader")

	data, err := io.ReadAll(content)
	if err != nil {
		_ = c.writeQueryError("XX000",
			fmt.Sprintf("failed to read the timeline history file %s: %v", historyName, err))
		return
	}

	_ = c.writeMessages(
		pgRowDescription("filename", "content"),
		pgDataRow(historyName, string(data)),
		pgMessage('C', "TIMELINE_HISTORY"),
		pgMessage('Z', byte('I')))
}

type startReplicationArgs struct {
	startLSN       LSN
	timeline       uint32
	explicitTimeli bool
}

// parseStartReplication parses "START_REPLICATION [SLOT <slot>] PHYSICAL <lsn> [TIMELINE <n>]"
func parseStartReplication(fields []string) (startReplicationArgs, error) {
	args := startReplicationArgs{}
	lsnSeen := false
	for i := 1; i < len(fields); i++ {
		token := strings.ToUpper(fields[i])
		switch token {
		case "SLOT":
			if i+1 >= len(fields) {
				return args, fmt.Errorf("SLOT requires an argument")
			}
			i++ // skip the slot name, it is ignored
			continue
		case "PHYSICAL", "LOGICAL":
			if token == "LOGICAL" {
				return args, fmt.Errorf("wal-send supports only PHYSICAL replication")
			}
			continue
		case "TIMELINE":
			if i+1 >= len(fields) {
				return args, fmt.Errorf("TIMELINE requires an argument")
			}
			tli, err := strconv.ParseUint(fields[i+1], 10, 32)
			if err != nil {
				return args, fmt.Errorf("invalid timeline %q", fields[i+1])
			}
			args.timeline = uint32(tli)
			args.explicitTimeli = true
			i++
			continue
		}
		lsn, err := ParseLSN(fields[i])
		if err != nil {
			return args, fmt.Errorf("invalid start LSN %q", fields[i])
		}
		args.startLSN = lsn
		lsnSeen = true
	}
	if !lsnSeen {
		return args, fmt.Errorf("missing start LSN")
	}
	return args, nil
}

// handleStartReplication owns the connection until both sides finish COPY.
// End-of-timeline leaves the connection available for a new START_REPLICATION.
func (c *walSendClient) handleStartReplication(ctx context.Context, fields []string) {
	args, err := parseStartReplication(fields)
	if err != nil {
		_ = c.writeQueryError("XX000", err.Error())
		return
	}
	stream := newWalSendStream(c, args)
	if err := stream.prepare(ctx); err != nil {
		_ = c.writeQueryError("XX000", err.Error())
		return
	}
	if stream.nextTimeline != nil && stream.pos() == stream.nextTimeline.lsn {
		_ = c.completeReplication(stream.nextTimeline)
		return // exactly at an old timeline's end: do not enter COPY
	}
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	if err := c.writeMessages(pgMessage('W', byte(0), int16(0))); err != nil {
		return
	}
	readerDone := make(chan struct{})
	clientCopyDone := make(chan struct{}, 1)
	go func() {
		defer close(readerDone)
		defer cancelStream()
		stream.serveClientMessages(streamCtx, clientCopyDone)
	}()
	keepaliveCtx, cancelKeepalive := context.WithCancel(streamCtx)
	keepaliveDone := stream.startKeepaliveLoop(keepaliveCtx)
	next, streamErr := stream.run(streamCtx)
	cancelKeepalive()
	<-keepaliveDone
	if next != nil && streamErr == nil {
		// No CopyData may follow CopyDone, including status-request replies.
		if err := stream.finishSending(); err != nil {
			_ = c.conn.Close()
		}
		select {
		case <-readerDone:
		case <-ctx.Done():
			_ = c.conn.Close()
			<-readerDone
		}
	}
	select {
	case <-clientCopyDone:
		<-readerDone
		if stream.finishSending() == nil {
			_ = c.completeReplication(next)
		}
		return
	default:
	}
	if streamErr != nil && streamCtx.Err() == nil && !errors.Is(streamErr, context.Canceled) {
		tracelog.ErrorLogger.Printf("wal-send: %s: the replication stream failed: %v", c.conn.RemoteAddr(), streamErr)
		_ = c.writeQueryError("XX000", streamErr.Error())
	}
	_ = c.conn.Close()
	<-readerDone
}

func (c *walSendClient) completeReplication(next *walSendTimelineSwitch) error {
	var msgs [][]byte
	if next != nil {
		msgs = append(msgs, pgRowDescription("next_tli", "next_tli_startpos"),
			pgDataRow(strconv.FormatUint(uint64(next.timeline), 10), next.lsn.String()))
	}
	// PostgreSQL sends a completion for streaming and for the command itself.
	return c.writeMessages(append(msgs, pgMessage('C', "START_STREAMING"),
		pgMessage('C', "START_REPLICATION"), pgMessage('Z', byte('I')))...)
}

func (st *walSendStream) finishSending() error {
	c := st.client
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if st.sendingDone {
		return nil
	}
	st.sendingDone = true
	return c.writeMessagesLocked(pgMessageFromBytes('c', nil))
}

func (st *walSendStream) sendKeepalive(replyRequested bool) error {
	c := st.client
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if st.sendingDone {
		return nil
	}
	return c.writeMessagesLocked(pgKeepaliveMsg(st.pos(), replyRequested))
}

// pos returns the current streaming position
func (st *walSendStream) pos() LSN {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.writePos
}

// setPos updates the current streaming position
func (st *walSendStream) setPos(pos LSN) {
	st.mu.Lock()
	st.writePos = pos
	st.mu.Unlock()
}

func walSendPageSize() uint64 {
	return uint64(viper.GetInt(conf.PgWalPageSize))
}

// walSendStream streams WAL to a single client over an established copy-both mode
type walSendStream struct {
	client       *walSendClient
	args         startReplicationArgs
	pageSize     uint64
	timelineID   uint32
	nextTimeline *walSendTimelineSwitch
	sendingDone  bool // protected by client.writeMu

	// writePos is shared by the streaming and keepalive goroutines
	mu       sync.Mutex
	writePos LSN

	// lastWaitedSegment deduplicates "waiting for segment" log messages
	// (only used by the streaming goroutine)
	lastWaitedSegment string
}

func newWalSendStream(c *walSendClient, args startReplicationArgs) *walSendStream {
	pageSize := walSendPageSize()
	if pageSize == 0 || pageSize%8192 != 0 || WalSegmentSize%pageSize != 0 {
		pageSize = 8192
	}
	return &walSendStream{client: c, args: args, pageSize: pageSize}
}

func (st *walSendStream) prepare(ctx context.Context) error {
	identify, err := st.client.server.identifySystem(ctx)
	if err != nil {
		return err
	}
	st.timelineID = identify.timeline
	if st.args.explicitTimeli && st.args.timeline != 0 {
		st.timelineID = st.args.timeline
	}
	pos := st.args.startLSN
	if pos == 0 {
		pos = identify.xlogpos
	}
	st.nextTimeline, err = st.findSuccessorTimeline(ctx, st.timelineID, pos)
	if err != nil {
		return err
	}
	if st.nextTimeline == nil || st.nextTimeline.lsn != pos {
		pos = LSN(uint64(pos) / st.pageSize * st.pageSize)
	}
	st.setPos(pos)
	return nil
}

// run keeps one timeline throughout COPY. It reads the old timeline only up to
// the validated fork point, then lets the handler perform timeline negotiation.
func (st *walSendStream) run(ctx context.Context) (*walSendTimelineSwitch, error) {
	c, server, timeline := st.client, st.client.server, st.timelineID
	prefetcher, cancelPrefetch, err := startWalSendPrefetcher(ctx, st, timeline)
	if err != nil {
		tracelog.WarningLogger.Printf("wal-send: start prefetching: %v; reading directly", err)
	}
	defer func() {
		if cancelPrefetch != nil {
			cancelPrefetch()
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		pos := st.pos()
		if st.nextTimeline != nil && pos == st.nextTimeline.lsn {
			return st.nextTimeline, nil
		}
		segNo := uint64(pos) / WalSegmentSize
		name := formatWALFileName(timeline, segNo)
		segment, fromPrefetch := st.fetchPrefetched(prefetcher, name)
		if segment == nil {
			segment, err = internal.DownloadAndDecompressStorageFile(ctx, server.reader, name)
			if _, missing := err.(internal.ArchiveNonExistenceError); missing {
				if st.nextTimeline == nil {
					partial, probeErr := internal.StorageFileExists(ctx, server.reader, name+".partial")
					if probeErr != nil {
						return nil, probeErr
					}
					if !partial {
						// Ordinary archive-head polling must remain direct object
						// access, without repeatedly listing the entire archive.
						st.waitOnePoll(ctx, name)
						continue
					}
					st.nextTimeline, err = st.findSuccessorTimeline(ctx, timeline, pos)
					if err != nil {
						return nil, err
					}
				}
				if st.nextTimeline != nil {
					if pos == st.nextTimeline.lsn {
						return st.nextTimeline, nil
					}
					// Both the partial file and the descendant's complete segment
					// contain the ancestor's bytes before the validated fork.
					segment, err = internal.DownloadAndDecompressStorageFile(ctx, server.reader, name+".partial")
					if _, missing := err.(internal.ArchiveNonExistenceError); missing {
						segment, err = internal.DownloadAndDecompressStorageFile(ctx, server.reader,
							formatWALFileName(st.nextTimeline.sourceTimeline, segNo))
					}
				} else {
					st.waitOnePoll(ctx, name)
					continue
				}
			}
			if _, missing := err.(internal.ArchiveNonExistenceError); missing {
				st.waitOnePoll(ctx, name)
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("read WAL segment %s: %w", name, err)
			}
		}
		tracelog.InfoLogger.Printf("wal-send: %s: streaming WAL segment %s at %s%s",
			c.conn.RemoteAddr(), name, pos, prefetchSource(fromPrefetch))
		offset := uint64(pos) % WalSegmentSize
		end := WalSegmentSize
		if st.nextTimeline != nil {
			end = min(end, uint64(st.nextTimeline.lsn)-segNo*WalSegmentSize)
		}
		err = st.streamSegment(ctx, segment, offset, end)
		closeErr := segment.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			return nil, fmt.Errorf("stream WAL segment %s: %w", name, err)
		}
		st.setPos(LSN(segNo*WalSegmentSize + end))
		if prefetcher != nil {
			prefetcher.remove(name)
		}
	}
}

// fetchPrefetched returns the prefetched segment file from the local disk,
// or nil when it is not downloaded yet
func (st *walSendStream) fetchPrefetched(prefetcher *walSendPrefetcher,
	segmentFileName string) (segment io.ReadCloser, fromPrefetch bool) {
	if prefetcher == nil {
		return nil, false
	}
	file, ok := prefetcher.fetch(segmentFileName)
	if !ok {
		return nil, false
	}
	return file, true
}

func prefetchSource(fromPrefetch bool) string {
	if fromPrefetch {
		return " (prefetched)"
	}
	return ""
}

// waitOnePoll sleeps for one poll interval while the requested WAL segment
// is missing in the storage, logging the wait once per segment name
func (st *walSendStream) waitOnePoll(ctx context.Context, segmentFileName string) {
	if st.lastWaitedSegment != segmentFileName {
		tracelog.InfoLogger.Printf("wal-send: waiting for the WAL segment %s to appear in the storage",
			segmentFileName)
		st.lastWaitedSegment = segmentFileName
	}
	select {
	case <-ctx.Done():
	case <-time.After(st.client.server.pollInterval):
	}
}

// walSendErrorReader preserves errors returned together with a full chunk;
// io.ReadFull otherwise drops such errors once its requested size is satisfied.
type walSendErrorReader struct {
	io.Reader
	err error
}

func (r *walSendErrorReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err != nil && err != io.EOF {
		r.err = err
	}
	return n, err
}

// Validate the decoder's EOF/checksum before releasing the last chunk, so a
// corrupt download cannot become a complete segment on the receiving side.
func (st *walSendStream) streamSegment(ctx context.Context, segment io.Reader, offset, end uint64) error {
	r := &walSendErrorReader{Reader: segment}
	if _, err := io.CopyN(io.Discard, r, int64(offset)); err != nil || r.err != nil {
		return fmt.Errorf("skip segment prefix: %w", errors.Join(err, r.err))
	}
	remaining := end - offset
	buf := make([]byte, MaxWalSendChunkSize)
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunk := buf[:min(remaining, uint64(len(buf)))]
		if _, err := io.ReadFull(r, chunk); err != nil || r.err != nil {
			return fmt.Errorf("read segment data: %w", errors.Join(err, r.err))
		}
		if remaining == uint64(len(chunk)) {
			// Reading beyond the requested range is necessary for a compressed
			// partial segment too: its checksum covers the entire stored file.
			n, err := io.Copy(io.Discard, io.LimitReader(r, int64(WalSegmentSize-end)+1))
			if err != nil || r.err != nil {
				return fmt.Errorf("finalize segment: %w", errors.Join(err, r.err))
			}
			if uint64(n) > WalSegmentSize-end {
				return fmt.Errorf("WAL segment exceeds %d bytes", WalSegmentSize)
			}
		}
		pos := st.pos()
		walEnd := pos + LSN(len(chunk))
		if err := st.client.writeMessages(pgXLogDataMsg(pos, walEnd, chunk)); err != nil {
			return err
		}
		st.setPos(walEnd)
		remaining -= uint64(len(chunk))
	}
	return nil
}

// startKeepaliveLoop sends keepalive messages to the client while it is served,
// so that it does not time out waiting for WAL
func (st *walSendStream) startKeepaliveLoop(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(st.client.server.pollInterval):
				_ = st.sendKeepalive(true)
			}
		}
	}()
	return done
}

// serveClientMessages reads copy-mode messages from the client:
// standby status updates, CopyDone, CopyFail and Terminate.
// Returns as soon as the client goes away. A graceful CopyDone is
// reported on the given channel, so that the caller can distinguish it
// from disconnects and failures.
func (st *walSendStream) serveClientMessages(ctx context.Context, copyDone chan<- struct{}) {
	c := st.client
	for {
		msgType, payload, err := readBackendMessage(c.reader)
		if err != nil {
			if ctx.Err() == nil && err != io.EOF {
				tracelog.ErrorLogger.Printf("wal-send: read a copy message from %s: %v", c.conn.RemoteAddr(), err)
			}
			return
		}
		switch msgType {
		case 'd': // CopyData
			if len(payload) < 1 {
				return
			}
			st.handleCopyData(payload)
		case 'c': // CopyDone
			tracelog.DebugLogger.Printf("wal-send: %s sent CopyDone", c.conn.RemoteAddr())
			select {
			case copyDone <- struct{}{}:
			default:
			}
			return
		case 'f': // CopyFail
			tracelog.DebugLogger.Printf("wal-send: %s sent CopyFail", c.conn.RemoteAddr())
			return
		case 'X': // Terminate
			tracelog.DebugLogger.Printf("wal-send: %s terminated the connection", c.conn.RemoteAddr())
			return
		default:
			tracelog.WarningLogger.Printf("wal-send: unexpected copy message type '%c' from %s",
				msgType, c.conn.RemoteAddr())
		}
	}
}

// handleCopyData processes a single copy data message from the client.
// In physical replication the only expected message is the standby status update.
func (st *walSendStream) handleCopyData(payload []byte) {
	c := st.client
	if payload[0] != 'r' {
		return
	}
	if len(payload) < 34 {
		tracelog.WarningLogger.Printf("wal-send: malformed standby status update from %s", c.conn.RemoteAddr())
		return
	}

	replyRequested := payload[33] != 0
	walFlushPos := LSN(binary.BigEndian.Uint64(payload[17:25]))
	tracelog.DebugLogger.Printf("wal-send: %s acknowledged WAL up to %s, reply requested: %v",
		c.conn.RemoteAddr(), walFlushPos, replyRequested)

	if replyRequested {
		_ = st.sendKeepalive(false)
	}
}

// ---------- low-level protocol helpers ----------

// pgMessage builds a protocol message with typed payload parts
func pgMessage(typ byte, payload ...any) []byte {
	var body []byte
	for _, p := range payload {
		switch v := p.(type) {
		case byte:
			body = append(body, v)
		case int16:
			body = binary.BigEndian.AppendUint16(body, uint16(v))
		case int32:
			body = binary.BigEndian.AppendUint32(body, uint32(v))
		case uint64:
			body = binary.BigEndian.AppendUint64(body, v)
		case int64:
			body = binary.BigEndian.AppendUint64(body, uint64(v))
		case string:
			body = append(append(body, v...), 0)
		default:
			panic(fmt.Sprintf("unsupported pgMessage payload type %T", p))
		}
	}
	return pgMessageFromBytes(typ, body)
}

func pgMessageFromBytes(typ byte, body []byte) []byte {
	msg := make([]byte, 0, 5+len(body))
	msg = append(msg, typ)
	msg = binary.BigEndian.AppendUint32(msg, uint32(4+len(body)))
	return append(msg, body...)
}

func pgParameterStatus(name, value string) []byte {
	return pgMessage('S', name, value)
}

func pgRowDescription(columnNames ...string) []byte {
	var body []byte
	body = binary.BigEndian.AppendUint16(body, uint16(len(columnNames)))
	for _, name := range columnNames {
		body = append(body, pgColumnDescription(name)...)
	}
	return pgMessageFromBytes('T', body)
}

func pgColumnDescription(name string) []byte {
	var body []byte
	body = append(append(body, name...), 0)
	body = binary.BigEndian.AppendUint32(body, 0)          // table OID: unspecified
	body = binary.BigEndian.AppendUint16(body, 0)          // column number
	body = binary.BigEndian.AppendUint32(body, 25)         // type OID: text
	body = binary.BigEndian.AppendUint16(body, 0xFFFF)     // type length: null
	body = binary.BigEndian.AppendUint32(body, 0xFFFFFFFF) // type modifier: null
	body = binary.BigEndian.AppendUint16(body, 0)          // format code: text
	return body
}

func pgDataRow(values ...any) []byte {
	var body []byte
	body = binary.BigEndian.AppendUint16(body, uint16(len(values)))
	for _, value := range values {
		if value == nil {
			body = binary.BigEndian.AppendUint32(body, 0xFFFFFFFF)
			continue
		}
		text := value.(string)
		body = append(binary.BigEndian.AppendUint32(body, uint32(len(text))), text...)
	}
	return pgMessageFromBytes('D', body)
}

// writeQueryError finishes a failed simple query the way the protocol
// requires: ErrorResponse followed by ReadyForQuery
func (c *walSendClient) writeQueryError(code, message string) error {
	return c.writeMessages(c.queryError(code, message), pgMessage('Z', byte('I')))
}

func (c *walSendClient) queryError(code, message string) []byte {
	var body []byte
	appendField := func(k, v string) {
		body = append(append(append(body, k[0]), v...), 0)
	}
	appendField("S", "ERROR")
	appendField("V", "ERROR")
	appendField("C", code)
	appendField("M", message)
	body = append(body, 0)
	return pgMessageFromBytes('E', body)
}

// pgXLogDataMsg builds an XLogData copy data message.
// NOTE: physical replication XLogData has no timeline field,
// unlike the logical replication messages
func pgXLogDataMsg(walStart, walEnd LSN, data []byte) []byte {
	var body []byte
	body = append(body, 'w')
	body = binary.BigEndian.AppendUint64(body, uint64(walStart))
	body = binary.BigEndian.AppendUint64(body, uint64(walEnd))
	body = binary.BigEndian.AppendUint64(body, currentPgTime())
	body = append(body, data...)
	return pgCopyData(body)
}

// pgKeepaliveMsg builds a primary keepalive copy data message
func pgKeepaliveMsg(walEnd LSN, replyRequested bool) []byte {
	var body []byte
	body = append(body, 'k')
	body = binary.BigEndian.AppendUint64(body, uint64(walEnd))
	body = binary.BigEndian.AppendUint64(body, currentPgTime())
	if replyRequested {
		body = append(body, 1)
	} else {
		body = append(body, 0)
	}
	return pgCopyData(body)
}

func pgCopyData(body []byte) []byte {
	return pgMessageFromBytes('d', body)
}

// currentPgTime returns the number of microseconds since 2000-01-01, as the protocol requires
func currentPgTime() uint64 {
	return uint64(time.Since(pgUnixEpoch).Microseconds())
}

// writeMessages serializes writes to the connection from multiple goroutines
func (c *walSendClient) writeMessages(msgs ...[]byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.writeMessagesLocked(msgs...)
}

func (c *walSendClient) writeMessagesLocked(msgs ...[]byte) error {
	for _, msg := range msgs {
		_, err := c.writer.Write(msg)
		if err != nil {
			return fmt.Errorf("write a protocol message: %w", err)
		}
	}
	err := c.writer.Flush()
	if err != nil {
		return fmt.Errorf("flush protocol messages: %w", err)
	}
	return nil
}

// readBackendMessage reads a single regular protocol message
func readBackendMessage(r io.Reader) (msgType byte, payload []byte, err error) {
	header := make([]byte, 5)
	_, err = io.ReadFull(r, header)
	if err != nil {
		return 0, nil, err
	}
	msgType = header[0]
	msgLen := binary.BigEndian.Uint32(header[1:5])
	if msgLen < 4 {
		return 0, nil, fmt.Errorf("invalid message length %d", msgLen)
	}
	if msgLen-4 > walSendMaxMessageLen {
		// do not allocate from an untrusted length
		return 0, nil, fmt.Errorf("message length %d exceeds the limit %d", msgLen-4, walSendMaxMessageLen)
	}
	payload = make([]byte, msgLen-4)
	_, err = io.ReadFull(r, payload)
	if err != nil {
		return 0, nil, err
	}
	return msgType, payload, nil
}

// readStartupMessage reads a startup packet and unpacks its parameters
func readStartupMessage(r io.Reader) (params map[string]string, version int32, err error) {
	lenBuf := make([]byte, 4)
	_, err = io.ReadFull(r, lenBuf)
	if err != nil {
		return nil, 0, err
	}
	msgLen := binary.BigEndian.Uint32(lenBuf)
	if msgLen < 8 || msgLen > 65536 {
		return nil, 0, fmt.Errorf("invalid startup message length %d", msgLen)
	}

	body := make([]byte, msgLen-4)
	_, err = io.ReadFull(r, body)
	if err != nil {
		return nil, 0, err
	}
	version = int32(binary.BigEndian.Uint32(body[:4]))

	params = make(map[string]string)
	pos := 4
	for pos < len(body) {
		key, next, ok := readStartupString(body, pos)
		if !ok {
			break
		}
		pos = next
		if key == "" {
			// end of the parameter list
			break
		}
		value, next, ok := readStartupString(body, pos)
		if !ok {
			params[key] = ""
			break
		}
		params[key] = value
		pos = next
	}
	return params, version, nil
}

func readStartupString(body []byte, pos int) (value string, next int, ok bool) {
	for i := pos; i < len(body); i++ {
		if body[i] == 0 {
			return string(body[pos:i]), i + 1, true
		}
	}
	// unterminated string, take the rest
	return string(body[pos:]), len(body), false
}

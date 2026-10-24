package postgres

import (
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

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
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
	return s.identifySystemCached(ctx, s.pollInterval)
}

func (s *walSendServer) identifySystemCached(ctx context.Context, refreshInterval time.Duration) (walSendIdentify, error) {
	s.identifyMu.Lock()
	defer s.identifyMu.Unlock()
	s.mu.Lock()
	if s.identifyValid && time.Since(s.identifyRefreshedAt) < refreshInterval {
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
	identify, err := s.identifyArchiveHead(ctx, identify, haveBackup)
	if err != nil {
		return walSendIdentify{}, err
	}
	s.mu.Lock()
	s.identify, s.identifyValid, s.identifyRefreshedAt = identify, true, time.Now()
	s.mu.Unlock()
	return identify, nil
}

func (s *walSendServer) identifyArchiveHead(ctx context.Context, identify walSendIdentify, haveBackup bool) (walSendIdentify, error) {
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
		end := s.archiveTimelineEnd(timeline, history, identify, haveBackup)
		identify.timeline, identify.xlogpos = timeline, end
		break
	}
	return identify, nil
}

func (s *walSendServer) archiveTimelineEnd(timeline uint32, history []walSendHistoryEntry,
	backup walSendIdentify, haveBackup bool) LSN {
	s.mu.Lock()
	end := s.timelines[timeline].end
	s.mu.Unlock()
	if len(history) > 0 {
		end = max(end, history[len(history)-1].end)
	}
	if haveBackup && timeline == backup.timeline {
		end = max(end, backup.xlogpos)
	}
	return end
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

	// Advertise the archive's PostgreSQL version even on the first connection,
	// before the asynchronous warmup has finished. Callers share the identity cache.
	if _, err := s.identifySystem(ctx); err != nil {
		tracelog.WarningLogger.Printf("wal-send: initialize client identity: %v", err)
		return
	}
	client := newWalSendClient(s, conn)
	client.Serve(ctx)
}

// walSendClient is a single replication protocol client connection
type walSendClient struct {
	server  *walSendServer
	conn    net.Conn
	backend *pgproto3.Backend
	writeMu sync.Mutex
}

func newWalSendClient(server *walSendServer, conn net.Conn) *walSendClient {
	backend := pgproto3.NewBackend(conn, conn)
	backend.SetMaxBodyLen(walSendMaxMessageLen)
	return &walSendClient{server: server, conn: conn, backend: backend}
}

func (c *walSendClient) Serve(ctx context.Context) {
	ready, err := c.handshake()
	if err != nil {
		tracelog.ErrorLogger.Printf("wal-send: handshake with %s failed: %v", c.conn.RemoteAddr(), err)
		return
	}
	if !ready {
		return
	}

	for {
		msg, err := c.backend.Receive()
		if err != nil {
			if ctx.Err() == nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				tracelog.ErrorLogger.Printf("wal-send: read message from %s: %v", c.conn.RemoteAddr(), err)
			}
			return
		}

		switch msg := msg.(type) {
		case *pgproto3.Query:
			c.handleQuery(ctx, msg.String)
		case *pgproto3.Terminate:
			tracelog.DebugLogger.Printf("wal-send: client %s terminated the connection", c.conn.RemoteAddr())
			return
		default:
			tracelog.ErrorLogger.Printf("wal-send: unexpected message type %T from %s",
				msg, c.conn.RemoteAddr())
			return
		}
	}
}

// handshake reads the startup packet and performs trust authentication
func (c *walSendClient) handshake() (bool, error) {
	for {
		msg, err := c.backend.ReceiveStartupMessage()
		if err != nil {
			return false, err
		}
		switch msg := msg.(type) {
		case *pgproto3.SSLRequest, *pgproto3.GSSEncRequest:
			// Encryption negotiation uses a single unframed response byte.
			_, err := c.conn.Write([]byte{'N'})
			if err != nil {
				return false, fmt.Errorf("decline encryption request: %w", err)
			}
		case *pgproto3.CancelRequest:
			return false, nil // no tracked query to cancel; close without a reply
		case *pgproto3.StartupMessage:
			if replication, ok := msg.Parameters["replication"]; !ok || replication == "false" {
				return false, fmt.Errorf("client did not request replication mode")
			}
			return true, c.sendPostStartupMessages()
		}
	}
}

func (c *walSendClient) pgVersionString() string {
	if major := c.server.pgVersion(); major > 0 {
		return fmt.Sprintf("%d.0", major)
	}
	return "16.0"
}

func (c *walSendClient) sendPostStartupMessages() error {
	// BackendKeyData: pid and secret key, both are unused by us
	key := make([]byte, 4)
	_, _ = rand.Read(key)
	return c.writeMessages(
		&pgproto3.AuthenticationOk{},
		&pgproto3.ParameterStatus{Name: "server_version", Value: c.pgVersionString()},
		&pgproto3.ParameterStatus{Name: "integer_datetimes", Value: "on"},
		&pgproto3.ParameterStatus{Name: "server_encoding", Value: "UTF8"},
		&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"},
		&pgproto3.ParameterStatus{Name: "DateStyle", Value: "ISO, MDY"},
		&pgproto3.ParameterStatus{Name: "TimeZone", Value: "UTC"},
		&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"},
		&pgproto3.ParameterStatus{Name: "in_hot_standby", Value: "off"},
		&pgproto3.BackendKeyData{ProcessID: uint32(os.Getpid()), SecretKey: key},
		&pgproto3.ReadyForQuery{TxStatus: 'I'})
}

// handleQuery dispatches a replication command
func (c *walSendClient) handleQuery(ctx context.Context, query string) {
	// real clients often terminate replication commands with a semicolon
	query = strings.TrimRight(query, " \t\n\r;")
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
			&pgproto3.DataRow{Values: [][]byte{[]byte(slotName), []byte("0/0"), nil, nil}},
			&pgproto3.CommandComplete{CommandTag: []byte("CREATE_REPLICATION_SLOT")},
			&pgproto3.ReadyForQuery{TxStatus: 'I'})
	case "DROP_REPLICATION_SLOT":
		_ = c.writeMessages(
			&pgproto3.CommandComplete{CommandTag: []byte("DROP_REPLICATION_SLOT")},
			&pgproto3.ReadyForQuery{TxStatus: 'I'})
	case "START_REPLICATION":
		c.handleStartReplication(ctx, fields)
	case "BASE_BACKUP":
		c.handleBaseBackup(ctx, query)
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
	_ = c.writeMessages(pgRowDescription(name), &pgproto3.DataRow{Values: [][]byte{[]byte(value)}},
		&pgproto3.CommandComplete{CommandTag: []byte("SHOW")}, &pgproto3.ReadyForQuery{TxStatus: 'I'})
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
		&pgproto3.DataRow{Values: [][]byte{[]byte(identify.systemIdentifier),
			[]byte(strconv.FormatUint(uint64(identify.timeline), 10)), []byte(identify.xlogpos.String()), nil}},
		&pgproto3.CommandComplete{CommandTag: []byte("IDENTIFY_SYSTEM")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'})
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
		&pgproto3.DataRow{Values: [][]byte{[]byte(historyName), data}},
		&pgproto3.CommandComplete{CommandTag: []byte("TIMELINE_HISTORY")},
		&pgproto3.ReadyForQuery{TxStatus: 'I'})
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
	c.streamReplication(ctx, stream)
}

func (c *walSendClient) streamReplication(ctx context.Context, stream *walSendStream) {
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	if err := c.writeMessages(&pgproto3.CopyBothResponse{}); err != nil {
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
	var msgs []pgproto3.BackendMessage
	if next != nil {
		msgs = append(msgs, pgRowDescription("next_tli", "next_tli_startpos"),
			&pgproto3.DataRow{Values: [][]byte{[]byte(strconv.FormatUint(uint64(next.timeline), 10)), []byte(next.lsn.String())}})
	}
	// PostgreSQL sends a completion for streaming and for the command itself.
	return c.writeMessages(append(msgs, &pgproto3.CommandComplete{CommandTag: []byte("START_STREAMING")},
		&pgproto3.CommandComplete{CommandTag: []byte("START_REPLICATION")}, &pgproto3.ReadyForQuery{TxStatus: 'I'})...)
}

func (st *walSendStream) finishSending() error {
	c := st.client
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if st.sendingDone {
		return nil
	}
	st.sendingDone = true
	return c.writeMessagesLocked(&pgproto3.CopyDone{})
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
	c, timeline := st.client, st.timelineID
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
			segment, err = st.openStorageSegment(ctx, name)
			if err != nil {
				return nil, fmt.Errorf("read WAL segment %s: %w", name, err)
			}
			if segment == nil {
				if st.nextTimeline != nil && pos == st.nextTimeline.lsn {
					return st.nextTimeline, nil
				}
				st.waitOnePoll(ctx, name)
				continue
			}
		}
		tracelog.InfoLogger.Printf("wal-send: %s: streaming WAL segment %s at %s%s",
			c.conn.RemoteAddr(), name, pos, prefetchSource(fromPrefetch))
		if err := st.streamAndCloseSegment(ctx, segment, pos); err != nil {
			return nil, fmt.Errorf("stream WAL segment %s: %w", name, err)
		}
		if prefetcher != nil {
			prefetcher.remove(name)
		}
	}
}

// A nil segment means the caller should wait, or finish COPY at a newly found fork.
func (st *walSendStream) openStorageSegment(ctx context.Context, name string) (io.ReadCloser, error) {
	segment, err := internal.DownloadAndDecompressStorageFile(ctx, st.client.server.reader, name)
	if _, missing := err.(internal.ArchiveNonExistenceError); missing {
		return st.openTimelinePrefix(ctx, name)
	}
	return segment, err
}

func (st *walSendStream) openTimelinePrefix(ctx context.Context, name string) (io.ReadCloser, error) {
	reader := st.client.server.reader
	pos := st.pos()
	if st.nextTimeline == nil {
		// A boundary promotion has no .partial marker. Keep discovering later
		// timelines through the server's shared, slower archive-identity cache.
		next, err := st.findSuccessorTimeline(ctx, st.timelineID, pos)
		if err != nil {
			return nil, err
		}
		st.nextTimeline = next
	}
	if st.nextTimeline == nil || pos == st.nextTimeline.lsn {
		return nil, nil
	}
	// Both the partial file and the descendant's complete segment contain
	// the ancestor's bytes before the validated fork.
	segment, err := internal.DownloadAndDecompressStorageFile(ctx, reader, name+".partial")
	if _, missing := err.(internal.ArchiveNonExistenceError); missing {
		segNo := uint64(pos) / WalSegmentSize
		segment, err = internal.DownloadAndDecompressStorageFile(ctx, reader,
			formatWALFileName(st.nextTimeline.sourceTimeline, segNo))
	}
	if _, missing := err.(internal.ArchiveNonExistenceError); missing {
		return nil, nil
	}
	return segment, err
}

func (st *walSendStream) streamAndCloseSegment(ctx context.Context, segment io.ReadCloser, pos LSN) error {
	segNo := uint64(pos) / WalSegmentSize
	offset := uint64(pos) % WalSegmentSize
	end := WalSegmentSize
	if st.nextTimeline != nil {
		end = min(end, uint64(st.nextTimeline.lsn)-segNo*WalSegmentSize)
	}
	err := st.streamSegment(ctx, segment, offset, end)
	closeErr := segment.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	st.setPos(LSN(segNo*WalSegmentSize + end))
	return nil
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
		msg, err := c.backend.Receive()
		if err != nil {
			if ctx.Err() == nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				tracelog.ErrorLogger.Printf("wal-send: read a copy message from %s: %v", c.conn.RemoteAddr(), err)
			}
			return
		}
		switch msg := msg.(type) {
		case *pgproto3.CopyData:
			if len(msg.Data) < 1 {
				return
			}
			st.handleCopyData(msg.Data)
		case *pgproto3.CopyDone:
			tracelog.DebugLogger.Printf("wal-send: %s sent CopyDone", c.conn.RemoteAddr())
			select {
			case copyDone <- struct{}{}:
			default:
			}
			return
		case *pgproto3.CopyFail:
			tracelog.DebugLogger.Printf("wal-send: %s sent CopyFail", c.conn.RemoteAddr())
			return
		case *pgproto3.Terminate:
			tracelog.DebugLogger.Printf("wal-send: %s terminated the connection", c.conn.RemoteAddr())
			return
		default:
			tracelog.WarningLogger.Printf("wal-send: unexpected copy message type %T from %s",
				msg, c.conn.RemoteAddr())
		}
	}
}

// handleCopyData processes a single copy data message from the client.
// In physical replication the only expected message is the standby status update.
func (st *walSendStream) handleCopyData(payload []byte) {
	c := st.client
	if payload[0] != pglogrepl.StandbyStatusUpdateByteID {
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

// pgRowDescription describes the text columns used by replication commands.
func pgRowDescription(columnNames ...string) *pgproto3.RowDescription {
	fields := make([]pgproto3.FieldDescription, len(columnNames))
	for i, name := range columnNames {
		fields[i] = pgproto3.FieldDescription{
			Name: []byte(name), DataTypeOID: pgtype.TextOID, DataTypeSize: -1,
			TypeModifier: -1, Format: pgproto3.TextFormat,
		}
	}
	return &pgproto3.RowDescription{Fields: fields}
}

// writeQueryError finishes a failed simple query with ErrorResponse and ReadyForQuery.
func (c *walSendClient) writeQueryError(code, message string) error {
	return c.writeMessages(&pgproto3.ErrorResponse{
		Severity: "ERROR", SeverityUnlocalized: "ERROR", Code: code, Message: message,
	}, &pgproto3.ReadyForQuery{TxStatus: 'I'})
}

// pgproto3 frames CopyData; its replication-specific payload is opaque to it.
// Physical XLogData has no timeline field, unlike logical replication messages.
func pgXLogDataMsg(walStart, walEnd LSN, data []byte) *pgproto3.CopyData {
	body := make([]byte, 0, 25+len(data))
	body = append(body, pglogrepl.XLogDataByteID)
	body = binary.BigEndian.AppendUint64(body, uint64(walStart))
	body = binary.BigEndian.AppendUint64(body, uint64(walEnd))
	body = binary.BigEndian.AppendUint64(body, currentPgTime())
	body = append(body, data...)
	return &pgproto3.CopyData{Data: body}
}

func pgKeepaliveMsg(walEnd LSN, replyRequested bool) *pgproto3.CopyData {
	body := make([]byte, 0, 18)
	body = append(body, pglogrepl.PrimaryKeepaliveMessageByteID)
	body = binary.BigEndian.AppendUint64(body, uint64(walEnd))
	body = binary.BigEndian.AppendUint64(body, currentPgTime())
	if replyRequested {
		body = append(body, 1)
	} else {
		body = append(body, 0)
	}
	return &pgproto3.CopyData{Data: body}
}

// currentPgTime returns microseconds since 2000-01-01, as replication requires.
func currentPgTime() uint64 {
	return uint64(time.Since(pgUnixEpoch).Microseconds())
}

// writeMessages serializes each Send/Flush batch, including keepalive replies.
func (c *walSendClient) writeMessages(msgs ...pgproto3.BackendMessage) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.writeMessagesLocked(msgs...)
}

func (c *walSendClient) writeMessagesLocked(msgs ...pgproto3.BackendMessage) error {
	for _, msg := range msgs {
		c.backend.Send(msg)
	}
	if err := c.backend.Flush(); err != nil {
		return fmt.Errorf("flush protocol messages: %w", err)
	}
	return nil
}

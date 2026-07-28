package mysql

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/server"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// Handler is the go-mysql replication handler for one replica connection.
// It implements both server.ReplicationHandler (go-mysql interface) and
// the internal binlogHandler interface (fetchLogs callback)
type Handler struct {
	server.EmptyReplicationHandler
	ctx           context.Context //nolint:containedctx // detached binlog replication server outlives any request
	cancel        context.CancelFunc
	replicaSource string
	rootFolder    storage.Folder
	dstDir        string
	startTS       time.Time
	untilTS       time.Time
	endBinlogTS   time.Time

	// streamer is the go-mysql event queue for the current replica connection.
	// It is set once in HandleBinlogDump / HandleBinlogDumpGTID before the
	// streaming goroutine starts, so there is no concurrent write.
	streamer *replication.BinlogStreamer

	// requiredGTIDs is the replica's already-executed set from
	// COM_BINLOG_DUMP_GTID; transactions it contains are skipped. This
	// command is MySQL-only; MariaDB replicas negotiate GTID state via
	// session variables and COM_BINLOG_DUMP, which is not wired up here.
	sentGTIDs      mysql.GTIDSet
	requiredGTIDs  *mysql.MysqlGTIDSet
	skipCurrentTxn bool

	// --- streaming pipeline fields (set by initStreaming, used by fetchLogs) ---

	// parser parses raw binlog files and forwards events to handleEvent.
	parser *replication.BinlogParser
	// startPos is the replica's initial COM_BINLOG_DUMP position; honored as
	// the parse offset only on the first file, subsequent files start at 4.
	startPos mysql.Position
	// firstFile gates the first-file offset (startPos.Pos vs 4).
	firstFile bool
	// logCh is a buffered channel used to pipeline S3 downloads and streaming:
	// fetchLogs sends paths here (handleBinlog) while streamWorker processes them.
	logCh chan string
	// errCh receives the first streaming error from streamWorker, or is closed
	// on success, allowing wait() to collect the result.
	errCh chan error
}

func newHandler(ctx context.Context, replicaSource string, root storage.Folder, dst string,
	startTS, untilTS, endBinlogTS time.Time) *Handler {
	ctx, cancel := context.WithCancel(ctx)
	sent, _ := mysql.ParseGTIDSet(mysql.MySQLFlavor, "")
	return &Handler{
		ctx:           ctx,
		cancel:        cancel,
		replicaSource: replicaSource,
		rootFolder:    root,
		dstDir:        dst,
		startTS:       startTS,
		untilTS:       untilTS,
		endBinlogTS:   endBinlogTS,
		sentGTIDs:     sent,
	}
}

func (h *Handler) handleEventError(err error) {
	if err == nil {
		return
	}
	tracelog.ErrorLogger.Println("Error during replication", err)
	ok := h.streamer.AddErrorToStreamer(err)
	for !ok {
		ok = h.streamer.AddErrorToStreamer(err)
	}
}

// https://github.com/percona/percona-server/blob/8.0/libbinlogevents/include/control_events.h#L53-L108
func (h *Handler) addRotateEvent(pos mysql.Position) error {
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

	return h.streamer.AddEventToStreamer(&rotateBinlogEvent)
}

func (h *Handler) waitReplicationIsDoneSafe() {
	if h.sentGTIDs.IsEmpty() {
		tracelog.InfoLogger.Println("S3 objects finished. No GTIDs were sent. Shutting down immediately.")
		os.Exit(0)
	}

	tracelog.InfoLogger.Printf("All S3 binlogs processed. Waiting for replica to catch up to GTID: %s", h.sentGTIDs.String())

	dsn, err := parseMySQLDatasource(h.replicaSource)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("Failed to parse replica datasource: %v", err)
	}
	var conn *client.Conn
	connCount := 0
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()

	for {
		if h.ctx.Err() != nil {
			tracelog.WarningLogger.Println("Client disconnected while waiting for completion. Handler shutting down, awaiting reconnect...")
			return
		}

		if conn == nil {
			if conn, err = connectMySQL(h.ctx, dsn, ""); err != nil {
				connCount++
				if connCount >= 10 {
					tracelog.ErrorLogger.Fatalf("Failed to connect to replica SQL 10 times, giving up: %v", err)
				} else if connCount > 1 {
					tracelog.WarningLogger.Printf("Failed to connect to replica SQL (times: %d): %v", connCount, err)
				} else {
					tracelog.WarningLogger.Printf("Failed to connect to replica SQL: %v", err)
				}
				time.Sleep(1 * time.Second)
				continue
			}
			connCount = 0
		}

		r, err := conn.Execute("SELECT @@global.gtid_executed")
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to query replica GTID state: %v", err)
			conn.Close()
			conn = nil
			time.Sleep(1 * time.Second)
			continue
		}
		executedStr, _ := r.GetString(0, 0)
		r.Close()

		replicaSet, _ := mysql.ParseGTIDSet("mysql", executedStr)
		if replicaSet != nil && replicaSet.Contain(h.sentGTIDs) {
			tracelog.InfoLogger.Println("Replica has successfully caught up! We are safely done.")
			os.Exit(0)
		}

		time.Sleep(1 * time.Second)
	}
}

// handleEvent is the per-event callback passed to BinlogParser.ParseFile.
func (h *Handler) handleEvent(e *replication.BinlogEvent) error {
	if h.ctx.Err() != nil {
		return h.ctx.Err()
	}
	if int64(e.Header.Timestamp) > h.untilTS.Unix() {
		return nil
	}
	switch e.Header.EventType {
	case replication.GTID_EVENT:
		if h.decideSkipForGTID(e) {
			return nil
		}
	case replication.ANONYMOUS_GTID_EVENT, replication.GTID_TAGGED_LOG_EVENT,
		replication.FORMAT_DESCRIPTION_EVENT, replication.PREVIOUS_GTIDS_EVENT,
		replication.ROTATE_EVENT, replication.STOP_EVENT, replication.INCIDENT_EVENT:
		// txn boundary or file-boundary marker; never appears inside a txn
		h.skipCurrentTxn = false
	default:
		if h.skipCurrentTxn {
			return nil
		}
	}
	return h.streamer.AddEventToStreamer(e)
}

// decideSkipForGTID updates skip state from a GTID_EVENT; returns true if
// the caller should drop the event because the replica already applied it.
func (h *Handler) decideSkipForGTID(e *replication.BinlogEvent) bool {
	h.skipCurrentTxn = false
	ge := &replication.GTIDEvent{}
	if ge.Decode(e.RawData[replication.EventHeaderSize:]) != nil {
		return false
	}
	one, err := ge.GTIDNext()
	if err != nil {
		return false
	}
	if h.requiredGTIDs != nil && h.requiredGTIDs.Contain(one) {
		tracelog.DebugLogger.Printf("Skipping already-applied transaction %s", one)
		h.skipCurrentTxn = true
		return true
	}
	if err := h.sentGTIDs.Update(one.String()); err != nil {
		tracelog.WarningLogger.Printf("Failed to record sent GTID %s: %v", one, err)
	}
	return false
}

// initStreaming initializes the pipeline fields on h and starts the streamWorker
// worker goroutine. It must be called once per connection, before fetchLogs.
func (h *Handler) initStreaming(startPos mysql.Position) {
	p := replication.NewBinlogParser()
	p.SetRawMode(true)
	p.SetFlavor(mysql.MySQLFlavor)
	p.SetVerifyChecksum(true)
	h.parser = p
	h.startPos = startPos
	h.firstFile = true
	h.logCh = make(chan string, binlogFetchAhead)
	h.errCh = make(chan error, 1)
	go h.streamWorker()
}

// streamWorker reads downloaded binlog paths from logCh and streams them to
// the replica. It runs until logCh is closed (by wait) or a streaming error
// occurs. It is the sole goroutine that accesses parser/startPos/firstFile.
func (h *Handler) streamWorker() {
	defer close(h.errCh)
	for binlogPath := range h.logCh {
		if err := h.streamLog(binlogPath); err != nil {
			tracelog.ErrorLogger.Printf("Error during file streaming %s: %v", path.Base(binlogPath), err)
			h.errCh <- err
			for p := range h.logCh {
				// clean up
				os.Remove(p)
			}
			return
		}
	}
}

// streamLog parses one downloaded binlog and pushes its events to the replica.
// The first file uses the replica's requested position; later files start at
// offset 4 (the binlog magic header).
func (h *Handler) streamLog(binlogPath string) error {
	defer os.Remove(binlogPath)

	if h.ctx.Err() != nil {
		return h.ctx.Err()
	}

	offset := int64(4)
	if h.firstFile {
		offset = int64(h.startPos.Pos)
		h.firstFile = false
	}

	tracelog.InfoLogger.Printf("Streaming %s to replica", path.Base(binlogPath))
	err := h.parser.ParseFile(binlogPath, offset, h.handleEvent)
	return err
}

func (h *Handler) handleBinlog(binlogPath string) error {
	select {
	case err := <-h.errCh:
		// streamWorker already failed; stop feeding it.
		return err
	case h.logCh <- binlogPath:
		return nil
	}
}

func (h *Handler) wait() error {
	close(h.logCh)
	return <-h.errCh
}

func (h *Handler) streamBinlogFiles(startPos mysql.Position) {
	err := os.MkdirAll(h.dstDir, 0777)
	tracelog.ErrorLogger.FatalfOnError("Failed to make dst dir: %v", err)

	if err := h.addRotateEvent(startPos); err != nil {
		tracelog.ErrorLogger.Printf("Error while sending rotate event: %v", err)
		h.handleEventError(err)
		return
	}

	h.initStreaming(startPos)
	tracelog.InfoLogger.Printf("Start event streaming")

	err = fetchLogs(h.ctx, h.rootFolder, h.dstDir, h.startTS, h.untilTS, h.endBinlogTS, h)
	if err != nil {
		tracelog.ErrorLogger.Printf("Error during logs streaming: %v", err)
		_ = h.wait()
		h.handleEventError(err)
		return
	}

	tracelog.InfoLogger.Printf("Log fetching finished, waiting for streaming to finish")
	err = h.wait()
	if err != nil {
		tracelog.ErrorLogger.Printf("Error during logs streaming: %v", err)
		h.handleEventError(err)
		return
	}

	h.waitReplicationIsDoneSafe()
}

func (h *Handler) HandleRegisterSlave(data []byte) error {
	return nil
}

func (h *Handler) HandleBinlogDump(pos mysql.Position) (*replication.BinlogStreamer, error) {
	tracelog.InfoLogger.Printf("HandleBinlogDump: requested position %s:%d", pos.Name, pos.Pos)
	h.streamer = replication.NewBinlogStreamer()
	go h.streamBinlogFiles(pos)
	return h.streamer, nil
}

func (h *Handler) HandleBinlogDumpGTID(gtidSet *mysql.MysqlGTIDSet) (*replication.BinlogStreamer, error) {
	tracelog.InfoLogger.Printf("HandleBinlogDumpGTID: GTID=%s", gtidSet.String())
	h.requiredGTIDs = gtidSet
	h.streamer = replication.NewBinlogStreamer()
	go h.streamBinlogFiles(mysql.Position{Name: "host-binlog-file", Pos: 4})
	return h.streamer, nil
}

func (h *Handler) HandleQuery(query string) (*mysql.Result, error) {
	switch strings.ToLower(query) {
	case "select @master_binlog_checksum":
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"master_binlog_checksum"}, [][]interface{}{{"CRC32"}})
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil
	case "select @source_binlog_checksum":
		// "1" - CRC algorithm from zlib
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"source_binlog_checksum"}, [][]interface{}{{"1"}})
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil
	case "show global variables like 'binlog_checksum'":
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"BINLOG_CHECKSUM"}, [][]interface{}{{"CRC32"}})
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil
	case "select @@global.server_id":
		serverID, err := conf.GetRequiredSetting(conf.MysqlBinlogServerID)
		tracelog.ErrorLogger.FatalOnError(err)
		resultSet, err := mysql.BuildSimpleTextResultset([]string{"SERVER_ID"}, [][]interface{}{{serverID}})
		tracelog.ErrorLogger.FatalOnError(err)
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil
	case "select @@global.gtid_mode":
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"GTID_MODE"}, [][]interface{}{{"ON"}})
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil
	case "select @@global.server_uuid":
		// the server uuid received by the query does not affect replication.
		// during replication, the uuid is taken from events
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"SERVER_UUID"}, [][]interface{}{{"0"}})
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil
	case "select @@global.rpl_semi_sync_master_enabled":
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"@@global.rpl_semi_sync_master_enabled"}, [][]interface{}{{"0"}})
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil
	case "select @@global.rpl_semi_sync_source_enabled":
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"@@global.rpl_semi_sync_source_enabled"}, [][]interface{}{{"0"}})
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil
	default:
		tracelog.DebugLogger.Printf("Unhandled query: %s", query)
		return nil, nil
	}
}

func HandleBinlogServer(ctx context.Context, since string, until string, untilBinlogLastModified string) {
	// get necessary settings
	st, err := internal.ConfigureStorage(ctx)
	tracelog.ErrorLogger.FatalOnError(err)
	startTS, untilTS, endBinlogTS, err := getTimestamps(ctx, st.RootFolder(), since, until, untilBinlogLastModified)
	tracelog.ErrorLogger.FatalOnError(err)

	// validate WALG_MYSQL_BINLOG_SERVER_REPLICA_SOURCE
	replicaSource, err := conf.GetRequiredSetting(conf.MysqlBinlogServerReplicaSource)
	tracelog.ErrorLogger.FatalOnError(err)
	_, err = parseMySQLDatasource(replicaSource)
	tracelog.ErrorLogger.FatalOnError(err)

	dstDir, err := internal.GetLogsDstSettings(conf.MysqlBinlogDstSetting)
	tracelog.ErrorLogger.FatalOnError(err)

	serverAddress, err := conf.GetRequiredSetting(conf.MysqlBinlogServerHost)
	tracelog.ErrorLogger.FatalOnError(err)
	serverPort, err := conf.GetRequiredSetting(conf.MysqlBinlogServerPort)
	tracelog.ErrorLogger.FatalOnError(err)

	l, err := net.Listen("tcp", serverAddress+":"+serverPort)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.InfoLogger.Printf("Listening on %s, wait connection", l.Addr())

	srv := server.NewServer("5.7.42", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, nil, nil)
	// This loop continues accepting connections until the process exits.
	// It will be terminated by os.Exit() call in waitReplicationIsDoneSafe.
	for {
		c, err := l.Accept()
		if err != nil {
			tracelog.ErrorLogger.Printf("Error accepting connection: %v", err)
			continue
		}
		tracelog.InfoLogger.Printf("Connection accepted from %s", c.RemoteAddr())

		user, err := conf.GetRequiredSetting(conf.MysqlBinlogServerUser)
		if err != nil {
			tracelog.ErrorLogger.Printf("Config error: %v", err)
			c.Close()
			continue
		}
		password, err := conf.GetRequiredSetting(conf.MysqlBinlogServerPassword)
		if err != nil {
			tracelog.ErrorLogger.Printf("Config error: %v", err)
			c.Close()
			continue
		}

		go handleBinlogConnection(ctx, c, srv, replicaSource, st.RootFolder(), dstDir,
			startTS, untilTS, endBinlogTS, user, password)
	}
}

func handleBinlogConnection(
	ctx context.Context,
	c net.Conn,
	srv *server.Server,
	replicaSource string,
	folder storage.Folder,
	dstDir string,
	startTS, untilTS, endBinlogTS time.Time,
	user string,
	password string,
) {
	h := newHandler(ctx, replicaSource, folder, dstDir, startTS, untilTS, endBinlogTS)
	defer func() {
		h.cancel()
		c.Close()
		tracelog.InfoLogger.Printf("Client disconnected, waiting for new connection...")
	}()

	authHandler := server.NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)
	if errAuth := authHandler.AddUser(user, password); errAuth != nil {
		tracelog.ErrorLogger.Printf("Failed to set user auth: %v", errAuth)
		return
	}

	conn, err := srv.NewCustomizedConn(c, authHandler, h)
	if err != nil {
		if strings.Contains(err.Error(), "EOF") || strings.Contains(err.Error(), "bad") {
			tracelog.WarningLogger.Printf("Handshake dropped (network issue/proxy): %v", err)
		} else {
			tracelog.ErrorLogger.Printf("Error creating connection: %v", err)
		}
		return
	}
	defer conn.Close()

	for {
		if err := conn.HandleCommand(); err != nil {
			tracelog.WarningLogger.Printf("Connection closed: %v", err)
			return
		}
	}
}

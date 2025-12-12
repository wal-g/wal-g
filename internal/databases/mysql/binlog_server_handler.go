package mysql

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/server"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

var (
	startTS      time.Time
	untilTS      time.Time
	lastSentGTID string
)

type Handler struct {
	server.EmptyReplicationHandler
	globalStreamer *replication.BinlogStreamer
	activeConn     *server.Conn
	activeCtx      context.Context
	streamerMutex  sync.Mutex
	// FOR TEST!!!
	producerRunning bool
	producerWg      sync.WaitGroup

	replicationDone bool
	doneMutex       sync.Mutex
	connectionCount int
	lastGTIDSet     *mysql.MysqlGTIDSet
	gtidMutex       sync.Mutex

	streamerClosed bool
	lastPosition   mysql.Position

	waitingForReconnection bool
	reconnectionTimeout    time.Duration
	lastDisconnectTime     time.Time
	disconnectMutex        sync.Mutex

	connectionStableTime  time.Time
	connectionStableMutex sync.Mutex
	minStableTime         time.Duration

	producerCtx    context.Context
	producerCancel context.CancelFunc
	producerMutex  sync.Mutex

	// New fields for better reconnection handling
	currentBinlogFile string
	currentBinlogPos  uint32
	stateMutex        sync.RWMutex
	lastEventTime     time.Time
}

func addRotateEvent(s *replication.BinlogStreamer, pos mysql.Position) error {
	serverID, err := conf.GetRequiredSetting(conf.MysqlBinlogServerID)
	if err != nil {
		return err
	}
	ServerIDNum, err := strconv.Atoi(serverID)
	if err != nil {
		return err
	}

	// create rotate event
	rotateBinlogEvent := replication.BinlogEvent{}

	messageBodySize := 8 + len(pos.Name) + 1
	eventLength := replication.EventHeaderSize + messageBodySize + replication.BinlogChecksumLength

	rotateBinlogEvent.RawData = make([]byte, eventLength)

	binlogEventPos := 4
	// type - 1 byte
	rotateBinlogEvent.RawData[binlogEventPos] = byte(replication.ROTATE_EVENT)
	binlogEventPos++
	// server_id- 4 bytes
	binary.LittleEndian.PutUint32(rotateBinlogEvent.RawData[binlogEventPos:], uint32(ServerIDNum))
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

	return s.AddEventToStreamer(&rotateBinlogEvent)
}

func handleEventError(err error, s *replication.BinlogStreamer) {
	if err == nil {
		return
	}
	tracelog.ErrorLogger.Println("Error during replication", err)
	_ = s.AddErrorToStreamer(err)
}

func waitReplicationIsDone(ctx context.Context) error {
	replicaSource, err := conf.GetRequiredSetting(conf.MysqlBinlogServerReplicaSource)
	if err != nil {
		return err
	}
	db, err := sql.Open("mysql", replicaSource)
	if err != nil {
		return err
	}
	defer func(db *sql.DB) {
		_ = db.Close()
	}(db)

	for {
		select {
		case <-ctx.Done():
			tracelog.InfoLogger.Println("waitReplicationIsDone cancelled because client disconnected.")
			return ctx.Err()
		default:
		}

		var gtidExecuted string
		err = db.QueryRowContext(ctx, "SELECT @@GLOBAL.GTID_EXECUTED").Scan(&gtidExecuted)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				tracelog.InfoLogger.Printf("SQL query cancelled during waitReplicationIsDone: %v", err)
			}
			return err
		}
		gtidSet, err := mysql.ParseGTIDSet("mysql", gtidExecuted)

		if err != nil {
			return err
		}

		lastSentGTIDSet, err := mysql.ParseGTIDSet("mysql", lastSentGTID)
		if err != nil {
			return err
		}

		tracelog.DebugLogger.Printf("Expected GTID set: %v; MySQL GTID set: %v", lastSentGTIDSet.String(), gtidSet.String())

		if gtidSet.Contain(lastSentGTIDSet) {
			tracelog.InfoLogger.Println("Replication is done, replica has applied all events.")
			return nil
		}

		select {
		case <-time.After(1 * time.Second):
		case <-ctx.Done():
			tracelog.InfoLogger.Println("waitReplicationIsDone cancelled during sleep because client disconnected.")
			return ctx.Err()
		}
	}
}

func (h *Handler) updateState(binlogFile string, binlogPos uint32, gtidSet *mysql.MysqlGTIDSet) {
	h.stateMutex.Lock()
	defer h.stateMutex.Unlock()

	h.currentBinlogFile = binlogFile
	h.currentBinlogPos = binlogPos
	h.lastEventTime = time.Now()

	if gtidSet != nil {
		h.gtidMutex.Lock()
		h.lastGTIDSet = gtidSet
		h.gtidMutex.Unlock()
	}
}

func (h *Handler) getState() (string, uint32, *mysql.MysqlGTIDSet, time.Time) {
	h.stateMutex.RLock()
	defer h.stateMutex.RUnlock()

	h.gtidMutex.Lock()
	gtidSet := h.lastGTIDSet
	h.gtidMutex.Unlock()

	return h.currentBinlogFile, h.currentBinlogPos, gtidSet, h.lastEventTime
}

func (h *Handler) sendEventsFromBinlogFiles(logFilesProvider *storage.ObjectProvider, pos mysql.Position, s *replication.BinlogStreamer, requestedGtidSet *mysql.MysqlGTIDSet) {
	h.producerWg.Add(1)
	defer func() {
		tracelog.InfoLogger.Println("Producer goroutine is shutting down. Signaling end of stream to the client...")
		s.AddErrorToStreamer(io.EOF)

		h.streamerMutex.Lock()
		h.producerRunning = false
		h.streamerClosed = true
		h.streamerMutex.Unlock()

		tracelog.InfoLogger.Println("Binlog streaming goroutine finished and producer flag is reset.")
		h.producerWg.Done()
	}()

	h.producerMutex.Lock()
	producerCtx := h.producerCtx
	h.producerMutex.Unlock()

	p := replication.NewBinlogParser()
	p.SetRawMode(true)
	p.SetFlavor(mysql.MySQLFlavor)
	p.SetVerifyChecksum(true)

	var streamStarted bool = false
	var currentTransactionGTID mysql.GTIDSet
	var shouldStreamCurrentTransaction bool = requestedGtidSet == nil

	f := func(e *replication.BinlogEvent) error {
		select {
		case <-producerCtx.Done():
			tracelog.InfoLogger.Println("Producer context cancelled, stopping event processing")
			return io.EOF
		default:
		}

		for {
			select {
			case <-producerCtx.Done():
				tracelog.InfoLogger.Println("Producer context cancelled during pause")
				return io.EOF
			default:
			}

			h.streamerMutex.Lock()
			hasActiveConn := h.activeConn != nil && h.activeCtx.Err() == nil
			closed := h.streamerClosed
			h.streamerMutex.Unlock()

			if closed {
				tracelog.InfoLogger.Println("Streamer is closed, stopping event processing")
				return io.EOF
			}

			if hasActiveConn {
				break
			}

			tracelog.DebugLogger.Println("No active connection, pausing event streaming")
			time.Sleep(500 * time.Millisecond)
		}

		if !streamStarted {
			if e.Header.EventType == replication.FORMAT_DESCRIPTION_EVENT || e.Header.EventType == replication.PREVIOUS_GTIDS_EVENT {
				tracelog.DebugLogger.Printf("Streaming initial metadata event: %s", e.Header.EventType)
				if e.Header.EventType == replication.PREVIOUS_GTIDS_EVENT {
					streamStarted = true
				}
				return s.AddEventToStreamer(e)
			}
			return nil
		}

		if e.Header.EventType == replication.FORMAT_DESCRIPTION_EVENT || e.Header.EventType == replication.PREVIOUS_GTIDS_EVENT {
			tracelog.DebugLogger.Printf("Skipping subsequent metadata event: %s", e.Header.EventType)
			return nil
		}

		if e.Header.EventType == replication.GTID_EVENT {
			gtidEvent := new(replication.GTIDEvent)
			if err := gtidEvent.Decode(e.RawData[replication.EventHeaderSize:]); err != nil {
				tracelog.ErrorLogger.Printf("Could not decode GTID event: %v", err)
				return err
			}

			u, _ := uuid.FromBytes(gtidEvent.SID)
			currentGtidStr := u.String() + ":" + strconv.FormatInt(gtidEvent.GNO, 10)

			var err error
			currentTransactionGTID, err = mysql.ParseGTIDSet(mysql.MySQLFlavor, currentGtidStr)
			if err != nil {
				return err
			}

			if requestedGtidSet != nil && requestedGtidSet.Contain(currentTransactionGTID) {
				shouldStreamCurrentTransaction = false
				tracelog.DebugLogger.Printf("Skipping already known GTID: %s", currentGtidStr)
			} else {
				shouldStreamCurrentTransaction = true
				tracelog.DebugLogger.Printf("Streaming new GTID: %s", currentGtidStr)
			}
		}

		if shouldStreamCurrentTransaction {
			if untilTS.Unix() > 0 && int64(e.Header.Timestamp) > untilTS.Unix() {
				tracelog.InfoLogger.Printf("Reached until_ts target. Stopping stream.")
				shouldStreamCurrentTransaction = false
				return io.EOF
			}
			if currentTransactionGTID != nil {
				lastSentGTID = currentTransactionGTID.String()
				h.updateState("", e.Header.LogPos, currentTransactionGTID.(*mysql.MysqlGTIDSet))
			}

			return s.AddEventToStreamer(e)
		}
		return nil
	}

	dstDir, _ := internal.GetLogsDstSettings(conf.MysqlBinlogDstSetting)

	for {
		select {
		case <-producerCtx.Done():
			tracelog.InfoLogger.Println("Producer context cancelled, exiting producer main loop")
			return
		default:
		}

		logFile, err := logFilesProvider.GetObject()

		if errors.Is(err, storage.ErrNoMoreObjects) {
			tracelog.InfoLogger.Println("All binlog files have been streamed. Now waiting for the replica to apply all events.")
			time.Sleep(3 * time.Second)

			for {
				h.waitForStableConnection()
				h.streamerMutex.Lock()
				ctx := h.activeCtx
				h.streamerMutex.Unlock()

				if ctx == nil {
					tracelog.InfoLogger.Println("No active connection to verify replica status. Waiting for reconnection.")
					if h.waitForReconnectionOrTimeout() {
						continue
					} else {
						tracelog.WarningLogger.Println("Reconnection timed out. Cannot verify replica status. Shutting down producer.")
						return
					}
				}

				errWait := waitReplicationIsDone(ctx)
				if errWait != nil {
					if errors.Is(errWait, context.Canceled) {
						tracelog.InfoLogger.Printf("Client disconnected during final wait. Waiting for reconnection.")
						if h.waitForReconnectionOrTimeout() {
							continue
						}
					} else {
						tracelog.ErrorLogger.Printf("Failed to confirm replica status: %v. Waiting for reconnection.", errWait)
						h.waitForReconnectionOrTimeout()
					}
					tracelog.WarningLogger.Println("Could not verify replica status due to errors or timeouts. Shutting down producer.")
					return
				}
				break
			}

			tracelog.InfoLogger.Println("Replica has successfully applied all required binlogs. Replication is complete. Shutting down producer.")
			h.doneMutex.Lock()
			h.replicationDone = true
			h.doneMutex.Unlock()
			return
		}

		if err != nil {
			tracelog.ErrorLogger.Printf("FATAL: Error getting binlog file from provider: %v", err)
			return
		}

		binlogName := utility.TrimFileExtension(logFile.GetName())
		newPosition := mysql.Position{Name: binlogName, Pos: 4}
		tracelog.InfoLogger.Printf("Switching to and streaming binlog file %s", binlogName)
		binlogPath := path.Join(dstDir, binlogName)

		if err := addRotateEvent(s, newPosition); err != nil {
			tracelog.ErrorLogger.Printf("Failed to send RotateEvent for %s: %v", binlogName, err)
		}

		h.updateState(binlogName, uint32(pos.Pos), nil)

		startOffset := int64(pos.Pos)
		if pos.Name != "" && pos.Name != newPosition.Name {
			startOffset = 4
		}

		err = p.ParseFile(binlogPath, startOffset, f)

		if err != nil && !errors.Is(err, io.EOF) {
			tracelog.InfoLogger.Printf("Parsing of %s stopped due to a client disconnect or stream error: %v", binlogPath, err)
		}

		_ = os.Remove(binlogPath)
		pos = newPosition
	}
}

func (h *Handler) waitForStableConnection() {
	maxWait := 30 * time.Second
	checkInterval := 500 * time.Millisecond
	deadline := time.Now().Add(maxWait)

	for time.Now().Before(deadline) {
		h.streamerMutex.Lock()
		hasActiveConn := h.activeConn != nil && h.activeCtx != nil
		h.streamerMutex.Unlock()

		if hasActiveConn {
			h.connectionStableMutex.Lock()
			isStable := time.Since(h.connectionStableTime) >= h.minStableTime
			h.connectionStableMutex.Unlock()

			if isStable {
				tracelog.InfoLogger.Println("Stable connection found, continuing...")
				return
			}
		}

		tracelog.DebugLogger.Printf("Waiting for stable connection... (remaining: %v)", time.Until(deadline))
		time.Sleep(checkInterval)
	}

	tracelog.InfoLogger.Printf("No stable connection found within %v", maxWait)
}

func (h *Handler) waitForReconnectionOrTimeout() bool {
	h.disconnectMutex.Lock()
	h.waitingForReconnection = true
	h.lastDisconnectTime = time.Now()
	if h.reconnectionTimeout == 0 {
		h.reconnectionTimeout = 60 * time.Second // Default timeout
	}
	timeout := h.reconnectionTimeout
	h.disconnectMutex.Unlock()

	tracelog.InfoLogger.Printf("Waiting for reconnection within %v...", timeout)

	deadline := time.Now().Add(timeout)
	checkInterval := 1 * time.Second

	for time.Now().Before(deadline) {
		h.streamerMutex.Lock()
		hasActiveConn := h.activeConn != nil && h.activeCtx != nil
		h.streamerMutex.Unlock()

		if hasActiveConn {
			h.connectionStableMutex.Lock()
			isStable := time.Since(h.connectionStableTime) >= h.minStableTime
			h.connectionStableMutex.Unlock()

			if isStable {
				h.disconnectMutex.Lock()
				h.waitingForReconnection = false
				h.disconnectMutex.Unlock()
				tracelog.InfoLogger.Println("Client reconnected successfully and connection is stable!")
				return true
			}
		}

		time.Sleep(checkInterval)
	}

	h.disconnectMutex.Lock()
	h.waitingForReconnection = false
	h.disconnectMutex.Unlock()
	tracelog.InfoLogger.Printf("Reconnection timeout reached (%v). Producer will exit.", timeout)
	return false
}

func (h *Handler) getOrCreateStreamer(pos mysql.Position, gtidSet *mysql.MysqlGTIDSet, isGTID bool) (*replication.BinlogStreamer, error) {
	h.streamerMutex.Lock()
	defer h.streamerMutex.Unlock()

	h.doneMutex.Lock()
	isDone := h.replicationDone
	h.doneMutex.Unlock()
	if isDone {
		tracelog.InfoLogger.Println("Replication already completed, refusing new connection.")
		return nil, errors.New("replication completed")
	}

	h.connectionStableMutex.Lock()
	h.connectionStableTime = time.Now()
	h.connectionStableMutex.Unlock()

	h.gtidMutex.Lock()
	if h.connectionCount > 0 && h.lastGTIDSet != nil {
		tracelog.InfoLogger.Printf("Reconnection detected (connection #%d). Client requests GTID: %s. Last sent GTID by server was: %s.",
			h.connectionCount+1, gtidSet.String(), h.lastGTIDSet.String())
	}
	h.connectionCount++
	h.gtidMutex.Unlock()

	if h.producerRunning && h.globalStreamer != nil {
		tracelog.InfoLogger.Println("Reconnecting to existing binlog stream.")
		return h.globalStreamer, nil
	}

	tracelog.InfoLogger.Println("First connection or producer not running. Starting a new, persistent binlog stream.")

	h.globalStreamer = replication.NewBinlogStreamer()
	h.streamerClosed = false

	h.producerMutex.Lock()
	h.producerCtx, h.producerCancel = context.WithCancel(context.Background())
	h.producerMutex.Unlock()

	var startPos mysql.Position
	var stTime time.Time
	var err error

	if isGTID {
		startPos = mysql.Position{Name: "host-binlog-file", Pos: 4}
		stTime = startTS
	} else {
		st, err := internal.ConfigureStorage()
		if err != nil {
			return nil, err
		}
		stTime, err = GetBinlogTS(st.RootFolder(), pos.Name)
		if err != nil {
			return nil, err
		}
		startPos = pos
	}

	err = syncBinlogFiles(startPos, stTime, h.globalStreamer, h, gtidSet)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to start binlog producer: %v", err)
		h.globalStreamer = nil
		return nil, err
	}

	h.producerRunning = true
	tracelog.InfoLogger.Println("Producer goroutine started successfully.")

	return h.globalStreamer, nil
}

func syncBinlogFiles(pos mysql.Position, startTS time.Time, s *replication.BinlogStreamer, h *Handler, gtidSet *mysql.MysqlGTIDSet) error {
	st, err := internal.ConfigureStorage()
	if err != nil {
		return err
	}
	dstDir, err := internal.GetLogsDstSettings(conf.MysqlBinlogDstSetting)
	if err != nil {
		return err
	}
	logFilesProvider := storage.NewLowMemoryObjectProvider()

	go h.sendEventsFromBinlogFiles(logFilesProvider, pos, s, gtidSet)
	go provideLogs(st.RootFolder(), dstDir, startTS, untilTS, logFilesProvider)

	return nil
}

func (h *Handler) HandleBinlogDump(pos mysql.Position) (*replication.BinlogStreamer, error) {
	tracelog.InfoLogger.Printf("HandleBinlogDump: requested position %s:%d", pos.Name, pos.Pos)
	return h.getOrCreateStreamer(pos, nil, false)
}

func (h *Handler) HandleBinlogDumpGTID(gtidSet *mysql.MysqlGTIDSet) (*replication.BinlogStreamer, error) {
	tracelog.InfoLogger.Printf("HandleBinlogDumpGTID: requested GTID set %s", gtidSet.String())
	return h.getOrCreateStreamer(mysql.Position{}, gtidSet, true)
}

func (h *Handler) HandleRegisterSlave(data []byte) error { return nil }

func (h *Handler) HandleQuery(query string) (*mysql.Result, error) {
	lowerQuery := strings.ToLower(query)

	switch {
	case strings.HasPrefix(lowerQuery, "select @@global.server_id"), strings.HasPrefix(lowerQuery, "select @server_id"):
		serverID, err := conf.GetRequiredSetting(conf.MysqlBinlogServerID)
		if err != nil {
			return nil, err
		}
		resultSet, err := mysql.BuildSimpleTextResultset([]string{"@@global.server_id"}, [][]interface{}{{serverID}})
		if err != nil {
			return nil, err
		}
		return &mysql.Result{Status: 34, Resultset: resultSet}, nil

	case strings.HasPrefix(lowerQuery, "select @@global.server_uuid"):
		serverUUID := "f08b7f64-d5ae-11f0-9ec7-02386a6fce7c" // for test
		resultSet, err := mysql.BuildSimpleTextResultset([]string{"@@global.server_uuid"}, [][]interface{}{{serverUUID}})
		if err != nil {
			return nil, err
		}
		return &mysql.Result{Status: 34, Resultset: resultSet}, nil

	case strings.HasPrefix(lowerQuery, "select @master_binlog_checksum"), strings.HasPrefix(lowerQuery, "select @@global.binlog_checksum"):
		resultSet, err := mysql.BuildSimpleTextResultset([]string{"@@global.binlog_checksum"}, [][]interface{}{{"CRC32"}})
		if err != nil {
			return nil, err
		}
		return &mysql.Result{Status: 34, Resultset: resultSet}, nil

	case strings.HasPrefix(lowerQuery, "select @@global.gtid_mode"):
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"@@global.gtid_mode"}, [][]interface{}{{"ON"}})
		return &mysql.Result{Status: 34, Resultset: resultSet}, nil

	case strings.HasPrefix(lowerQuery, "select @@global.rpl_semi_sync_master_enabled"), strings.HasPrefix(lowerQuery, "select @@global.rpl_semi_sync_source_enabled"):
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"@@global.rpl_semi_sync_master_enabled"}, [][]interface{}{{0}})
		return &mysql.Result{Status: 34, Resultset: resultSet}, nil

	default:
		tracelog.DebugLogger.Printf("Unhandled query: %s", query)
		return nil, nil
	}
}

func handleSingleConnection(c net.Conn, globalHandler *Handler, user, password string) {
	ctx, cancel := context.WithCancel(context.Background())
	remoteAddr := c.RemoteAddr().String()

	defer func() {
		cancel()

		tracelog.InfoLogger.Printf("Cleaning up connection from %s", remoteAddr)

		globalHandler.streamerMutex.Lock()
		if globalHandler.activeConn != nil {
			globalHandler.activeConn = nil
			globalHandler.activeCtx = nil
			tracelog.InfoLogger.Printf("Active connection cleared for %s", remoteAddr)
		} else {
			tracelog.DebugLogger.Printf("No active connection to clear for %s", remoteAddr)
		}
		globalHandler.streamerMutex.Unlock()

		_ = c.Close()
	}()

	tracelog.InfoLogger.Printf("Processing connection from %s", remoteAddr)

	conn, err := server.NewConn(c, user, password, globalHandler)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to create connection from %s: %v", remoteAddr, err)
		return
	}

	tracelog.InfoLogger.Printf("Connection created for %s. Setting as active.", remoteAddr)

	globalHandler.streamerMutex.Lock()
	globalHandler.activeConn = conn
	globalHandler.activeCtx = ctx
	globalHandler.streamerMutex.Unlock()

	globalHandler.connectionStableMutex.Lock()
	globalHandler.connectionStableTime = time.Now()
	globalHandler.connectionStableMutex.Unlock()

	for {
		if err := conn.HandleCommand(); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || strings.Contains(err.Error(), "broken pipe") {
				tracelog.InfoLogger.Printf("Client %s disconnected gracefully.", remoteAddr)
			} else {
				tracelog.InfoLogger.Printf("Client %s disconnected with an error: %v", remoteAddr, err)
			}
			break
		}
	}
}

func HandleBinlogServer(since string, until string) {
	st, err := internal.ConfigureStorage()
	tracelog.ErrorLogger.FatalOnError(err)
	startTS, untilTS, _, err = getTimestamps(st.RootFolder(), since, until, "")
	tracelog.ErrorLogger.FatalOnError(err)

	// validate WALG_MYSQL_BINLOG_SERVER_REPLICA_SOURCE
	replicaSource, err := conf.GetRequiredSetting(conf.MysqlBinlogServerReplicaSource)
	tracelog.ErrorLogger.FatalOnError(err)
	_, err = mysqldriver.ParseDSN(replicaSource)
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.InfoLogger.Printf("Starting binlog server")

	serverAddress, err := conf.GetRequiredSetting(conf.MysqlBinlogServerHost)
	tracelog.ErrorLogger.FatalOnError(err)
	serverPort, err := conf.GetRequiredSetting(conf.MysqlBinlogServerPort)
	tracelog.ErrorLogger.FatalOnError(err)
	l, err := net.Listen("tcp", serverAddress+":"+serverPort)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.InfoLogger.Printf("Listening on %s, wait connection", l.Addr())

	user, err := conf.GetRequiredSetting(conf.MysqlBinlogServerUser)
	tracelog.ErrorLogger.FatalOnError(err)
	password, err := conf.GetRequiredSetting(conf.MysqlBinlogServerPassword)
	tracelog.ErrorLogger.FatalOnError(err)

	globalHandler := &Handler{
		reconnectionTimeout: 60 * time.Second,
		minStableTime:       2 * time.Second,
	}

	for {
		c, err := l.Accept()
		if err != nil {
			tracelog.ErrorLogger.Printf("Error accepting connection: %v", err)
			continue
		}
		tracelog.InfoLogger.Printf("connection accepted from %s", c.RemoteAddr())

		handleSingleConnection(c, globalHandler, user, password)

		tracelog.InfoLogger.Printf("Client disconnected, waiting for new connection")

		globalHandler.doneMutex.Lock()
		isDone := globalHandler.replicationDone
		globalHandler.doneMutex.Unlock()

		if isDone {
			tracelog.InfoLogger.Println("Replication completed, shutting down server")
			break
		}
	}
}

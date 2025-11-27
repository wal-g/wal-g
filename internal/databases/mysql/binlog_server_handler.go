package mysql

import (
	"database/sql"
	"encoding/binary"
	"errors"
	"hash/crc32"
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
	gtidMutex    sync.RWMutex
)

type Handler struct {
	server.EmptyReplicationHandler
	globalStreamer *replication.BinlogStreamer
	streamerMutex  sync.Mutex
	syncOnce       sync.Once
	gtidSet        *mysql.MysqlGTIDSet
	clientActive   bool
	clientMutex    sync.RWMutex
}

func (h *Handler) setClientActive(active bool) {
	h.clientMutex.Lock()
	defer h.clientMutex.Unlock()
	h.clientActive = active
}

func (h *Handler) isClientActive() bool {
	h.clientMutex.RLock()
	defer h.clientMutex.RUnlock()
	return h.clientActive
}

func handleEventError(err error, s *replication.BinlogStreamer) {
	if err == nil {
		return
	}

	errStr := err.Error()
	if strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "connection reset") ||
		strings.Contains(errStr, "use of closed network connection") ||
		strings.Contains(errStr, "write: connection was bad") {
		tracelog.InfoLogger.Printf("Client disconnected: %v", err)
		return
	}

	tracelog.ErrorLogger.Printf("Error during replication: %v", err)

	for i := 0; i < 3; i++ {
		if s.AddErrorToStreamer(err) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// see: https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Rotate__event.html
func addRotateEvent(s *replication.BinlogStreamer, pos mysql.Position) error {
	serverID, err := conf.GetRequiredSetting(conf.MysqlBinlogServerID)
	tracelog.ErrorLogger.FatalOnError(err)
	ServerIDNum, err := strconv.Atoi(serverID)
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

func waitReplicationIsDone() error {
	replicaSource, err := conf.GetRequiredSetting(conf.MysqlBinlogServerReplicaSource)
	if err != nil {
		return err
	}

	if !strings.Contains(replicaSource, "timeout=") {
		if strings.Contains(replicaSource, "?") {
			replicaSource += "&timeout=30s&readTimeout=30s&writeTimeout=30s"
		} else {
			replicaSource += "?timeout=30s&readTimeout=30s&writeTimeout=30s"
		}
	}

	db, err := sql.Open("mysql", replicaSource)
	if err != nil {
		return err
	}
	defer db.Close()

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(5 * time.Minute)

	startTime := time.Now()
	maxWaitTime := 5 * time.Minute

	for {
		if time.Since(startTime) > maxWaitTime {
			return errors.New("timeout waiting for replication after 5 minutes")
		}

		if err := db.Ping(); err != nil {
			tracelog.WarningLogger.Printf("Cannot ping MySQL: %v, retrying...", err)
			time.Sleep(2 * time.Second)
			continue
		}

		// get executed GTID set from replica
		gtidSet, err := getMySQLGTIDExecuted(db, "mysql")
		if err != nil {
			tracelog.WarningLogger.Printf("Error getting GTID: %v, retrying...", err)
			time.Sleep(2 * time.Second)
			continue
		}

		gtidMutex.RLock()
		currentLastSentGTID := lastSentGTID
		gtidMutex.RUnlock()

		if currentLastSentGTID == "" {
			tracelog.DebugLogger.Println("No GTID sent yet, waiting...")
			time.Sleep(1 * time.Second)
			continue
		}

		lastSentGTIDSet, err := mysql.ParseGTIDSet("mysql", currentLastSentGTID)
		if err != nil {
			return err
		}

		tracelog.DebugLogger.Printf("Expected GTID set: %v; MySQL GTID set: %v", lastSentGTIDSet.String(), gtidSet.String())

		if gtidSet.Contain(lastSentGTIDSet) {
			tracelog.InfoLogger.Println("Replication is done")
			return nil
		}
		time.Sleep(1 * time.Second)
	}
}

func sendEventsFromBinlogFiles(logFilesProvider *storage.ObjectProvider, pos mysql.Position, s *replication.BinlogStreamer, gtidSet *mysql.MysqlGTIDSet, handler *Handler) {
	defer func() {
		if r := recover(); r != nil {
			tracelog.ErrorLogger.Printf("PANIC in sendEventsFromBinlogFiles: %v", r)
		}
	}()

	err := addRotateEvent(s, pos)
	if err != nil {
		handleEventError(err, s)
		return
	}

	p := replication.NewBinlogParser()
	p.SetRawMode(true)
	p.SetFlavor(mysql.MySQLFlavor)
	p.SetVerifyChecksum(true)

	var skipTx bool
	eventsProcessed := 0
	lastEventTime := time.Now()

	f := func(e *replication.BinlogEvent) error {
		if !handler.isClientActive() {
			return errors.New("client disconnected")
		}

		if time.Since(lastEventTime) > 30*time.Second {
			tracelog.WarningLogger.Printf("No events processed for 30 seconds, last event count: %d", eventsProcessed)
		}
		lastEventTime = time.Now()
		eventsProcessed++

		if eventsProcessed%1000 == 0 {
			tracelog.DebugLogger.Printf("Processed %d events, current GTID: %s", eventsProcessed, lastSentGTID)
		}

		if int64(e.Header.Timestamp) > untilTS.Unix() {
			tracelog.DebugLogger.Printf("Event timestamp %d exceeds until timestamp %d, skipping", e.Header.Timestamp, untilTS.Unix())
			return nil
		}

		if e.Header.EventType == replication.GTID_EVENT {
			gtidEvent := &replication.GTIDEvent{}
			err = gtidEvent.Decode(e.RawData[replication.EventHeaderSize:])
			if err != nil {
				return err
			}
			u, _ := uuid.FromBytes(gtidEvent.SID)
			thisGtidStr := u.String() + ":" + strconv.Itoa(int(gtidEvent.GNO))
			thisGtidSet, err := mysql.ParseMysqlGTIDSet(thisGtidStr)
			if err != nil {
				return err
			}
			skipTx = gtidSet != nil && gtidSet.Contain(thisGtidSet)

			gtidMutex.Lock()
			lastSentGTID = thisGtidStr
			gtidMutex.Unlock()

			if skipTx {
				return nil
			}
		}
		if skipTx {
			return nil
		}

		err := s.AddEventToStreamer(e)
		if err != nil {
			return err
		}
		return nil
	}

	dstDir, _ := internal.GetLogsDstSettings(conf.MysqlBinlogDstSetting)

	for {
		if !handler.isClientActive() {
			tracelog.InfoLogger.Println("Client disconnected, stopping binlog processing")
			return
		}

		logFile, err := logFilesProvider.GetObject()
		if errors.Is(err, storage.ErrNoMoreObjects) {
			tracelog.InfoLogger.Println("No more binlog files, waiting for replication to complete")
			err := waitReplicationIsDone()
			if err != nil {
				tracelog.ErrorLogger.Printf("Error while waiting MySQL applied binlogs: %v", err)
				os.Exit(1)
			}
			os.Exit(0)
		}

		if err != nil {
			handleEventError(err, s)
			break
		}

		binlogName := utility.TrimFileExtension(logFile.GetName())
		tracelog.InfoLogger.Printf("Processing binlog file %s", binlogName)
		binlogPath := path.Join(dstDir, binlogName)

		fileInfo, err := os.Stat(binlogPath)
		if err != nil {
			tracelog.ErrorLogger.Printf("Cannot stat binlog file %s: %v", binlogPath, err)
			handleEventError(err, s)
			continue
		}

		tracelog.InfoLogger.Printf("Synced binlog file %s (size: %d bytes)", binlogName, fileInfo.Size())

		err = p.ParseFile(binlogPath, int64(pos.Pos), f)
		if err != nil {
			if strings.Contains(err.Error(), "client disconnected") ||
				strings.Contains(err.Error(), "broken pipe") ||
				strings.Contains(err.Error(), "connection reset") {
				tracelog.InfoLogger.Printf("Client disconnected while processing %s", binlogName)
				return
			}
			handleEventError(err, s)
		}

		pos.Pos = 4
	}
}

func syncBinlogFiles(pos mysql.Position, startTS time.Time, s *replication.BinlogStreamer, gtidSet *mysql.MysqlGTIDSet, handler *Handler) error {
	// get necessary settings
	st, err := internal.ConfigureStorage()
	if err != nil {
		return err
	}
	dstDir, err := internal.GetLogsDstSettings(conf.MysqlBinlogDstSetting)
	if err != nil {
		return err
	}
	logFilesProvider := storage.NewLowMemoryObjectProvider()
	// start sync
	go sendEventsFromBinlogFiles(logFilesProvider, pos, s, gtidSet, handler)
	go provideLogs(st.RootFolder(), dstDir, startTS, untilTS, logFilesProvider)

	return nil
}

func (h *Handler) HandleRegisterSlave(data []byte) error {
	h.setClientActive(true)
	return nil
}

func (h *Handler) HandleBinlogDump(pos mysql.Position) (*replication.BinlogStreamer, error) {
	h.streamerMutex.Lock()
	defer h.streamerMutex.Unlock()

	tracelog.InfoLogger.Printf("HandleBinlogDump: requested position %s:%d", pos.Name, pos.Pos)

	h.setClientActive(true)

	if h.globalStreamer != nil {
		tracelog.InfoLogger.Println("Returning existing streamer for reconnection")
		return h.globalStreamer, nil
	}

	h.globalStreamer = replication.NewBinlogStreamer()

	var syncErr error
	h.syncOnce.Do(func() {
		st, err := internal.ConfigureStorage()
		if err != nil {
			syncErr = err
			return
		}
		startTime, err := GetBinlogTS(st.RootFolder(), pos.Name)
		if err != nil {
			syncErr = err
			return
		}
		syncErr = syncBinlogFiles(pos, startTime, h.globalStreamer, h.gtidSet, h)
	})

	if syncErr != nil {
		return nil, syncErr
	}

	return h.globalStreamer, nil
}

func (h *Handler) HandleBinlogDumpGTID(gtidSet *mysql.MysqlGTIDSet) (*replication.BinlogStreamer, error) {
	h.streamerMutex.Lock()
	defer h.streamerMutex.Unlock()

	tracelog.InfoLogger.Printf("HandleBinlogDumpGTID called with GTID set: %v", gtidSet)

	h.setClientActive(true)

	if h.globalStreamer != nil {
		tracelog.InfoLogger.Println("Returning existing streamer for reconnection")
		return h.globalStreamer, nil
	}

	h.gtidSet = gtidSet
	h.globalStreamer = replication.NewBinlogStreamer()

	var syncErr error
	h.syncOnce.Do(func() {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					tracelog.ErrorLogger.Printf("PANIC in binlog sync: %v", r)
					syncErr = errors.New("panic in binlog sync")
				}
			}()

			err := syncBinlogFiles(mysql.Position{Name: "host-binlog-file", Pos: 4}, startTS, h.globalStreamer, h.gtidSet, h)
			if err != nil {
				tracelog.ErrorLogger.Printf("Error in syncBinlogFiles: %v", err)
				syncErr = err
			}
		}()

		time.Sleep(100 * time.Millisecond)
	})

	if syncErr != nil {
		return nil, syncErr
	}

	return h.globalStreamer, nil
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

func HandleBinlogServer(since string, until string) {
	os.Stdout.Sync()
	os.Stderr.Sync()

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

	// This loop continues accepting connections until the process exits.
	// It will be terminated by os.Exit() call in sendEventsFromBinlogFiles.
	for {
		c, err := l.Accept()
		if err != nil {
			tracelog.ErrorLogger.Printf("Error accepting connection: %v", err)
			continue
		}
		tracelog.InfoLogger.Printf("connection accepted from %s", c.RemoteAddr())

		user, err := conf.GetRequiredSetting(conf.MysqlBinlogServerUser)
		tracelog.ErrorLogger.FatalOnError(err)
		password, err := conf.GetRequiredSetting(conf.MysqlBinlogServerPassword)
		tracelog.ErrorLogger.FatalOnError(err)

		handler := &Handler{
			clientActive: true,
		}

		conn, err := server.NewConn(c, user, password, handler)
		if err != nil {
			tracelog.ErrorLogger.Printf("Error creating connection: %v", err)
			utility.LoggedClose(c, "Failed to close connection after error")
			continue
		}
		tracelog.InfoLogger.Printf("connection created")

		go func() {
			defer func() {
				handler.setClientActive(false)
				utility.LoggedClose(c, "Failed to close connection")
			}()

			for {
				if err := conn.HandleCommand(); err != nil {
					errStr := err.Error()
					if strings.Contains(errStr, "broken pipe") ||
						strings.Contains(errStr, "connection reset") ||
						strings.Contains(errStr, "EOF") {
						tracelog.InfoLogger.Printf("Client disconnected normally: %v", err)
					} else {
						tracelog.WarningLogger.Printf("Connection closed: %v", err)
					}
					break
				}
			}
			tracelog.InfoLogger.Printf("Client disconnected, waiting for new connection")
		}()
	}
}

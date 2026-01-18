package mysql

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"os"
	"path"
	"path/filepath"
	"runtime/debug"
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
	startTS         time.Time
	untilTS         time.Time
	sessionMutex    sync.Mutex
	activeSession   *SessionContext
	activeSessionID int

	initialConfigMu    sync.Mutex
	initialConfigSet   bool
	initialStartBinlog string
	initialStartTS     time.Time
	initialUntilTS     time.Time
)

type ReplicationState struct {
	lastSentGTID string
	mu           sync.Mutex
}

type SessionContext struct {
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	state       *ReplicationState
	id          int
	conn        net.Conn
	cleanupOnce sync.Once
	cleanupDone chan struct{}

	mu           sync.Mutex
	provider     *storage.ObjectProvider
	tempDir      string
	isCleaningUp bool
}

func NewSessionContext(id int, conn net.Conn) *SessionContext {
	ctx, cancel := context.WithCancel(context.Background())
	return &SessionContext{
		ctx:         ctx,
		cancel:      cancel,
		state:       &ReplicationState{},
		id:          id,
		conn:        conn,
		cleanupDone: make(chan struct{}),
	}
}

func (sc *SessionContext) Cleanup() {
	sc.cleanupOnce.Do(func() {
		tracelog.InfoLogger.Printf("[Session %d] Starting cleanup", sc.id)
		defer close(sc.cleanupDone)

		sessionMutex.Lock()
		isActive := activeSession == sc
		sessionMutex.Unlock()

		if sc.cancel != nil {
			tracelog.InfoLogger.Printf("[Session %d] Cancelling context", sc.id)
			sc.cancel()
		}

		if sc.conn != nil && isActive {
			tracelog.InfoLogger.Printf("[Session %d] Force closing connection to ensure cleanup", sc.id)
			_ = sc.conn.Close()
		}

		sc.mu.Lock()
		sc.isCleaningUp = true
		provider := sc.provider
		tempDir := sc.tempDir
		sc.mu.Unlock()

		if provider != nil && !provider.IsClosed() {
			tracelog.InfoLogger.Printf("[Session %d] Closing provider", sc.id)
			provider.Close()
			tracelog.InfoLogger.Printf("[Session %d] Provider closed", sc.id)
		}

		tracelog.InfoLogger.Printf("[Session %d] Waiting for goroutines to finish (timeout 5s)", sc.id)
		done := make(chan struct{})
		go func() {
			defer func() {
				if r := recover(); r != nil {
					tracelog.ErrorLogger.Printf("[Session %d] Panic in Cleanup waiter: %v\nStack: %s", sc.id, r, debug.Stack())
				}
				close(done)
			}()
			sc.wg.Wait()
		}()

		select {
		case <-done:
			tracelog.InfoLogger.Printf("[Session %d] All goroutines finished", sc.id)
		case <-time.After(5 * time.Second):
			tracelog.WarningLogger.Printf("[Session %d] Timeout waiting for goroutines (continuing anyway)", sc.id)
		}

		if tempDir != "" {
			tracelog.InfoLogger.Printf("[Session %d] Removing temporary directory: %s", sc.id, tempDir)
			err := os.RemoveAll(tempDir)
			if err != nil {
				tracelog.WarningLogger.Printf("[Session %d] Failed to remove temp dir %s: %v", sc.id, tempDir, err)
			}
		}

		tracelog.InfoLogger.Printf("[Session %d] Cleanup completed", sc.id)
	})
}

func (sc *SessionContext) WaitCleanup(timeout time.Duration) bool {
	select {
	case <-sc.cleanupDone:
		return true
	case <-time.After(timeout):
		return false
	}
}

func handleEventError(err error, s *replication.BinlogStreamer) {
	if err == nil {
		return
	}
	tracelog.ErrorLogger.Println("Error during replication event processing:", err)
	ok := s.AddErrorToStreamer(err)
	if !ok {
		tracelog.WarningLogger.Println("Could not add error to streamer, streamer might be closed")
	}
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

	rotateBinlogEvent := replication.BinlogEvent{}

	messageBodySize := 8 + len(pos.Name) + 1
	eventLength := replication.EventHeaderSize + messageBodySize + replication.BinlogChecksumLength

	rotateBinlogEvent.RawData = make([]byte, eventLength)
	binlogEventPos := 4
	rotateBinlogEvent.RawData[binlogEventPos] = byte(replication.ROTATE_EVENT)
	binlogEventPos++
	binary.LittleEndian.PutUint32(rotateBinlogEvent.RawData[binlogEventPos:], uint32(ServerIDNum))
	binlogEventPos += 4
	binary.LittleEndian.PutUint32(rotateBinlogEvent.RawData[binlogEventPos:], uint32(eventLength))
	binlogEventPos += 4
	binary.LittleEndian.PutUint32(rotateBinlogEvent.RawData[binlogEventPos:], 0)
	binlogEventPos += 4
	binary.LittleEndian.PutUint16(rotateBinlogEvent.RawData[binlogEventPos:], 0)
	binlogEventPos += 2

	binary.LittleEndian.PutUint64(rotateBinlogEvent.RawData[binlogEventPos:], uint64(pos.Pos))
	binlogEventPos += 8
	copy(rotateBinlogEvent.RawData[binlogEventPos:], pos.Name)
	binlogEventPos += len(pos.Name)
	rotateBinlogEvent.RawData[binlogEventPos] = 0
	binlogEventPos++

	checksum := crc32.ChecksumIEEE(rotateBinlogEvent.RawData[0 : replication.EventHeaderSize+messageBodySize])
	binary.LittleEndian.PutUint32(rotateBinlogEvent.RawData[binlogEventPos:], checksum)

	return s.AddEventToStreamer(&rotateBinlogEvent)
}

func waitReplicationIsDone(ctx context.Context, state *ReplicationState, sessionID int) error {
	tracelog.InfoLogger.Printf("[Session %d] Waiting for MySQL replica to apply all transactions...", sessionID)
	replicaSource, err := conf.GetRequiredSetting(conf.MysqlBinlogServerReplicaSource)
	if err != nil {
		return err
	}

	db, err := sql.Open("mysql", replicaSource)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		tracelog.WarningLogger.Printf("[Session %d] Could not ping replica DB: %v", sessionID, err)
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			tracelog.InfoLogger.Printf("[Session %d] Context cancelled while waiting for replication", sessionID)
			return ctx.Err()
		case <-ticker.C:
			gtidSet, err := getMySQLGTIDExecuted(db, "mysql")
			if err != nil {
				tracelog.WarningLogger.Printf("[Session %d] Failed to get GTID executed: %v", sessionID, err)
				continue
			}

			state.mu.Lock()
			currentLastGTID := state.lastSentGTID
			state.mu.Unlock()

			if currentLastGTID == "" {
				tracelog.InfoLogger.Printf("[Session %d] No Last GTID recorded, exiting wait", sessionID)
				return nil
			}

			lastSentGTIDSet, err := mysql.ParseGTIDSet("mysql", currentLastGTID)
			if err != nil {
				return err
			}

			tracelog.DebugLogger.Printf("[Session %d] Expected GTID: %v; MySQL GTID: %v",
				sessionID, lastSentGTIDSet.String(), gtidSet.String())

			if gtidSet.Contain(lastSentGTIDSet) {
				tracelog.InfoLogger.Printf("[Session %d] Replication is done (Target GTID reached)", sessionID)
				return nil
			}
		}
	}
}

func sendEventsFromBinlogFiles(sc *SessionContext, logFilesProvider *storage.ObjectProvider,
	pos mysql.Position, s *replication.BinlogStreamer) {

	defer func() {
		if r := recover(); r != nil {
			tracelog.ErrorLogger.Printf("[Session %d] Panic in sendEventsFromBinlogFiles: %v\nStack: %s",
				sc.id, r, debug.Stack())
		}
		tracelog.InfoLogger.Printf("[Session %d] Event sender goroutine finished", sc.id)
		sc.wg.Done()
	}()

	tracelog.InfoLogger.Printf("[Session %d] Starting to send events from %s position %d", sc.id, pos.Name, pos.Pos)

	select {
	case <-sc.ctx.Done():
		tracelog.InfoLogger.Printf("[Session %d] Context cancelled before adding rotate event", sc.id)
		return
	default:
	}

	err := addRotateEvent(s, pos)
	if err != nil {
		handleEventError(err, s)
		return
	}

	p := replication.NewBinlogParser()
	p.SetRawMode(true)
	p.SetFlavor(mysql.MySQLFlavor)
	p.SetVerifyChecksum(true)

	f := func(e *replication.BinlogEvent) error {
		select {
		case <-sc.ctx.Done():
			tracelog.DebugLogger.Printf("[Session %d] Context cancelled during event processing", sc.id)
			return sc.ctx.Err()
		default:
		}

		if int64(e.Header.Timestamp) > untilTS.Unix() {
			tracelog.InfoLogger.Printf("[Session %d] Reached untilTS, stopping event processing", sc.id)
			return nil
		}

		if e.Header.EventType == replication.GTID_EVENT {
			gtidEvent := &replication.GTIDEvent{}
			err = gtidEvent.Decode(e.RawData[replication.EventHeaderSize:])
			if err == nil {
				u, _ := uuid.FromBytes(gtidEvent.SID)

				sc.state.mu.Lock()
				sc.state.lastSentGTID = u.String() + ":1-" + strconv.Itoa(int(gtidEvent.GNO))
				tracelog.DebugLogger.Printf("[Session %d] Updated lastSentGTID: %s", sc.id, sc.state.lastSentGTID)
				sc.state.mu.Unlock()
			} else {
				tracelog.WarningLogger.Printf("[Session %d] Failed to decode GTID event: %v", sc.id, err)
			}
		}

		return s.AddEventToStreamer(e)
	}

	sc.mu.Lock()
	targetDir := sc.tempDir
	sc.mu.Unlock()

	if targetDir == "" {
		targetDir, _ = internal.GetLogsDstSettings(conf.MysqlBinlogDstSetting)
	}

	for {
		select {
		case <-sc.ctx.Done():
			tracelog.InfoLogger.Printf("[Session %d] Context cancelled, stopping event sender", sc.id)
			return
		default:
		}

		logFile, err := logFilesProvider.GetObject(sc.ctx)
		if errors.Is(err, storage.ErrNoMoreObjects) {
			tracelog.InfoLogger.Printf("[Session %d] No more binlog objects. Waiting for replication verification...", sc.id)
			err := waitReplicationIsDone(sc.ctx, sc.state, sc.id)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					tracelog.InfoLogger.Printf("[Session %d] Context cancelled during replication wait", sc.id)
					return
				}
				tracelog.WarningLogger.Printf("[Session %d] Error while waiting MySQL applied binlogs: %v", sc.id, err)
			} else {
				tracelog.InfoLogger.Printf("[Session %d] Replication seemingly done", sc.id)
			}
			return
		}

		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, storage.ErrProviderClosed) {
				tracelog.InfoLogger.Printf("[Session %d] Context cancelled or provider closed while getting log file: %v", sc.id, err)
				return
			}
			tracelog.ErrorLogger.Printf("[Session %d] Error getting log file: %v", sc.id, err)
			return
		}

		binlogName := utility.TrimFileExtension(logFile.GetName())
		tracelog.InfoLogger.Printf("[Session %d] Processing binlog file %s", sc.id, binlogName)

		binlogPath := path.Join(targetDir, binlogName)

		err = p.ParseFile(binlogPath, int64(pos.Pos), f)

		rmErr := os.Remove(binlogPath)
		if rmErr != nil {
			tracelog.WarningLogger.Printf("[Session %d] Failed to remove temp binlog file %s: %v", sc.id, binlogPath, rmErr)
		}

		if err != nil {
			if errors.Is(err, context.Canceled) {
				tracelog.InfoLogger.Printf("[Session %d] Context cancelled during parsing file %s", sc.id, binlogPath)
				return
			}
			tracelog.ErrorLogger.Printf("[Session %d] Error parsing file %s: %v", sc.id, binlogPath, err)
			return
		}

		pos.Pos = 4
	}
}

func syncBinlogFiles(sc *SessionContext, pos mysql.Position, streamStartTS time.Time,
	s *replication.BinlogStreamer) error {

	tracelog.InfoLogger.Printf("[Session %d] syncBinlogFiles: Starting sync from %s:%d, startTS=%v",
		sc.id, pos.Name, pos.Pos, streamStartTS)

	st, err := internal.ConfigureStorage()
	if err != nil {
		return err
	}

	baseDstDir, err := internal.GetLogsDstSettings(conf.MysqlBinlogDstSetting)
	if err != nil {
		return err
	}

	sessionDir := filepath.Join(baseDstDir, fmt.Sprintf("session_%d", sc.id))
	logFilesProvider := storage.NewLowMemoryObjectProvider()

	sc.mu.Lock()
	if sc.isCleaningUp {
		sc.mu.Unlock()
		tracelog.InfoLogger.Printf("[Session %d] Aborting syncBinlogFiles because session is cleaning up", sc.id)
		return context.Canceled
	}

	sc.provider = logFilesProvider
	sc.tempDir = sessionDir
	sc.wg.Add(2)
	sc.mu.Unlock()

	go func() {
		defer func() {
			sc.wg.Done()
			if r := recover(); r != nil {
				tracelog.ErrorLogger.Printf("[Session %d] Panic in provideLogs: %v\nStack: %s",
					sc.id, r, debug.Stack())
			}
			tracelog.InfoLogger.Printf("[Session %d] Log provider goroutine finished", sc.id)
		}()
		provideLogs(sc.ctx, st.RootFolder(), sessionDir, streamStartTS, untilTS, logFilesProvider, sc.id)
	}()

	go func() {
		sendEventsFromBinlogFiles(sc, logFilesProvider, pos, s)
	}()

	return nil
}

type Handler struct {
	server.EmptyReplicationHandler
	session *SessionContext
}

func (h Handler) HandleRegisterSlave(data []byte) error {
	tracelog.InfoLogger.Printf("[Session %d] ====>> Received RegisterSlave command", h.session.id)

	if len(data) >= 4 {
		serverID := binary.LittleEndian.Uint32(data[0:4])
		tracelog.InfoLogger.Printf("[Session %d] RegisterSlave: ServerID=%d, DataLen=%d",
			h.session.id, serverID, len(data))
	}

	return nil
}

func (h Handler) HandleBinlogDump(pos mysql.Position) (*replication.BinlogStreamer, error) {
	tracelog.InfoLogger.Printf("[Session %d] ====>> HandleBinlogDump requested: File=%s, Pos=%d",
		h.session.id, pos.Name, pos.Pos)
	s := replication.NewBinlogStreamer()

	st, err := internal.ConfigureStorage()
	if err != nil {
		return nil, err
	}

	startTime, err := GetBinlogTS(st.RootFolder(), pos.Name)
	if err != nil {
		tracelog.ErrorLogger.Printf("[Session %d] Failed to determine start TS for %s: %v. Fallback to global.",
			h.session.id, pos.Name, err)
		return nil, err
	}

	h.session.state.mu.Lock()
	h.session.state.lastSentGTID = ""
	h.session.state.mu.Unlock()

	err = syncBinlogFiles(h.session, pos, startTime, s)
	return s, err
}

func (h Handler) HandleBinlogDumpGTID(gtidSet *mysql.MysqlGTIDSet) (*replication.BinlogStreamer, error) {
	tracelog.InfoLogger.Printf("[Session %d] ====>> HandleBinlogDumpGTID CALLED! GTID Set: %s",
		h.session.id, gtidSet.String())

	s := replication.NewBinlogStreamer()

	var executionStartTS time.Time
	var startBinlogName string

	initialConfigMu.Lock()
	if initialConfigSet {
		startBinlogName = initialStartBinlog
		executionStartTS = initialStartTS
		tracelog.InfoLogger.Printf("[Session %d] Using cached initial config: binlog=%s, startTS=%v",
			h.session.id, startBinlogName, executionStartTS)
		initialConfigMu.Unlock()
	} else {
		initialConfigMu.Unlock()

		st, err := internal.ConfigureStorage()
		if err != nil {
			tracelog.ErrorLogger.Printf("[Session %d] ConfigureStorage failed: %v", h.session.id, err)
			return nil, err
		}

		executionStartTS = startTS
		startBinlogName = "mysql-bin.000001"

		binlogName, err := getLastUploadedBinlogBeforeGTID(st.RootFolder(), gtidSet, mysql.MySQLFlavor)
		if err == nil && binlogName != "" {
			ts, err := GetBinlogTS(st.RootFolder(), binlogName)
			if err == nil {
				tracelog.InfoLogger.Printf("[Session %d] Optimized restart: Client has %s, starting from binlog %s (TS: %v)",
					h.session.id, gtidSet.String(), binlogName, ts)
				executionStartTS = ts
				startBinlogName = binlogName
			} else {
				tracelog.WarningLogger.Printf("[Session %d] Found start binlog %s but failed to get TS: %v. Fallback to first binlog.",
					h.session.id, binlogName, err)
			}
		} else {
			tracelog.InfoLogger.Printf("[Session %d] Could not optimize start binlog from GTID: %v. Falling back to first available binlog.",
				h.session.id, err)

			folder := st.RootFolder().GetSubFolder(BinlogPath)
			objects, _, err := folder.ListFolder()
			if err != nil {
				return nil, err
			}

			var firstBinlog string
			var firstTS time.Time = time.Now()
			found := false
			for _, obj := range objects {
				if !strings.HasSuffix(obj.GetName(), ".zst") {
					continue
				}
				name := utility.TrimFileExtension(obj.GetName())
				ts, err := GetBinlogTS(st.RootFolder(), name)
				if err != nil {
					continue
				}
				if ts.After(startTS) && ts.Before(firstTS) {
					firstTS = ts
					firstBinlog = name
					found = true
				}
			}
			if !found {
				return nil, fmt.Errorf("no suitable starting binlog found after %v", startTS)
			}

			executionStartTS = firstTS
			startBinlogName = firstBinlog
			tracelog.InfoLogger.Printf("[Session %d] Fallback to first binlog: %s (TS: %v)", h.session.id, startBinlogName, executionStartTS)
		}

		initialConfigMu.Lock()
		initialConfigSet = true
		initialStartBinlog = startBinlogName
		initialStartTS = executionStartTS
		initialUntilTS = untilTS
		tracelog.InfoLogger.Printf("[Session %d] Saved initial config: binlog=%s, startTS=%v, untilTS=%v",
			h.session.id, initialStartBinlog, initialStartTS, initialUntilTS)
		initialConfigMu.Unlock()
	}

	h.session.state.mu.Lock()
	h.session.state.lastSentGTID = ""
	h.session.state.mu.Unlock()

	tracelog.InfoLogger.Printf("[Session %d] Calling syncBinlogFiles with pos=%s:4, startTS=%v",
		h.session.id, startBinlogName, executionStartTS)

	err := syncBinlogFiles(h.session, mysql.Position{Name: startBinlogName, Pos: 4}, executionStartTS, s)
	if err != nil {
		tracelog.ErrorLogger.Printf("[Session %d] syncBinlogFiles failed: %v", h.session.id, err)
		return nil, err
	}

	tracelog.InfoLogger.Printf("[Session %d] HandleBinlogDumpGTID completed successfully", h.session.id)
	return s, nil
}

func (h Handler) HandleQuery(query string) (*mysql.Result, error) {
	queryLower := strings.ToLower(query)

	if !strings.HasPrefix(queryLower, "select @@") && !strings.HasPrefix(queryLower, "select unix_timestamp") {
		tracelog.InfoLogger.Printf("[Session %d] HandleQuery: %s", h.session.id, query)
	}

	switch queryLower {
	case "select unix_timestamp()":
		timestamp := time.Now().Unix()
		resultSet, err := mysql.BuildSimpleTextResultset(
			[]string{"UNIX_TIMESTAMP()"},
			[][]interface{}{{timestamp}},
		)
		if err != nil {
			tracelog.ErrorLogger.Printf("[Session %d] Failed to build UNIX_TIMESTAMP result: %v", h.session.id, err)
			return nil, err
		}
		tracelog.DebugLogger.Printf("[Session %d] Returning UNIX_TIMESTAMP: %d", h.session.id, timestamp)
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil

	case "select @master_binlog_checksum":
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"master_binlog_checksum"}, [][]interface{}{{"CRC32"}})
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil

	case "select @source_binlog_checksum":
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"source_binlog_checksum"}, [][]interface{}{{"1"}})
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil

	case "show global variables like 'binlog_checksum'":
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"BINLOG_CHECKSUM"}, [][]interface{}{{"CRC32"}})
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil

	case "select @@global.server_id":
		serverID, err := conf.GetRequiredSetting(conf.MysqlBinlogServerID)
		if err != nil {
			return nil, err
		}
		resultSet, err := mysql.BuildSimpleTextResultset([]string{"SERVER_ID"}, [][]interface{}{{serverID}})
		if err != nil {
			return nil, err
		}
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil

	case "select @@global.gtid_mode":
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"GTID_MODE"}, [][]interface{}{{"ON"}})
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil

	case "select @@global.server_uuid":
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"SERVER_UUID"}, [][]interface{}{{"0"}})
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil

	case "select @@global.rpl_semi_sync_master_enabled":
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"@@global.rpl_semi_sync_master_enabled"}, [][]interface{}{{"0"}})
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil

	case "select @@global.rpl_semi_sync_source_enabled":
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"@@global.rpl_semi_sync_source_enabled"}, [][]interface{}{{"0"}})
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil

	default:
		if strings.HasPrefix(queryLower, "set @") {
			tracelog.DebugLogger.Printf("[Session %d] Handling SET variable: %s", h.session.id, query)
			return &mysql.Result{Status: 0, Warnings: 0, InsertId: 0, AffectedRows: 0}, nil
		}

		if strings.HasPrefix(queryLower, "set names") {
			tracelog.DebugLogger.Printf("[Session %d] Handling SET NAMES: %s", h.session.id, query)
			return &mysql.Result{Status: 0, Warnings: 0, InsertId: 0, AffectedRows: 0}, nil
		}

		tracelog.WarningLogger.Printf("[Session %d] UNHANDLED query: %s", h.session.id, query)
		return &mysql.Result{Status: 0, Warnings: 0, InsertId: 0, AffectedRows: 0}, nil
	}
}

func HandleBinlogServer(since string, until string) {
	defer func() {
		if r := recover(); r != nil {
			tracelog.ErrorLogger.Printf("Panic in HandleBinlogServer: %v\nStack: %s", r, debug.Stack())
			os.Exit(1)
		}
	}()
	initialConfigMu.Lock()
	initialConfigSet = false
	initialStartBinlog = ""
	initialStartTS = time.Time{}
	initialUntilTS = time.Time{}
	initialConfigMu.Unlock()

	st, err := internal.ConfigureStorage()
	tracelog.ErrorLogger.FatalOnError(err)
	startTS, untilTS, _, err = getTimestamps(st.RootFolder(), since, until, "")
	tracelog.ErrorLogger.FatalOnError(err)

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

	tracelog.InfoLogger.Printf("Listening on %s", l.Addr())

	for {
		tracelog.InfoLogger.Println("Waiting for connection...")
		c, err := l.Accept()
		if err != nil {
			tracelog.ErrorLogger.Printf("Listener Accept error: %v", err)
			continue
		}

		tracelog.InfoLogger.Printf("Connection accepted from %s", c.RemoteAddr().String())

		sessionMutex.Lock()

		oldSession := activeSession

		activeSessionID++
		sessionID := activeSessionID
		session := NewSessionContext(sessionID, c)
		activeSession = session

		sessionMutex.Unlock()

		if oldSession != nil {
			tracelog.InfoLogger.Printf("Cancelling previous session %d...", oldSession.id)
			oldSession.Cleanup()
			if !oldSession.WaitCleanup(3 * time.Second) {
				tracelog.WarningLogger.Printf("Previous session %d cleanup timeout", oldSession.id)
			} else {
				tracelog.InfoLogger.Printf("Previous session %d cleaned up successfully", oldSession.id)
			}
		}

		tracelog.InfoLogger.Printf("[Session %d] Created new session", sessionID)

		go handleConnection(c, session)
	}
}

func handleConnection(c net.Conn, session *SessionContext) {
	defer func() {
		if r := recover(); r != nil {
			tracelog.ErrorLogger.Printf("[Session %d] PANIC in connection handler: %v\nStack: %s",
				session.id, r, debug.Stack())
		}

		tracelog.InfoLogger.Printf("[Session %d] Starting connection cleanup", session.id)

		session.Cleanup()
		tracelog.InfoLogger.Printf("[Session %d] Connection handler finished", session.id)
	}()

	user, err := conf.GetRequiredSetting(conf.MysqlBinlogServerUser)
	if err != nil {
		tracelog.ErrorLogger.Printf("[Session %d] Config error (user): %v", session.id, err)
		return
	}
	password, err := conf.GetRequiredSetting(conf.MysqlBinlogServerPassword)
	if err != nil {
		tracelog.ErrorLogger.Printf("[Session %d] Config error (password): %v", session.id, err)
		return
	}

	h := Handler{session: session}
	conn, err := server.NewConn(c, user, password, h)
	if err != nil {
		tracelog.ErrorLogger.Printf("[Session %d] Handshake/Conn error from %s: %v",
			session.id, c.RemoteAddr().String(), err)
		return
	}

	tracelog.InfoLogger.Printf("[Session %d] MySQL Handshake successful with %s. Entering command loop.",
		session.id, c.RemoteAddr().String())

	commandCount := 0
	lastCommandTime := time.Now()
	receivedRegisterSlave := false
	registerSlaveTime := time.Time{}

	for {
		select {
		case <-session.ctx.Done():
			tracelog.InfoLogger.Printf("[Session %d] Context cancelled, exiting command loop (commands: %d)",
				session.id, commandCount)
			return
		default:
		}

		c.SetReadDeadline(time.Now().Add(60 * time.Second))

		commandCount++
		now := time.Now()
		timeSinceLastCmd := now.Sub(lastCommandTime)

		if commandCount%5 == 1 || timeSinceLastCmd > 5*time.Second {
			tracelog.InfoLogger.Printf("[Session %d] Waiting for command #%d (last: %v ago, registered: %v)",
				session.id, commandCount, timeSinceLastCmd, receivedRegisterSlave)
		} else {
			tracelog.DebugLogger.Printf("[Session %d] Waiting for command #%d (last command was %v ago)",
				session.id, commandCount, timeSinceLastCmd)
		}

		if receivedRegisterSlave && !registerSlaveTime.IsZero() {
			timeSinceRegister := now.Sub(registerSlaveTime)
			if timeSinceRegister > 5*time.Second {
				tracelog.WarningLogger.Printf("[Session %d] Client registered as slave %v ago but hasn't sent binlog dump command!",
					session.id, timeSinceRegister)
			}
		}

		err := conn.HandleCommand()
		if err != nil {
			if err == io.EOF {
				tracelog.InfoLogger.Printf("[Session %d] Client closed connection (EOF) after %d commands",
					session.id, commandCount)
			} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				tracelog.WarningLogger.Printf("[Session %d] Command timeout after %v (commands: %d)",
					session.id, timeSinceLastCmd, commandCount)
			} else {
				tracelog.InfoLogger.Printf("[Session %d] Connection error: %v (commands: %d)",
					session.id, err, commandCount)
			}
			return
		}

		if commandCount == 10 {
			receivedRegisterSlave = true
			registerSlaveTime = now
			tracelog.InfoLogger.Printf("[Session %d] RegisterSlave received at command #%d", session.id, commandCount)
		}

		if commandCount == 11 {
			tracelog.InfoLogger.Printf("[Session %d] BinlogDumpGTID processed, waiting for session to complete...", session.id)
			<-session.ctx.Done()
			tracelog.InfoLogger.Printf("[Session %d] Session context done, exiting command loop", session.id)
			return
		}

		lastCommandTime = now
		c.SetReadDeadline(time.Time{})
	}
}

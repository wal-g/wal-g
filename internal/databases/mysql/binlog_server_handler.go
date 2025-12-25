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
	startTS time.Time
	untilTS time.Time
)

type Handler struct {
	server.EmptyReplicationHandler
	globalStreamer *replication.BinlogStreamer
	streamerMutex  sync.Mutex
	cancelFunc     context.CancelFunc
	currentTempDir string
	sessionWg      sync.WaitGroup
}

func handleEventError(err error, s *replication.BinlogStreamer) {
	if err == nil {
		return
	}
	tracelog.ErrorLogger.Println("Error during replication", err)
	go func() {
		s.AddErrorToStreamer(err)
	}()
}

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

func waitReplicationIsDone(ctx context.Context, lastSentGTID string) error {
	replicaSource, err := conf.GetRequiredSetting(conf.MysqlBinlogServerReplicaSource)
	if err != nil {
		return err
	}
	db, err := sql.Open("mysql", replicaSource)
	if err != nil {
		return err
	}
	defer db.Close()

	lastSentGTIDSet, err := mysql.ParseGTIDSet("mysql", lastSentGTID)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		gtidSet, err := getMySQLGTIDExecuted(db, "mysql")
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to get executed GTID set: %v", err)
		} else {
			tracelog.DebugLogger.Printf("Expected GTID set: %v; MySQL GTID set: %v", lastSentGTIDSet.String(), gtidSet.String())

			if gtidSet.Contain(lastSentGTIDSet) {
				tracelog.InfoLogger.Println("Replication is done")
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}
}

func gtidSetContains(set *mysql.MysqlGTIDSet, sid uuid.UUID, gno int64) bool {
	if set == nil {
		return false
	}
	if uuidSet, ok := set.Sets[sid.String()]; ok {
		for _, interval := range uuidSet.Intervals {
			if gno >= interval.Start && gno <= interval.Stop {
				return true
			}
		}
	}
	return false
}

func addObjectWithContext(ctx context.Context, p *storage.ObjectProvider, obj storage.Object) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	done := make(chan error, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				tracelog.InfoLogger.Printf("addObjectWithContext: recovered from panic (provider already closed)")
			}
		}()

		select {
		case <-ctx.Done():
			return
		default:
		}

		err := p.AddObject(obj)

		select {
		case done <- err:
		default:
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

func sendEventsFromBinlogFiles(ctx context.Context, logFilesProvider *storage.ObjectProvider, pos mysql.Position, s *replication.BinlogStreamer, dstDir string, excludeGTIDs *mysql.MysqlGTIDSet) {
	err := addRotateEvent(s, pos)
	handleEventError(err, s)

	p := replication.NewBinlogParser()
	p.SetRawMode(true)
	p.SetFlavor(mysql.MySQLFlavor)
	p.SetVerifyChecksum(true)

	var currentSessionGTID string
	var skippingTx bool

	f := func(e *replication.BinlogEvent) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if e.Header.EventType == replication.ROTATE_EVENT || e.Header.EventType == replication.FORMAT_DESCRIPTION_EVENT {
			return s.AddEventToStreamer(e)
		}

		if int64(e.Header.Timestamp) > untilTS.Unix() {
			return nil
		}

		if e.Header.EventType == replication.GTID_EVENT {
			gtidEvent := &replication.GTIDEvent{}
			err = gtidEvent.Decode(e.RawData[replication.EventHeaderSize:])
			tracelog.ErrorLogger.FatalOnError(err)

			u, _ := uuid.FromBytes(gtidEvent.SID)
			currentSessionGTID = u.String() + ":1-" + strconv.Itoa(int(gtidEvent.GNO))

			if excludeGTIDs != nil && gtidSetContains(excludeGTIDs, u, int64(gtidEvent.GNO)) {
				skippingTx = true
			} else {
				skippingTx = false
			}
		}

		if skippingTx {
			return nil
		}

		err := s.AddEventToStreamer(e)
		return err
	}

	for {
		select {
		case <-ctx.Done():
			tracelog.InfoLogger.Println("Context canceled, stopping binlog rotation loop")
			return
		default:
		}

		logFile, err := logFilesProvider.GetObject()
		if errors.Is(err, storage.ErrNoMoreObjects) {
			err := waitReplicationIsDone(ctx, currentSessionGTID)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					tracelog.InfoLogger.Println("WaitReplication canceled due to new connection")
					return
				}
				tracelog.InfoLogger.Println("Error while waiting MySQL applied binlogs: ", err)
			} else {
				os.Exit(0)
			}
			return
		}
		if err != nil {
			handleEventError(err, s)
			break
		}

		binlogName := utility.TrimFileExtension(logFile.GetName())
		binlogPath := path.Join(dstDir, binlogName)

		select {
		case <-ctx.Done():
			tracelog.InfoLogger.Println("Context canceled before parsing binlog")
			return
		default:
		}

		tracelog.InfoLogger.Printf("Synced binlog file %s in dir %s", binlogName, dstDir)

		err = p.ParseFile(binlogPath, int64(pos.Pos), f)
		if err != nil {
			select {
			case <-ctx.Done():
				tracelog.InfoLogger.Println("Context canceled during parsing")
				return
			default:
				handleEventError(err, s)
			}
		}

		select {
		case <-ctx.Done():
			return
		default:
			if err := os.Remove(binlogPath); err != nil && !os.IsNotExist(err) {
				tracelog.WarningLogger.Printf("Failed to remove binlog %s: %v", binlogPath, err)
			}
		}

		pos.Pos = 4
	}
}

func syncBinlogFiles(ctx context.Context, pos mysql.Position, startTS time.Time, s *replication.BinlogStreamer, dstDir string, excludeGTIDs *mysql.MysqlGTIDSet, wg *sync.WaitGroup) error {
	st, err := internal.ConfigureStorage()
	if err != nil {
		return err
	}

	if err := os.MkdirAll(dstDir, 0750); err != nil {
		return err
	}

	logFilesProvider := storage.NewLowMemoryObjectProvider()

	wg.Add(2)

	go func() {
		defer wg.Done()
		sendEventsFromBinlogFiles(ctx, logFilesProvider, pos, s, dstDir, excludeGTIDs)
	}()

	go func() {
		defer wg.Done()
		defer logFilesProvider.Close()
		provideLogsWithContext(ctx, st.RootFolder(), dstDir, startTS, untilTS, logFilesProvider)
	}()

	return nil
}

func (h *Handler) HandleRegisterSlave(data []byte) error {
	tracelog.InfoLogger.Printf("HandleRegisterSlave called, data len=%d", len(data))
	return nil
}

func cleanupDir(dir string) {
	if dir != "" {
		tracelog.InfoLogger.Printf("Cleaning up old session dir: %s", dir)
		os.RemoveAll(dir)
	}
}

func (h *Handler) prepareNewSession() (string, error) {
	baseDir, err := internal.GetLogsDstSettings(conf.MysqlBinlogDstSetting)
	if err != nil {
		return "", err
	}
	sessionID := uuid.New().String()
	sessionDir := path.Join(baseDir, "session_"+sessionID)
	h.currentTempDir = sessionDir
	return sessionDir, nil
}

func (h *Handler) HandleBinlogDump(pos mysql.Position) (*replication.BinlogStreamer, error) {
	h.streamerMutex.Lock()
	defer h.streamerMutex.Unlock()

	tracelog.InfoLogger.Printf("HandleBinlogDump: requested position %s:%d", pos.Name, pos.Pos)

	h.cancelAndWaitPreviousSession()

	h.globalStreamer = replication.NewBinlogStreamer()
	ctx, cancel := context.WithCancel(context.Background())
	h.cancelFunc = cancel

	st, err := internal.ConfigureStorage()
	if err != nil {
		cancel()
		return nil, err
	}

	startTime, err := GetBinlogTS(st.RootFolder(), pos.Name)
	if err != nil {
		cancel()
		return nil, err
	}

	sessionDir, err := h.prepareNewSession()
	if err != nil {
		cancel()
		return nil, err
	}

	err = syncBinlogFiles(ctx, pos, startTime, h.globalStreamer, sessionDir, nil, &h.sessionWg)
	if err != nil {
		cancel()
		return nil, err
	}

	return h.globalStreamer, nil
}

func (h *Handler) HandleBinlogDumpGTID(gtidSet *mysql.MysqlGTIDSet) (*replication.BinlogStreamer, error) {
	tracelog.InfoLogger.Printf(">>> HandleBinlogDumpGTID ENTRY: requested %v", gtidSet)
	defer tracelog.InfoLogger.Printf("<<< HandleBinlogDumpGTID EXIT")
	h.streamerMutex.Lock()
	defer h.streamerMutex.Unlock()

	tracelog.InfoLogger.Printf("HandleBinlogDumpGTID: requested %v", gtidSet)

	h.cancelAndWaitPreviousSession()

	h.globalStreamer = replication.NewBinlogStreamer()
	ctx, cancel := context.WithCancel(context.Background())
	h.cancelFunc = cancel

	sessionDir, err := h.prepareNewSession()
	if err != nil {
		cancel()
		return nil, err
	}

	err = syncBinlogFiles(ctx, mysql.Position{Name: "host-binlog-file", Pos: 4}, startTS, h.globalStreamer, sessionDir, gtidSet, &h.sessionWg)
	if err != nil {
		cancel()
		return nil, err
	}

	return h.globalStreamer, nil
}

func (h *Handler) cancelAndWaitPreviousSession() {
	if h.cancelFunc != nil {
		tracelog.InfoLogger.Println("Canceling previous session context")
		h.cancelFunc()
		h.cancelFunc = nil
	}

	tracelog.InfoLogger.Println("Waiting for previous session goroutines to finish")
	h.sessionWg.Wait()
	tracelog.InfoLogger.Println("Previous session goroutines finished")

	if h.currentTempDir != "" {
		cleanupDir(h.currentTempDir)
		h.currentTempDir = ""
	}

	h.globalStreamer = nil
}

func (h *Handler) cancelCurrentSession() {
	tracelog.InfoLogger.Println("cancelCurrentSession called")
	h.streamerMutex.Lock()
	defer h.streamerMutex.Unlock()
	tracelog.InfoLogger.Println("cancelCurrentSession: got lock, calling cancelAndWaitPreviousSession")
	h.cancelAndWaitPreviousSession()
	tracelog.InfoLogger.Println("cancelCurrentSession: done")
}

func (h *Handler) HandleQuery(query string) (*mysql.Result, error) {
	tracelog.InfoLogger.Printf("HandleQuery called: %s", query)
	switch strings.ToLower(query) {
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
		tracelog.ErrorLogger.FatalOnError(err)
		resultSet, err := mysql.BuildSimpleTextResultset([]string{"SERVER_ID"}, [][]interface{}{{serverID}})
		tracelog.ErrorLogger.FatalOnError(err)
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
		tracelog.DebugLogger.Printf("Unhandled query: %s", query)
		return &mysql.Result{Status: 0, Warnings: 0, InsertId: 0, AffectedRows: 0}, nil
	}
}

func (h *Handler) HandleOtherCommand(cmd byte, data []byte) error {
	tracelog.InfoLogger.Printf("HandleOtherCommand: cmd=%d, len=%d, first bytes=%x",
		cmd, len(data), data[:min(20, len(data))])
	return fmt.Errorf("unsupported command: %d", cmd)
}

func HandleBinlogServer(since string, until string) {
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
	tracelog.InfoLogger.Printf("Listening on %s, wait connection", l.Addr())

	globalHandler := &Handler{}

	for {
		func() {
			defer func() {
				if r := recover(); r != nil {
					tracelog.ErrorLogger.Printf("PANIC in connection handler: %v\n%s", r, debug.Stack())
				}
			}()

			tracelog.InfoLogger.Println("Waiting for new connection on Accept()...")
			c, err := l.Accept()
			if err != nil {
				tracelog.ErrorLogger.Printf("Error accepting connection: %v", err)
				return
			}
			tracelog.InfoLogger.Printf("TCP connection accepted from %s", c.RemoteAddr())

			user, err := conf.GetRequiredSetting(conf.MysqlBinlogServerUser)
			tracelog.ErrorLogger.FatalOnError(err)
			password, err := conf.GetRequiredSetting(conf.MysqlBinlogServerPassword)
			tracelog.ErrorLogger.FatalOnError(err)

			tracelog.InfoLogger.Println("Creating MySQL protocol connection...")
			conn, err := server.NewConn(c, user, password, globalHandler)
			if err != nil {
				tracelog.ErrorLogger.Printf("Error creating connection: %v", err)
				c.Close()
				return
			}
			tracelog.InfoLogger.Println("MySQL protocol connection created, entering command loop")

			for {
				tracelog.DebugLogger.Println("Waiting for next command...")
				err := conn.HandleCommand()
				tracelog.DebugLogger.Printf("HandleCommand returned: err=%v", err)
				if err != nil {
					if errors.Is(err, io.EOF) || strings.Contains(err.Error(), "connection reset") || strings.Contains(err.Error(), "broken pipe") {
						tracelog.InfoLogger.Printf("Connection closed by peer: %v", err)
					} else {
						tracelog.WarningLogger.Printf("Connection error: %v", err)
					}
					break
				}
			}
			tracelog.InfoLogger.Println("Client disconnected, canceling current session")
			globalHandler.cancelCurrentSession()
			tracelog.InfoLogger.Println("Session canceled, returning to Accept()")
		}()
	}
}

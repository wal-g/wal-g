package mysql

import (
	"context"
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
	startTS time.Time
	untilTS time.Time
)

type Handler struct {
	server.EmptyReplicationHandler
	ctx          context.Context
	cancel       context.CancelFunc
	lastSentGTID string
	mu           sync.Mutex
}

func newHandler() *Handler {
	ctx, cancel := context.WithCancel(context.Background())
	return &Handler{
		ctx:    ctx,
		cancel: cancel,
	}
}

func handleEventError(err error, s *replication.BinlogStreamer) {
	if err == nil {
		return
	}
	tracelog.ErrorLogger.Println("Error during replication", err)
	ok := s.AddErrorToStreamer(err)
	for !ok {
		ok = s.AddErrorToStreamer(err)
	}
}

func addRotateEvent(s *replication.BinlogStreamer, pos mysql.Position) error {
	serverID, err := conf.GetRequiredSetting(conf.MysqlBinlogServerID)
	if err != nil {
		return err
	}
	serverIDNum, err := strconv.Atoi(serverID)
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
	binary.LittleEndian.PutUint32(rotateBinlogEvent.RawData[binlogEventPos:], uint32(serverIDNum))
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

func (h *Handler) waitReplicationIsDone() error {
	replicaSource, err := conf.GetRequiredSetting(conf.MysqlBinlogServerReplicaSource)
	if err != nil {
		return err
	}
	db, err := sql.Open("mysql", replicaSource)
	if err != nil {
		return err
	}
	defer db.Close()

	for {
		select {
		case <-h.ctx.Done():
			return h.ctx.Err()
		default:
		}

		gtidSet, err := getMySQLGTIDExecuted(db, "mysql")
		if err != nil {
			return err
		}

		h.mu.Lock()
		lastGTID := h.lastSentGTID
		h.mu.Unlock()

		if lastGTID == "" {
			return nil
		}

		lastSentGTIDSet, err := mysql.ParseGTIDSet("mysql", lastGTID)
		if err != nil {
			return err
		}

		tracelog.DebugLogger.Printf("Expected GTID set: %v; MySQL GTID set: %v",
			lastSentGTIDSet.String(), gtidSet.String())

		if gtidSet.Contain(lastSentGTIDSet) {
			tracelog.InfoLogger.Println("Replication is done")
			return nil
		}
		time.Sleep(1 * time.Second)
	}
}

func (h *Handler) sendEventsFromBinlogFiles(logFilesProvider *storage.ObjectProvider, pos mysql.Position, s *replication.BinlogStreamer) {
	err := addRotateEvent(s, pos)
	handleEventError(err, s)

	p := replication.NewBinlogParser()
	p.SetRawMode(true)
	p.SetFlavor(mysql.MySQLFlavor)
	p.SetVerifyChecksum(true)

	f := func(e *replication.BinlogEvent) error {
		if h.ctx.Err() != nil {
			return h.ctx.Err()
		}
		if int64(e.Header.Timestamp) > untilTS.Unix() {
			return nil
		}
		if e.Header.EventType == replication.GTID_EVENT {
			gtidEvent := &replication.GTIDEvent{}
			if err := gtidEvent.Decode(e.RawData[replication.EventHeaderSize:]); err == nil {
				u, _ := uuid.FromBytes(gtidEvent.SID)
				h.mu.Lock()
				h.lastSentGTID = u.String() + ":1-" + strconv.Itoa(int(gtidEvent.GNO))
				h.mu.Unlock()
			}
		}
		return s.AddEventToStreamer(e)
	}

	dstDir, _ := internal.GetLogsDstSettings(conf.MysqlBinlogDstSetting)

	for {
		if h.ctx.Err() != nil {
			return
		}

		logFile, err := logFilesProvider.GetObject(h.ctx)
		if errors.Is(err, storage.ErrNoMoreObjects) {
			if err := h.waitReplicationIsDone(); err != nil {
				tracelog.InfoLogger.Println("Error while waiting MySQL applied binlogs:", err)
			}
			return
		}
		if err != nil {
			handleEventError(err, s)
			return
		}

		binlogName := utility.TrimFileExtension(logFile.GetName())
		tracelog.InfoLogger.Printf("Synced binlog file %s", binlogName)
		binlogPath := path.Join(dstDir, binlogName)

		err = p.ParseFile(binlogPath, int64(pos.Pos), f)
		if err != nil && h.ctx.Err() == nil {
			handleEventError(err, s)
		}

		os.Remove(binlogPath)
		pos.Pos = 4
	}
}

func (h *Handler) syncBinlogFiles(pos mysql.Position, streamStartTS time.Time, s *replication.BinlogStreamer) error {
	st, err := internal.ConfigureStorage()
	if err != nil {
		return err
	}
	dstDir, err := internal.GetLogsDstSettings(conf.MysqlBinlogDstSetting)
	if err != nil {
		return err
	}

	logFilesProvider := storage.NewLowMemoryObjectProvider()

	go h.sendEventsFromBinlogFiles(logFilesProvider, pos, s)
	go provideLogs(h.ctx, st.RootFolder(), dstDir, streamStartTS, untilTS, logFilesProvider)

	return nil
}

func (h *Handler) HandleRegisterSlave(data []byte) error {
	return nil
}

func (h *Handler) HandleBinlogDump(pos mysql.Position) (*replication.BinlogStreamer, error) {
	tracelog.InfoLogger.Printf("HandleBinlogDump: requested position %s:%d", pos.Name, pos.Pos)

	st, err := internal.ConfigureStorage()
	if err != nil {
		return nil, err
	}
	startTime, err := GetBinlogTS(st.RootFolder(), pos.Name)
	if err != nil {
		return nil, err
	}

	s := replication.NewBinlogStreamer()
	err = h.syncBinlogFiles(pos, startTime, s)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (h *Handler) HandleBinlogDumpGTID(gtidSet *mysql.MysqlGTIDSet) (*replication.BinlogStreamer, error) {
	tracelog.InfoLogger.Printf("HandleBinlogDumpGTID: GTID=%s", gtidSet.String())

	s := replication.NewBinlogStreamer()
	err := h.syncBinlogFiles(mysql.Position{Name: "host-binlog-file", Pos: 4}, startTS, s)
	if err != nil {
		return nil, err
	}
	return s, nil
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
	st, err := internal.ConfigureStorage()
	tracelog.ErrorLogger.FatalOnError(err)
	startTS, untilTS, _, err = getTimestamps(st.RootFolder(), since, until, "")
	tracelog.ErrorLogger.FatalOnError(err)

	replicaSource, err := conf.GetRequiredSetting(conf.MysqlBinlogServerReplicaSource)
	tracelog.ErrorLogger.FatalOnError(err)
	_, err = mysqldriver.ParseDSN(replicaSource)
	tracelog.ErrorLogger.FatalOnError(err)

	serverAddress, err := conf.GetRequiredSetting(conf.MysqlBinlogServerHost)
	tracelog.ErrorLogger.FatalOnError(err)
	serverPort, err := conf.GetRequiredSetting(conf.MysqlBinlogServerPort)
	tracelog.ErrorLogger.FatalOnError(err)

	l, err := net.Listen("tcp", serverAddress+":"+serverPort)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.InfoLogger.Printf("Listening on %s, wait connection", l.Addr())

	for {
		c, err := l.Accept()
		if err != nil {
			tracelog.ErrorLogger.Printf("Error accepting connection: %v", err)
			continue
		}
		tracelog.InfoLogger.Printf("connection accepted from %s", c.RemoteAddr())

		h := newHandler()

		user, err := conf.GetRequiredSetting(conf.MysqlBinlogServerUser)
		if err != nil {
			tracelog.ErrorLogger.Printf("Config error: %v", err)
			c.Close()
			h.cancel()
			continue
		}
		password, err := conf.GetRequiredSetting(conf.MysqlBinlogServerPassword)
		if err != nil {
			tracelog.ErrorLogger.Printf("Config error: %v", err)
			c.Close()
			h.cancel()
			continue
		}

		conn, err := server.NewConn(c, user, password, h)
		if err != nil {
			tracelog.ErrorLogger.Printf("Error creating connection: %v", err)
			c.Close()
			h.cancel()
			continue
		}

		go func(conn *server.Conn, c net.Conn, h *Handler) {
			defer func() {
				h.cancel()
				c.Close()
				tracelog.InfoLogger.Printf("Client disconnected, waiting for new connection")
			}()
			for {
				if err := conn.HandleCommand(); err != nil {
					tracelog.WarningLogger.Printf("Connection closed: %v", err)
					return
				}
			}
		}(conn, c, h)
	}
}

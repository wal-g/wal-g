package mysql

import (
	"database/sql"
	"encoding/binary"
	"errors"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/server"
	"github.com/google/uuid"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

var (
	startTS      time.Time
	untilTS      time.Time
	lastSentGTID string
)

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

// see: https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Rotate__event.html
func addRotateEvent(s *replication.BinlogStreamer, pos mysql.Position) error {
	// create rotate event
	rotateBinlogEvent := replication.BinlogEvent{}

	// 8 bytes for position
	rotateBinlogEvent.RawData = make([]byte, replication.EventHeaderSize+8, replication.EventHeaderSize+8+len(pos.Name))
	// generate header
	// timestamp default 4 bytes
	binlogEventPos := 4
	rotateBinlogEvent.RawData[binlogEventPos] = byte(replication.ROTATE_EVENT)
	binlogEventPos++

	serverID, err := internal.GetRequiredSetting(internal.MysqlBinlogServerID)
	tracelog.ErrorLogger.FatalOnError(err)
	ServerIDNum, err := strconv.Atoi(serverID)
	tracelog.ErrorLogger.FatalOnError(err)
	binary.LittleEndian.PutUint32(rotateBinlogEvent.RawData[binlogEventPos:], uint32(ServerIDNum))
	binlogEventPos += 4
	binary.LittleEndian.PutUint32(rotateBinlogEvent.RawData[binlogEventPos:], uint32(replication.EventHeaderSize+8+len(pos.Name)))
	binlogEventPos += 4
	binary.LittleEndian.PutUint32(rotateBinlogEvent.RawData[binlogEventPos:], 0)
	binlogEventPos += 4
	binary.LittleEndian.PutUint16(rotateBinlogEvent.RawData[binlogEventPos:], 0)
	binlogEventPos += 2
	// set data
	binary.LittleEndian.PutUint64(rotateBinlogEvent.RawData[binlogEventPos:], uint64(pos.Pos))
	rotateBinlogEvent.RawData = append(rotateBinlogEvent.RawData, []byte(pos.Name)...)

	return s.AddEventToStreamer(&rotateBinlogEvent)
}

func waitReplicationIsDone() error {
	replicaSource, err := internal.GetRequiredSetting(internal.MysqlBinlogServerReplicaSource)
	if err != nil {
		return err
	}
	db, err := sql.Open("mysql", replicaSource)
	if err != nil {
		return err
	}
	for {
		// get executed GTID set from replica
		gtidSet, err := getMySQLGTIDExecuted(db, "mysql")
		if err != nil {
			return err
		}

		lastSentGTIDSet, err := mysql.ParseGTIDSet("mysql", lastSentGTID)
		if err != nil {
			return err
		}

		if gtidSet.Contain(lastSentGTIDSet) {
			tracelog.InfoLogger.Println("Replication is done")
			return nil
		}
		time.Sleep(1 * time.Second)
	}
}

func sendEventsFromBinlogFiles(logFilesProvider *storage.ObjectProvider, pos mysql.Position, s *replication.BinlogStreamer) {
	err := addRotateEvent(s, pos)
	handleEventError(err, s)

	p := replication.NewBinlogParser()

	f := func(e *replication.BinlogEvent) error {
		if int64(e.Header.Timestamp) > untilTS.Unix() {
			return nil
		}
		if e.Header.EventType == replication.GTID_EVENT {
			u, _ := uuid.FromBytes(e.Event.(*replication.GTIDEvent).SID)
			lastSentGTID = u.String() + ":1-" + strconv.Itoa(int(e.Event.(*replication.GTIDEvent).GNO))
		}
		err := s.AddEventToStreamer(e)
		return err
	}
	dstDir, _ := internal.GetLogsDstSettings(internal.MysqlBinlogDstSetting)

	for {
		logFile, err := logFilesProvider.GetObject()
		if errors.Is(err, storage.ErrNoMoreObjects) {
			err := waitReplicationIsDone()
			if err != nil {
				tracelog.InfoLogger.Println("Error while waiting replication is done: ", err)
			}
			os.Exit(0)
		}
		handleEventError(err, s)
		if err != nil {
			break
		}
		binlogName := utility.TrimFileExtension(logFile.GetName())
		tracelog.InfoLogger.Printf("Synced binlog file %s", binlogName)
		binlogPath := path.Join(dstDir, binlogName)
		err = p.ParseFile(binlogPath, int64(pos.Pos), f)
		handleEventError(err, s)

		err = os.Remove(binlogPath)
		handleEventError(err, s)
		pos.Pos = 4
	}
}

func syncBinlogFiles(pos mysql.Position, startTS time.Time, s *replication.BinlogStreamer) error {
	// get necessary settings
	folder, err := internal.ConfigureFolder()
	if err != nil {
		return err
	}
	dstDir, err := internal.GetLogsDstSettings(internal.MysqlBinlogDstSetting)
	if err != nil {
		return err
	}
	logFilesProvider := storage.NewLowMemoryObjectProvider()
	// start sync
	go sendEventsFromBinlogFiles(logFilesProvider, pos, s)
	go provideLogs(folder, dstDir, startTS, untilTS, logFilesProvider)

	return nil
}

type Handler struct {
	server.EmptyReplicationHandler
}

func (h Handler) HandleRegisterSlave(data []byte) error {
	return nil
}

func (h Handler) HandleBinlogDump(pos mysql.Position) (*replication.BinlogStreamer, error) {
	s := replication.NewBinlogStreamer()

	folder, err := internal.ConfigureFolder()
	if err != nil {
		return nil, err
	}

	startTime, err := GetBinlogTS(folder, pos.Name)
	if err != nil {
		return nil, err
	}
	err = syncBinlogFiles(pos, startTime, s)
	return s, err
}

func (h Handler) HandleBinlogDumpGTID(gtidSet *mysql.MysqlGTIDSet) (*replication.BinlogStreamer, error) {
	s := replication.NewBinlogStreamer()

	err := syncBinlogFiles(mysql.Position{Name: "host-binlog-file", Pos: 4}, startTS, s)
	return s, err
}

func (h Handler) HandleQuery(query string) (*mysql.Result, error) {
	switch strings.ToLower(query) {
	case "select @master_binlog_checksum":
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"master_binlog_checksum"}, [][]interface{}{{"NONE"}})
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil
	case "show global variables like 'binlog_checksum'":
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"BINLOG_CHECKSUM"}, [][]interface{}{{"NONE"}})
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil
	case "select @@global.server_id":
		serverID, err := internal.GetRequiredSetting(internal.MysqlBinlogServerID)
		tracelog.ErrorLogger.FatalOnError(err)
		resultSet, err := mysql.BuildSimpleTextResultset([]string{"SERVER_ID"}, [][]interface{}{{serverID}})
		tracelog.ErrorLogger.FatalOnError(err)
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil
	case "select @source_binlog_checksum":
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"source_binlog_checksum"}, [][]interface{}{{"1"}})
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
	default:
		return nil, nil
	}
}

func HandleBinlogServer(since string, until string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)
	startTS, untilTS, _, err = getTimestamps(folder, since, until, "")
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.InfoLogger.Printf("Starting binlog server")

	serverAddress, err := internal.GetRequiredSetting(internal.MysqlBinlogServerHost)
	tracelog.ErrorLogger.FatalOnError(err)
	serverPort, err := internal.GetRequiredSetting(internal.MysqlBinlogServerPort)
	tracelog.ErrorLogger.FatalOnError(err)
	l, err := net.Listen("tcp", serverAddress+":"+serverPort)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.InfoLogger.Printf("Listening on %s, wait connection", l.Addr())

	c, err := l.Accept()
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.InfoLogger.Printf("connection accepted")

	user, err := internal.GetRequiredSetting(internal.MysqlBinlogServerUser)
	tracelog.ErrorLogger.FatalOnError(err)
	password, err := internal.GetRequiredSetting(internal.MysqlBinlogServerPassword)
	tracelog.ErrorLogger.FatalOnError(err)
	conn, err := server.NewConn(c, user, password, Handler{})
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.InfoLogger.Printf("connection created")

	for {
		if err := conn.HandleCommand(); err != nil {
			tracelog.WarningLogger.Printf("Error handling command: %v", err)
			break
		}
	}
}

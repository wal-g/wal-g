package mysql

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/server"
	mysqldriver "github.com/go-sql-driver/mysql"
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
	ctx           context.Context
	cancel        context.CancelFunc
	replicaSource string
	rootFolder    storage.Folder
	dstDir        string

	// requiredGTIDs is the replica's already-executed set from
	// COM_BINLOG_DUMP_GTID; transactions it contains are skipped. This
	// command is MySQL-only; MariaDB replicas negotiate GTID state via
	// session variables and COM_BINLOG_DUMP, which is not wired up here.
	sentGTIDs      mysql.GTIDSet
	requiredGTIDs  *mysql.MysqlGTIDSet
	skipCurrentTxn bool
}

func newHandler(replicaSource string, root storage.Folder, dst string) *Handler {
	ctx, cancel := context.WithCancel(context.Background())
	sent, _ := mysql.ParseGTIDSet(mysql.MySQLFlavor, "")
	return &Handler{
		ctx:           ctx,
		cancel:        cancel,
		replicaSource: replicaSource,
		rootFolder:    root,
		dstDir:        dst,
		sentGTIDs:     sent,
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

// https://github.com/percona/percona-server/blob/8.0/libbinlogevents/include/control_events.h#L53-L108
func addRotateEvent(s *replication.BinlogStreamer, pos mysql.Position) error {
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

	return s.AddEventToStreamer(&rotateBinlogEvent)
}

func (h *Handler) waitReplicationIsDoneSafe() {
	if h.sentGTIDs.IsEmpty() {
		tracelog.InfoLogger.Println("S3 objects finished. No GTIDs were sent. Shutting down immediately.")
		os.Exit(0)
	}

	tracelog.InfoLogger.Printf("All S3 binlogs processed. Waiting for replica to catch up to GTID: %s", h.sentGTIDs.String())

	db, err := sql.Open("mysql", h.replicaSource)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("Failed to connect to replica SQL port: %v", err)
	}
	defer db.Close()

	for {
		if h.ctx.Err() != nil {
			tracelog.WarningLogger.Println("Client disconnected while waiting for completion. Handler shutting down, awaiting reconnect...")
			return
		}

		var executedStr string
		err := db.QueryRowContext(h.ctx, "SELECT @@global.gtid_executed").Scan(&executedStr)
		if err == nil {
			replicaSet, _ := mysql.ParseGTIDSet("mysql", executedStr)
			if replicaSet != nil && replicaSet.Contain(h.sentGTIDs) {
				tracelog.InfoLogger.Println("Replica has successfully caught up! We are safely done.")
				os.Exit(0)
			}
		} else {
			tracelog.WarningLogger.Printf("Failed to query replica GTID state: %v", err)
		}

		time.Sleep(1 * time.Second)
	}
}

func (h *Handler) downloadBinlog(logFolder storage.Folder, logFile storage.Object) (string, func(), error) {
	binlogName := utility.TrimFileExtension(logFile.GetName())
	binlogPath := path.Join(h.dstDir, binlogName)

	os.Remove(binlogPath)

	tracelog.InfoLogger.Printf("Downloading %s to disk...", binlogName)
	err := internal.DownloadFileTo(internal.NewFolderReader(logFolder), binlogName, binlogPath)
	if err != nil {
		return "", nil, fmt.Errorf("failed to download %s: %w", binlogName, err)
	}

	deleteFile := func() {
		if rmErr := os.Remove(binlogPath); rmErr != nil && !os.IsNotExist(rmErr) {
			tracelog.WarningLogger.Printf("Failed to remove temporary binlog file %s: %v", binlogPath, rmErr)
		}
	}

	return binlogPath, deleteFile, nil
}

func (h *Handler) makeEventHandler(s *replication.BinlogStreamer) func(*replication.BinlogEvent) error {
	return func(e *replication.BinlogEvent) error {
		if h.ctx.Err() != nil {
			return h.ctx.Err()
		}
		if int64(e.Header.Timestamp) > untilTS.Unix() {
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
		return s.AddEventToStreamer(e)
	}
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

func (h *Handler) streamSingleBinlog(
	p *replication.BinlogParser,
	logFolder storage.Folder,
	logFile storage.Object,
	startPos *mysql.Position,
	s *replication.BinlogStreamer,
) error {
	binlogName := utility.TrimFileExtension(logFile.GetName())

	binlogPath, deleteFile, err := h.downloadBinlog(logFolder, logFile)
	if err != nil {
		return err
	}
	defer deleteFile()

	tracelog.InfoLogger.Printf("Streaming %s to replica", binlogName)
	processPos := int64(startPos.Pos)
	startPos.Pos = 4

	return p.ParseFile(binlogPath, processPos, h.makeEventHandler(s))
}

func (h *Handler) streamBinlogFiles(startPos mysql.Position, s *replication.BinlogStreamer) {
	if err := addRotateEvent(s, startPos); err != nil {
		handleEventError(err, s)
	}

	logFolder := h.rootFolder.GetSubFolder(BinlogPath)
	logsToFetch, err := getLogsCoveringInterval(logFolder, startTS, true, utility.MaxTime)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to get logs list from storage: %v", err)
		return
	}

	if err := os.MkdirAll(h.dstDir, 0777); err != nil {
		tracelog.ErrorLogger.Printf("Failed to create dst dir: %v", err)
		return
	}

	p := replication.NewBinlogParser()
	p.SetRawMode(true)
	p.SetFlavor(mysql.MySQLFlavor)
	p.SetVerifyChecksum(true)

	for _, logFile := range logsToFetch {
		if h.ctx.Err() != nil {
			return
		}

		err := h.streamSingleBinlog(p, logFolder, logFile, &startPos, s)
		if err != nil && h.ctx.Err() == nil {
			handleEventError(err, s)
			return
		}
	}

	h.waitReplicationIsDoneSafe()
}

func (h *Handler) HandleRegisterSlave(data []byte) error {
	return nil
}

func (h *Handler) HandleBinlogDump(pos mysql.Position) (*replication.BinlogStreamer, error) {
	tracelog.InfoLogger.Printf("HandleBinlogDump: requested position %s:%d", pos.Name, pos.Pos)
	s := replication.NewBinlogStreamer()
	go h.streamBinlogFiles(pos, s)
	return s, nil
}

func (h *Handler) HandleBinlogDumpGTID(gtidSet *mysql.MysqlGTIDSet) (*replication.BinlogStreamer, error) {
	tracelog.InfoLogger.Printf("HandleBinlogDumpGTID: GTID=%s", gtidSet.String())
	h.requiredGTIDs = gtidSet
	s := replication.NewBinlogStreamer()
	go h.streamBinlogFiles(mysql.Position{Name: "host-binlog-file", Pos: 4}, s)
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
	// get necessary settings
	st, err := internal.ConfigureStorage()
	tracelog.ErrorLogger.FatalOnError(err)
	startTS, untilTS, _, err = getTimestamps(st.RootFolder(), since, until, "")
	tracelog.ErrorLogger.FatalOnError(err)

	// validate WALG_MYSQL_BINLOG_SERVER_REPLICA_SOURCE
	replicaSource, err := conf.GetRequiredSetting(conf.MysqlBinlogServerReplicaSource)
	tracelog.ErrorLogger.FatalOnError(err)
	_, err = mysqldriver.ParseDSN(replicaSource)
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

		go handleBinlogConnection(c, srv, replicaSource, st.RootFolder(), dstDir, user, password)
	}
}

func handleBinlogConnection(
	c net.Conn,
	srv *server.Server,
	replicaSource string,
	folder storage.Folder,
	dstDir string,
	user string,
	password string,
) {
	h := newHandler(replicaSource, folder, dstDir)
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

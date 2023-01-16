package mysql

import (
	"encoding/binary"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/server"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
	"net"
	"path"
)

func prepareToSync(folder storage.Folder, pos *mysql.Position) error {
	// download necessary binlog files
	dstDir, err := internal.GetLogsDstSettings(internal.MysqlBinlogDstSetting)
	if err != nil {
		return err
	}
	handler := newIndexHandler(dstDir)
	err = fetchLogsByBinlogName(folder, dstDir, pos.Name, handler)
	if err != nil {
		return err
	}
	err = handler.createIndexFile()
	return err
}

func addRotateEvent(s *replication.BinlogStreamer, pos *mysql.Position) error {
	rotateBinlogEvent := replication.BinlogEvent{}
	rotateBinlogEvent.RawData = make([]byte, 4+1+4+4+4+2+8)
	// generate header
	// timestamp default 4 bytes
	binlogEventPos := 4
	rotateBinlogEvent.RawData[binlogEventPos] = byte(replication.ROTATE_EVENT)
	binlogEventPos++
	binary.LittleEndian.PutUint32(rotateBinlogEvent.RawData[binlogEventPos:], 10000)
	binlogEventPos += 4
	binary.LittleEndian.PutUint32(rotateBinlogEvent.RawData[binlogEventPos:], uint32(4+1+4+4+4+2+8+len(pos.Name)))
	binlogEventPos += 4
	binary.LittleEndian.PutUint32(rotateBinlogEvent.RawData[binlogEventPos:], 0)
	binlogEventPos += 4
	binary.LittleEndian.PutUint16(rotateBinlogEvent.RawData[binlogEventPos:], 0)
	binlogEventPos += 2
	// set data
	binary.LittleEndian.PutUint64(rotateBinlogEvent.RawData[binlogEventPos:], uint64(pos.Pos))
	binlogEventPos += 8
	rotateBinlogEvent.RawData = append(rotateBinlogEvent.RawData, []byte(pos.Name)...)
	return s.AddEventToStreamer(&rotateBinlogEvent)
}

func startSync(folder storage.Folder, pos *mysql.Position, s *replication.BinlogStreamer) {
	// we should add rotate event to streamer, because replication protocol requires it
	err := addRotateEvent(s, pos)
	if err != nil {
		ok := s.AddErrorToStreamer(err)
		for !ok {
			ok = s.AddErrorToStreamer(err)
		}
	}

	p := replication.NewBinlogParser()

	f := func(e *replication.BinlogEvent) error {
		err := s.AddEventToStreamer(e)
		return err
	}
	logFolder := folder.GetSubFolder(BinlogPath)
	logFiles, err := getLogsAfterBinlog(logFolder, pos.Name)
	if err != nil {
		ok := s.AddErrorToStreamer(err)
		for !ok {
			ok = s.AddErrorToStreamer(err)
		}
	}
	dstDir, _ := internal.GetLogsDstSettings(internal.MysqlBinlogDstSetting)
	for _, logFile := range logFiles {
		binlogName := utility.TrimFileExtension(logFile.GetName())
		tracelog.InfoLogger.Printf("Synced binlog file %s", binlogName)
		binlogPath := path.Join(dstDir, binlogName)
		err := p.ParseFile(binlogPath, int64(pos.Pos), f)

		if err != nil {
			ok := s.AddErrorToStreamer(err)
			for !ok {
				ok = s.AddErrorToStreamer(err)
			}
		}
		pos.Pos = 4
	}
	tracelog.InfoLogger.Println("Binlog sync finished")
}

type Handler struct {
	server.EmptyReplicationHandler
}

func (h Handler) HandleRegisterSlave(data []byte) error {
	return nil
}

func (h Handler) HandleBinlogDump(pos mysql.Position) (*replication.BinlogStreamer, error) {
	s := replication.NewBinlogStreamer()
	folder, _ := internal.ConfigureFolder()
	err := prepareToSync(folder, &pos)
	if err != nil {
		return nil, err
	}
	go startSync(folder, &pos, s)
	return s, nil
}

func (h Handler) HandleBinlogDumpGTID(gtidSet *mysql.MysqlGTIDSet) (*replication.BinlogStreamer, error) {
	s := replication.NewBinlogStreamer()
	folder, _ := internal.ConfigureFolder()

	pos, err := getPositionBeforeGTID(folder, gtidSet, "mysql")
	if err != nil {
		return nil, err
	}

	err = prepareToSync(folder, pos)
	if err != nil {
		return nil, err
	}
	go startSync(folder, pos, s)
	return s, nil
}

func (h Handler) HandleQuery(query string) (*mysql.Result, error) {
	switch query {
	case "SELECT @master_binlog_checksum":
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"master_binlog_checksum"}, [][]interface{}{{"NONE"}})
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil
	case "SHOW GLOBAL VARIABLES LIKE 'BINLOG_CHECKSUM'":
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"BINLOG_CHECKSUM"}, [][]interface{}{{"NONE"}})
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil
	case "SELECT @@GLOBAL.SERVER_ID":
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"SERVER_ID"}, [][]interface{}{{"10000"}})
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil
	case "SELECT @source_binlog_checksum":
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"source_binlog_checksum"}, [][]interface{}{{"1"}})
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil
	case "SELECT @@GLOBAL.GTID_MODE":
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"GTID_MODE"}, [][]interface{}{{"ON"}})
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil
	case "SELECT @@GLOBAL.SERVER_UUID":
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"SERVER_UUID"}, [][]interface{}{{"123"}})
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil
	default:
		return nil, nil
	}
}

func HandleBinlogServer() {
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
			break
		}
	}
}

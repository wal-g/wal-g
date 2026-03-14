package mysql

import (
	"context"
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

type BinlogFileEntry struct {
	Name    string
	Path    string
	MaxGTID string
}

type BinlogServerState struct {
	mu           sync.RWMutex
	files        []BinlogFileEntry
	provideErr   error
	provideDone  bool
	newFileReady *sync.Cond

	replicationDoneMu sync.Mutex
	replicationDone   bool
}

func newBinlogServerState() *BinlogServerState {
	s := &BinlogServerState{}
	s.newFileReady = sync.NewCond(&s.mu)
	return s
}

func (s *BinlogServerState) addFile(entry BinlogFileEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.files = append(s.files, entry)
	s.newFileReady.Broadcast()
}

func (s *BinlogServerState) setProvideError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.provideErr = err
	s.newFileReady.Broadcast()
}

func (s *BinlogServerState) setProvideDone() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.provideDone = true
	s.newFileReady.Broadcast()
}

func (s *BinlogServerState) waitForFileAtIndex(ctx context.Context, idx int) (BinlogFileEntry, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		if ctx.Err() != nil {
			return BinlogFileEntry{}, false, ctx.Err()
		}
		if idx < len(s.files) {
			return s.files[idx], true, nil
		}
		if s.provideDone {
			return BinlogFileEntry{}, false, nil
		}
		if s.provideErr != nil {
			return BinlogFileEntry{}, false, s.provideErr
		}
		s.newFileReady.Wait()
	}
}

func (s *BinlogServerState) findStartIndex(clientGTIDSet mysql.GTIDSet) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if clientGTIDSet == nil || len(s.files) == 0 {
		return 0
	}

	result := 0
	for i, entry := range s.files {
		if entry.MaxGTID == "" {
			break
		}
		maxGTIDSet, err := mysql.ParseGTIDSet("mysql", entry.MaxGTID)
		if err != nil {
			break
		}
		if clientGTIDSet.Contain(maxGTIDSet) {
			result = i + 1
		} else {
			break
		}
	}
	return result
}

func (s *BinlogServerState) markReplicationDone() bool {
	s.replicationDoneMu.Lock()
	defer s.replicationDoneMu.Unlock()
	if s.replicationDone {
		return false
	}
	s.replicationDone = true
	return true
}

type Handler struct {
	server.EmptyReplicationHandler
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex

	state *BinlogServerState
}

func newHandler(state *BinlogServerState) *Handler {
	ctx, cancel := context.WithCancel(context.Background())
	return &Handler{
		ctx:    ctx,
		cancel: cancel,
		state:  state,
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

func (s *BinlogServerState) cleanupFiles() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, entry := range s.files {
		if err := os.Remove(entry.Path); err != nil && !os.IsNotExist(err) {
			tracelog.WarningLogger.Printf("Failed to remove binlog file %s: %v", entry.Path, err)
		} else {
			tracelog.InfoLogger.Printf("Removed binlog file %s", entry.Path)
		}
	}
}

func (h *Handler) waitReplicationIsDone() error {
	h.state.mu.Lock()
	for !h.state.provideDone {
		if h.ctx.Err() != nil {
			h.state.mu.Unlock()
			return h.ctx.Err()
		}
		h.state.newFileReady.Wait()
	}
	h.state.mu.Unlock()

	if !h.state.markReplicationDone() {
		return nil
	}

	tracelog.InfoLogger.Println("Replication is done")
	go func() {
		time.Sleep(2 * time.Second)
		tracelog.InfoLogger.Println("Shutting down binlog server")
		h.state.cleanupFiles()
		os.Exit(0)
	}()
	return nil
}

func (h *Handler) sendEventsFromBinlogFiles(startIdx int, pos mysql.Position, s *replication.BinlogStreamer) {
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
		return s.AddEventToStreamer(e)
	}

	currentIdx := startIdx
	currentPos := int64(pos.Pos)

	for {
		if h.ctx.Err() != nil {
			return
		}

		entry, hasMore, err := h.state.waitForFileAtIndex(h.ctx, currentIdx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			handleEventError(err, s)
			return
		}

		if !hasMore {
			if err := h.waitReplicationIsDone(); err != nil {
				if !errors.Is(err, context.Canceled) {
					tracelog.InfoLogger.Println("Error while waiting replication done:", err)
				}
			}
			return
		}

		tracelog.InfoLogger.Printf("Sending binlog file %s (index=%d)", entry.Name, currentIdx)

		err = p.ParseFile(entry.Path, currentPos, f)
		if err != nil && h.ctx.Err() == nil {
			handleEventError(err, s)
			return
		}

		currentIdx++
		currentPos = 4
	}
}

func (h *Handler) HandleRegisterSlave(data []byte) error {
	return nil
}

func (h *Handler) HandleBinlogDump(pos mysql.Position) (*replication.BinlogStreamer, error) {
	tracelog.InfoLogger.Printf("HandleBinlogDump: requested position %s:%d", pos.Name, pos.Pos)
	s := replication.NewBinlogStreamer()
	go h.sendEventsFromBinlogFiles(0, pos, s)
	return s, nil
}

func (h *Handler) HandleBinlogDumpGTID(gtidSet *mysql.MysqlGTIDSet) (*replication.BinlogStreamer, error) {
	tracelog.InfoLogger.Printf("HandleBinlogDumpGTID: GTID=%s", gtidSet.String())

	startIdx := h.state.findStartIndex(gtidSet)
	tracelog.InfoLogger.Printf("HandleBinlogDumpGTID: starting from file index=%d", startIdx)

	s := replication.NewBinlogStreamer()
	go h.sendEventsFromBinlogFiles(startIdx, mysql.Position{Name: "host-binlog-file", Pos: 4}, s)
	return s, nil
}

func (h *Handler) HandleQuery(query string) (*mysql.Result, error) {
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
		return nil, nil
	}
}

func extractMaxGTID(binlogPath string) string {
	result := ""

	p := replication.NewBinlogParser()
	p.SetRawMode(true)
	p.SetFlavor(mysql.MySQLFlavor)
	p.SetVerifyChecksum(true)

	err := p.ParseFile(binlogPath, 4, func(e *replication.BinlogEvent) error {
		if e.Header.EventType == replication.GTID_EVENT {
			gtidEvent := &replication.GTIDEvent{}
			if err := gtidEvent.Decode(e.RawData[replication.EventHeaderSize:]); err == nil {
				u, _ := uuid.FromBytes(gtidEvent.SID)
				result = u.String() + ":1-" + strconv.Itoa(int(gtidEvent.GNO))
			}
		}
		return nil
	})
	if err != nil {
		tracelog.WarningLogger.Printf("extractMaxGTID: failed to parse %s: %v", binlogPath, err)
	}

	return result
}

func provideLogsGlobal(ctx context.Context, folder storage.Folder, dstDir string, startTS, endTS time.Time, state *BinlogServerState) {
	if err := os.MkdirAll(dstDir, 0777); err != nil {
		state.setProvideError(err)
		return
	}

	logFolder := folder.GetSubFolder(BinlogPath)
	logsToFetch, err := getLogsCoveringInterval(logFolder, startTS, true, utility.MaxTime)
	if err != nil {
		state.setProvideError(err)
		return
	}

	for _, logFile := range logsToFetch {
		if ctx.Err() != nil {
			return
		}

		binlogName := utility.TrimFileExtension(logFile.GetName())
		binlogPath := path.Join(dstDir, binlogName)
		tracelog.InfoLogger.Printf("downloading %s into %s", binlogName, binlogPath)

		if removeErr := os.Remove(binlogPath); removeErr != nil && !os.IsNotExist(removeErr) {
			tracelog.WarningLogger.Printf("failed to remove existing file %s: %v", binlogPath, removeErr)
		}

		if err = internal.DownloadFileTo(internal.NewFolderReader(logFolder), binlogName, binlogPath); err != nil {
			tracelog.ErrorLogger.Printf("failed to download %s: %v", binlogName, err)
			state.setProvideError(err)
			return
		}

		maxGTID := extractMaxGTID(binlogPath)
		state.addFile(BinlogFileEntry{
			Name:    binlogName,
			Path:    binlogPath,
			MaxGTID: maxGTID,
		})

		timestamp, err := GetBinlogStartTimestamp(binlogPath, mysql.MySQLFlavor)
		if err != nil {
			state.setProvideError(err)
			return
		}
		if timestamp.After(endTS) {
			break
		}
	}

	state.setProvideDone()
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

	dstDir, err := internal.GetLogsDstSettings(conf.MysqlBinlogDstSetting)
	tracelog.ErrorLogger.FatalOnError(err)

	globalState := newBinlogServerState()

	provideCtx, provideCancel := context.WithCancel(context.Background())
	defer provideCancel()

	go provideLogsGlobal(provideCtx, st.RootFolder(), dstDir, startTS, untilTS, globalState)

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

		h := newHandler(globalState)

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

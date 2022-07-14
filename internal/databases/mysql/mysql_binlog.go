package mysql

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"github.com/siddontang/go-log/log"
	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"

	"github.com/wal-g/wal-g/utility"
)

const BinlogSentinelPath = "binlog_sentinel_" + utility.VersionStr + ".json"

// 128k should be enough to parse prev_gtids event
const BinlogReadHeaderSize = 128 * 1024

type BinlogSentinelDto struct {
	GTIDArchived string `json:"GtidArchived"`
}

func (dto *BinlogSentinelDto) String() string {
	result, _ := json.Marshal(dto)
	return string(result)
}

func FetchBinlogSentinel(folder storage.Folder, sentinelDto interface{}) error {
	reader, err := folder.ReadObject(BinlogSentinelPath)
	if err != nil {
		return err
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, sentinelDto)
	if err != nil {
		return err
	}
	return nil
}

func UploadBinlogSentinel(folder storage.Folder, sentinelDto interface{}) error {
	sentinelName := BinlogSentinelPath
	dtoBody, err := json.Marshal(sentinelDto)
	if err != nil {
		return internal.NewSentinelMarshallingError(sentinelName, err)
	}

	return folder.PutObject(sentinelName, bytes.NewReader(dtoBody))
}

func GetBinlogPreviousGTIDs(filename string, flavor string) (*mysql.MysqlGTIDSet, error) {
	var found bool
	previousGTID := &replication.PreviousGTIDsEvent{}

	parser := replication.NewBinlogParser()
	parser.SetFlavor(flavor)
	parser.SetVerifyChecksum(false) // the faster, the better
	parser.SetRawMode(true)         // choose events to parse manually
	err := parser.ParseFile(filename, 0, func(event *replication.BinlogEvent) error {
		if event.Header.EventType == replication.PREVIOUS_GTIDS_EVENT {
			err := previousGTID.Decode(event.RawData[replication.EventHeaderSize:])
			if err != nil {
				return err
			}
			found = true
			return fmt.Errorf("shallow file read finished")
		}
		return nil
	})

	if err != nil && !found {
		return nil, errors.Wrapf(err, "binlog-push: could not parse binlog file '%s'\n", filename)
	}

	res, err := mysql.ParseMysqlGTIDSet(previousGTID.GTIDSets)
	if err != nil {
		return nil, err
	}
	result, ok := res.(*mysql.MysqlGTIDSet)
	if !ok {
		tracelog.ErrorLogger.Fatalf("cannot cast nextPreviousGTIDs to MysqlGTIDSet. Should never be here. Actual type: %T\n", res)
	}
	return result, nil
}

func GetBinlogPreviousGTIDsRemote(folder storage.Folder, filename string, flavor string) (*mysql.MysqlGTIDSet, error) {
	binlogName := utility.TrimFileExtension(filename)
	fh, err := internal.DownloadAndDecompressStorageFile(folder, binlogName)
	if err != nil {
		return nil, fmt.Errorf("failed to read binlog %s: %w", binlogName, err)
	}
	defer utility.LoggedClose(fh, "failed to close binlog")
	tmp, err := os.CreateTemp("", binlogName)
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	defer utility.LoggedClose(tmp, "failed to close temp file")
	_, err = io.CopyN(tmp, fh, BinlogReadHeaderSize)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read binlog beginning")
	}
	prevGtid, err := GetBinlogPreviousGTIDs(tmp.Name(), flavor)
	if err != nil {
		return nil, fmt.Errorf("failed to parse binlog %s: %w", binlogName, err)
	}
	return prevGtid, nil
}

func GetBinlogStartTimestamp(filename string, flavor string) (time.Time, error) {
	var ts uint32
	parser := replication.NewBinlogParser()
	parser.SetFlavor(flavor)
	parser.SetVerifyChecksum(false) // the faster, the better
	parser.SetRawMode(true)         // choose events to parse manually
	err := parser.ParseFile(filename, 0, func(event *replication.BinlogEvent) error {
		ts = event.Header.Timestamp
		return fmt.Errorf("shallow file read finished")
	})
	if err != nil && ts == 0 {
		return time.Time{}, fmt.Errorf("failed to parse binlog %s: %w", filename, err)
	}
	return time.Unix(int64(ts), 0), nil
}

/*
Mysql binlog file names looks like foobar.000001, foobar.000002 (with leading zeroes)
And it looks like they can be compared lexicographically, but..
The next name after foobar.999999 is foobar.1000000 (7 digits) and it cannot be compared so.
*/
func BinlogNum(filename string) int {
	p := strings.LastIndexAny(filename, ".")
	if p < 0 {
		tracelog.ErrorLogger.Panicf("unexpected binlog name: %v", filename)
	}
	num, err := strconv.Atoi(filename[p+1:])
	if err != nil {
		tracelog.ErrorLogger.Panicf("unexpected binlog name: %v", filename)
	}
	return num
}

func BinlogPrefix(filename string) string {
	p := strings.LastIndexAny(filename, ".")
	if p < 0 {
		tracelog.ErrorLogger.Panicf("unexpected binlog name: %v", filename)
	}
	return filename[:p]
}

// BinlogStream is a replication.BinlogSyncer masquerading as an io.Reader.
// We use it to extract a raw binary log from the server.
type BinlogStream struct {
	replication.BinlogSyncer
	replication.BinlogStreamer

	hasStarted    bool
	lastData      []byte
	lastOffset    uint32
	lastEventSize uint32
}

// GetBinlogSyncerConfig parses apart walg_mysql_datasource_name into the format
// that replication.BinlogSyncer wants
func GetBinlogSyncerConfig(datasource string, flavor string) (*replication.BinlogSyncerConfig, error) {
	driverConfig, err := mysqlDriver.ParseDSN(datasource)
	if err != nil {
		return nil, err
	}

	cfg := &replication.BinlogSyncerConfig{
		Host:           driverConfig.Addr,
		User:           driverConfig.User,
		Password:       driverConfig.Passwd,
		ServerID:       99991, // we pretend to be a replication server with this ID
		Flavor:         flavor,
		RawModeEnabled: true,
	}

	// if you specify a CA cert in the wal-g settings, it should act like we
	// specified "&tls=custom"
	caFile, ok := internal.GetSetting(internal.MysqlSslCaSetting)
	if ok {
		driverConfig.TLSConfig = "custom"
	}

	switch driverConfig.TLSConfig {
	case "true":
		cfg.TLSConfig = &tls.Config{}
	case "skip-verify":
		cfg.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	case "custom":
		rootCertPool := x509.NewCertPool()
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
			return nil, fmt.Errorf("failed to load certificate from %s", caFile)
		}
		cfg.TLSConfig = &tls.Config{RootCAs: rootCertPool}
	}

	return cfg, err
}

// NewBinlogStream begins retrieval of a raw binary log from the MySQL server
func NewBinlogStream(db *sql.DB, binlog string) (*BinlogStream, error) {
	flavor, err := getMySQLFlavor(db)
	if err != nil {
		return nil, err
	}

	datasourceName, err := internal.GetRequiredSetting(internal.MysqlDatasourceNameSetting)
	if err != nil {
		return nil, err
	}

	cfg, err := GetBinlogSyncerConfig(datasourceName, flavor)
	if err != nil {
		return nil, err
	}

	// the go-mysql integrated logging library is super noisy
	log.SetLevel(log.LevelError)
	stream := BinlogStream{BinlogSyncer: *replication.NewBinlogSyncer(*cfg)}

	// even if server is using GTIDs we want a specific file at offset 0
	// (we're trying to sync this specific binlog file)
	streamer, err := stream.StartSync(mysql.Position{Name: binlog, Pos: 0})
	if err != nil {
		return nil, err
	}
	stream.BinlogStreamer = *streamer
	return &stream, nil
}

// Read converts the per-event output from BinlogStreamer.GetEvent() to the []byte
// output expected by the io.Reader interface.
func (b *BinlogStream) Read(p []byte) (int, error) {
	if b.lastData == nil {
		// no existing data left from a prior binlog event, read next event
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		event, err := b.GetEvent(ctx)
		cancel()
		if err != nil {
			return 0, err
		}

		// handle special event types
		switch event.Header.EventType {
		case replication.ROTATE_EVENT:
			if event.Header.Timestamp == 0 || event.Header.LogPos == 0 {
				// fake rotate event, ignore
				return 0, nil
			}
		case replication.HEARTBEAT_EVENT:
			// heartbeat events aren't "real data" according to
			// https://dev.mysql.com/doc/internals/en/heartbeat-event.html
			// and break mysqlbinlog, ignore
			return 0, nil
		case replication.FORMAT_DESCRIPTION_EVENT:
			// beginning of binlog
			if b.hasStarted {
				// we are now reading binlog #2, stop.
				return 0, io.EOF
			}
			// start the stream by writing the binlog header
			b.hasStarted = true
			b.lastData = event.RawData
			return copy(p, replication.BinLogFileHeader), nil
		}

		n := copy(p, event.RawData)
		if event.Header.EventSize > uint32(n) {
			// the event is larger than the buffer allocated by read,
			// so we need to save the leftovers for next Read()
			b.lastData = event.RawData
			b.lastOffset = uint32(n - 1)
			b.lastEventSize = event.Header.EventSize
		}
		return n, nil
	}

	// prior event has leftover data that we need to finish
	n := copy(p, b.lastData[b.lastOffset:])
	if b.lastOffset+uint32(n) >= b.lastEventSize {
		// we have now finished writing prior data
		b.lastOffset = 0
		b.lastData = nil
	} else {
		// still not done with last event,
		// increment the offset up by however many bytes we just read
		b.lastOffset += uint32(n)
	}
	return n, nil
}

package mysql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pkg/errors"
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

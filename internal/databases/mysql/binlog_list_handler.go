package mysql

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/jedib0t/go-pretty/table"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
	"io"
	"os"
	"path"
)

var shallowReadFinished = fmt.Errorf("minimal amount of data found")

type BinlogInfo struct {
	Name    string `json:"name"`
	GTIDSet string `json:"gtid_set"`
	Size    int64  `json:"size"`
}

func HandleBinlogList(local, shallow, pretty, json bool) {
	db, err := getMySQLConnection()
	tracelog.ErrorLogger.FatalOnError(err)
	defer utility.LoggedClose(db, "")

	binlogsFolder, err := getMySQLBinlogsFolder(db)
	tracelog.ErrorLogger.FatalOnError(err)

	binlogs, err := getMySQLSortedBinlogs(db)
	tracelog.ErrorLogger.FatalOnError(err)

	allBinlogs := []BinlogInfo{}
	for _, binLog := range binlogs {
		binlogInfo, err := listBinLog(path.Join(binlogsFolder, binLog), binLog, shallow)
		tracelog.ErrorLogger.FatalOnError(err)

		allBinlogs = append(allBinlogs, binlogInfo)
	}

	switch {
	case json:
		err = internal.WriteAsJSON(allBinlogs, os.Stdout, pretty)
		tracelog.ErrorLogger.FatalOnError(err)
	default:
		writePrettyBinlogListDetails(allBinlogs, os.Stdout)
	}
}

func listBinLog(filename string, binLog string, shallow bool) (BinlogInfo, error) {
	result := BinlogInfo{}

	gtidSet := newGtidSet()

	parser := replication.NewBinlogParser()
	parser.SetFlavor("mysql")
	parser.SetVerifyChecksum(false) // the faster, the better
	parser.SetRawMode(true)         // choose events to parse manually
	err := parser.ParseFile(filename, 0, func(event *replication.BinlogEvent) error {
		switch event.Header.EventType {
		case replication.GTID_EVENT:
			e := &replication.GTIDEvent{}
			err := e.Decode(event.RawData[19:])
			if err != nil {
				return err
			}
			// SID - 16 byte sequence
			// GNO - 8 byte integer
			gtidSet.append(uuid.FromBytesOrNil(e.SID), e.GNO)
			if shallow {
				return shallowReadFinished
			}
			tracelog.WarningLogger.Printf("GTID_EVENT %+v", e)

		case replication.ANONYMOUS_GTID_EVENT:
			e := &replication.GTIDEvent{}
			err := e.Decode(event.RawData[19:])
			if err != nil {
				return err
			}
			//tracelog.WarningLogger.Printf("ANONYMOUS_GTID_EVENT %+v", e)
		case replication.PREVIOUS_GTIDS_EVENT:
			e := &replication.PreviousGTIDsEvent{}
			err := e.Decode(event.RawData[19:])
			if err != nil {
				return err
			}
			tracelog.WarningLogger.Printf("PREVIOUS_GTIDS_EVENT %+v", e)
		case replication.MARIADB_GTID_LIST_EVENT:
			tracelog.ErrorLogger.Fatalf("MARIADB_GTID_LIST_EVENT not supported")
		case replication.MARIADB_GTID_EVENT:
			tracelog.ErrorLogger.Fatalf("MARIADB_GTID_EVENT not supported")
		}
		return nil
	})

	if err != nil && err != shallowReadFinished {
		return result, errors.Wrapf(err, "binlog-list: could not parse file '%s'\n", filename)
	}

	fi, err := os.Stat(filename)
	if err != nil {
		return result, errors.Wrapf(err, "binlog-list: file stat error '%s'", err.Error())
	}

	result.Name = binLog
	result.GTIDSet = gtidSet.ToString()
	result.Size = fi.Size()

	return result, nil
}

func writePrettyBinlogListDetails(binlogInfo []BinlogInfo, output io.Writer) {
	writer := table.NewWriter()
	writer.SetOutputMirror(output)
	defer writer.Render()
	writer.AppendHeader(table.Row{"#", "Name", "GTID Set", "Size"})
	for idx := range binlogInfo {
		b := &binlogInfo[idx]
		writer.AppendRow(table.Row{
			idx,
			b.Name,
			b.GTIDSet,
			b.Size,
		})
	}
}

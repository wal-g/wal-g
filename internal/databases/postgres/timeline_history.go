package postgres

/*
This module can represent a Timeline History file which can be
retrieved from Postgres using the TIMELINEHISTORY SQL command.
A Timeline History file belongs to a timeline (which also will be the last Row in the file), and contains multiple rows.
Each row describes at what LSN that timeline came to be.

Example history file:
1	0/2A33FF50	no recovery target specified

2	0/2A3400E8	no recovery target specified

By storing the Timeline History file in this object,
it can easilly be searched for the timeline that a specific LSN belongs too.
Furthermore it can be read as an IOReader (having a Name() and Read() function) to easilly writeout by the WalUploader.
*/

import (
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pglogrepl"
)

// The TimeLineHistFileRow struct represents one line in the TimeLineHistory file
type TimeLineHistFileRow struct {
	TimeLineID uint32
	StartLSN   pglogrepl.LSN
	Comment    string
}

// The TimeLineHistFile struct represents a TimeLineHistory file containing TimeLineHistFileRows.
// Since TimeLineHistFileRows are only parsed 0 or 1 rimes, the data is only
// preserved as []byte and parsed to TimeLineHistFileRows when required.
type TimeLineHistFile struct {
	TimeLineID uint32
	Filename   string
	data       []byte
	readIndex  int
}

// NewTimeLineHistFile is a helper function to define a new TimeLineHistFile
func NewTimeLineHistFile(timelineid uint32, filename string, body []byte) (TimeLineHistFile, error) {
	tlh := TimeLineHistFile{TimeLineID: timelineid, Filename: filename, data: body}
	return tlh, nil
}

// rows parses the data ([]byte) from a TimeLineHistFile and returns the TimeLineHistFileRows that are contained.
func (tlh TimeLineHistFile) rows() ([]TimeLineHistFileRow, error) {
	var rows []TimeLineHistFileRow
	r := regexp.MustCompile(`[^\s]+`)
	for _, row := range strings.Split(string(tlh.data), "\n") {
		// Remove comments and split by one or more whitespace characters
		// FindAllStrings removes front spaces, and returns up to 3 cols.
		cols := r.FindAllString(strings.Split(row, "#")[0], 3)
		if len(cols) >= 2 {
			tlhr := TimeLineHistFileRow{}
			tlid, err := strconv.Atoi(cols[0])
			if err != nil {
				return rows, err
			}
			tlhr.TimeLineID = uint32(tlid)
			tlhr.StartLSN, err = pglogrepl.ParseLSN(cols[1])
			if err != nil {
				return rows, err
			}
			if len(cols) > 2 {
				tlhr.Comment = cols[2]
			}
			rows = append(rows, tlhr)
		}
	}
	return rows, nil
}

// LSNToTimeLine uses rows() to get all TimeLineHistFileRows and from those rows get the timeline that a LS belongs too.
func (tlh TimeLineHistFile) LSNToTimeLine(lsn pglogrepl.LSN) (uint32, error) {
	rows, err := tlh.rows()
	if err != nil {
		return 0, err
	}
	// Sorting makes LSNToTimeLine more efficient
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].TimeLineID < rows[j].TimeLineID
	})
	for _, row := range rows {
		if lsn < row.StartLSN {
			return row.TimeLineID, nil
		}
	}
	return tlh.TimeLineID, nil
}

// Name returns the filename of this wal segment. This is a convenience function used by the WalUploader.
func (tlh TimeLineHistFile) Name() string {
	return tlh.Filename
}

// Read is what makes the WalSegment an io.Reader, which can be handled by WalUploader.UploadWalFile.
func (tlh TimeLineHistFile) Read(p []byte) (n int, err error) {
	n = copy(p, tlh.data[tlh.readIndex:])
	tlh.readIndex += n
	if len(tlh.data) <= tlh.readIndex {
		return n, io.EOF
	}
	return n, nil
}

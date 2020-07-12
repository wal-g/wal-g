package internal

import (
	"github.com/jackc/pglogrepl"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// The TimeLineHistFileRow struct represents one line in the TimeLineHistory file
type TimeLineHistFileRow struct {
	TimeLineID int
	StartLSN   pglogrepl.LSN
	Comment	   string
}

// The TimeLineHistFileRow struct represents one line in the TimeLineHistory file
type TimeLineHistFile struct {
	TimeLineID int32
	Filename string
	data     []byte
	readIndex int
}

func NewTimeLineHistFile(timelineid int32, filename string, body []byte) (TimeLineHistFile, error) {
	tlh := TimeLineHistFile{TimeLineID: timelineid, Filename: filename, data: body}
	return tlh, nil
}

// LSNToTimeLine  the timeline that is applicable to a specific LSN
func (tlh TimeLineHistFile) Rows() ([]TimeLineHistFileRow, error) {
	var err error
	var rows []TimeLineHistFileRow
	r := regexp.MustCompile("[^\\s]+")
	for _, row := range strings.Split(string(tlh.data), "\n") {
		// Remove comments and split by one or more whitespace characters
		// FIndAllStrings removes front spaces, and returns up to 3 cols.
		cols := r.FindAllString(strings.Split(row, "#")[0], 3)
		if len(cols) >= 2 {
			tlhr := TimeLineHistFileRow{}
			tlhr.TimeLineID, err = strconv.Atoi(cols[0])
			if err != nil {
				return rows, err
			}
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

// LSNToTimeLine  the timeline that is applicable to a specific LSN
func (tlh TimeLineHistFile) LSNToTimeLine(lsn pglogrepl.LSN) (int32, error) {
	rows, err := tlh.Rows()
	if err != nil {
		return 0, err
	}
	// Sorting makes LSNToTimeLine more efficient
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].TimeLineID < rows[j].TimeLineID
	})
	for _, row := range rows {
		if lsn < row.StartLSN {
			return int32(row.TimeLineID), nil
		}
	}
	return tlh.TimeLineID, nil
}
/*
Example history file:
1	0/2A33FF50	no recovery target specified

2	0/2A3400E8	no recovery target specified

3	0/2A36D148	no recovery target specified

4	0/2A373A40	no recovery target specified

5	0/2A37A720	no recovery target specified

6	0/2A3817B8	no recovery target specified

7	0/2A497520	no recovery target specified

8	0/2A5ACF08	no recovery target specified

9	0/2A6C2498	no recovery target specified

10	0/30F1FF48	no recovery target specified

11	0/3F552470	no recovery target specified

12	0/416A43F8	no recovery target specified

13	0/437F6630	no recovery target specified

14	0/45948B20	no recovery target specified
*/

// Name returns the filename of this wal segment
func (tlh TimeLineHistFile) Name() string {
	return tlh.Filename
}

// Read is what makes the WalSegment a io.Reader, which can be handled by WalUploader.UploadWalFile
func (tlh TimeLineHistFile) Read(p []byte) (n int, err error) {
	n = copy(p, tlh.data[tlh.readIndex:])
	tlh.readIndex += n
	if len(tlh.data) <= tlh.readIndex {
		return n, io.EOF
	}
	return n, nil
}


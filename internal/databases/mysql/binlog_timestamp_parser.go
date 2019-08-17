package mysql

import (
	"bytes"
	"encoding/binary"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"io"
	"os"
	"time"
)

const (
	BinlogMagicOffset     = 4
	TimestampHeaderLength = 4
)

func parseFirstTimestampFromHeader(fileReadSeekCloser ioextensions.ReadSeekCloser) (int32, error) {
	defer fileReadSeekCloser.Close()

	if _, err := fileReadSeekCloser.Seek(BinlogMagicOffset, io.SeekStart); err != nil {
		return 0, errors.Wrapf(err, "Unable to parse header timestamp from file due %v", err)
	}
	headerEventBytes := make([]byte, TimestampHeaderLength)
	if _, err := fileReadSeekCloser.Read(headerEventBytes); err != nil {
		return 0, errors.Wrapf(err, "Unable to parse header timestamp from file due %v", err)
	}

	var timestamp int32
	if err := binary.Read(bytes.NewReader(headerEventBytes), binary.LittleEndian, &timestamp); err != nil {
		return 0, errors.Wrapf(err, "Unable to parse header timestamp from file due %v", err)
	}

	return timestamp, nil
}

func parseFromBinlog(filePath string) (time.Time, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return time.Time{}, err
	}

	timestamp, err := parseFirstTimestampFromHeader(file)
	if err != nil {
		return time.Time{}, err
	}

	return time.Unix(int64(timestamp), 0), nil
}

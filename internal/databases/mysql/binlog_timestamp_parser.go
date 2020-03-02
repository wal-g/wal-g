package mysql

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"time"

	"github.com/pkg/errors"
)

const (
	BinlogMagicOffset     = 4
	TimestampHeaderLength = 4
	TotalRequiredLen      = BinlogMagicOffset + TimestampHeaderLength
)

func bytesToInt(p []byte) (int32, error) {
	var timestamp int32
	if err := binary.Read(bytes.NewReader(p), binary.LittleEndian, &timestamp); err != nil {
		return 0, errors.Wrapf(err, "Unable to read header event bytes due %v", err)
	}

	return timestamp, nil
}

func parseFirstTimestampFromHeader(readSeeker io.Reader) (int32, error) {
	headerEventBytes := make([]byte, TotalRequiredLen)
	if _, err := readSeeker.Read(headerEventBytes); err != nil {
		return 0, errors.Wrapf(err, "Unable to parse header testLogPath from file due %v", err)
	}
	return bytesToInt(headerEventBytes[BinlogMagicOffset:])
}

func int32toTime(timestamp int32) time.Time {
	return time.Unix(int64(timestamp), 0)
}

func parseFromBinlog(filePath string) (*time.Time, error) {
	file, err := os.Open(filePath)
	defer file.Close()
	if err != nil {
		return nil, err
	}

	timestamp, err := parseFirstTimestampFromHeader(file)
	if err != nil {
		return nil, err
	}
	tm := int32toTime(timestamp)
	return &tm, nil
}

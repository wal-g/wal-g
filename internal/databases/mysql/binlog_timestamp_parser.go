package mysql

import (
	"bytes"
	"encoding/binary"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"io"
	"os"
	"time"
)

func ParseFirstTimestampFromHeader(fileReadSeekCloser ioextensions.ReadSeekCloser) (int32, error) {
	defer fileReadSeekCloser.Close()

	if _, err := fileReadSeekCloser.Seek(4, io.SeekStart); err != nil {
		return 0, err
	}

	timestamp := make([]byte, 4)

	if _, err := fileReadSeekCloser.Read(timestamp); err != nil {
		return 0, err
	}

	var num int32
	err := binary.Read(bytes.NewReader(timestamp), binary.LittleEndian, &num)

	if err != nil {
		return 0, err
	}

	return num, nil
}

func parseFromBinlog(filePath string) (time.Time, error) {
	file, err := os.Open(filePath)

	if err != nil {
		return time.Time{}, err
	}

	timestamp, err := ParseFirstTimestampFromHeader(file)

	if err != nil {
		return time.Time{}, err
	}

	return time.Unix(int64(timestamp), 0), nil
}

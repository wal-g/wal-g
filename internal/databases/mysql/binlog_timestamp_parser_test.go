package mysql

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

const testFilename string = "./testdata/binlog_test"

func TestParseBinlogTimestampFromHeader(t *testing.T) {
	timestamp := time.Unix(int64(1565528401), 0)

	parsed, err := parseFromBinlog(testFilename)

	assert.NoError(t, err)
	assert.NotNil(t, parsed)
	assert.Equal(t, timestamp, parsed)
}

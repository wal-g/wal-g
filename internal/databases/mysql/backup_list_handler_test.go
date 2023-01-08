package mysql

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

var emptyColumnsBackups = []BackupDetail{
	{
		BackupName: "backupName",
	},
	{
		Hostname: "HostName",
	},
}

var shortValuesBackups = []BackupDetail{
	{
		BackupName: "b1",
		Hostname:   "h1",
	},
	{
		BackupName: "b2",
		Hostname:   "h2",
	},
}

var longValuesBackups = []BackupDetail{
	{
		BackupName: "veryVeryVeryLongBackupName1",
		Hostname:   "veryVeryVeryLongHostName1",
	},
	{
		BackupName: "veryVeryVeryLongBackupName2",
		Hostname:   "veryVeryVeryLongHostName2",
	},
}

func TestWriteBackupListDetails_NoBackups(t *testing.T) {
	expectedOutput := "name last_modified start_time finish_time hostname binlog_start binlog_end uncompressed_size compressed_size is_permanent\n"
	buffer := bytes.Buffer{}
	writeBackupListDetails(make([]BackupDetail, 0), &buffer)
	assert.Equal(t, expectedOutput, buffer.String())
}

func TestWriteBackupListDetails_EmptyColumnsValues(t *testing.T) {
	expectedOutput := "name       last_modified        start_time                     finish_time                    hostname binlog_start binlog_end uncompressed_size compressed_size is_permanent\n" +
		"           0001-01-01T00:00:00Z Monday, 01-Jan-01 00:00:00 UTC Monday, 01-Jan-01 00:00:00 UTC HostName                         0                 0               false\n" +
		"backupName 0001-01-01T00:00:00Z Monday, 01-Jan-01 00:00:00 UTC Monday, 01-Jan-01 00:00:00 UTC                                  0                 0               false\n"
	buffer := bytes.Buffer{}
	writeBackupListDetails(emptyColumnsBackups, &buffer)
	assert.Equal(t, expectedOutput, buffer.String())
}

func TestWriteBackupListDetails_ShortColumnsValues(t *testing.T) {
	expectedOutput := "name last_modified        start_time                     finish_time                    hostname binlog_start binlog_end uncompressed_size compressed_size is_permanent\n" +
		"b2   0001-01-01T00:00:00Z Monday, 01-Jan-01 00:00:00 UTC Monday, 01-Jan-01 00:00:00 UTC h2                               0                 0               false\n" +
		"b1   0001-01-01T00:00:00Z Monday, 01-Jan-01 00:00:00 UTC Monday, 01-Jan-01 00:00:00 UTC h1                               0                 0               false\n"
	buffer := bytes.Buffer{}
	writeBackupListDetails(shortValuesBackups, &buffer)
	assert.Equal(t, expectedOutput, buffer.String())
}

func TestWriteBackupListDetails_LongColumnsValues(t *testing.T) {
	expectedOutput := "name                        last_modified        start_time                     finish_time                    hostname                  binlog_start binlog_end uncompressed_size compressed_size is_permanent\n" +
		"veryVeryVeryLongBackupName2 0001-01-01T00:00:00Z Monday, 01-Jan-01 00:00:00 UTC Monday, 01-Jan-01 00:00:00 UTC veryVeryVeryLongHostName2                         0                 0               false\n" +
		"veryVeryVeryLongBackupName1 0001-01-01T00:00:00Z Monday, 01-Jan-01 00:00:00 UTC Monday, 01-Jan-01 00:00:00 UTC veryVeryVeryLongHostName1                         0                 0               false\n"
	buffer := bytes.Buffer{}
	writeBackupListDetails(longValuesBackups, &buffer)
	assert.Equal(t, expectedOutput, buffer.String())
}

func TestWritePrettyBackupListDetails_NoBackups(t *testing.T) {
	expectedOutput := "+---+------+---------------+------------+-------------+----------+--------------+------------+-------------------+-----------------+-----------+\n" +
		"| # | NAME | LAST MODIFIED | START TIME | FINISH TIME | HOSTNAME | BINLOG START | BINLOG END | UNCOMPRESSED SIZE | COMPRESSED SIZE | PERMANENT |\n" +
		"+---+------+---------------+------------+-------------+----------+--------------+------------+-------------------+-----------------+-----------+\n" +
		"+---+------+---------------+------------+-------------+----------+--------------+------------+-------------------+-----------------+-----------+\n"
	buffer := bytes.Buffer{}
	writePrettyBackupListDetails(make([]BackupDetail, 0), &buffer)
	assert.Equal(t, expectedOutput, buffer.String())
}

func TestWritePrettyBackupListDetails_EmptyColumnsValues(t *testing.T) {
	expectedOutput := "+---+------------+--------------------------------+--------------------------------+--------------------------------+----------+--------------+------------+-------------------+-----------------+-----------+\n" +
		"| # | NAME       | LAST MODIFIED                  | START TIME                     | FINISH TIME                    | HOSTNAME | BINLOG START | BINLOG END | UNCOMPRESSED SIZE | COMPRESSED SIZE | PERMANENT |\n" +
		"+---+------------+--------------------------------+--------------------------------+--------------------------------+----------+--------------+------------+-------------------+-----------------+-----------+\n" +
		"| 0 | backupName | Monday, 01-Jan-01 00:00:00 UTC | Monday, 01-Jan-01 00:00:00 UTC | Monday, 01-Jan-01 00:00:00 UTC |          |              |            |                 0 |               0 | false     |\n" +
		"| 1 |            | Monday, 01-Jan-01 00:00:00 UTC | Monday, 01-Jan-01 00:00:00 UTC | Monday, 01-Jan-01 00:00:00 UTC | HostName |              |            |                 0 |               0 | false     |\n" +
		"+---+------------+--------------------------------+--------------------------------+--------------------------------+----------+--------------+------------+-------------------+-----------------+-----------+\n"
	buffer := bytes.Buffer{}
	writePrettyBackupListDetails(emptyColumnsBackups, &buffer)
	assert.Equal(t, expectedOutput, buffer.String())
}

func TestWritePrettyBackupListDetails_ShortColumnsValues(t *testing.T) {
	expectedOutput := "+---+------+--------------------------------+--------------------------------+--------------------------------+----------+--------------+------------+-------------------+-----------------+-----------+\n" +
		"| # | NAME | LAST MODIFIED                  | START TIME                     | FINISH TIME                    | HOSTNAME | BINLOG START | BINLOG END | UNCOMPRESSED SIZE | COMPRESSED SIZE | PERMANENT |\n" +
		"+---+------+--------------------------------+--------------------------------+--------------------------------+----------+--------------+------------+-------------------+-----------------+-----------+\n" +
		"| 0 | b1   | Monday, 01-Jan-01 00:00:00 UTC | Monday, 01-Jan-01 00:00:00 UTC | Monday, 01-Jan-01 00:00:00 UTC | h1       |              |            |                 0 |               0 | false     |\n" +
		"| 1 | b2   | Monday, 01-Jan-01 00:00:00 UTC | Monday, 01-Jan-01 00:00:00 UTC | Monday, 01-Jan-01 00:00:00 UTC | h2       |              |            |                 0 |               0 | false     |\n" +
		"+---+------+--------------------------------+--------------------------------+--------------------------------+----------+--------------+------------+-------------------+-----------------+-----------+\n"
	buffer := bytes.Buffer{}
	writePrettyBackupListDetails(shortValuesBackups, &buffer)
	assert.Equal(t, expectedOutput, buffer.String())
}

func TestWritePrettyBackupListDetails_LongColumnsValues(t *testing.T) {
	expectedOutput := "+---+-----------------------------+--------------------------------+--------------------------------+--------------------------------+---------------------------+--------------+------------+-------------------+-----------------+-----------+\n" +
		"| # | NAME                        | LAST MODIFIED                  | START TIME                     | FINISH TIME                    | HOSTNAME                  | BINLOG START | BINLOG END | UNCOMPRESSED SIZE | COMPRESSED SIZE | PERMANENT |\n" +
		"+---+-----------------------------+--------------------------------+--------------------------------+--------------------------------+---------------------------+--------------+------------+-------------------+-----------------+-----------+\n" +
		"| 0 | veryVeryVeryLongBackupName1 | Monday, 01-Jan-01 00:00:00 UTC | Monday, 01-Jan-01 00:00:00 UTC | Monday, 01-Jan-01 00:00:00 UTC | veryVeryVeryLongHostName1 |              |            |                 0 |               0 | false     |\n" +
		"| 1 | veryVeryVeryLongBackupName2 | Monday, 01-Jan-01 00:00:00 UTC | Monday, 01-Jan-01 00:00:00 UTC | Monday, 01-Jan-01 00:00:00 UTC | veryVeryVeryLongHostName2 |              |            |                 0 |               0 | false     |\n" +
		"+---+-----------------------------+--------------------------------+--------------------------------+--------------------------------+---------------------------+--------------+------------+-------------------+-----------------+-----------+\n"
	buffer := bytes.Buffer{}
	writePrettyBackupListDetails(longValuesBackups, &buffer)
	assert.Equal(t, expectedOutput, buffer.String())
}

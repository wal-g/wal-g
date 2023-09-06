package mysql

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var timeNow = time.Now()

var emptyColumnsBackups = []BackupDetail{
	{
		BackupName: "backupName",
	},
	{
		Hostname: "HostName",
	},
	{
		ModifyTime: timeNow,
	},
}

var shortValuesBackups = []BackupDetail{
	{
		BackupName: "b1",
		Hostname:   "h1",
		ModifyTime: timeNow,
	},
	{
		BackupName: "b2",
		Hostname:   "h2",
		ModifyTime: timeNow,
	},
}

var longValuesBackups = []BackupDetail{
	{
		BackupName: "veryVeryVeryLongBackupName1",
		Hostname:   "veryVeryVeryLongHostName1",
		ModifyTime: timeNow,
	},
	{
		BackupName: "veryVeryVeryLongBackupName2",
		Hostname:   "veryVeryVeryLongHostName2",
		ModifyTime: timeNow,
	},
}

func TestWriteBackupListDetails_NoBackups(t *testing.T) {
	expectedOutput := "name last_modified start_time finish_time hostname binlog_start binlog_end uncompressed_size compressed_size is_permanent\n"
	buffer := bytes.Buffer{}
	writeBackupListDetails(make([]BackupDetail, 0), &buffer)
	assert.Equal(t, expectedOutput, buffer.String())
}

func TestWriteBackupListDetails_EmptyColumnsValues(t *testing.T) {
	expectedOutput := "name       last_modified             start_time                     finish_time                    hostname binlog_start binlog_end uncompressed_size compressed_size is_permanent\n" +
		fmt.Sprintf("           %s Monday, 01-Jan-01 00:00:00 UTC Monday, 01-Jan-01 00:00:00 UTC                                  0                 0               false\n", timeNow.Format(time.RFC3339)) +
		"           0001-01-01T00:00:00Z      Monday, 01-Jan-01 00:00:00 UTC Monday, 01-Jan-01 00:00:00 UTC HostName                         0                 0               false\n" +
		"backupName 0001-01-01T00:00:00Z      Monday, 01-Jan-01 00:00:00 UTC Monday, 01-Jan-01 00:00:00 UTC                                  0                 0               false\n"
	buffer := bytes.Buffer{}
	writeBackupListDetails(emptyColumnsBackups, &buffer)
	assert.Equal(t, expectedOutput, buffer.String())
}

func TestWriteBackupListDetails_ShortColumnsValues(t *testing.T) {
	expectedOutput := "name last_modified             start_time                     finish_time                    hostname binlog_start binlog_end uncompressed_size compressed_size is_permanent\n" +
		fmt.Sprintf("b2   %s Monday, 01-Jan-01 00:00:00 UTC Monday, 01-Jan-01 00:00:00 UTC h2                               0                 0               false\n", timeNow.Format(time.RFC3339)) +
		fmt.Sprintf("b1   %s Monday, 01-Jan-01 00:00:00 UTC Monday, 01-Jan-01 00:00:00 UTC h1                               0                 0               false\n", timeNow.Format(time.RFC3339))
	buffer := bytes.Buffer{}
	writeBackupListDetails(shortValuesBackups, &buffer)
	assert.Equal(t, expectedOutput, buffer.String())
}

func TestWriteBackupListDetails_LongColumnsValues(t *testing.T) {
	expectedOutput := "name                        last_modified             start_time                     finish_time                    hostname                  binlog_start binlog_end uncompressed_size compressed_size is_permanent\n" +
		fmt.Sprintf("veryVeryVeryLongBackupName2 %s Monday, 01-Jan-01 00:00:00 UTC Monday, 01-Jan-01 00:00:00 UTC veryVeryVeryLongHostName2                         0                 0               false\n", timeNow.Format(time.RFC3339)) +
		fmt.Sprintf("veryVeryVeryLongBackupName1 %s Monday, 01-Jan-01 00:00:00 UTC Monday, 01-Jan-01 00:00:00 UTC veryVeryVeryLongHostName1                         0                 0               false\n", timeNow.Format(time.RFC3339))
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
	expectedOutput := "+---+------------+----------------------------------+--------------------------------+--------------------------------+----------+--------------+------------+-------------------+-----------------+-----------+\n" +
		"| # | NAME       | LAST MODIFIED                    | START TIME                     | FINISH TIME                    | HOSTNAME | BINLOG START | BINLOG END | UNCOMPRESSED SIZE | COMPRESSED SIZE | PERMANENT |\n" +
		"+---+------------+----------------------------------+--------------------------------+--------------------------------+----------+--------------+------------+-------------------+-----------------+-----------+\n" +
		"| 0 | backupName | Monday, 01-Jan-01 00:00:00 UTC   | Monday, 01-Jan-01 00:00:00 UTC | Monday, 01-Jan-01 00:00:00 UTC |          |              |            |                 0 |               0 | false     |\n" +
		"| 1 |            | Monday, 01-Jan-01 00:00:00 UTC   | Monday, 01-Jan-01 00:00:00 UTC | Monday, 01-Jan-01 00:00:00 UTC | HostName |              |            |                 0 |               0 | false     |\n" +
		fmt.Sprintf("| 2 |            | %s | Monday, 01-Jan-01 00:00:00 UTC | Monday, 01-Jan-01 00:00:00 UTC |          |              |            |                 0 |               0 | false     |\n", timeNow.Format(time.RFC850)) +
		"+---+------------+----------------------------------+--------------------------------+--------------------------------+----------+--------------+------------+-------------------+-----------------+-----------+\n"
	buffer := bytes.Buffer{}
	writePrettyBackupListDetails(emptyColumnsBackups, &buffer)
	assert.Equal(t, expectedOutput, buffer.String())
}

func TestWritePrettyBackupListDetails_ShortColumnsValues(t *testing.T) {
	expectedOutput := "+---+------+----------------------------------+--------------------------------+--------------------------------+----------+--------------+------------+-------------------+-----------------+-----------+\n" +
		"| # | NAME | LAST MODIFIED                    | START TIME                     | FINISH TIME                    | HOSTNAME | BINLOG START | BINLOG END | UNCOMPRESSED SIZE | COMPRESSED SIZE | PERMANENT |\n" +
		"+---+------+----------------------------------+--------------------------------+--------------------------------+----------+--------------+------------+-------------------+-----------------+-----------+\n" +
		fmt.Sprintf("| 0 | b1   | %s | Monday, 01-Jan-01 00:00:00 UTC | Monday, 01-Jan-01 00:00:00 UTC | h1       |              |            |                 0 |               0 | false     |\n", timeNow.Format(time.RFC850)) +
		fmt.Sprintf("| 1 | b2   | %s | Monday, 01-Jan-01 00:00:00 UTC | Monday, 01-Jan-01 00:00:00 UTC | h2       |              |            |                 0 |               0 | false     |\n", timeNow.Format(time.RFC850)) +
		"+---+------+----------------------------------+--------------------------------+--------------------------------+----------+--------------+------------+-------------------+-----------------+-----------+\n"
	buffer := bytes.Buffer{}
	writePrettyBackupListDetails(shortValuesBackups, &buffer)
	assert.Equal(t, expectedOutput, buffer.String())
}

func TestWritePrettyBackupListDetails_LongColumnsValues(t *testing.T) {
	expectedOutput := "+---+-----------------------------+----------------------------------+--------------------------------+--------------------------------+---------------------------+--------------+------------+-------------------+-----------------+-----------+\n" +
		"| # | NAME                        | LAST MODIFIED                    | START TIME                     | FINISH TIME                    | HOSTNAME                  | BINLOG START | BINLOG END | UNCOMPRESSED SIZE | COMPRESSED SIZE | PERMANENT |\n" +
		"+---+-----------------------------+----------------------------------+--------------------------------+--------------------------------+---------------------------+--------------+------------+-------------------+-----------------+-----------+\n" +
		fmt.Sprintf("| 0 | veryVeryVeryLongBackupName1 | %s | Monday, 01-Jan-01 00:00:00 UTC | Monday, 01-Jan-01 00:00:00 UTC | veryVeryVeryLongHostName1 |              |            |                 0 |               0 | false     |\n", timeNow.Format(time.RFC850)) +
		fmt.Sprintf("| 1 | veryVeryVeryLongBackupName2 | %s | Monday, 01-Jan-01 00:00:00 UTC | Monday, 01-Jan-01 00:00:00 UTC | veryVeryVeryLongHostName2 |              |            |                 0 |               0 | false     |\n", timeNow.Format(time.RFC850)) +
		"+---+-----------------------------+----------------------------------+--------------------------------+--------------------------------+---------------------------+--------------+------------+-------------------+-----------------+-----------+\n"
	buffer := bytes.Buffer{}
	writePrettyBackupListDetails(longValuesBackups, &buffer)
	assert.Equal(t, expectedOutput, buffer.String())
}

package mysql

import (
	"context"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsXtrabackup(t *testing.T) {
	var tests = []struct {
		exp  bool
		name string
		args []string
	}{
		{true, "/bin/sh", []string{"-c", "xtrabackup –backup"}},
		{true, "xtrabackup", []string{"–backup"}},
		{true, "mariabackup", []string{"--backup", "--stream=xbstream"}},
		{true, "/bin/sh", []string{"-c", "mariabackup --backup --stream=xbstream"}},
		{false, "mysqldump", []string{"--backup"}},
	}

	for _, tt := range tests {
		testName := tt.name + " " + strings.Join(tt.args, " ")
		t.Run(testName, func(t *testing.T) {
			cmd := exec.CommandContext(context.Background(), tt.name, tt.args...)
			assert.Equal(t, tt.exp, isXtrabackup(cmd))
		})
	}
}

/*
Test data from: (default Ubuntu 18.04 xtrabackup)
xtrabackup version 2.4.9 based on MySQL server 5.7.13 Linux (x86_64) (revision id: a467167cdd4)
*/
const xtrabackup_checkpoints_example = `
	backup_type = full-backuped
	from_lsn = 0
	to_lsn = 3738001
	last_lsn = 3738068
	compact = 0
	recover_binlog_info = 0`

func TestReadXtrabackupInfo(t *testing.T) {
	info := NewXtrabackupInfo(xtrabackup_checkpoints_example)
	assert.Equal(t, uint64(0), uint64(*info.FromLSN))
	assert.Equal(t, uint64(3738001), uint64(*info.ToLSN))
	assert.Equal(t, uint64(3738068), uint64(*info.LastLSN))
}

func TestParseBinlogPos(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		expectedFile string
		expectedPos  int64
	}{
		{
			name:         "mariabackup format with GTID",
			input:        "filename 'mysql-bin.000002', position '607', GTID of the last change '0-1-7'",
			expectedFile: "mysql-bin.000002",
			expectedPos:  607,
		},
		{
			name:         "xtrabackup format without GTID",
			input:        "filename 'mysql-bin.000003', position '154'",
			expectedFile: "mysql-bin.000003",
			expectedPos:  154,
		},
		{
			name:         "mariadb 11.8 binlog naming",
			input:        "filename 'mariadb-bin.000001', position '1298', GTID of the last change '0-1-6'",
			expectedFile: "mariadb-bin.000001",
			expectedPos:  1298,
		},
		{
			name:         "empty string",
			input:        "",
			expectedFile: "",
			expectedPos:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file, pos := parseBinlogPos(tt.input)
			assert.Equal(t, tt.expectedFile, file)
			assert.Equal(t, tt.expectedPos, pos)
		})
	}
}

const xtrabackup_info_example = `uuid = abc-123
name = 
tool_name = mariabackup
tool_command = --backup --user=sbtest --host=localhost
tool_version = 10.11.14-MariaDB
ibbackup_version = 10.11.14-MariaDB
server_version = 10.11.14-MariaDB-0ubuntu0.24.04.1-log
start_time = 2026-03-06 18:50:10
end_time = 2026-03-06 18:50:11
lock_time = 1772823010
binlog_pos = filename 'mysql-bin.000002', position '607', GTID of the last change '0-1-7'
innodb_from_lsn = 0
innodb_to_lsn = 50656
partial = N
incremental = N
format = file
compressed = N`

func TestParseBinlogPosFromXtrabackupInfo(t *testing.T) {
	info := NewXtrabackupInfo(xtrabackup_info_example)
	assert.Equal(t, "mysql-bin.000002", info.BinLogFileName)
	assert.Equal(t, int64(607), info.BinLogFilePosition)
}

const mariadb_backup_info_example = `uuid = def-456
name = 
tool_name = mariabackup
tool_command = --backup --user=root --host=localhost
tool_version = 11.8.1-MariaDB
ibbackup_version = 11.8.1-MariaDB
server_version = 11.8.1-MariaDB
start_time = 2026-03-06 19:00:00
end_time = 2026-03-06 19:00:01
lock_time = 1772823600
binlog_pos = filename 'mariadb-bin.000001', position '1298', GTID of the last change '0-1-6'
innodb_from_lsn = 0
innodb_to_lsn = 49153
partial = N
incremental = N
format = file
compressed = N`

func TestParseBinlogPosFromMariaDBBackupInfo(t *testing.T) {
	info := NewXtrabackupInfo(mariadb_backup_info_example)
	assert.Equal(t, "mariadb-bin.000001", info.BinLogFileName)
	assert.Equal(t, int64(1298), info.BinLogFilePosition)
}

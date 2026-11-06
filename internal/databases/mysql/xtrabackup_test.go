package mysql

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			cmd := exec.CommandContext(t.Context(), tt.name, tt.args...)
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
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "xtrabackup_checkpoints"),
		[]byte(xtrabackup_checkpoints_example), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "xtrabackup_info"),
		[]byte(xtrabackup_info_multiline_gtid_example), 0600))

	info, err := readXtrabackupInfo(dir)
	require.NoError(t, err)
	assert.Equal(t, new(LSN(0)), info.FromLSN)
	assert.Equal(t, new(LSN(3738001)), info.ToLSN)
	assert.Equal(t, new(LSN(3738068)), info.LastLSN)
	assert.Equal(t, "mysql-bin.000123", info.BinlogPos.FileName)
	assert.Equal(t, int64(456), info.BinlogPos.FilePosition)
	assert.Equal(t, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-110,"+
		"bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-200", info.BinlogPos.LastGTID)
}

func TestParseBinlogPos(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		expectedFile string
		expectedPos  int64
		expectedGTID string
	}{
		{
			name:         "mariabackup format with GTID",
			input:        "filename 'mysql-bin.000002', position '607', GTID of the last change '0-1-7'",
			expectedFile: "mysql-bin.000002",
			expectedPos:  607,
			expectedGTID: "0-1-7",
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
			expectedGTID: "0-1-6",
		},
		{
			// Real example: xtrabackup (not just mariabackup) records a
			// GTID when GTID_MODE is on; filenames can be hostname-based.
			name:         "xtrabackup with GTID and hostname-based filename",
			input:        "filename 'mysql-bin-log-example-com.000406', position '197', GTID of the last change '3f6cd603-8d0b-11f1-8432-d00dc21b3e49:1-2647395'",
			expectedFile: "mysql-bin-log-example-com.000406",
			expectedPos:  197,
			expectedGTID: "3f6cd603-8d0b-11f1-8432-d00dc21b3e49:1-2647395",
		},
		{
			name: "xtrabackup with multiline GTID",
			input: `filename 'mysql-bin.000123', position '456', GTID of the last change 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-110,
bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-200'`,
			expectedFile: "mysql-bin.000123",
			expectedPos:  456,
			expectedGTID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-110," +
				"bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-200",
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
			pos := parseBinlogPos(tt.input)
			assert.Equal(t, tt.expectedFile, pos.FileName)
			assert.Equal(t, tt.expectedPos, pos.FilePosition)
			assert.Equal(t, tt.expectedGTID, pos.LastGTID)
		})
	}
}

// Real xtrabackup_info from a review comment: plain xtrabackup on MySQL
// 8.0, not mariabackup -- tool_name, tool_command and server_version all
// say so, and it records a GTID too since GTID_MODE is on.
const xtrabackup_info_example = `uuid = d6ab1d07-a5f3-11f1-8407-d00dc21b3e49
name =
tool_name = xtrabackup
tool_command = --backup --stream=xbstream --lock-ddl --lock-ddl-timeout=3600 --parallel=1 --register-redo-log-consumer --datadir=/var/lib/mysql --extra-lsndir=/tmp/wal-g1802792818
tool_version = 8.0.35-35
ibbackup_version = 8.0.35-35
server_version = 8.0.43-34
start_time = 2026-09-01 10:56:27
end_time = 2026-09-01 10:56:52
lock_time = 2
binlog_pos = filename 'mysql-bin-log-example-com.000406', position '197', GTID of the last change '3f6cd603-8d0b-11f1-8432-d00dc21b3e49:1-2647395'
innodb_from_lsn = 0
innodb_to_lsn = 3186294961
partial = N
incremental = N
format = xbstream
compressed = N
encrypted = N`

func TestParseBinlogPosFromXtrabackupInfo(t *testing.T) {
	info := NewXtrabackupInfo(xtrabackup_checkpoints_example, xtrabackup_info_example)
	assert.Equal(t, new(LSN(0)), info.FromLSN)
	assert.Equal(t, new(LSN(3738001)), info.ToLSN)
	assert.Equal(t, new(LSN(3738068)), info.LastLSN)
	assert.Equal(t, "mysql-bin-log-example-com.000406", info.BinlogPos.FileName)
	assert.Equal(t, int64(197), info.BinlogPos.FilePosition)
	assert.Equal(t, "3f6cd603-8d0b-11f1-8432-d00dc21b3e49:1-2647395", info.BinlogPos.LastGTID)
}

// Synthetic XtraBackup 2.4 / MySQL 5.7 metadata with multiline GTID
const xtrabackup_info_multiline_gtid_example = `uuid = d6ab1d07-a5f3-11f1-8407-d00dc21b3e49
name =
tool_name = xtrabackup
tool_command = --backup --stream=xbstream --parallel=1 --datadir=/var/lib/mysql --extra-lsndir=/tmp/wal-g1802792818
tool_version = 2.4.29
ibbackup_version = 2.4.29
server_version = 5.7.44
start_time = 2026-09-01 10:56:27
end_time = 2026-09-01 10:56:52
lock_time = 2
binlog_pos = filename 'mysql-bin.000123', position '456', GTID of the last change 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-110,
bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-200'
innodb_from_lsn = 0
innodb_to_lsn = 3186294961
partial = N
incremental = N
format = xbstream
compact = N
compressed = N
encrypted = N`

func TestParseBinlogPosFromXtrabackupInfoMultilineGTIDCRLF(t *testing.T) {
	info := NewXtrabackupInfo(xtrabackup_checkpoints_example,
		strings.ReplaceAll(xtrabackup_info_multiline_gtid_example, "\n", "\r\n"))
	assert.Equal(t, new(LSN(0)), info.FromLSN)
	assert.Equal(t, new(LSN(3738001)), info.ToLSN)
	assert.Equal(t, new(LSN(3738068)), info.LastLSN)
	assert.Equal(t, "mysql-bin.000123", info.BinlogPos.FileName)
	assert.Equal(t, int64(456), info.BinlogPos.FilePosition)
	assert.Equal(t, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-110,"+
		"bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-200", info.BinlogPos.LastGTID)
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
	info := NewXtrabackupInfo(xtrabackup_checkpoints_example, mariadb_backup_info_example)
	assert.Equal(t, new(LSN(0)), info.FromLSN)
	assert.Equal(t, new(LSN(3738001)), info.ToLSN)
	assert.Equal(t, new(LSN(3738068)), info.LastLSN)
	assert.Equal(t, "mariadb-bin.000001", info.BinlogPos.FileName)
	assert.Equal(t, int64(1298), info.BinlogPos.FilePosition)
	assert.Equal(t, "0-1-6", info.BinlogPos.LastGTID)
}

func TestIsMariaDBBackup(t *testing.T) {
	var tests = []struct {
		exp           bool
		serverVersion string
	}{
		{true, "11.4.10-MariaDB-deb12-log"},
		{true, "10.6.18-MariaDB"},
		{false, "8.0.35-27"},
		{false, "8.0.36"},
		{false, ""},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.exp, isMariaDBBackup(tt.serverVersion), tt.serverVersion)
	}
}

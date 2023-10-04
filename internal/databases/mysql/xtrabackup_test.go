package mysql

import (
	"context"
	"github.com/stretchr/testify/assert"
	"os/exec"
	"strings"
	"testing"
)

func TestIsXtrabackup(t *testing.T) {
	var tests = []struct {
		exp  bool
		name string
		args []string
	}{
		{true, "/bin/sh", []string{"-c", "xtrabackup –backup"}},
		{true, "xtrabackup", []string{"–backup"}},
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

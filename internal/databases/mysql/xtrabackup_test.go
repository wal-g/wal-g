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

//go:build integration

package pgdebug

import (
	"fmt"
	"net"
	"os/exec"
	"strings"
	"testing"
)

func TestShowVersionIntegration(t *testing.T) {
	psql, err := exec.LookPath("psql")
	if err != nil {
		t.Skip("psql not found in PATH")
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go Serve(l, "wal-g debug 0.1")

	out, err := exec.Command(psql,
		fmt.Sprintf("host=%s port=%d sslmode=disable gssencmode=disable dbname=test", "127.0.0.1", l.Addr().(*net.TCPAddr).Port),
		"-tA", "-c", "show version",
	).Output()
	if err != nil {
		t.Fatalf("psql failed: %v: %s", err, out)
	}
	if got := strings.TrimSpace(string(out)); got != "wal-g debug 0.1" {
		t.Fatalf("got %q, want %q", got, "wal-g debug 0.1")
	}
}

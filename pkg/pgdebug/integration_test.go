//go:build integration

package pgdebug

import (
	"fmt"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"
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
	go func() { _ = Serve(l, Options{Version: "wal-g debug 0.1"}) }()
	waitForPort(t, l)

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

func TestShowBackupStatsIntegration(t *testing.T) {
	psql, err := exec.LookPath("psql")
	if err != nil {
		t.Skip("psql not found in PATH")
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go func() {
		_ = Serve(l, Options{
			Version: "wal-g debug 0.1",
			BackupStatsProvider: func() (BackupStats, error) {
				return BackupStats{
					Name:          "base_D_000000010000000000000002_00000001",
					StartLSN:      "0/2000000",
					FilesUploaded: 12,
					BytesUploaded: 34000,
				}, nil
			},
		})
	}()
	waitForPort(t, l)

	out, err := exec.Command(psql,
		fmt.Sprintf("host=%s port=%d sslmode=disable gssencmode=disable dbname=test", "127.0.0.1", l.Addr().(*net.TCPAddr).Port),
		"-tA", "-c", "show backup_stats",
	).Output()
	if err != nil {
		t.Fatalf("psql failed: %v: %s", err, out)
	}
	got := strings.TrimSpace(string(out))
	want := "base_D_000000010000000000000002_00000001|0/2000000||12|34000|0"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// waitForPort waits until the server accepts connections
func waitForPort(t *testing.T, l net.Listener) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", l.Addr().String(), time.Second)
		if err == nil {
			_ = conn.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("server did not start: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

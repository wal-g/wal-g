package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// Opt in with WALG_TEST_PG_BIN=/path/to/postgres/bin. These tests exercise stock
// clients and real recovery, independently of pglogrepl's protocol assumptions.
func walSendPostgresBin(t *testing.T) string {
	t.Helper()
	bin := os.Getenv("WALG_TEST_PG_BIN")
	if bin == "" {
		t.Skip("set WALG_TEST_PG_BIN to run stock PostgreSQL compatibility tests")
	}
	return bin
}

func walSendPostgresCommand(t *testing.T, ctx context.Context, bin, program string, args ...string) []byte {
	t.Helper()
	output, err := exec.CommandContext(ctx, filepath.Join(bin, program), args...).CombinedOutput()
	require.NoError(t, err, "%s: %s", program, output)
	return output
}

func walSendPostgresMajor(t *testing.T, bin string) int {
	t.Helper()
	output := walSendPostgresCommand(t, t.Context(), bin, "postgres", "--version")
	var major int
	_, err := fmt.Sscanf(string(output), "postgres (PostgreSQL) %d", &major)
	require.NoError(t, err)
	return major
}

func TestWalSendStockReceivewalTimelineSwitch(t *testing.T) {
	bin := walSendPostgresBin(t)
	folder := memory.NewFolder("archive/", memory.NewKVS())
	// Stock clients reject a server that advertises a newer major version.
	version, err := json.Marshal(postgres.BackupSentinelDto{PgVersion: walSendPostgresMajor(t, bin) * 10000})
	require.NoError(t, err)
	putPlainObject(t, folder.GetSubFolder(utility.BaseBackupPath),
		"base_000000010000000000000001"+utility.SentinelSuffix, version)
	walFolder := folder.GetSubFolder(utility.WalPath)
	oldSegment, newSegment := makeWalSendSegmentData(1), makeWalSendSegmentData(2)
	putPlainObject(t, walFolder, "000000010000000000000001", oldSegment)
	putPlainObject(t, walFolder, "000000010000000000000002.partial", newSegment[:walSendTestSegmentSize/2])
	putPlainObject(t, walFolder, "000000020000000000000002", newSegment)
	putPlainObject(t, walFolder, "00000002.history", []byte("1\t0/2800000\tpromotion\n"))
	address := startWalSendTestServer(t, folder, 50*time.Millisecond)
	_, port, err := net.SplitHostPort(address)
	require.NoError(t, err)
	dir := t.TempDir()
	// An existing partial makes pg_receivewal resume on timeline 1 even though
	// IDENTIFY_SYSTEM correctly reports timeline 2 as the archive head.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "000000010000000000000001.partial"), nil, 0600))
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	walSendPostgresCommand(t, ctx, bin, "pg_receivewal", "-h", "127.0.0.1", "-p", port,
		"-D", dir, "--no-loop", "--no-sync", "--verbose", "--endpos=0/2FFFFFF")
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"000000010000000000000001", oldSegment},
		{"000000020000000000000002", newSegment},
	} {
		data, err := os.ReadFile(filepath.Join(dir, tc.name))
		require.NoError(t, err)
		require.True(t, bytes.Equal(tc.data, data), "incorrect contents in %s", tc.name)
	}
	_, err = os.Stat(filepath.Join(dir, "00000002.history"))
	require.NoError(t, err)
}

func walSendPostgresAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := l.Addr().String()
	require.NoError(t, l.Close())
	return address
}

func walSendPostgresStart(t *testing.T, ctx context.Context, bin, dir, address string) {
	t.Helper()
	_, port, err := net.SplitHostPort(address)
	require.NoError(t, err)
	log := dir + ".log"
	output, err := exec.CommandContext(ctx, filepath.Join(bin, "pg_ctl"), "-D", dir, "-l", log,
		"-o", fmt.Sprintf("-h 127.0.0.1 -p %s -c unix_socket_directories=''", port), "-w", "-t", "20", "start").CombinedOutput()
	if err != nil {
		data, _ := os.ReadFile(log)
		t.Fatalf("pg_ctl start: %v: %s\n%s", err, output, data)
	}
	t.Cleanup(func() {
		output, err := exec.Command(filepath.Join(bin, "pg_ctl"), "-D", dir, "-w", "-t", "10", "-m", "immediate", "stop").CombinedOutput()
		if err != nil {
			t.Errorf("pg_ctl stop: %v: %s", err, output)
		}
		if t.Failed() {
			data, _ := os.ReadFile(log)
			t.Logf("PostgreSQL log:\n%s", data)
		}
	})
}

func walSendPostgresSQL(t *testing.T, ctx context.Context, address, query string) string {
	t.Helper()
	host, port, err := net.SplitHostPort(address)
	require.NoError(t, err)
	conn, err := pgconn.Connect(ctx, fmt.Sprintf("host=%s port=%s user=walg_test dbname=postgres sslmode=disable", host, port))
	require.NoError(t, err)
	defer func() { _ = conn.Close(context.Background()) }()
	results, err := conn.Exec(ctx, query).ReadAll()
	require.NoError(t, err)
	last := results[len(results)-1]
	if len(last.Rows) > 0 {
		return string(last.Rows[0][0])
	}
	return ""
}

func walSendArchivePostgresWAL(t *testing.T, ctx context.Context, dir, address string, folder storage.Folder) {
	t.Helper()
	current := walSendPostgresSQL(t, ctx, address, "SELECT pg_walfile_name(pg_current_wal_lsn())")
	entries, err := os.ReadDir(filepath.Join(dir, "pg_wal"))
	require.NoError(t, err)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || name == current || (len(name) != 24 && !strings.HasSuffix(name, ".history") && !strings.HasSuffix(name, ".partial")) {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, "pg_wal", name))
		require.NoError(t, err)
		putPlainObject(t, folder.GetSubFolder(utility.WalPath), name, data)
	}
}

func TestWalSendRealPostgresStandby(t *testing.T) {
	bin := walSendPostgresBin(t)
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	dir := t.TempDir()
	primary, standby := filepath.Join(dir, "primary"), filepath.Join(dir, "standby")
	walSendPostgresCommand(t, ctx, bin, "initdb", "-D", primary, "-A", "trust", "-U", "walg_test", "--no-locale", "--no-sync")
	primaryAddress := walSendPostgresAddress(t)
	walSendPostgresStart(t, ctx, bin, primary, primaryAddress)
	walSendPostgresSQL(t, ctx, primaryAddress, "CREATE TABLE wal_send_probe (id integer); INSERT INTO wal_send_probe VALUES (1)")
	_, port, err := net.SplitHostPort(primaryAddress)
	require.NoError(t, err)
	walSendPostgresCommand(t, ctx, bin, "pg_basebackup", "-h", "127.0.0.1", "-p", port, "-U", "walg_test",
		"-D", standby, "--wal-method=none", "--checkpoint=fast", "--no-sync")
	base := filepath.Join(dir, "base")
	require.NoError(t, os.Mkdir(base, 0700))
	require.NoError(t, os.CopyFS(base, os.DirFS(standby)))
	sysIDText := walSendPostgresSQL(t, ctx, primaryAddress, "SELECT system_identifier FROM pg_control_system()")
	var sysID uint64
	_, err = fmt.Sscan(sysIDText, &sysID)
	require.NoError(t, err)
	finish, err := postgres.ParseLSN(walSendPostgresSQL(t, ctx, primaryAddress, "SELECT pg_current_wal_flush_lsn()"))
	require.NoError(t, err)
	// Commit before switching: a multi-statement simple query would commit
	// after pg_switch_wal, leaving the commit record in the unarchived segment.
	walSendPostgresSQL(t, ctx, primaryAddress, "INSERT INTO wal_send_probe VALUES (42)")
	walSendPostgresSQL(t, ctx, primaryAddress, "SELECT pg_switch_wal()")
	folder := memory.NewFolder("archive/", memory.NewKVS())
	walSendArchivePostgresWAL(t, ctx, primary, primaryAddress, folder)
	sentinel, err := json.Marshal(postgres.BackupSentinelDto{
		SystemIdentifier: &sysID, PgVersion: walSendPostgresMajor(t, bin) * 10000, BackupFinishLSN: &finish,
	})
	require.NoError(t, err)
	putPlainObject(t, folder.GetSubFolder(utility.BaseBackupPath), "base_000000010000000000000001"+utility.SentinelSuffix, sentinel)
	archiveAddress := startWalSendTestServer(t, folder, 50*time.Millisecond)
	_, archivePort, err := net.SplitHostPort(archiveAddress)
	require.NoError(t, err)
	config := fmt.Sprintf("primary_conninfo = 'host=127.0.0.1 port=%s user=walg_test sslmode=disable'\n", archivePort)
	require.NoError(t, os.WriteFile(filepath.Join(standby, "postgresql.auto.conf"), []byte(config), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(standby, "standby.signal"), nil, 0600))
	standbyAddress := walSendPostgresAddress(t)
	walSendPostgresStart(t, ctx, bin, standby, standbyAddress)
	require.Eventually(t, func() bool {
		rows := walSendPostgresSQL(t, ctx, standbyAddress, "SELECT string_agg(id::text, ',' ORDER BY id) FROM wal_send_probe")
		return strings.TrimSpace(rows) == "1,42"
	}, 10*time.Second, 50*time.Millisecond, "standby must replay WAL written after the base backup")

	// Promote the recovered standby and archive timeline 2, keeping the original
	// timeline-1 sentinel. A fresh copy of that same backup must follow the fork.
	walSendPostgresCommand(t, ctx, bin, "pg_ctl", "-D", standby, "-w", "promote")
	walSendPostgresSQL(t, ctx, standbyAddress, "INSERT INTO wal_send_probe VALUES (84)")
	walSendPostgresSQL(t, ctx, standbyAddress, "SELECT pg_switch_wal()")
	walSendArchivePostgresWAL(t, ctx, standby, standbyAddress, folder)
	require.Eventually(t, func() bool {
		conn := dialWalSendTestServer(t, ctx, archiveAddress)
		defer func() { _ = conn.Close(context.Background()) }()
		results, err := conn.Exec(ctx, "IDENTIFY_SYSTEM").ReadAll()
		require.NoError(t, err)
		return string(results[0].Rows[0][1]) == "2"
	}, 5*time.Second, 50*time.Millisecond, "archive head must advance without a new backup")
	restored := filepath.Join(dir, "restored")
	require.NoError(t, os.Mkdir(restored, 0700))
	require.NoError(t, os.CopyFS(restored, os.DirFS(base)))
	require.NoError(t, os.WriteFile(filepath.Join(restored, "postgresql.auto.conf"), []byte(config), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(restored, "standby.signal"), nil, 0600))
	restoredAddress := walSendPostgresAddress(t)
	walSendPostgresStart(t, ctx, bin, restored, restoredAddress)
	require.Eventually(t, func() bool {
		rows := walSendPostgresSQL(t, ctx, restoredAddress, "SELECT string_agg(id::text, ',' ORDER BY id) FROM wal_send_probe")
		return strings.TrimSpace(rows) == "1,42,84"
	}, 10*time.Second, 50*time.Millisecond, "standby must follow the real timeline switch")
}

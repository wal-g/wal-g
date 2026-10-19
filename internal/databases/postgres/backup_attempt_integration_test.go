package postgres_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Opt-in test using disposable local PostgreSQL servers and filesystem storage.
// WALG_TEST_PG_BIN must contain PostgreSQL 15+ binaries; WALG_TEST_BINARY is a
// freshly built wal-g. It does not connect to an existing cluster.
func TestBackupAttemptsStandbyIntegration(t *testing.T) {
	pgBin, walg := os.Getenv("WALG_TEST_PG_BIN"), os.Getenv("WALG_TEST_BINARY")
	if pgBin == "" || walg == "" {
		t.Skip("set WALG_TEST_PG_BIN and WALG_TEST_BINARY for standby backup/restore integration")
	}
	root := t.TempDir()
	t.Logf("disposable cluster directory: %s", root)
	store := filepath.Join(root, "store")
	require.NoError(t, os.Mkdir(store, 0700))
	config := filepath.Join(root, "wal-g.json")
	configJSON, err := json.Marshal(map[string]any{
		"WALG_FILE_PREFIX": store, "WALG_COMPRESSION_METHOD": "lz4", "WALG_DELTA_MAX_STEPS": 2,
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(config, configJSON, 0600))

	port := func() string {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		p := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
		require.NoError(t, listener.Close())
		return p
	}
	primaryPort, firstPort, secondPort := port(), port(), port()
	// Package test initialization exports WAL-G defaults to the environment.
	// Do not let those (or the operator's PG settings) override this test config.
	var cleanEnv []string
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "WALG_") && !strings.HasPrefix(entry, "WALE_") && !strings.HasPrefix(entry, "PG") {
			cleanEnv = append(cleanEnv, entry)
		}
	}
	env := func(p string) []string {
		return append(append([]string(nil), cleanEnv...), "PGHOST="+root, "PGPORT="+p, "PGUSER=postgres", "PGDATABASE=postgres")
	}
	run := func(p, program string, args ...string) string {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, program, args...)
		cmd.Env = env(p)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		if program == walg && len(args) > 1 && args[1] == "backup-push" {
			t.Log(stderr.String())
		}
		require.NoError(t, err, "%s %v: %s\n%s", program, args, out, stderr.String())
		return strings.TrimSpace(string(out))
	}
	sql := func(p, query string) string {
		return run(p, filepath.Join(pgBin, "psql"), "-XAt", "-v", "ON_ERROR_STOP=1", "-c", query)
	}
	appendConfig := func(data, extra string) {
		f, err := os.OpenFile(filepath.Join(data, "postgresql.conf"), os.O_APPEND|os.O_WRONLY, 0600)
		require.NoError(t, err)
		_, err = f.WriteString("\n" + extra + "\n")
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}
	start := func(data, p string) {
		appendConfig(data, fmt.Sprintf("port=%s\nlisten_addresses=''\nunix_socket_directories='%s'", p, root))
		// Cleanup is installed before starting, so failed starts cannot strand a server.
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			_ = exec.CommandContext(ctx, filepath.Join(pgBin, "pg_ctl"), "-D", data, "-m", "immediate", "-w", "stop").Run()
		})
		run(p, filepath.Join(pgBin, "pg_ctl"), "-D", data, "-l", data+".log", "-w", "start")
	}
	primary := filepath.Join(root, "primary")
	first, second := filepath.Join(root, "first"), filepath.Join(root, "second")
	run(primaryPort, filepath.Join(pgBin, "initdb"), "-D", primary, "-U", "postgres", "-A", "trust", "--no-locale")
	appendConfig(primary, fmt.Sprintf("wal_level=replica\nmax_wal_senders=10\ncheckpoint_timeout='1h'\nmax_wal_size='2GB'\narchive_mode=on\narchive_command='%s --config=%s wal-push %%p'", walg, config))
	start(primary, primaryPort)
	sql(primaryPort, "CREATE TABLE attempt_data AS SELECT generate_series(1,3) AS id")
	run(primaryPort, filepath.Join(pgBin, "pg_basebackup"), "-D", first, "-X", "stream", "-R")
	// Identical initial copies ensure that both replicas have the same restartpoint.
	require.NoError(t, os.CopyFS(second, os.DirFS(first)))
	require.NoError(t, os.Chmod(second, 0700))
	for _, data := range []string{first, second} {
		appendConfig(data, "archive_mode=off")
	}
	start(first, firstPort)
	start(second, secondPort)
	for _, p := range []string{firstPort, secondPort} {
		require.Equal(t, "3", sql(p, "SELECT count(*) FROM attempt_data"))
	}
	require.Equal(t, sql(firstPort, "SELECT redo_lsn FROM pg_control_checkpoint()"),
		sql(secondPort, "SELECT redo_lsn FROM pg_control_checkpoint()"))

	// Run local backup-push on two independent replicas concurrently.
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	commands := make([]*exec.Cmd, 0, 2)
	for i, data := range []string{first, second} {
		cmd := exec.CommandContext(ctx, walg, "--config="+config, "backup-push", data, "--full")
		cmd.Env = env([]string{firstPort, secondPort}[i])
		log, err := os.Create(filepath.Join(root, fmt.Sprintf("push-%d.log", i)))
		require.NoError(t, err)
		defer log.Close()
		cmd.Stdout, cmd.Stderr = log, log
		require.NoError(t, cmd.Start())
		commands = append(commands, cmd)
	}
	for _, cmd := range commands {
		require.NoError(t, cmd.Wait(), "backup logs in %s", root)
	}

	// Remember every completed backup object before another attempt starts.
	snapshot := make(map[string][32]byte)
	baseDir := filepath.Join(store, "basebackups_005")
	require.NoError(t, filepath.WalkDir(baseDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		if err == nil {
			snapshot[path] = sha256.Sum256(data)
		}
		return err
	}))

	// Change replayed data without advancing the restartpoint, then exercise the
	// independent BASE_BACKUP/streaming path at the same starting LSN.
	sql(primaryPort, "INSERT INTO attempt_data VALUES (4)")
	require.Eventually(t, func() bool { return sql(firstPort, "SELECT count(*) FROM attempt_data") == "4" },
		10*time.Second, 100*time.Millisecond)
	run(firstPort, walg, "--config="+config, "backup-push", "--full")
	for path, expected := range snapshot {
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, expected, sha256.Sum256(data), "previous backup object changed: %s", path)
	}
	type backupInfo struct {
		Name     string `json:"backup_name"`
		StartLSN uint64 `json:"start_lsn"`
	}
	var backups []backupInfo
	list := run(primaryPort, walg, "--config="+config, "backup-list", "--detail", "--json")
	require.NoError(t, json.Unmarshal([]byte(list), &backups))
	require.Len(t, backups, 3)
	seen := make(map[string]bool)
	for _, backup := range backups {
		require.False(t, seen[backup.Name], "duplicate physical backup identity")
		seen[backup.Name] = true
		require.Equal(t, backups[0].StartLSN, backup.StartLSN, "test must exercise identical starting LSNs")
	}

	// Model an existing legacy-name full backup, then restore a real mixed
	// old-full -> new-delta -> new-delta chain. The data format is unchanged.
	legacy := strings.Split(backups[0].Name, "_U_")[0]
	require.NoError(t, os.CopyFS(filepath.Join(baseDir, legacy), os.DirFS(filepath.Join(baseDir, backups[0].Name))))
	const sentinelSuffix = "_backup_stop_sentinel.json"
	sentinel, err := os.ReadFile(filepath.Join(baseDir, backups[0].Name+sentinelSuffix))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(baseDir, legacy+sentinelSuffix), sentinel, 0600))
	parent := legacy
	for id := 5; id <= 6; id++ {
		sql(primaryPort, fmt.Sprintf("INSERT INTO attempt_data VALUES (%d)", id))
		result := run(primaryPort, walg, "--config="+config, "backup-push", primary, "--delta-from-name="+parent, "--json")
		var pushed struct {
			Name string `json:"name"`
		}
		require.NoError(t, json.Unmarshal([]byte(result), &pushed))
		require.Contains(t, pushed.Name, "_D_")
		parent = pushed.Name
		backups = append(backups, backupInfo{Name: pushed.Name})
	}

	// Ensure the finish records needed by restore have reached the archive.
	sql(primaryPort, "SELECT pg_switch_wal()")
	require.Eventually(t, func() bool {
		files, err := filepath.Glob(filepath.Join(primary, "pg_wal", "archive_status", "*.ready"))
		return err == nil && len(files) == 0
	}, 30*time.Second, 100*time.Millisecond)
	for i, backup := range backups {
		data, p := filepath.Join(root, fmt.Sprintf("restore-%d", i)), port()
		run(primaryPort, walg, "--config="+config, "backup-fetch", data, backup.Name)
		// Do not reconnect a restored standby to the original primary.
		_ = os.Remove(filepath.Join(data, "standby.signal"))
		require.NoError(t, os.WriteFile(filepath.Join(data, "postgresql.auto.conf"), nil, 0600))
		require.NoError(t, os.WriteFile(filepath.Join(data, "recovery.signal"), nil, 0600))
		appendConfig(data, fmt.Sprintf("archive_mode=off\nrestore_command='%s --config=%s wal-fetch %%f %%p'\nrecovery_target='immediate'\nrecovery_target_action='promote'", walg, config))
		start(data, p)
		expected := "3"
		if i >= 2 { // detailed listing sorts the full backups by start time
			expected = strconv.Itoa(i + 2)
		}
		require.Equal(t, expected, sql(p, "SELECT count(*) FROM attempt_data"), backup.Name)
	}
}

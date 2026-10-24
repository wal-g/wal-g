package postgres_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
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

// The Docker integration suite supplies an isolated S3 service and a WAL-G
// binary. A filesystem config can also exercise this harness locally.
func TestWalSendCLIStandby(t *testing.T) { walSendCLIStandby(t, 16) }

func walSendCLIStandby(t *testing.T, walMB int) {
	t.Helper()
	bin := walSendPostgresBin(t)
	walg, configPath := os.Getenv("WALG_TEST_WALG_BIN"), os.Getenv("WALG_TEST_CLI_CONFIG")
	if walg == "" || configPath == "" {
		t.Skip("set WALG_TEST_WALG_BIN and WALG_TEST_CLI_CONFIG to run the CLI/storage test")
	}
	if walMB != 16 && walSendPostgresMajor(t, bin) < 11 {
		t.Skip("initdb --wal-segsize requires PostgreSQL 11+")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	defer cancel()
	dir := t.TempDir()
	primary, standby := filepath.Join(dir, "primary"), filepath.Join(dir, "standby")
	args := []string{"-D", primary, "-A", "trust", "-U", "walg_test", "--no-locale", "--no-sync"}
	if walMB != 16 {
		args = append(args, fmt.Sprintf("--wal-segsize=%d", walMB))
	}
	walSendPostgresCommand(t, ctx, bin, "initdb", args...)
	primaryAddress := walSendPostgresAddress(t)
	walSendPostgresStart(t, ctx, bin, primary, primaryAddress)
	data, err := os.ReadFile(configPath)
	require.NoError(t, err)
	var config map[string]any
	require.NoError(t, json.Unmarshal(data, &config))
	// Each test has its own prefix; it never clears a caller's existing archive.
	prefixFound := false
	archiveID := "wal-send-" + rand.Text()
	for _, key := range []string{"WALG_S3_PREFIX", "WALE_S3_PREFIX", "WALG_FILE_PREFIX"} {
		if prefix, ok := config[key].(string); ok && prefix != "" {
			prefix = strings.TrimRight(prefix, "/") + "/" + archiveID
			config[key] = prefix
			if key == "WALG_FILE_PREFIX" {
				require.NoError(t, os.MkdirAll(prefix, 0700))
				t.Cleanup(func() { _ = os.RemoveAll(prefix) })
			}
			prefixFound = true
		}
	}
	require.True(t, prefixFound, "CLI config must specify an S3 or filesystem prefix")
	host, port, err := net.SplitHostPort(primaryAddress)
	require.NoError(t, err)
	for key, value := range map[string]string{
		"PGHOST": host, "PGPORT": port, "PGUSER": "walg_test", "PGDATABASE": "postgres", "PGSSLMODE": "disable",
		"WALG_PG_WAL_SIZE": strconv.Itoa(walMB), "WALG_COMPRESSION_METHOD": "lz4", "WALG_DELTA_MAX_STEPS": "0",
		"WALG_UPLOAD_CONCURRENCY": "2", "WALG_DOWNLOAD_CONCURRENCY": "2",
	} {
		config[key] = value
	}
	require.NotEmpty(t, config["WALG_PGP_KEY_PATH"], "CLI test must exercise encryption as well as LZ4")
	data, err = json.Marshal(config)
	require.NoError(t, err)
	configPath = filepath.Join(dir, "walg.json")
	require.NoError(t, os.WriteFile(configPath, data, 0600))
	// Package test initializers export Viper defaults (including 16 MiB WAL).
	// Those environment values would override this test's explicit config.
	var cliEnv []string
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(key, "WALG_") || strings.HasPrefix(key, "WALE_") ||
			strings.HasPrefix(key, "PG") || strings.HasPrefix(key, "AWS_") {
			continue
		}
		cliEnv = append(cliEnv, entry)
	}
	command := func(args ...string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, walg, append([]string{"--config", configPath}, args...)...)
		cmd.Env = cliEnv
		return cmd
	}
	cli := func(args ...string) []byte {
		t.Helper()
		output, err := command(args...).CombinedOutput()
		require.NoError(t, err, "wal-g %s: %s", args[0], output)
		return output
	}
	walSendPostgresSQL(t, ctx, primaryAddress, "CREATE TABLE wal_send_cli_probe(id integer); INSERT INTO wal_send_cli_probe VALUES (1)")
	cli("backup-push", primary)
	walSendPostgresSQL(t, ctx, primaryAddress, "INSERT INTO wal_send_cli_probe VALUES (42)")
	walSendPostgresSQL(t, ctx, primaryAddress, "SELECT pg_switch_wal()")
	current := walSendPostgresCurrentWAL(t, ctx, primaryAddress)
	entries, err := os.ReadDir(filepath.Join(primary, "pg_wal"))
	require.NoError(t, err)
	for _, entry := range entries {
		if len(entry.Name()) == 24 && entry.Name() < current {
			cli("wal-push", filepath.Join(primary, "pg_wal", entry.Name()))
		}
	}
	cli("backup-fetch", standby, "LATEST")
	// Streaming must supply every WAL required by the backup. There is no
	// restore_command and no locally retained segment to hide a streaming gap.
	require.NoError(t, os.RemoveAll(filepath.Join(standby, "pg_wal")))
	require.NoError(t, os.MkdirAll(filepath.Join(standby, "pg_wal", "archive_status"), 0700))
	archiveAddress := walSendPostgresAddress(t)
	cmd := command("wal-send", "--address", archiveAddress, "--poll-interval=50ms")
	var logs bytes.Buffer
	cmd.Stdout, cmd.Stderr = &logs, &logs
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		cancel()
		_ = cmd.Wait()
		if t.Failed() {
			t.Logf("wal-send: %s", logs.String())
		}
	})
	require.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", archiveAddress, 100*time.Millisecond)
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, 5*time.Second, 20*time.Millisecond)
	_, archivePort, err := net.SplitHostPort(archiveAddress)
	require.NoError(t, err)
	recovery := fmt.Sprintf("primary_conninfo = 'host=127.0.0.1 port=%s user=walg_test sslmode=disable'\nrecovery_target_timeline = 'latest'\n", archivePort)
	if walSendPostgresMajor(t, bin) >= 12 {
		require.NoError(t, os.WriteFile(filepath.Join(standby, "postgresql.auto.conf"), []byte(recovery), 0600))
		require.NoError(t, os.WriteFile(filepath.Join(standby, "standby.signal"), nil, 0600))
	} else {
		require.NoError(t, os.WriteFile(filepath.Join(standby, "recovery.conf"), []byte("standby_mode = 'on'\n"+recovery), 0600))
	}
	standbyAddress := walSendPostgresAddress(t)
	walSendPostgresStart(t, ctx, bin, standby, standbyAddress)
	require.Eventually(t, func() bool {
		return walSendPostgresSQL(t, ctx, standbyAddress,
			"SELECT string_agg(id::text, ',' ORDER BY id) FROM wal_send_cli_probe") == "1,42"
	}, 10*time.Second, 50*time.Millisecond, "standby must replay encrypted LZ4 WAL from the CLI archive")
}

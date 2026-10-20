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
func TestBackupReservationStandbyIntegration(t *testing.T) {
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

	for _, modes := range [][2]bool{{false, false}, {false, true}, {true, true}} {
		label := fmt.Sprintf("streaming-%t-%t", modes[0], modes[1])
		t.Run(label, func(t *testing.T) {
			caseStore := filepath.Join(root, label)
			require.NoError(t, os.Mkdir(caseStore, 0700))
			caseConfig := filepath.Join(root, label+".json")
			settings, err := json.Marshal(map[string]any{
				"WALG_FILE_PREFIX": caseStore, "WALG_COMPRESSION_METHOD": "lz4", "WALG_DELTA_MAX_STEPS": 0,
				"WALG_NETWORK_RATE_LIMIT": 100000000,
			})
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(caseConfig, settings, 0600))
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			push := func(p, data string, streaming bool) *exec.Cmd {
				args := []string{"--config=" + caseConfig, "backup-push", "--full", "--reserve-backup-name"}
				if !streaming {
					args = append(args, data)
				}
				cmd := exec.CommandContext(ctx, walg, args...)
				cmd.Env = env(p)
				return cmd
			}
			commands := []*exec.Cmd{push(firstPort, first, modes[0]), push(secondPort, second, modes[1])}
			logs := make([]bytes.Buffer, len(commands))
			for i, cmd := range commands {
				cmd.Stdout, cmd.Stderr = &logs[i], &logs[i]
				require.NoError(t, cmd.Start())
			}
			winners := 0
			for i, cmd := range commands {
				if err := cmd.Wait(); err == nil {
					winners++
				} else {
					require.Contains(t, logs[i].String(), "cannot reserve backup name", logs[i].String())
				}
			}
			require.Equal(t, 1, winners, "exactly one independent replica must win")
			var backups []struct {
				Name string `json:"backup_name"`
			}
			list := run(primaryPort, walg, "--config="+caseConfig, "backup-list", "--detail", "--json")
			require.NoError(t, json.Unmarshal([]byte(list), &backups))
			require.Len(t, backups, 1)
			name := backups[0].Name
			require.Regexp(t, "^base_[0-9A-F]{24}$", name)

			// Both producer paths must reject a sequential duplicate too, without
			// changing any completed backup bytes.
			snapshot := make(map[string][32]byte)
			baseDir := filepath.Join(caseStore, "basebackups_005")
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
			for _, streaming := range []bool{false, true} {
				out, err := push(firstPort, first, streaming).CombinedOutput()
				require.Error(t, err)
				require.Contains(t, string(out), "cannot reserve backup name")
			}
			for path, expected := range snapshot {
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, expected, sha256.Sum256(data), path)
			}

			// WAL is archived by the primary into the shared archive config;
			// backup data lives in a fresh per-case prefix.
			sql(primaryPort, "SELECT pg_switch_wal()")
			require.Eventually(t, func() bool {
				files, err := filepath.Glob(filepath.Join(primary, "pg_wal", "archive_status", "*.ready"))
				return err == nil && len(files) == 0
			}, 30*time.Second, 100*time.Millisecond)
			data, p := filepath.Join(root, "restore-"+label), port()
			run(primaryPort, walg, "--config="+caseConfig, "backup-fetch", data, name)
			_ = os.Remove(filepath.Join(data, "standby.signal"))
			require.NoError(t, os.WriteFile(filepath.Join(data, "postgresql.auto.conf"), nil, 0600))
			require.NoError(t, os.WriteFile(filepath.Join(data, "recovery.signal"), nil, 0600))
			appendConfig(data, fmt.Sprintf("archive_mode=off\nrestore_command='%s --config=%s wal-fetch %%f %%p'\nrecovery_target='immediate'\nrecovery_target_action='promote'", walg, config))
			start(data, p)
			require.Equal(t, "3", sql(p, "SELECT count(*) FROM attempt_data"))
		})
	}
}

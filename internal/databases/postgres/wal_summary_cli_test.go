package postgres_test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWalSummaryIncrementalCLI(t *testing.T) {
	bin := walSendPostgresBin(t)
	if walSendPostgresMajor(t, bin) < 18 {
		t.Skip("WAL summary PoC requires PostgreSQL 18+")
	}
	walg, sourceConfig := os.Getenv("WALG_TEST_WALG_BIN"), os.Getenv("WALG_TEST_CLI_CONFIG")
	if walg == "" || sourceConfig == "" {
		t.Skip("set WALG_TEST_WALG_BIN and WALG_TEST_CLI_CONFIG")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	defer cancel()
	dir := t.TempDir()
	primary, restored := filepath.Join(dir, "primary"), filepath.Join(dir, "restored")
	walSendPostgresCommand(t, ctx, bin, "initdb", "-D", primary, "-A", "trust", "-U", "walg_test", "--no-locale", "--no-sync")
	file, err := os.OpenFile(filepath.Join(primary, "postgresql.conf"), os.O_APPEND|os.O_WRONLY, 0600)
	require.NoError(t, err)
	_, err = file.WriteString("\nsummarize_wal = on\nwal_summary_keep_time = 0\n")
	require.NoError(t, err)
	require.NoError(t, file.Close())
	address := walSendPostgresAddress(t)
	walSendPostgresStart(t, ctx, bin, primary, address)
	data, err := os.ReadFile(sourceConfig)
	require.NoError(t, err)
	var config map[string]any
	require.NoError(t, json.Unmarshal(data, &config))
	archiveID := "/wal-summary-" + rand.Text()
	prefixFound := false
	for _, key := range []string{"WALE_S3_PREFIX", "WALG_S3_PREFIX", "WALG_FILE_PREFIX"} {
		if prefix, ok := config[key].(string); ok && prefix != "" {
			config[key] = strings.TrimRight(prefix, "/") + archiveID
			prefixFound = true
			if key == "WALG_FILE_PREFIX" {
				require.NoError(t, os.MkdirAll(config[key].(string), 0700))
				t.Cleanup(func() { _ = os.RemoveAll(config[key].(string)) })
			}
		}
	}
	require.True(t, prefixFound, "CLI config must specify an isolated archive prefix")
	require.NotEmpty(t, config["WALG_PGP_KEY_PATH"])
	host, port, err := net.SplitHostPort(address)
	require.NoError(t, err)
	for key, value := range map[string]string{
		"PGHOST": host, "PGPORT": port, "PGUSER": "walg_test", "PGDATABASE": "postgres", "PGSSLMODE": "disable",
		"WALG_DELTA_MAX_STEPS": "3", "WALG_DELTA_ORIGIN": "LATEST", "WALG_USE_WAL_SUMMARY": "true",
		"WALG_USE_WAL_DELTA": "false", "WALG_COMPRESSION_METHOD": "lz4", "WALG_UPLOAD_CONCURRENCY": "2",
		"WALG_ALIVE_CHECK_INTERVAL": "10ms",
	} {
		config[key] = value
	}
	data, err = json.Marshal(config)
	require.NoError(t, err)
	configPath := filepath.Join(dir, "walg.json")
	require.NoError(t, os.WriteFile(configPath, data, 0600))
	var cliEnv []string
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if !strings.HasPrefix(key, "WALG_") && !strings.HasPrefix(key, "WALE_") &&
			!strings.HasPrefix(key, "PG") && !strings.HasPrefix(key, "AWS_") {
			cliEnv = append(cliEnv, entry)
		}
	}
	command := func(args ...string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, walg, append([]string{"--config", configPath}, args...)...)
		cmd.Env = cliEnv
		return cmd
	}
	cli := func(args ...string) []byte {
		t.Helper()
		output, err := command(args...).CombinedOutput()
		require.NoError(t, err, "%v: %s", args, output)
		return output
	}
	sql := func(query string) string { return walSendPostgresSQL(t, ctx, address, query) }
	finishSummary := func() {
		t.Helper()
		end := sql("SELECT pg_current_wal_insert_lsn()")
		// Summaries split at checkpoint redo records, not WAL file switches.
		sql("CHECKPOINT")
		require.Eventually(t, func() bool {
			return sql(fmt.Sprintf("SELECT summarized_lsn >= '%s'::pg_lsn FROM pg_get_wal_summarizer_state()", end)) == "t"
		}, 30*time.Second, 100*time.Millisecond, "publish the in-memory summary before deleting local files")
	}
	sql(`CREATE TABLE summary_probe AS SELECT id, repeat(md5(id::text), 8) AS payload FROM generate_series(1,10000) id;
		CREATE TABLE summary_truncate AS SELECT generate_series(1,1000) id`)
	cli("backup-push", primary)
	probePath := filepath.Join(primary, sql("SELECT pg_relation_filepath('summary_probe')"))
	before, err := os.Stat(probePath)
	require.NoError(t, err)
	sql("UPDATE summary_probe SET payload='delta-one' WHERE id IN (1,9000); TRUNCATE summary_truncate; INSERT INTO summary_truncate VALUES (42); CHECKPOINT")
	// Summaries, rather than mtime or a page scan, must find these changes.
	require.NoError(t, os.Chtimes(probePath, before.ModTime(), before.ModTime()))
	require.Contains(t, string(cli("backup-push", primary)), "Using WAL summaries:")
	require.Contains(t, string(cli("st", "ls", "wal_sum_005/")), ".summary")
	// Archive published summaries explicitly, then remove the local copies.
	// The next delta must combine archived history with new local summaries.
	finishSummary()
	summaries, err := os.ReadDir(filepath.Join(primary, "pg_wal", "summaries"))
	require.NoError(t, err)
	require.NotEmpty(t, summaries)
	for _, summary := range summaries {
		if strings.HasSuffix(summary.Name(), ".summary") {
			filename := filepath.Join(primary, "pg_wal", "summaries", summary.Name())
			cli("wal-summary-push", filename)
			require.NoError(t, os.Remove(filename))
		}
	}
	sql("UPDATE summary_probe SET payload='delta-two' WHERE id=5000; DELETE FROM summary_probe WHERE id=10000; INSERT INTO summary_probe VALUES (10001,'new')")
	require.Contains(t, string(cli("backup-push", primary)), "Using WAL summaries:")
	listing := string(cli("backup-list", "--json"))
	require.Equal(t, 2, strings.Count(listing, "_D_"))
	digestQuery := "SELECT md5(string_agg(id::text || payload, ',' ORDER BY id)) FROM summary_probe"
	want := sql(digestQuery)
	// Archive recovery WAL before the fault-injection checkpoints can recycle it.
	sql("SELECT pg_switch_wal()")
	current := walSendPostgresCurrentWAL(t, ctx, address)
	walFiles, err := os.ReadDir(filepath.Join(primary, "pg_wal"))
	require.NoError(t, err)
	for _, file := range walFiles {
		if len(file.Name()) == 24 && file.Name() < current {
			cli("wal-push", filepath.Join(primary, "pg_wal", file.Name()))
		}
	}
	// Losing required history must fail instead of silently reverting to page
	// scanning or publishing a completed backup with an incomplete block map.
	finishSummary()
	summaries, err = os.ReadDir(filepath.Join(primary, "pg_wal", "summaries"))
	require.NoError(t, err)
	for _, summary := range summaries {
		if strings.HasSuffix(summary.Name(), ".summary") {
			require.NoError(t, os.Remove(filepath.Join(primary, "pg_wal", "summaries", summary.Name())))
		}
	}
	cli("st", "rm", "wal_sum_005/")
	output, err := command("backup-push", primary).CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "WAL summary gap")
	require.Equal(t, 2, strings.Count(string(cli("backup-list", "--json")), "_D_"))
	cli("backup-fetch", restored, "LATEST")
	recovery := fmt.Sprintf("restore_command = '%s --config %s wal-fetch %%f %%p'\n", walg, configPath)
	require.NoError(t, os.WriteFile(filepath.Join(restored, "postgresql.auto.conf"), []byte(recovery), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(restored, "recovery.signal"), nil, 0600))
	// PostgreSQL inherits the test initializer's configuration defaults; give
	// archive recovery the same isolated environment as the CLI invocations.
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(key, "WALG_") || strings.HasPrefix(key, "WALE_") || strings.HasPrefix(key, "AWS_") {
			t.Setenv(key, "")
		}
	}
	restoredAddress := walSendPostgresAddress(t)
	walSendPostgresStart(t, ctx, bin, restored, restoredAddress)
	require.Equal(t, want, walSendPostgresSQL(t, ctx, restoredAddress, digestQuery))
	require.Equal(t, "42", walSendPostgresSQL(t, ctx, restoredAddress, "SELECT string_agg(id::text, ',') FROM summary_truncate"))
}

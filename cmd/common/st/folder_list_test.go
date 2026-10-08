package st

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/limiters"
	"golang.org/x/time/rate"
)

type folderListFixture struct {
	Root            bool
	Ordinary        bool
	Backend         string
	Limited         bool
	Empty           bool
	FailurePage     int
	NotFoundPage    int
	PrimaryBucket   string
	SecondaryBucket string
	RejectRequests  bool
	FailHandler     bool
	FailStdout      bool
}

func TestFolderListCommand(t *testing.T) {
	if config := os.Getenv("WALG_TEST_LIST_CONFIG"); config != "" {
		var fixture folderListFixture
		require.NoError(t, json.Unmarshal([]byte(config), &fixture))
		if err := runFolderListChild(t, fixture); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if t.Failed() {
			fmt.Fprintln(os.Stderr, "child test assertions failed")
			os.Exit(1)
		}
		os.Exit(0)
	}
	prefix := []string{"--prefix", "0000000800000002"}
	for _, tc := range []struct {
		name      string
		args      []string
		fixture   folderListFixture
		wantError string
		wantNames bool
	}{
		{name: "pages", args: prefix, wantNames: true},
		{name: "root", args: prefix, fixture: folderListFixture{Root: true}, wantNames: true},
		{
			name: "gcs root", args: prefix, fixture: folderListFixture{Root: true, Backend: "gcs"},
			wantError: `list GCS objects with prefix "archive/0000000800000002"`,
		},
		{name: "limited", args: prefix, fixture: folderListFixture{Limited: true}, wantNames: true},
		{name: "empty", args: prefix, fixture: folderListFixture{Empty: true}},
		{
			name: "first failure", args: prefix, fixture: folderListFixture{FailurePage: 1},
			wantError: "AccessDenied",
		},
		{
			name: "later failure", args: prefix, fixture: folderListFixture{FailurePage: 2},
			wantError: "AccessDenied",
		},
		{
			name: "all failure first", args: append(prefix, "--target", "all"),
			fixture:   folderListFixture{PrimaryBucket: "broken", SecondaryBucket: "good"},
			wantError: "AccessDenied", wantNames: true,
		},
		{
			name: "all failure last", args: append(prefix, "--target", "all"),
			fixture: folderListFixture{SecondaryBucket: "broken"}, wantError: "AccessDenied", wantNames: true,
		},
		{
			name: "all success", args: append(prefix, "--target", "all"),
			fixture: folderListFixture{SecondaryBucket: "also-good"}, wantNames: true,
		},
		{name: "no prefix", fixture: folderListFixture{Ordinary: true}, wantNames: true},
		{
			name: "ordinary recursive", args: []string{"--recursive"},
			fixture: folderListFixture{Ordinary: true}, wantNames: true,
		},
		{
			name: "ordinary versions", args: []string{"--all-versions"},
			fixture: folderListFixture{Ordinary: true}, wantNames: true,
		},
		{
			name: "ordinary all", args: []string{"--target", "all"},
			fixture: folderListFixture{Ordinary: true, SecondaryBucket: "broken"}, wantNames: true,
		},
		{
			name: "empty prefix", args: []string{"--prefix="},
			fixture: folderListFixture{RejectRequests: true}, wantError: "must not be empty",
		},
		{
			name: "glob conflict", args: append(prefix, "--glob"),
			fixture: folderListFixture{RejectRequests: true}, wantError: "--glob",
		},
		{
			name: "recursive conflict", args: append(prefix, "-r"),
			fixture: folderListFixture{RejectRequests: true}, wantError: "--recursive",
		},
		{
			name: "versions conflict", args: append(prefix, "--all-versions"),
			fixture: folderListFixture{RejectRequests: true}, wantError: "--all-versions",
		},
		{
			name: "unsupported", args: prefix, fixture: folderListFixture{Backend: "file"},
			wantError: "not supported",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			command := folderListProcess(t, tc.fixture, tc.args)
			var stdout, stderr bytes.Buffer
			command.Stdout, command.Stderr = &stdout, &stderr
			err := command.Run()
			require.NotContains(t, stderr.String(), folderListFixtureFailure)
			if tc.wantError != "" {
				require.Error(t, err, stdout.String()+stderr.String())
				require.Contains(t, stderr.String(), tc.wantError)
			} else {
				require.NoError(t, err, stdout.String()+stderr.String())
				require.Contains(t, stdout.String(), "last modified")
			}
			if tc.wantNames {
				require.Contains(t, stdout.String(), "000000080000000200000001.lz4")
				require.Contains(t, stdout.String(), "000000080000000200000002.zst")
				require.Contains(t, stdout.String(), "123")
				require.Contains(t, stdout.String(), "2026-10-06 12:00:00 +0000 UTC")
				require.NotContains(t, stdout.String(), "archive/wal_005/")
			} else {
				require.NotContains(t, stdout.String(), "000000080000000200000001.lz4")
			}
			if tc.wantError != "" && !tc.wantNames {
				require.Empty(t, stdout.String())
			}
		})
	}
}

func TestFolderListCommandNoSuchKey(t *testing.T) {
	for _, page := range []int{1, 2} {
		t.Run(fmt.Sprintf("page=%d", page), func(t *testing.T) {
			fixture := folderListFixture{NotFoundPage: page}
			command := folderListProcess(t, fixture, []string{"--prefix", "0000000800000002"})
			var stdout, stderr bytes.Buffer
			command.Stdout, command.Stderr = &stdout, &stderr
			err := command.Run()
			require.NoError(t, err, stderr.String())
			require.NotContains(t, stderr.String(), folderListFixtureFailure)
			require.Contains(t, stdout.String(), "last modified")
			if page == 1 {
				require.NotContains(t, stderr.String(), "WARNING:")
				require.NotContains(t, stdout.String(), "000000080000000200000001.lz4")
			} else {
				require.Contains(t, stderr.String(), "WARNING:")
				require.Contains(t, stderr.String(), "S3 listing incomplete after successful pages")
				require.Contains(t, stdout.String(), "000000080000000200000001.lz4")
			}
			require.NotContains(t, stdout.String(), "000000080000000200000002.zst")
		})
	}
}

func TestFolderListCommandFlushWarning(t *testing.T) {
	for _, ordinary := range []bool{false, true} {
		t.Run(fmt.Sprintf("ordinary=%t", ordinary), func(t *testing.T) {
			fixture := folderListFixture{Ordinary: ordinary, FailStdout: true}
			var flags []string
			if !ordinary {
				flags = []string{"--prefix", "0000000800000002"}
			}
			command := folderListProcess(t, fixture, flags)
			var stdout, stderr bytes.Buffer
			command.Stdout, command.Stderr = &stdout, &stderr
			err := command.Run()
			require.NoError(t, err, stderr.String())
			require.Empty(t, stdout.String())
			require.NotContains(t, stderr.String(), folderListFixtureFailure)
			require.Contains(t, stderr.String(), "WARNING:")
			require.Contains(t, stderr.String(), "failed to flush storage listing output")
		})
	}
}

func folderListProcess(t *testing.T, fixture folderListFixture, flags []string) *exec.Cmd {
	t.Helper()
	config, err := json.Marshal(fixture)
	require.NoError(t, err)
	args := []string{"-test.run=^TestFolderListCommand$", "--", "st", "ls"}
	if !fixture.Root {
		args = append(args, "wal_005/")
	}
	args = append(args, flags...)
	command := exec.Command(os.Args[0], args...)
	command.Env = append(os.Environ(), "WALG_TEST_LIST_CONFIG="+string(config))
	return command
}

func TestFolderListChildReportsHandlerFailure(t *testing.T) {
	for _, failPage := range []int{0, 2} {
		t.Run(fmt.Sprintf("listing error page=%d", failPage), func(t *testing.T) {
			fixture := folderListFixture{FailHandler: true, FailurePage: failPage}
			command := folderListProcess(t, fixture, []string{"--prefix", "0000000800000002"})
			output, err := command.CombinedOutput()
			require.Error(t, err, string(output))
			require.Contains(t, string(output), folderListFixtureFailure)
			if failPage == 0 {
				require.Contains(t, string(output), "000000080000000200000002.zst")
				require.Contains(t, string(output), "child test assertions failed")
			} else {
				require.Contains(t, string(output), "AccessDenied")
			}
		})
	}
}

func runFolderListChild(t *testing.T, fixture folderListFixture) error {
	if fixture.FailStdout {
		output, err := os.Open(os.DevNull)
		require.NoError(t, err)
		defer output.Close()
		os.Stdout = output
	}
	viper.Reset()
	viper.Set("WALG_S3_PREFIX", "s3://good/archive")
	viper.Set("AWS_REGION", "us-east-1")
	viper.Set("AWS_ACCESS_KEY_ID", "test")
	viper.Set("AWS_SECRET_ACCESS_KEY", "test")
	viper.Set("AWS_S3_FORCE_PATH_STYLE", true)
	viper.Set("S3_MAX_RETRIES", 0)
	viper.Set("S3_ENABLE_VERSIONING", "disabled")
	viper.Set("UPLOAD_CONCURRENCY", 1)
	switch fixture.Backend {
	case "file":
		viper.Reset()
		viper.Set("WALG_FILE_PREFIX", t.TempDir())
	case "gcs":
		viper.Reset()
		viper.Set("WALG_GS_PREFIX", "gs://bucket/archive")
		viper.Set("GCS_NORMALIZE_PREFIX", false)
		t.Setenv("STORAGE_EMULATOR_HOST", "storage.example")
		http.DefaultTransport = &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
			return nil, errors.New("test network failure")
		}}
	}
	if fixture.Limited {
		limiters.NetworkLimiter = rate.NewLimiter(1024, 1024)
	}
	if fixture.PrimaryBucket != "" {
		viper.Set("WALG_S3_PREFIX", "s3://"+fixture.PrimaryBucket+"/archive")
	}
	server := folderListServer(t, fixture)
	defer server.Close()
	viper.Set("AWS_ENDPOINT", server.URL)
	if fixture.SecondaryBucket != "" {
		viper.Set("WALG_FAILOVER_STORAGES", map[string]interface{}{"secondary": map[string]interface{}{
			"WALG_S3_PREFIX": "s3://" + fixture.SecondaryBucket + "/archive", "AWS_ENDPOINT": server.URL,
			"AWS_REGION": "us-east-1", "AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test",
			"AWS_S3_FORCE_PATH_STYLE": true, "S3_MAX_RETRIES": 0, "S3_ENABLE_VERSIONING": "disabled",
			"UPLOAD_CONCURRENCY": 1,
		}})
	}
	root := &cobra.Command{Use: "wal-g", SilenceUsage: true, SilenceErrors: true}
	root.AddCommand(StorageToolsCmd)
	for i, arg := range os.Args {
		if arg == "--" {
			root.SetArgs(os.Args[i+1:])
			break
		}
	}
	return root.Execute()
}

const folderListFixtureFailure = "folder listing fixture failed"

type folderListTestReporter struct {
	t *testing.T
}

func (r folderListTestReporter) Errorf(format string, args ...interface{}) {
	fmt.Fprintln(os.Stderr, folderListFixtureFailure)
	r.t.Errorf(format, args...)
}

func folderListServer(t *testing.T, fixture folderListFixture) *httptest.Server {
	calls := make(map[string]int)
	reporter := folderListTestReporter{t: t}
	check := assert.New(reporter)
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !check.False(fixture.RejectRequests, "validation must precede storage requests") {
			http.Error(w, "unexpected listing", http.StatusBadRequest)
			return
		}
		check.Equal("GET", r.Method)
		q := r.URL.Query()
		base := "archive/wal_005/"
		if fixture.Root {
			base = "archive/"
		}
		wantPrefix := base + "0000000800000002"
		if fixture.Ordinary {
			wantPrefix = base
			check.Equal("/", q.Get("delimiter"))
		} else {
			check.Empty(q.Get("delimiter"))
		}
		check.Equal(wantPrefix, q.Get("prefix"))
		check.Equal("2", q.Get("list-type"))
		calls[r.URL.Path]++
		page := calls[r.URL.Path]
		if !check.LessOrEqual(page, 2) {
			http.Error(w, "unexpected page", http.StatusBadRequest)
			return
		}
		if page == 1 {
			check.Empty(q.Get("continuation-token"))
		} else {
			check.Equal("next", q.Get("continuation-token"))
		}
		status, body := http.StatusOK, `<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`
		switch {
		case page == fixture.NotFoundPage:
			status, body = http.StatusNotFound, `<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>`
		case page == fixture.FailurePage || page == 2 && r.URL.Path == "/broken":
			status, body = http.StatusForbidden, `<Error><Code>AccessDenied</Code><Message>denied</Message></Error>`
		case !fixture.Empty:
			next, suffix := "<NextContinuationToken>next</NextContinuationToken>", "lz4"
			if page == 2 {
				next, suffix = "", "zst"
			}
			body = fmt.Sprintf(`<ListBucketResult><IsTruncated>%t</IsTruncated>%s`+
				`<Contents><Key>%s0000000800000002%08d.%s</Key>`+
				`<LastModified>2026-10-06T12:00:00Z</LastModified><Size>123</Size></Contents></ListBucketResult>`,
				page == 1, next, base, page, suffix)
		}
		if fixture.FailHandler && page == 2 {
			reporter.Errorf("injected handler assertion")
		}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(status)
		if _, err := io.WriteString(w, body); err != nil {
			reporter.Errorf("write response: %v", err)
		}
	}))
}

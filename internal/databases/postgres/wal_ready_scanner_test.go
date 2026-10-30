package postgres

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	conf "github.com/wal-g/wal-g/internal/config"
)

func readyScannerFixture(t *testing.T, first string, limit int32) (*walReadyScanner, *int) {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, archiveStatusDir), 0700))
	s, err := newWALReadyScanner(dir, first, limit)
	require.NoError(t, err)
	calls := new(int)
	s.readDir = func(dir string) ([]os.DirEntry, error) { *calls++; return os.ReadDir(dir) }
	return s, calls
}

func readyScannerAdd(t *testing.T, s *walReadyScanner, names ...string) {
	t.Helper()
	for _, name := range names {
		require.NoError(t, os.WriteFile(filepath.Join(s.dir, name), nil, 0600))
	}
}

func TestWALReadyScannerContiguousBatchAvoidsListing(t *testing.T) {
	s, calls := readyScannerFixture(t, "000000010000000000000000", 31)
	var want []string
	for i := 1; i <= 31; i++ {
		want = append(want, fmt.Sprintf("0000000100000000000000%02X.ready", i))
	}
	readyScannerAdd(t, s, want...)
	got, err := s.candidates(time.Now())
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Zero(t, *calls)
}

func TestWALReadyScannerContinuesPredictionBetweenFullScans(t *testing.T) {
	s, calls := readyScannerFixture(t, "000000010000000000000000", 31)
	now := time.Now()
	_, err := s.candidates(now)
	require.NoError(t, err)
	require.Equal(t, 1, *calls)
	first := "000000010000000000000001.ready"
	readyScannerAdd(t, s, first)
	got, err := s.candidates(now.Add(pollPauseDuration))
	require.NoError(t, err)
	require.Equal(t, []string{first}, got)
	require.Equal(t, 1, *calls)
	second := "000000010000000000000002.ready"
	readyScannerAdd(t, s, second)
	got, err = s.candidates(now.Add(2 * pollPauseDuration))
	require.NoError(t, err)
	require.Equal(t, []string{second}, got)
	require.Equal(t, 1, *calls)
}

func TestWALReadyScannerFallbackAndThrottle(t *testing.T) {
	for _, first := range []string{"000000010000000000000000", "00000001.history"} {
		t.Run(first, func(t *testing.T) {
			s, calls := readyScannerFixture(t, first, 31)
			want := []string{"000000010000000000000003.ready", "00000002.history.ready", "000000020000000000000001.ready"}
			readyScannerAdd(t, s, append(want, "000000010000000000000002.done")...)
			now := time.Now()
			got, err := s.candidates(now)
			require.NoError(t, err)
			require.ElementsMatch(t, want, got)
			for i := 1; i < 50; i++ {
				got, err = s.candidates(now.Add(time.Duration(i) * pollPauseDuration))
				require.NoError(t, err)
				require.Empty(t, got)
			}
			require.Equal(t, 1, *calls)
			got, err = s.candidates(now.Add(s.fullScanInterval))
			require.NoError(t, err)
			require.ElementsMatch(t, want, got)
			require.Equal(t, 2, *calls)
		})
	}
}

func TestWALReadyScannerRollover(t *testing.T) {
	s, calls := readyScannerFixture(t, "0000000100000000000000FF", 1)
	next := "000000010000000100000000.ready"
	readyScannerAdd(t, s, next)
	got, err := s.candidates(time.Now())
	require.NoError(t, err)
	require.Equal(t, []string{next}, got)
	require.Zero(t, *calls)
}

func TestWALReadyScannerConfig(t *testing.T) {
	old := viper.Get(conf.PgWalFullScanInterval)
	t.Cleanup(func() { viper.Set(conf.PgWalFullScanInterval, old) })
	for _, tc := range []struct {
		name    string
		value   any
		want    time.Duration
		invalid bool
	}{
		{"default", nil, 5 * time.Second, false},
		{"custom", "750ms", 750 * time.Millisecond, false},
		{"zero", "0", 0, false},
		{"zero_duration", "0s", 0, false},
		{"negative", "-1s", 0, true},
		{"malformed", "bad", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			viper.Set(conf.PgWalFullScanInterval, tc.value)
			s, err := newWALReadyScanner(t.TempDir(), "000000010000000000000000", 31)
			if tc.invalid {
				require.ErrorContains(t, err, conf.PgWalFullScanInterval)
				require.Nil(t, s)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, s.fullScanInterval)
		})
	}
}

func TestWALReadyScannerCustomInterval(t *testing.T) {
	old := viper.Get(conf.PgWalFullScanInterval)
	viper.Set(conf.PgWalFullScanInterval, "750ms")
	t.Cleanup(func() { viper.Set(conf.PgWalFullScanInterval, old) })
	s, calls := readyScannerFixture(t, "000000010000000000000000", 31)
	now := time.Now()
	_, err := s.candidates(now)
	require.NoError(t, err)
	gap := "000000010000000000000003.ready"
	readyScannerAdd(t, s, gap)
	got, err := s.candidates(now.Add(749 * time.Millisecond))
	require.NoError(t, err)
	require.Empty(t, got)
	require.Equal(t, 1, *calls)
	got, err = s.candidates(now.Add(750 * time.Millisecond))
	require.NoError(t, err)
	require.Equal(t, []string{gap}, got)
	require.Equal(t, 2, *calls)
}

func TestWALReadyScannerZeroDisablesThrottle(t *testing.T) {
	old := viper.Get(conf.PgWalFullScanInterval)
	viper.Set(conf.PgWalFullScanInterval, "0")
	t.Cleanup(func() { viper.Set(conf.PgWalFullScanInterval, old) })
	s, calls := readyScannerFixture(t, "000000010000000000000000", 1)
	first := "000000010000000000000001.ready"
	history := "00000002.history.ready"
	readyScannerAdd(t, s, first, history)
	now := time.Now()
	got, err := s.candidates(now)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{first, history}, got)
	require.Equal(t, 1, *calls, "a full predicted batch must not skip the listing")
	next := "000000010000000000000002.ready"
	readyScannerAdd(t, s, next)
	got, err = s.candidates(now.Add(pollPauseDuration))
	require.NoError(t, err)
	require.ElementsMatch(t, []string{first, next, history}, got)
	require.Equal(t, 2, *calls, "zero must list again at the next poll")
}

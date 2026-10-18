package postgres

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func readyScannerFixture(t *testing.T, first string, limit int32) (*walReadyScanner, *int) {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, archiveStatusDir), 0700))
	s := newWALReadyScanner(dir, first, limit)
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
			got, err = s.candidates(now.Add(walReadyFullScanInterval))
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

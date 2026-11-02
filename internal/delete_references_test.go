package internal_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
)

func TestDeletionReferencesMergesRuns(t *testing.T) {
	refs, err := internal.NewDeletionReferences()
	require.NoError(t, err)
	defer refs.Close()
	for i := 40000; i >= 0; i-- {
		require.NoError(t, refs.Add(fmt.Sprint(i*2)))
	}
	require.NoError(t, refs.Seal())
	for _, i := range []int{0, 2, 1310, 65536, 80000} {
		exists, err := refs.Contains(fmt.Sprint(i))
		require.NoError(t, err)
		require.True(t, exists)
	}
	for _, i := range []int{1, 1311, 65535, 80001} {
		exists, err := refs.Contains(fmt.Sprint(i))
		require.NoError(t, err)
		require.False(t, exists)
	}
}

func TestDeletionManifestRejectsUnknownReferences(t *testing.T) {
	for _, data := range []string{`{}`, `{"Files":{"a":{"StoragePath":null}}}`, `{"Files":{"a":{"StoragePath":"live","StoragePath":""}}}`, `{"Files":null}`, `{"Files":{"a":null}}`, `{"Files":{"a":{"StoragePath":"../other"}}}`, `{"Files":{}} trailing`} {
		problem, err := internal.WalkDeletionManifest(strings.NewReader(data), func(string, int64) error { return nil }, "")
		require.NoError(t, err)
		require.Error(t, problem, data)
	}
}

func TestDeletionReferencesUniqueAccounting(t *testing.T) {
	previous, err := internal.NewDeletionReferences()
	require.NoError(t, err)
	defer previous.Close()
	require.NoError(t, previous.Add("old"))
	require.NoError(t, previous.Seal())
	current, err := internal.NewDeletionReferences()
	require.NoError(t, err)
	defer current.Close()
	require.NoError(t, current.AddSized("old", 10))
	require.NoError(t, current.AddSized("new", 20))
	require.NoError(t, current.AddSized("new", 20))
	require.NoError(t, current.Seal())
	size, err := current.SumNotIn(previous)
	require.NoError(t, err)
	require.EqualValues(t, 20, size)
}

package storagetools

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type failingListWriter struct {
	bytes.Buffer
	remaining int
}

func (w *failingListWriter) Write(p []byte) (int, error) {
	if len(p) > w.remaining {
		n, _ := w.Buffer.Write(p[:w.remaining])
		w.remaining = 0
		return n, errors.New("disk full")
	}
	w.remaining -= len(p)
	return w.Buffer.Write(p)
}

func TestWriteObjectsListWarnsOnFlushFailure(t *testing.T) {
	for _, tc := range []struct {
		name    string
		objects []ListElement
	}{
		{name: "empty listing"},
		{name: "object listing", objects: []ListElement{NewListObject(storage.NewLocalObject(
			"000000080000000200000001.lz4", time.Unix(0, 0).UTC(), 123,
		))}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var warnings bytes.Buffer
			defer tracelog.WarningLogger.SetOutput(tracelog.WarningLogger.Writer())
			tracelog.WarningLogger.SetOutput(&warnings)
			output := &failingListWriter{remaining: 8}
			require.NoError(t, WriteObjectsList(tc.objects, output))
			require.Equal(t, 8, output.Len())
			require.Contains(t, warnings.String(), "WARNING:")
			require.Contains(t, warnings.String(), "failed to flush storage listing output: disk full")

			warnings.Reset()
			var successfulOutput bytes.Buffer
			require.NoError(t, WriteObjectsList(tc.objects, &successfulOutput))
			require.Contains(t, successfulOutput.String(), "last modified")
			require.Empty(t, warnings.String())
		})
	}
}

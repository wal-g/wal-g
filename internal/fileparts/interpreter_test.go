package fileparts

import (
	"archive/tar"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
)

func TestTarInterpreterRestoresFileParts(t *testing.T) {
	const name = "/db/file.data"
	const data = "ABCDEFGHIJK"
	directory := t.TempDir()
	interpreter := NewTarInterpreter(directory, internal.NewFileTarInterpreter(directory), &Info{
		PartSize: 4,
		Files:    map[string]int64{name: int64(len(data))},
	})

	for offset := 8; offset >= 0; offset -= 4 {
		end := min(offset+4, len(data))
		header := &tar.Header{
			Name: name, Typeflag: tar.TypeReg, Mode: 0600, Size: int64(end - offset), Format: tar.FormatPAX,
			PAXRecords: map[string]string{
				OffsetPAX:   strconv.Itoa(offset),
				FileSizePAX: strconv.Itoa(len(data)),
			},
		}
		require.NoError(t, interpreter.Interpret(bytes.NewBufferString(data[offset:end]), header))
	}

	require.NoError(t, interpreter.Finalize())
	restored, err := os.ReadFile(filepath.Join(directory, "db", "file.data"))
	require.NoError(t, err)
	require.Equal(t, data, string(restored))
}

func TestTarInterpreterDelegatesRegularFile(t *testing.T) {
	regular := &recordingTarInterpreter{}
	interpreter := NewTarInterpreter(t.TempDir(), regular, nil)
	header := &tar.Header{Name: "regular.data", Typeflag: tar.TypeReg, Size: 4}

	require.NoError(t, interpreter.Interpret(bytes.NewBufferString("data"), header))
	require.NoError(t, interpreter.Finalize())
	require.Same(t, header, regular.header)
	require.Equal(t, "data", regular.data)
}

type recordingTarInterpreter struct {
	header *tar.Header
	data   string
}

func (interpreter *recordingTarInterpreter) Interpret(reader io.Reader, header *tar.Header) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	interpreter.header = header
	interpreter.data = string(data)
	return nil
}

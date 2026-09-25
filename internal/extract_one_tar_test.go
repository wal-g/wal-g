package internal

import (
	"archive/tar"
	"bytes"
	"errors"
	"io"
	"testing"
)

type recordingTarInterpreter struct {
	entries []recordedTarEntry
	err     error
}

type recordedTarEntry struct {
	name string
	body string
}

func (i *recordingTarInterpreter) Interpret(reader io.Reader, header *tar.Header) error {
	body, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	i.entries = append(i.entries, recordedTarEntry{name: header.Name, body: string(body)})
	return i.err
}

func makeTarArchive(t *testing.T, entries ...recordedTarEntry) []byte {
	t.Helper()

	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	for _, entry := range entries {
		header := &tar.Header{Name: entry.name, Mode: 0o600, Size: int64(len(entry.body))}
		if err := writer.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write([]byte(entry.body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return archive.Bytes()
}

func TestExtractOneTarPassesAllEntriesToInterpreter(t *testing.T) {
	want := []recordedTarEntry{
		{name: "base/PG_VERSION", body: "16\n"},
		{name: "base/global/pg_control", body: "control data"},
	}
	interpreter := &recordingTarInterpreter{}

	err := extractOneTar(interpreter, bytes.NewReader(makeTarArchive(t, want...)))

	if err != nil {
		t.Fatalf("extractOneTar() error = %v", err)
	}
	if len(interpreter.entries) != len(want) {
		t.Fatalf("interpreter received %d entries, want %d", len(interpreter.entries), len(want))
	}
	for index := range want {
		if interpreter.entries[index] != want[index] {
			t.Errorf("entry %d = %#v, want %#v", index, interpreter.entries[index], want[index])
		}
	}
}

func TestExtractOneTarReturnsInterpreterError(t *testing.T) {
	wantErr := errors.New("cannot restore entry")
	interpreter := &recordingTarInterpreter{err: wantErr}

	err := extractOneTar(interpreter, bytes.NewReader(makeTarArchive(t, recordedTarEntry{name: "file", body: "data"})))

	if !errors.Is(err, wantErr) {
		t.Fatalf("extractOneTar() error = %v, want wrapped %v", err, wantErr)
	}
}

func TestExtractOneTarReturnsMalformedArchiveError(t *testing.T) {
	interpreter := &recordingTarInterpreter{}

	err := extractOneTar(interpreter, bytes.NewReader([]byte("not a tar archive")))

	if err == nil {
		t.Fatal("extractOneTar() error = nil, want an error")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("tar extract failed")) {
		t.Fatalf("extractOneTar() error = %v, want tar extract context", err)
	}
}

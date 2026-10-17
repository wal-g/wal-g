package ao

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestZeroFillFileToEOFKeepsPrefixAndFillsTail(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "ao-segment")
	prefix := []byte("decrypted AO bytes")
	if err := os.WriteFile(filePath, prefix, 0600); err != nil {
		t.Fatal(err)
	}

	logicalEOF := int64(len(prefix) + 8)
	if err := ZeroFillFileToEOF(filePath, logicalEOF, 0640, false); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatal(err)
	}
	want := make([]byte, logicalEOF)
	copy(want, prefix)
	if !bytes.Equal(got, want) {
		t.Fatalf("recovered file = %v, want %v", got, want)
	}
}

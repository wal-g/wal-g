package walg

import "testing"

func TestLSNParse(t *testing.T) {
	lsn, err := ParseLsn("2/E5000028")
	if err != nil {
		t.Fatal(err)
	}
	if lsn != 0x2E5000028 {
		t.Fatal("LSN was not parsed correctly")
	}
}

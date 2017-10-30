package walg

import "testing"

func TestControlFileRead(t *testing.T) {
	lsn, err := ParseLsn("2/E5000028")
	if err != nil {
		t.Fatal(err)
	}
	name, _, err := WALFileName(lsn, "testdata")
	if err != nil {
		t.Fatal(err)
	}
	if name != "0000000100000002000000E5" {
		t.Fatal("Wrong walfilename read from pg_control file's checkpoint: ", name)
	}
}

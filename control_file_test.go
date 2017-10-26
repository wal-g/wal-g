package walg

import "testing"

func TestControlFileRead(t *testing.T) {
	name, err := WALFileName("2/E5000028", "testdata/pg_control")
	if err != nil {
		t.Fatal(err)
	}
	if name != "0000000100000002000000E5" {
		t.Fatal("Wrong walfilename read from pg_control file's checkpoint: ", name)
	}
}

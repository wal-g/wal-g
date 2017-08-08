package walg_test

import (
	"github.com/katie31/wal-g"
	"testing"
)

var fileNames = []struct {
	input    string
	expected string
}{
	{"mock.lzo", "lzo"},
	{"mock.tar.lzo", "lzo"},
	{"mock.gzip", "gzip"},
	{"mockgzip", ""},
}

// Tests that backup name is successfully extracted from
// return values of pg_stop_backup(false)
func TestCheckType(t *testing.T) {
	for _, f := range fileNames {
		actual := walg.CheckType(f.input)
		if actual != f.expected {
			t.Errorf("decompress: CheckType expected `%s` but got `%s`", f.expected, actual)

		}
	}
}

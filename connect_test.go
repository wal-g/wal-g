package walg_test

import (
	"github.com/wal-g/wal-g"
	"testing"
)

var names = []struct {
	input    string
	expected string
	err      error
}{
	{"START WAL LOCATION: 4A/A8000028 (file 000000010000004A000000A8)" +
		"CHECKPOINT LOCATION: 4A/A8000060" +
		"BACKUP METHOD: streamed" +
		"BACKUP FROM: master" +
		"START TIME: 2017-07-24 22:54:04 UTC" +
		"LABEL: 2017-07-24 22:54:04.815749438 +0000 UTC",
		"base_000000010000004A000000A8", nil},
	{"((file 12890890G12490G))", "base_12890890G12490G", nil},
	{"", "", walg.NoMatchAvailableError{}},
	{"gewageaw", "", walg.NoMatchAvailableError{}},
}

// Tests that backup name is successfully extracted from
// return values of pg_stop_backup(false)
func TestFormatName(t *testing.T) {
	for _, n := range names {
		actual, err := walg.FormatName(n.input)
		if err != nil {
			err.Error()
		}
		if actual != n.expected && err != n.err {
			t.Errorf("connect: FormatName expected `%s` and `%v` but got `%s` and `%v`", n.expected, err, actual, n.err)

		}
	}
}

package mysql

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
)

// sids ordered so MysqlGTIDSet.String() (sorts uuids by bytes) is deterministic
const (
	sidA = "00000000-0000-0000-0000-0000000000aa"
	sidB = "00000000-0000-0000-0000-0000000000bb"
)

func TestGtidSetMinus(t *testing.T) {
	parse := func(s string) *mysql.MysqlGTIDSet {
		g, err := mysql.ParseMysqlGTIDSet(s)
		assert.NoError(t, err)
		return g.(*mysql.MysqlGTIDSet)
	}

	cases := []struct {
		name       string
		minuend    string
		subtrahend string
		want       string
	}{
		{"prefix removed", sidA + ":1-10", sidA + ":1-5", sidA + ":6-10"},
		{"hole in middle", sidA + ":1-10", sidA + ":4-6", sidA + ":1-3:7-10"},
		{"uuid fully removed", sidA + ":1-10," + sidB + ":1-5", sidA + ":1-10", sidB + ":1-5"},
		{"subtrahend uuid absent", sidA + ":1-10", sidB + ":1-5", sidA + ":1-10"},
		{"disjoint same uuid", sidA + ":1-10", sidA + ":20-30", sidA + ":1-10"},
		{"identical sets empty", sidA + ":1-10", sidA + ":1-10", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := parse(tc.minuend)
			gtidSetMinus(s, parse(tc.subtrahend))
			assert.Equal(t, tc.want, s.String())
		})
	}
}

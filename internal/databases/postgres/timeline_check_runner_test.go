package postgres

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTryFindHighestTimelineID(t *testing.T) {
	const stub = "0000000000000000"

	testCases := []struct {
		filenames []string
		expected  uint32
	}{
		{
			[]string{
				"AAAAAAAA" + stub,
				"FFFFFFFF" + stub,
				"00000000" + stub,
			},
			uint32(0xffffffff),
		},

		{
			[]string{
				"AAAAAAAA" + stub,
				"BBBBBBBB" + stub,
				"CCCCCCCC" + stub,
			},
			uint32(0xcccccccc),
		},

		{
			[]string{
				"AAAAAAAA" + stub,
				"CCCCCCCC" + stub,
				"BBBBBBBB" + stub,
			},
			uint32(0xcccccccc),
		},

		{
			[]string{
				"CCCCCCCC" + stub,
				"AAAAAAAA" + stub,
				"BBBBBBBB" + stub,
			},
			uint32(0xcccccccc),
		},

		{
			[]string{
				"XXXXXXXX" + stub,
				"5092RRRR" + stub,
			},
			uint32(0x0),
		},

		{
			[]string{
				"AAAAAAAA" + stub,
				"FFFFFFF" + stub,
				"00000000" + stub,
			},
			uint32(0xaaaaaaaa),
		},

		{
			[]string{
				"50922468" + stub,
			},
			uint32(0x50922468),
		},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
			actual := tryFindHighestTimelineID(tc.filenames)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

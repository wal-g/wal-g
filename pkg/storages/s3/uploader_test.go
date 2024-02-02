package s3

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPartitionStrings(t *testing.T) {
	testCases := []struct {
		strings   []string
		blockSize int
		expected  [][]string
	}{
		{[]string{"1", "2", "3", "4", "5"}, 2, [][]string{{"1", "2"}, {"3", "4"}, {"5"}}},
		{[]string{"1", "2", "3", "4", "5", "6"}, 3, [][]string{{"1", "2", "3"}, {"4", "5", "6"}}},
		{[]string{"1", "2", "3", "4", "5"}, 1000, [][]string{{"1", "2", "3", "4", "5"}}},
		{[]string{"1", "2", "3", "4", "5"}, 1, [][]string{{"1"}, {"2"}, {"3"}, {"4"}, {"5"}}},
		{[]string{"1", "2", "3", "4", "5"}, 0, [][]string{}},
		{[]string{"1", "2", "3", "4", "5"}, -1, [][]string{}},
		{[]string{"1", "2"}, 5, [][]string{{"1", "2"}}},
		{[]string{"1"}, 1, [][]string{{"1"}}},
		{[]string{}, 1, [][]string{}},
		{[]string{"foo", "bar", "baz"}, 4, [][]string{{"foo", "bar", "baz"}}},
		{[]string{"foo", "bar", "baz"}, 3, [][]string{{"foo", "bar", "baz"}}},
		{[]string{"foo", "bar", "baz"}, 2, [][]string{{"foo", "bar"}, {"baz"}}},
		{[]string{"foo", "bar", "baz"}, 1, [][]string{{"foo"}, {"bar"}, {"baz"}}},
		{[]string{"foo", "bar", "baz"}, 0, [][]string{}},
		{[]string{"foo", "bar", "baz"}, -1, [][]string{}},
		{
			[]string{
				"This is a long string that contains a lot of words and characters for testing purposes.",
				"The quick brown fox jumps over the lazy dog",
				"Lorem ipsum dolor sit amet, consectetur adipiscing elit",
				"Hello, World!",
				" ",
				"",
			},
			2,
			[][]string{
				{
					"This is a long string that contains a lot of words and characters for testing purposes.",
					"The quick brown fox jumps over the lazy dog",
				},
				{
					"Lorem ipsum dolor sit amet, consectetur adipiscing elit",
					"Hello, World!",
				},
				{
					" ",
					"",
				},
			},
		},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
			actual := partitionStrings(tc.strings, tc.blockSize)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

package testutils

import (
	"strconv"
	"strings"
)

func HexToBytes(hex string) []byte {
	result := make([]byte, 0)
	lastOffset := int64(0)
	currentOffset := int64(0)
	gapObserved := false
	for _, line := range strings.Split(hex, "\n") {
		line = strings.Trim(line, " \t")
		if len(line) == 0 {
			continue
		}

		if line == "*" {
			if gapObserved {
				panic("two lines of gap in hex")
			}
			gapObserved = true
			continue
		}

		runes := []rune(line)
		currentOffset, _ = strconv.ParseInt(string(runes[:8]), 16, 0)
		line = string(runes[10:58])

		if gapObserved {
			// add N rows of zeroes:
			zeroes := make([]byte, 16)
			rows := (currentOffset - (lastOffset + 16)) / 16
			for i := 0; i < int(rows); i++ {
				result = append(result, zeroes...)
			}
			gapObserved = false
		}
		lastOffset = currentOffset

		for _, num := range strings.Split(line, " ") {
			if len(num) == 0 {
				continue
			}
			// parse as int16 because FF doesn't fit signed int8
			num, err := strconv.ParseInt(num, 16, 16)
			if err != nil {
				panic(err)
			}
			result = append(result, byte(num))
		}
	}
	return result
}

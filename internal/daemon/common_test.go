package daemon

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDaemon_MessageBodyArrayConversion(t *testing.T) {
	testCases := [][]string{
		{},
		{"value"},
		{"first value", "second value", "third value"},
	}

	for i, args := range testCases {
		t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
			messageBody, err := ArgsToBytes(args...)
			assert.NoError(t, err)

			convertedArgs, err := BytesToArgs(messageBody)
			assert.NoError(t, err)

			assert.Equal(t, args, convertedArgs)
		})
	}
}

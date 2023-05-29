package st

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_validateFlags(t *testing.T) {
	tests := []struct {
		name           string
		source, target string
		concurrency    int
		wantErr        bool
	}{
		{"source empty", "", "abc", 1, true},
		{"source all", "all", "abc", 1, true},
		{"target all", "abc", "all", 1, true},
		{"same storages", "abc", "abc", 1, true},
		{"concurrency < 1", "source", "target", 0, true},
		{"valid", "source", "target", 1, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transferSourceStorage = tt.source
			targetStorage = tt.target
			transferConcurrency = tt.concurrency
			if err := validateFlags(); (err != nil) != tt.wantErr {
				t.Errorf("validateFlags() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_adjustMaxFiles(t *testing.T) {
	assert.Equal(t, math.MaxInt, adjustMaxFiles(-1))
	assert.Equal(t, 0, adjustMaxFiles(0))
	assert.Equal(t, 123, adjustMaxFiles(123))
}

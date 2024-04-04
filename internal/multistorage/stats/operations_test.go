package stats

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_scaleWeight(t *testing.T) {
	tests := []struct {
		name     string
		fileSize int64
		want     int
	}{
		{"weight 1000 for 0MB file", 0, 1000},
		{"weight 1000 for 1MB file", megaByte, 1000},
		{"weight 1301 for 2MB file", 2 * megaByte, 1301},
		{"weight 2000 for 10MB file", 10 * megaByte, 2000},
		{"weight 4000 for 1000MB file", 1000 * megaByte, 4000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want,
				int(scaleWeight(1000, sizeInMB(tt.fileSize))),
				"scaleWeight(%v, %v)", 1000, sizeInMB(tt.fileSize),
			)
		})
	}
}

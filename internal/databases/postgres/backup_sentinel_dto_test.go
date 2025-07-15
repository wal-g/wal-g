package postgres

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBackupSentinelDto_IsIncremental(t *testing.T) {
	testFromLSN := LSN(uint64(rand.Uint32())<<32 + uint64(rand.Uint32()))
	testFrom := "fromString"
	testFullName := "fullNameString"
	testCount := rand.Int()

	tests := []struct {
		name              string
		dto               BackupSentinelDto
		wantIsIncremental bool
	}{
		{
			name: "ShouldReturn_False_When_IncrementFromIsNil",
			dto: BackupSentinelDto{
				IncrementFrom: nil,
			},
			wantIsIncremental: false,
		},
		{
			name: "ShouldReturn_True_When_IncrementFromIsNotNil_And_RequiredFieldsFilled",
			dto: BackupSentinelDto{
				IncrementFromLSN:  &testFromLSN,
				IncrementFrom:     &testFrom,
				IncrementFullName: &testFullName,
				IncrementCount:    &testCount,
			},
			wantIsIncremental: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.wantIsIncremental, tt.dto.IsIncremental(), "IsIncremental()")
		})
	}
}

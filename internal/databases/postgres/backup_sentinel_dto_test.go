package postgres

import (
	"encoding/json"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestBackupSentinelDto_UnmarshalJSON_LowercaseSpec(t *testing.T) {
	// Test that lowercase "spec" (WAL-E format and new WAL-G format) is correctly parsed
	jsonData := `{
		"LSN": "0/5000000",
		"PgVersion": 160000,
		"spec": {
			"base_prefix": "/pgdata/test",
			"tablespaces": ["16451"],
			"16451": {"loc": "/pgdata/tablespace1", "link": "pg_tblspc/16451"}
		}
	}`

	var dto BackupSentinelDto
	err := json.Unmarshal([]byte(jsonData), &dto)
	require.NoError(t, err)
	require.NotNil(t, dto.TablespaceSpec, "TablespaceSpec should not be nil when using lowercase 'spec'")

	basePrefix, ok := dto.TablespaceSpec.BasePrefix()
	assert.True(t, ok)
	assert.Equal(t, "/pgdata/test", basePrefix)
	assert.Equal(t, []string{"16451"}, dto.TablespaceSpec.TablespaceNames())
}

func TestBackupSentinelDto_UnmarshalJSON_UppercaseSpec(t *testing.T) {
	// Test that uppercase "Spec" (legacy WAL-G format) is correctly parsed
	jsonData := `{
		"LSN": "0/5000000",
		"PgVersion": 160000,
		"Spec": {
			"base_prefix": "/pgdata/legacy",
			"tablespaces": ["16452"],
			"16452": {"loc": "/pgdata/tablespace2", "link": "pg_tblspc/16452"}
		}
	}`

	var dto BackupSentinelDto
	err := json.Unmarshal([]byte(jsonData), &dto)
	require.NoError(t, err)
	require.NotNil(t, dto.TablespaceSpec, "TablespaceSpec should not be nil when using uppercase 'Spec'")

	basePrefix, ok := dto.TablespaceSpec.BasePrefix()
	assert.True(t, ok)
	assert.Equal(t, "/pgdata/legacy", basePrefix)
	assert.Equal(t, []string{"16452"}, dto.TablespaceSpec.TablespaceNames())
}

func TestBackupSentinelDto_UnmarshalJSON_NoSpec(t *testing.T) {
	// Test that missing spec field results in nil TablespaceSpec (not an error)
	jsonData := `{
		"LSN": "0/5000000",
		"PgVersion": 160000
	}`

	var dto BackupSentinelDto
	err := json.Unmarshal([]byte(jsonData), &dto)
	require.NoError(t, err)
	assert.Nil(t, dto.TablespaceSpec, "TablespaceSpec should be nil when spec is missing")
}

func TestBackupSentinelDto_MarshalJSON_OutputsUppercase(t *testing.T) {
	// Test that marshaling outputs uppercase "Spec" for backward compatibility with existing WAL-G user scripts
	spec := NewTablespaceSpec("/pgdata/test")
	spec.addTablespace("16451", "/pgdata/tablespace1")

	dto := BackupSentinelDto{
		PgVersion:      160000,
		TablespaceSpec: &spec,
	}

	data, err := json.Marshal(dto)
	require.NoError(t, err)

	// Verify the output contains uppercase "Spec" (WAL-G format) for backward compatibility
	assert.Contains(t, string(data), `"Spec"`)
}

package greenplum

import (
	"testing"

	"github.com/blang/semver"
	"github.com/stretchr/testify/assert"
)

func TestParseGreenplumVersion(t *testing.T) {
	var tests = []struct {
		name   string
		input  string
		result Version
	}{
		{
			name:   "greenplum 6.25 instance",
			input:  "PostgreSQL 9.4.26 (Greenplum Database 6.25.3-mdb+yezzey+yagpcc-r+dev.40.gf0f10f9335 build dev-oss) on x86_64-pc-linux-gnu, compiled by gcc-11 (Ubuntu 11.4.0-1ubuntu1~22.04) 11.4.0, 64-bit compiled on Jul 24 2024 13:29:56",
			result: NewVersion(semver.MustParse("6.25.3"), Greenplum),
		},
		{
			name:   "cloudberry 1.6.0",
			input:  "PostgreSQL 14.4 (Cloudberry Database 1.6.0 build dev) on x86_64-pc-linux-gnu, compiled by gcc (Ubuntu 11.4.0-1ubuntu1~22.04) 11.4.0, 64-bit compiled on Sep 13 2024 07:33:38",
			result: NewVersion(semver.MustParse("1.6.0"), Cloudberry),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			version, err := parseGreenplumVersion(tt.input)
			assert.NoError(t, err)
			assert.Equalf(t, tt.result, version, tt.name)
		})
	}
}

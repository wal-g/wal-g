package greenplum

import (
	"testing"

	"github.com/apache/cloudberry-go-libs/dbconn"
	"github.com/stretchr/testify/assert"
)

func TestParseGreenplumVersion(t *testing.T) {
	var tests = []struct {
		name   string
		input  string
		semVer string
		dbType dbconn.DBType
	}{
		{
			name:   "greenplum 6.25 instance",
			input:  "PostgreSQL 9.4.26 (Greenplum Database 6.25.3-mdb+yezzey+yagpcc-r+dev.40.gf0f10f9335 build dev-oss) on x86_64-pc-linux-gnu, compiled by gcc-11 (Ubuntu 11.4.0-1ubuntu1~22.04) 11.4.0, 64-bit compiled on Jul 24 2024 13:29:56",
			semVer: "6.25.3",
			dbType: dbconn.GPDB,
		},
		{
			name:   "greengage 6.27 instance",
			input:  "PostgreSQL 9.4.26 (Greengage Database 6.27.0 build commit:0123456789abcdef) on x86_64-pc-linux-gnu, compiled by gcc-11 (Ubuntu 11.4.0-1ubuntu1~22.04) 11.4.0, 64-bit",
			semVer: "6.27.0",
			dbType: dbconn.GPDB,
		},
		{
			name:   "cloudberry 1.6.0",
			input:  "PostgreSQL 14.4 (Cloudberry Database 1.6.0 build dev) on x86_64-pc-linux-gnu, compiled by gcc (Ubuntu 11.4.0-1ubuntu1~22.04) 11.4.0, 64-bit compiled on Sep 13 2024 07:33:38",
			semVer: "1.6.0",
			dbType: dbconn.CBDB,
		},
		{
			name:   "apache cloudberry dev",
			input:  "PostgreSQL 14.4 (Apache Cloudberry 1.0.0+00da831 build dev) on x86_64-pc-linux-gnu, compiled by gcc (Ubuntu 9.4.0-1ubuntu1~20.04.2) 9.4.0, 64-bit compiled on Feb 25 2025 10:24:41",
			semVer: "1.0.0",
			dbType: dbconn.CBDB,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := ParseVersionInfo(tt.input)
			assert.Equalf(t, tt.semVer, v.SemVer.String(), tt.name)
			assert.Equalf(t, tt.dbType, v.Type, tt.name)
		})
	}
}

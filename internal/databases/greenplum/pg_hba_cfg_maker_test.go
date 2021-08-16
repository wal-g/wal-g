package greenplum_test

import (
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
)

func TestGeneratePgHbaConf(t *testing.T) {
	segments := map[int][]*cluster.SegConfig{
		-1: {{
			DbID:      1,
			ContentID: -1,
			Role:      "p",
			Port:      5432,
			Hostname:  "mdw",
			DataDir:   "/path/to/gpseg-1",
		}},
		0: {{
			DbID:      2,
			ContentID: 0,
			Role:      "p",
			Port:      6000,
			Hostname:  "sdw1",
			DataDir:   "/path/to/gpseg0",
		}},
		1: {{
			DbID:      3,
			ContentID: 1,
			Role:      "p",
			Port:      6001,
			Hostname:  "sdw2",
			DataDir:   "/path/to/gpseg1",
		}},
	}
	expectedOutput := greenplum.PgHbaTemplate + `
host    all             all             mdw              trust
host    all             gpadmin         sdw1              trust
host    all             gpadmin         sdw2              trust
host    replication     gpadmin         samehost                trust
host    replication     gpadmin         sdw1              trust
host    replication     gpadmin         sdw2              trust`

	pgHbaMaker := greenplum.NewPgHbaMaker(segments)
	output, err := pgHbaMaker.Make()
	assert.NoError(t, err)

	assert.Equal(t, expectedOutput, output, "generated pg_hba.conf does not match the expected one")
}

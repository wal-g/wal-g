package greenplum

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
)

const PgHbaTemplate = `# TYPE  DATABASE        USER            ADDRESS                 METHOD

# "local" is for Unix domain socket connections only
local   all             all                                     trust
# IPv4 local connections:
host    all             all             127.0.0.1/24            trust
# IPv6 local connections:
host    all             all             ::1/128                 trust
# Allow replication connections from localhost, by a user with the
# replication privilege.
#local   replication     gpadmin                                trust
#host    replication     gpadmin        127.0.0.1/32            trust
#host    replication     gpadmin        ::1/128                 trust
host    all             all             localhost               trust
`

func NewPgHbaMaker(segments map[int][]*cluster.SegConfig) PgHbaMaker {
	return PgHbaMaker{segments: segments}
}

type PgHbaMaker struct {
	segments map[int][]*cluster.SegConfig
}

func (m PgHbaMaker) Make() (string, error) {
	pgHbaRows := []string{PgHbaTemplate}

	masters, ok := m.segments[-1]
	if !ok {
		return "", errors.New("failed to make pg_hba: no master segment exists")
	}

	// add entries for both mdw and mdws hosts
	for _, cfg := range masters {
		row := fmt.Sprintf("host    all             all             %s              trust", cfg.Hostname)
		pgHbaRows = append(pgHbaRows, row)
	}

	writtenHosts := make(map[string]bool)
	// add entries for sdwN primary segments (w/o mdw/mdws hosts)
	for _, primary := range m.primarySegments() {
		if writtenHosts[primary.Hostname] {
			continue // do not write duplicate entries
		}
		row := fmt.Sprintf("host    all             gpadmin         %s              trust", primary.Hostname)
		writtenHosts[primary.Hostname] = true
		pgHbaRows = append(pgHbaRows, row)
	}

	// add entries for replication
	pgHbaRows = append(pgHbaRows, "host    replication     gpadmin         samehost                trust")

	writtenHosts = make(map[string]bool)
	// add replication entries for sdwN primary segments (w/o mdw/mdws hosts)
	for _, primary := range m.primarySegments() {
		if writtenHosts[primary.Hostname] {
			continue // do not write duplicate entries
		}
		row := fmt.Sprintf("host    replication     gpadmin         %s              trust", primary.Hostname)
		writtenHosts[primary.Hostname] = true
		pgHbaRows = append(pgHbaRows, row)
	}

	return strings.Join(pgHbaRows, "\n"), nil
}

// Return primary sdwN segments ordered by contentID
func (m PgHbaMaker) primarySegments() []*cluster.SegConfig {
	primarySegments := make([]*cluster.SegConfig, 0)
	for _, configs := range m.segments {
		for _, config := range configs {
			if config.ContentID == -1 {
				break // we are not interested in mdw/mdws segments
			}

			if SegmentRole(config.Role) == Primary {
				primarySegments = append(primarySegments, config)
			}
		}
	}

	sort.Slice(primarySegments, func(i, j int) bool { return primarySegments[i].ContentID < primarySegments[j].ContentID })
	return primarySegments
}

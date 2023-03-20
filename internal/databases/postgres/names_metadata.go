package postgres

import (
	"fmt"
)

type PathsByNamesMetadata map[string][]string

// TODO : improve that
func (meta PathsByNamesMetadata) processDatabaseInfos(infos []PgDatabaseInfo) {
	for _, info := range infos {
		// TODO : check that it is in default tablespace
		meta[info.Name] = append(meta[info.Name], fmt.Sprintf("/%s/%d/*", DefaultTablespace, info.Oid))
	}
}

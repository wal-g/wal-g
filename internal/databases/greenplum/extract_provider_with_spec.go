package greenplum

import (
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

func NewExtractProviderDBSpec(onlyDatabases []string) *postgres.ExtractProviderDBSpec {
	extractor := postgres.NewExtractProviderDBSpec(onlyDatabases)
	extractor.ExtractProviderImpl = ExtractProviderImpl{}
	return extractor
}

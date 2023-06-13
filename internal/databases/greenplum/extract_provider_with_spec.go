package greenplum

import (
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

func NewExtractProviderDBSpec(partialRestoreArgs []string) *postgres.ExtractProviderDBSpec {
	extractor := postgres.NewExtractProviderDBSpec(partialRestoreArgs)
	extractor.ExtractProviderImpl = ExtractProviderImpl{}
	return extractor
}

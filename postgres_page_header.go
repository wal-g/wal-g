package walg

import (
	"io"
	"github.com/wal-g/wal-g/walparser/parsingutil"
)

type PostgresPageHeader struct {
	pdLsnH            uint32
	pdLsnL            uint32
	pdChecksum        uint16
	pdFlags           uint16
	pdLower           uint16
	pdUpper           uint16
	pdSpecial         uint16
	pdPageSizeVersion uint16
}

func (header *PostgresPageHeader) Lsn() uint64 {
	return ((uint64(header.pdLsnH)) << 32) + uint64(header.pdLsnL)
}

func (header *PostgresPageHeader) IsValid() bool {
	return !((header.pdFlags&validFlags) != header.pdFlags ||
		header.pdLower < headerSize ||
		header.pdLower > header.pdUpper ||
		header.pdUpper > header.pdSpecial ||
		header.pdSpecial > WalPageSize ||
		(header.Lsn() == invalidLsn) ||
		header.pdPageSizeVersion != WalPageSize+layoutVersion)
}

// ParsePostgresPageHeader reads information from PostgreSQL page header. Exported for test reasons.
func ParsePostgresPageHeader(reader io.Reader) (*PostgresPageHeader, error) {
	pageHeader := PostgresPageHeader{}
	fields := []parsingutil.FieldToParse{
		*parsingutil.NewFieldToParse(&pageHeader.pdLsnH, "pdLsnH"),
		*parsingutil.NewFieldToParse(&pageHeader.pdLsnL, "pdLsnL"),
		*parsingutil.NewFieldToParse(&pageHeader.pdChecksum, "pdChecksum"),
		*parsingutil.NewFieldToParse(&pageHeader.pdFlags, "pdFlags"),
		*parsingutil.NewFieldToParse(&pageHeader.pdLower, "pdLower"),
		*parsingutil.NewFieldToParse(&pageHeader.pdUpper, "pdUpper"),
		*parsingutil.NewFieldToParse(&pageHeader.pdSpecial, "pdSpecial"),
		*parsingutil.NewFieldToParse(&pageHeader.pdPageSizeVersion, "pdPageSizeVersion"),
	}
	err := parsingutil.ParseMultipleFieldsFromReader(fields, reader)
	if err != nil {
		return nil, err
	}
	return &pageHeader, nil
}

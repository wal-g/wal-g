package walg

import (
	"github.com/wal-g/wal-g/walparser/parsingutil"
	"io"
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

// TODO : unit tests
func (header *PostgresPageHeader) IsValid() bool {
	return !((header.pdFlags&validFlags) != header.pdFlags ||
		header.pdLower < headerSize ||
		header.pdLower > header.pdUpper ||
		header.pdUpper > header.pdSpecial ||
		header.pdSpecial > DatabasePageSize ||
		(header.Lsn() == invalidLsn) ||
		header.pdPageSizeVersion != DatabasePageSize+layoutVersion)
}

// TODO : unit tests
// ParsePostgresPageHeader reads information from PostgreSQL page header. Exported for test reasons.
func ParsePostgresPageHeader(reader io.Reader) (*PostgresPageHeader, error) {
	pageHeader := PostgresPageHeader{}
	fields := []parsingutil.FieldToParse{
		{Field: &pageHeader.pdLsnH, Name: "pdLsnH"},
		{Field: &pageHeader.pdLsnL, Name: "pdLsnL"},
		{Field: &pageHeader.pdChecksum, Name: "pdChecksum"},
		{Field: &pageHeader.pdFlags, Name: "pdFlags"},
		{Field: &pageHeader.pdLower, Name: "pdLower"},
		{Field: &pageHeader.pdUpper, Name: "pdUpper"},
		{Field: &pageHeader.pdSpecial, Name: "pdSpecial"},
		{Field: &pageHeader.pdPageSizeVersion, Name: "pdPageSizeVersion"},
	}
	err := parsingutil.ParseMultipleFieldsFromReader(fields, reader)
	if err != nil {
		return nil, err
	}
	return &pageHeader, nil
}

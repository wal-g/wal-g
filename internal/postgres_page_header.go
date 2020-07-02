package internal

import (
	"io"

	"github.com/wal-g/wal-g/internal/walparser/parsingutil"
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

func (header *PostgresPageHeader) lsn() uint64 {
	return ((uint64(header.pdLsnH)) << 32) + uint64(header.pdLsnL)
}

func (header *PostgresPageHeader) isValid() bool {
	return !((header.pdFlags&validFlags) != header.pdFlags ||
		header.pdLower < headerSize ||
		header.pdLower > header.pdUpper ||
		header.pdUpper > header.pdSpecial ||
		int64(header.pdSpecial) > DatabasePageSize ||
		(header.lsn() == invalidLsn) ||
		int64(header.pdPageSizeVersion) != DatabasePageSize+layoutVersion)
}

func (header *PostgresPageHeader) isNew() bool {
	return header.pdUpper == 0 // #define PageIsNew(page) (((PageHeader) (page))->pd_upper == 0) in bufpage.h
}

// ParsePostgresPageHeader reads information from PostgreSQL page header. Exported for test reasons.
func parsePostgresPageHeader(reader io.Reader) (*PostgresPageHeader, error) {
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

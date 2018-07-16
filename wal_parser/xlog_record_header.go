package wal_parser

import "fmt"

const (
	// info flags

	XlrInfoMask     = 0x0F
	XlrRmgrInfoMask = 0xF0

	XlrSpecialRelUpdate  = 0x01
	XlrCheckConsistency  = 0x02
	XLogRecordHeaderSize = 24
)

type InconsistentXLogRecordTotalLengthError struct {
	totalRecordLength uint32
}

func (err InconsistentXLogRecordTotalLengthError) Error() string {
	return fmt.Sprintf("total record length is too small: %v, expected at least: %v", err.totalRecordLength, XLogRecordHeaderSize)
}

type InvalidXLogRecordResourceManagerIDError struct {
	resourceManagerID uint8
}

func (err InvalidXLogRecordResourceManagerIDError) Error() string {
	return fmt.Sprintf("resource manager id is invalid: %v, while it should be less then: %v", err.resourceManagerID, RmNextFreeID)
}

/* This struct corresponds to postgres struct XLogRecord.
 * For clarification you can find it in postgres:
 * src/include/access/xlogrecord.h
 */
type XLogRecordHeader struct {
	totalRecordLength uint32
	xactID            uint32
	prevRecordPtr     XLogRecordPtr
	info              uint8
	resourceManagerID uint8
	/* 2 bytes of padding here, initialize to zero */
	crc32Hash uint32
	/* XLogRecordBlockHeaders and XLogRecordDataHeader follow, no padding */
}

func (header *XLogRecordHeader) checkTotalRecordLengthConsistency() error {
	if header.totalRecordLength < XLogRecordHeaderSize {
		return InconsistentXLogRecordTotalLengthError{header.totalRecordLength}
	}
	return nil
}

func (header *XLogRecordHeader) checkResourceManagerIDValidity() error {
	if header.resourceManagerID >= RmNextFreeID {
		return InvalidXLogRecordResourceManagerIDError{header.resourceManagerID}
	}
	return nil
}

func (header *XLogRecordHeader) checkConsistency() error {
	err := header.checkTotalRecordLengthConsistency()
	if err != nil {
		return err
	}
	return header.checkResourceManagerIDValidity()
}

package postgres

import (
	bytes2 "bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParsePgControlData_IncorrectPgControlSize(t *testing.T) {
	bytes := make([]byte, pgControlSize-1)
	_, err := parsePgControlData(bytes2.NewReader(bytes))
	assert.Error(t, err)
}

// In pg_control versions 1099 and below timeline was located
// on different place, compared to the new versions:
// https://github.com/postgres/postgres/blob/REL_10_21/src/include/catalog/pg_control.h
func TestParsePgControlData_OldVersion(t *testing.T) {
	bytesContainsSystemId := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytesContainsSystemId, 9876)
	bytesContainsVersion := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytesContainsVersion, 1099)
	bytesContainsTimeline := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytesContainsTimeline, 7)

	bytes := bytesContainsSystemId
	bytes = append(bytes, bytesContainsVersion...)
	bytes = append(bytes, make([]byte, 44)...)
	bytes = append(bytes, bytesContainsTimeline...)
	bytes = append(bytes, make([]byte, pgControlSize-8-4-44-4)...)

	pgControlData, err := parsePgControlData(bytes2.NewReader(bytes))
	assert.Nil(t, err)
	assert.Equal(t, uint64(9876), pgControlData.GetSystemIdentifier())
	assert.Equal(t, uint32(7), pgControlData.GetCurrentTimeline())
}

// In pg_control versions 1100 and higher timeline was located
// on different place, compared to the old versions:
// https://github.com/postgres/postgres/blob/REL_11_0/src/include/catalog/pg_control.h
func TestParsePgControlData_NewVersion(t *testing.T) {
	bytesContainsSystemId := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytesContainsSystemId, 9876)
	bytesContainsVersion := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytesContainsVersion, 1100)
	bytesContainsTimeline := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytesContainsTimeline, 7)

	bytes := bytesContainsSystemId
	bytes = append(bytes, bytesContainsVersion...)
	bytes = append(bytes, make([]byte, 36)...)
	bytes = append(bytes, bytesContainsTimeline...)
	bytes = append(bytes, make([]byte, pgControlSize-8-4-36-4)...)

	pgControlData, err := parsePgControlData(bytes2.NewReader(bytes))
	assert.Nil(t, err)
	assert.Equal(t, uint64(9876), pgControlData.GetSystemIdentifier())
	assert.Equal(t, uint32(7), pgControlData.GetCurrentTimeline())
}

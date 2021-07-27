package internal

import (
	"encoding/binary"
	"github.com/wal-g/storages/storage"
	"io"
)

// PgControlData represents data contained in pg_control file
type PgControlData struct {
	systemIdentifier uint64 // systemIdentifier represents system ID of PG cluster ([0-7] bytes in pg_control)
	currentTimeline  uint32 // currentTimeline represents current timeline of PG cluster ([48-51] bytes in pg_control)
	// Any data from pg_control
}

// ExtractPgControl extract pg_control data of cluster by storage
func ExtractPgControl(folder storage.Folder) (*PgControlData, error) {
	reader, err := folder.ReadObject(PgControlPath)
	if err != nil {
		return nil, err
	}
	return extractPgControlData(reader)
}

func extractPgControlData(reader io.Reader) (*PgControlData, error) {
	bytes := make([]byte, 0)

	n, err := reader.Read(bytes)
	if err != nil || n < 8192 {
		return nil, err
	}

	systemId := binary.LittleEndian.Uint64(bytes[0:7])
	currentTl := binary.LittleEndian.Uint32(bytes[48:51])

	// Parse bytes from pg_control file and share this data
	return &PgControlData{
		systemIdentifier: systemId,
		currentTimeline:  currentTl,
	}, nil
}

func (data *PgControlData) GetSystemIdentifier() uint64 {
	return data.systemIdentifier
}

func (data *PgControlData) GetCurrentTimeline() uint32 {
	return data.currentTimeline
}

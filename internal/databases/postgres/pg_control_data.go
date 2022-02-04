package postgres

import (
	"encoding/binary"
	"io"
	"os"
	"path"

	"github.com/wal-g/tracelog"
)

const pgControlSize = 8192

// PgControlData represents data contained in pg_control file
type PgControlData struct {
	systemIdentifier uint64 // systemIdentifier represents system ID of PG cluster (f.e. [0-8] bytes in pg_control)
	currentTimeline  uint32 // currentTimeline represents current timeline of PG cluster (f.e. [48-52] bytes in pg_control v. 1100+)
	// Any data from pg_control
}

// ExtractPgControl extract pg_control data of cluster by storage
func ExtractPgControl(folder string) (*PgControlData, error) {
	pgControlReadCloser, err := os.Open(path.Join(folder, PgControlPath))
	if err != nil {
		return nil, err
	}

	result, err := extractPgControlData(pgControlReadCloser)
	if err != nil {
		closeErr := pgControlReadCloser.Close()
		tracelog.WarningLogger.Printf("Error on closing pg_control file: %v\n", closeErr)
		return nil, err
	}

	err = pgControlReadCloser.Close()
	if err != nil {
		return nil, err
	}

	return result, nil
}

func extractPgControlData(pgControlReader io.Reader) (*PgControlData, error) {
	bytes := make([]byte, pgControlSize)

	_, err := io.ReadAtLeast(pgControlReader, bytes, pgControlSize)
	if err != nil {
		return nil, err
	}

	systemID := binary.LittleEndian.Uint64(bytes[0:8])
	pgControlVersion := binary.LittleEndian.Uint32(bytes[8:12])
	currentTimeline := uint32(0)

	if pgControlVersion < 1100 {
		currentTimeline = binary.LittleEndian.Uint32(bytes[56:60])
	} else {
		currentTimeline = binary.LittleEndian.Uint32(bytes[48:52])
	}

	// Parse bytes from pg_control file and share this data
	return &PgControlData{
		systemIdentifier: systemID,
		currentTimeline:  currentTimeline,
	}, nil
}

func (data *PgControlData) GetSystemIdentifier() uint64 {
	return data.systemIdentifier
}

func (data *PgControlData) GetCurrentTimeline() uint32 {
	return data.currentTimeline
}

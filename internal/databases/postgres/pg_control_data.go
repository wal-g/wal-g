package postgres

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/wal-g/wal-g/utility"
)

const pgControlSize = 8192

// PgControlData represents data contained in pg_control file
type PgControlData struct {
	systemIdentifier uint64 // systemIdentifier represents system ID of PG cluster (e.g. [0-8] bytes in pg_control)
	currentTimeline  uint32 // currentTimeline represents current timeline of PG cluster (e.g. [48-52] bytes in pg_control v. 1100+)
	// Any data from pg_control
}

// SshRequisites using to decrease passed params TODO: When will use by other functions move to more suitable place
type SshRequisites struct {
	Host string
	Port string

	Username       string
	Password       string
	PrivateKeyPath string
}

// ExtractPgControl extracts pg_control data of cluster
func ExtractPgControl(folder string) (*PgControlData, error) {
	pgControlFile, err := os.Open(path.Join(folder, PgControlPath))
	if err != nil {
		return nil, fmt.Errorf("failed to open pg_control file: %s", err)
	}
	defer pgControlFile.Close()

	result, err := parsePgControlData(pgControlFile)
	if err != nil {
		return nil, fmt.Errorf("failed to parse pg_control data: %s", err)
	}

	return result, nil
}

// ExtractRemotePgControl extracts pg_control data of remote cluster. Requisites should be set in config
func ExtractRemotePgControl(folder string, requisites SshRequisites) (*PgControlData, error) {
	sftpClient, err := utility.NewSftpClient(requisites.Host, requisites.Port, requisites.Username, requisites.Password, requisites.PrivateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create sftp client: %s", err)
	}
	defer sftpClient.Close()

	pgControlFile, err := sftpClient.Open(path.Join(folder, PgControlPath))
	if err != nil {
		return nil, fmt.Errorf("failed to open pg_control file: %s", err)
	}
	defer pgControlFile.Close()

	result, err := parsePgControlData(pgControlFile)
	if err != nil {
		return nil, fmt.Errorf("failed to parse pg_control data: %s", err)
	}

	return result, nil
}

func parsePgControlData(pgControlReader io.Reader) (*PgControlData, error) {
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

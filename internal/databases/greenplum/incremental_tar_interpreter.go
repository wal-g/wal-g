package greenplum

import (
	"archive/tar"
	"io"
	"path"

	"github.com/spf13/viper"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/greenplum/ao"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

func NewIncrementalTarInterpreter(dbDataDirectory string, sentinel postgres.BackupSentinelDto, filesMetadata postgres.FilesMetadataDto,
	aoFilesMetadata ao.FilesMetadataDTO,
	filesToUnwrap map[string]bool, createNewIncrementalFiles bool) *IncrementalTarInterpreter {
	return &IncrementalTarInterpreter{
		FileTarInterpreter: postgres.NewFileTarInterpreter(dbDataDirectory, sentinel, filesMetadata, filesToUnwrap, createNewIncrementalFiles),
		fsync:              !viper.GetBool(conf.TarDisableFsyncSetting),
		aoFilesMetadata:    aoFilesMetadata,
	}
}

type IncrementalTarInterpreter struct {
	*postgres.FileTarInterpreter
	fsync           bool
	aoFilesMetadata ao.FilesMetadataDTO
}

func (i *IncrementalTarInterpreter) Interpret(reader io.Reader, header *tar.Header) error {
	aoMeta, ok := i.aoFilesMetadata.Files[header.Name]
	if !ok || !aoMeta.IsIncremented {
		return i.FileTarInterpreter.Interpret(reader, header)
	}

	targetPath := path.Join(i.DBDataDirectory, header.Name)
	return ao.ApplyFileIncrement(targetPath, reader, i.fsync)
}

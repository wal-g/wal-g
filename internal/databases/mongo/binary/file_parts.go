package binary

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
)

const (
	filePartOffsetPAX = "WAL-G.MongoDB.FilePartOffset"
	fileSizePAX       = "WAL-G.MongoDB.FileSize"
)

type restoredFileParts struct {
	completed map[int64]bool
	mode      int64
}

type FilePartsTarInterpreter struct {
	directory string
	regular   internal.TarInterpreter
	expected  map[string]models.FilePartsInfo
	fsync     bool
	mutex     sync.Mutex
	files     map[string]*restoredFileParts
}

func NewFilePartsTarInterpreter(directory string, expected map[string]models.FilePartsInfo) *FilePartsTarInterpreter {
	return &FilePartsTarInterpreter{
		directory: directory,
		regular:   internal.NewFileTarInterpreter(directory),
		expected:  expected,
		fsync:     !viper.GetBool(conf.TarDisableFsyncSetting),
		files:     make(map[string]*restoredFileParts),
	}
}

func (i *FilePartsTarInterpreter) Interpret(reader io.Reader, header *tar.Header) error {
	offsetValue, hasOffset := header.PAXRecords[filePartOffsetPAX]
	sizeValue, hasSize := header.PAXRecords[fileSizePAX]
	if !hasOffset && !hasSize {
		if _, split := i.expected[header.Name]; split {
			return errors.Errorf("MongoDB split file %s is missing part metadata", header.Name)
		}
		return i.regular.Interpret(reader, header)
	}
	if !hasOffset || !hasSize || (header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA) {
		return errors.Errorf("invalid MongoDB file part header for %s", header.Name)
	}
	offset, err := strconv.ParseInt(offsetValue, 10, 64)
	if err != nil {
		return errors.Wrapf(err, "invalid MongoDB file part offset for %s", header.Name)
	}
	size, err := strconv.ParseInt(sizeValue, 10, 64)
	if err != nil {
		return errors.Wrapf(err, "invalid MongoDB file size for %s", header.Name)
	}
	expected, ok := i.expected[header.Name]
	if !ok {
		return errors.Errorf("MongoDB file part %s is absent from the backup manifest", header.Name)
	}
	if expected.Size <= 0 || expected.PartSize <= 0 || size != expected.Size ||
		offset < 0 || offset >= size || offset%expected.PartSize != 0 ||
		header.Size != min(expected.PartSize, size-offset) {
		return errors.Errorf("invalid MongoDB file part range for %s: offset %d, length %d, file size %d",
			header.Name, offset, header.Size, size)
	}
	relative := strings.TrimPrefix(header.Name, "/")
	if !filepath.IsLocal(relative) {
		return errors.Errorf("invalid MongoDB file part path %s", header.Name)
	}
	target := filepath.Join(i.directory, relative)
	if err := i.prepareFile(target, header, size); err != nil {
		return err
	}

	i.mutex.Lock()
	delete(i.files[header.Name].completed, offset)
	i.mutex.Unlock()
	file, err := os.OpenFile(target, os.O_WRONLY, 0)
	if err != nil {
		return errors.Wrapf(err, "open MongoDB file part target %s", target)
	}
	_, copyErr := io.CopyN(io.NewOffsetWriter(file, offset), reader, header.Size)
	if copyErr == nil && i.fsync {
		copyErr = file.Sync()
	}
	closeErr := file.Close()
	if copyErr != nil {
		return errors.Wrapf(copyErr, "restore MongoDB file %s part at offset %d", header.Name, offset)
	}
	if closeErr != nil {
		return errors.Wrapf(closeErr, "close MongoDB file part target %s", target)
	}
	i.mutex.Lock()
	i.files[header.Name].completed[offset] = true
	i.mutex.Unlock()
	return nil
}

func (i *FilePartsTarInterpreter) prepareFile(target string, header *tar.Header, size int64) error {
	i.mutex.Lock()
	defer i.mutex.Unlock()
	if state, ok := i.files[header.Name]; ok {
		if state.mode != header.Mode {
			return errors.Errorf("inconsistent MongoDB file part mode for %s", header.Name)
		}
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		return errors.Wrap(err, "create MongoDB file part parent directory")
	}
	file, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY, os.FileMode(header.Mode)|0200)
	if err != nil {
		return errors.Wrapf(err, "create MongoDB split file %s", target)
	}
	truncateErr := file.Truncate(size)
	if truncateErr == nil {
		truncateErr = file.Chmod(os.FileMode(header.Mode) | 0200)
	}
	closeErr := file.Close()
	if truncateErr != nil {
		return errors.Wrapf(truncateErr, "initialize MongoDB split file %s", target)
	}
	if closeErr != nil {
		return errors.Wrapf(closeErr, "close MongoDB split file %s", target)
	}
	i.files[header.Name] = &restoredFileParts{
		completed: make(map[int64]bool),
		mode:      header.Mode,
	}
	return nil
}

func (i *FilePartsTarInterpreter) Validate() error {
	i.mutex.Lock()
	defer i.mutex.Unlock()
	for name, expected := range i.expected {
		if expected.Size <= 0 || expected.PartSize <= 0 {
			return errors.Errorf("invalid MongoDB split file manifest for %s", name)
		}
		state, ok := i.files[name]
		if !ok {
			return errors.Errorf("MongoDB split file %s is missing from the backup", name)
		}
		for offset := int64(0); offset < expected.Size; {
			if !state.completed[offset] {
				return errors.Errorf("MongoDB file %s is missing a part at offset %d", name, offset)
			}
			offset += min(expected.PartSize, expected.Size-offset)
		}
		info, err := os.Stat(filepath.Join(i.directory, strings.TrimPrefix(name, "/")))
		if err != nil {
			return errors.Wrapf(err, "check restored MongoDB split file %s", name)
		}
		if info.Size() != expected.Size {
			return fmt.Errorf("restored MongoDB split file %s has size %d, expected %d", name, info.Size(), expected.Size)
		}
	}
	for name, state := range i.files {
		target := filepath.Join(i.directory, strings.TrimPrefix(name, "/"))
		file, err := os.OpenFile(target, os.O_WRONLY, 0)
		if err != nil {
			return errors.Wrapf(err, "open restored MongoDB split file %s", name)
		}
		err = file.Chmod(os.FileMode(state.mode))
		if err == nil && i.fsync {
			err = file.Sync()
		}
		closeErr := file.Close()
		if err != nil {
			return errors.Wrapf(err, "restore MongoDB split file permissions for %s", name)
		}
		if closeErr != nil {
			return errors.Wrapf(closeErr, "close restored MongoDB split file %s", name)
		}
	}
	return nil
}

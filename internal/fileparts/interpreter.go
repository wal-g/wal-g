package fileparts

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
)

type restoredFileParts struct {
	completed map[int64]bool
	mode      int64
}

type TarInterpreter struct {
	directory string
	regular   internal.TarInterpreter
	expected  *Info
	fsync     bool
	mutex     sync.Mutex
	files     map[string]*restoredFileParts
}

func NewTarInterpreter(directory string, regular internal.TarInterpreter, expected *Info) *TarInterpreter {
	return &TarInterpreter{
		directory: directory,
		regular:   regular,
		expected:  expected,
		fsync:     !viper.GetBool(conf.TarDisableFsyncSetting),
		files:     make(map[string]*restoredFileParts),
	}
}

func (interpreter *TarInterpreter) Interpret(reader io.Reader, header *tar.Header) error {
	offsetValue, hasOffset := header.PAXRecords[OffsetPAX]
	sizeValue, hasSize := header.PAXRecords[FileSizePAX]
	if !hasOffset && !hasSize {
		if interpreter.expected != nil {
			if _, split := interpreter.expected.Files[header.Name]; split {
				return errors.Errorf("split file %s is missing part metadata", header.Name)
			}
		}
		return interpreter.regular.Interpret(reader, header)
	}
	if !hasOffset || !hasSize || (header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA) {
		return errors.Errorf("invalid file part header for %s", header.Name)
	}
	offset, err := strconv.ParseInt(offsetValue, 10, 64)
	if err != nil {
		return errors.Wrapf(err, "invalid file part offset for %s", header.Name)
	}
	size, err := strconv.ParseInt(sizeValue, 10, 64)
	if err != nil {
		return errors.Wrapf(err, "invalid file size for %s", header.Name)
	}
	if err := interpreter.validatePartRange(header, offset, size); err != nil {
		return err
	}
	relative := strings.TrimPrefix(header.Name, "/")
	if !filepath.IsLocal(relative) {
		return errors.Errorf("invalid file part path %s", header.Name)
	}
	target := filepath.Join(interpreter.directory, relative)
	return interpreter.writeFilePart(reader, header, target, offset, size)
}

func (interpreter *TarInterpreter) validatePartRange(header *tar.Header, offset, size int64) error {
	if interpreter.expected == nil {
		return errors.Errorf("file part %s is absent from the backup manifest", header.Name)
	}
	expectedSize, ok := interpreter.expected.Files[header.Name]
	if !ok {
		return errors.Errorf("file part %s is absent from the backup manifest", header.Name)
	}
	if expectedSize <= 0 || interpreter.expected.PartSize <= 0 || size != expectedSize ||
		offset < 0 || offset >= size || offset%interpreter.expected.PartSize != 0 ||
		header.Size != min(interpreter.expected.PartSize, size-offset) {
		return errors.Errorf("invalid file part range for %s: offset %d, length %d, file size %d",
			header.Name, offset, header.Size, size)
	}
	return nil
}

func (interpreter *TarInterpreter) writeFilePart(reader io.Reader, header *tar.Header,
	target string, offset, size int64,
) error {
	if err := interpreter.prepareFile(target, header, size); err != nil {
		return err
	}

	interpreter.mutex.Lock()
	delete(interpreter.files[header.Name].completed, offset)
	interpreter.mutex.Unlock()
	file, err := os.OpenFile(target, os.O_WRONLY, 0)
	if err != nil {
		return errors.Wrapf(err, "open file part target %s", target)
	}
	_, copyErr := io.CopyN(io.NewOffsetWriter(file, offset), reader, header.Size)
	if copyErr == nil && interpreter.fsync {
		copyErr = file.Sync()
	}
	closeErr := file.Close()
	if copyErr != nil {
		return errors.Wrapf(copyErr, "restore file %s part at offset %d", header.Name, offset)
	}
	if closeErr != nil {
		return errors.Wrapf(closeErr, "close file part target %s", target)
	}
	interpreter.mutex.Lock()
	interpreter.files[header.Name].completed[offset] = true
	interpreter.mutex.Unlock()
	return nil
}

func (interpreter *TarInterpreter) prepareFile(target string, header *tar.Header, size int64) error {
	interpreter.mutex.Lock()
	defer interpreter.mutex.Unlock()
	if state, ok := interpreter.files[header.Name]; ok {
		if state.mode != header.Mode {
			return errors.Errorf("inconsistent file part mode for %s", header.Name)
		}
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		return errors.Wrap(err, "create file part parent directory")
	}
	file, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY, os.FileMode(header.Mode)|0200)
	if err != nil {
		return errors.Wrapf(err, "create split file %s", target)
	}
	truncateErr := file.Truncate(size)
	if truncateErr == nil {
		truncateErr = file.Chmod(os.FileMode(header.Mode) | 0200)
	}
	closeErr := file.Close()
	if truncateErr != nil {
		return errors.Wrapf(truncateErr, "initialize split file %s", target)
	}
	if closeErr != nil {
		return errors.Wrapf(closeErr, "close split file %s", target)
	}
	interpreter.files[header.Name] = &restoredFileParts{
		completed: make(map[int64]bool),
		mode:      header.Mode,
	}
	return nil
}

func (interpreter *TarInterpreter) Finalize() error {
	return interpreter.Validate()
}

func (interpreter *TarInterpreter) Validate() error {
	interpreter.mutex.Lock()
	defer interpreter.mutex.Unlock()
	if interpreter.expected == nil {
		return nil
	}
	if interpreter.expected.PartSize <= 0 {
		return errors.New("invalid file part size in the backup manifest")
	}
	for name, expectedSize := range interpreter.expected.Files {
		if err := interpreter.validateFile(name, expectedSize); err != nil {
			return err
		}
	}
	for name, state := range interpreter.files {
		target := filepath.Join(interpreter.directory, strings.TrimPrefix(name, "/"))
		file, err := os.OpenFile(target, os.O_WRONLY, 0)
		if err != nil {
			return errors.Wrapf(err, "open restored split file %s", name)
		}
		err = file.Chmod(os.FileMode(state.mode))
		if err == nil && interpreter.fsync {
			err = file.Sync()
		}
		closeErr := file.Close()
		if err != nil {
			return errors.Wrapf(err, "restore split file permissions for %s", name)
		}
		if closeErr != nil {
			return errors.Wrapf(closeErr, "close restored split file %s", name)
		}
	}
	return nil
}

func (interpreter *TarInterpreter) validateFile(name string, expectedSize int64) error {
	if expectedSize <= 0 {
		return errors.Errorf("invalid split file manifest for %s", name)
	}
	state, ok := interpreter.files[name]
	if !ok {
		return errors.Errorf("split file %s is missing from the backup", name)
	}
	for offset := int64(0); offset < expectedSize; {
		if !state.completed[offset] {
			return errors.Errorf("file %s is missing a part at offset %d", name, offset)
		}
		offset += min(interpreter.expected.PartSize, expectedSize-offset)
	}
	info, err := os.Stat(filepath.Join(interpreter.directory, strings.TrimPrefix(name, "/")))
	if err != nil {
		return errors.Wrapf(err, "check restored split file %s", name)
	}
	if info.Size() != expectedSize {
		return fmt.Errorf("restored split file %s has size %d, expected %d", name, info.Size(), expectedSize)
	}
	return nil
}

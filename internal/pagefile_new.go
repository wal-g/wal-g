//
// This file holds new functionality for pagefile.go
//

package internal

import (
	"bytes"
	"encoding/binary"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/walparser/parsingutil"
	"io"
	"io/ioutil"
	"os"
)

type ReadWriterAt interface {
	io.ReaderAt
	io.WriterAt
	Stat() (os.FileInfo, error)
	Name() string
}

// RestoreMissingPages restores missing pages (zero blocks)
// of local file with their base backup version
func RestoreMissingPages(base io.Reader, file ReadWriterAt) error {
	tracelog.DebugLogger.Printf("Restoring missing pages from base backup: %s\n", file.Name())

	filePageCount, err := getPageCount(file)
	if err != nil {
		return err
	}
	for i := int64(0); i < filePageCount; i++ {
		err = writePage(file, i, base, false)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	// check if some extra pages left in base file reader
	if isEmpty := isTarReaderEmpty(base); !isEmpty {
		tracelog.DebugLogger.Printf("Skipping pages after end of the local file %s, " +
			"possibly the pagefile was truncated.\n", file.Name())
	}
	return nil
}

// CreateFileFromIncrement writes the pages from the increment to local file
// and write empty blocks in place of pages which are not present in the increment
func CreateFileFromIncrement(increment io.Reader, file ReadWriterAt) error {
	tracelog.DebugLogger.Printf("Generating file from increment %s\n", file.Name())

	fileSize, diffBlockCount, diffMap, err := getIncrementHeaderFields(increment)
	if err != nil {
		return err
	}

	// set represents all block numbers with non-empty pages
	deltaBlockNumbers := make(map[int64]bool, diffBlockCount)
	for i := uint32(0); i < diffBlockCount; i++ {
		blockNo := binary.LittleEndian.Uint32(diffMap[i*sizeofInt32 : (i+1)*sizeofInt32])
		deltaBlockNumbers[int64(blockNo)] = true
	}
	pageCount := int64(fileSize / uint64(DatabasePageSize))
	emptyPage := make([]byte, DatabasePageSize)
	for i := int64(0); i < pageCount; i++ {
		if deltaBlockNumbers[i] {
			err = writePage(file, i, increment, true)
			if err != nil {
				return err
			}
		} else {
			_, err = file.WriteAt(emptyPage, i*DatabasePageSize)
			if err != nil {
				return err
			}
		}
	}
	// check if some extra delta blocks left in reader
	if isEmpty := isTarReaderEmpty(increment); !isEmpty {
		tracelog.DebugLogger.Printf("Skipping extra increment blocks, file: %s\n", file.Name())
	}
	return nil
}

// WritePagesFromIncrement writes pages from delta backup according to diffMap
func WritePagesFromIncrement(increment io.Reader, file ReadWriterAt, overwriteExisting bool) error {
	tracelog.DebugLogger.Printf("Writing pages from increment: %s\n", file.Name())

	_, diffBlockCount, diffMap, err := getIncrementHeaderFields(increment)
	if err != nil {
		return err
	}
	filePageCount, err := getPageCount(file)
	if err != nil {
		return err
	}

	for i := uint32(0); i < diffBlockCount; i++ {
		blockNo := int64(binary.LittleEndian.Uint32(diffMap[i*sizeofInt32 : (i+1)*sizeofInt32]))
		if blockNo >= filePageCount {
			_, err := io.CopyN(ioutil.Discard, increment, DatabasePageSize)
			if err != nil {
				return err
			}
			continue
		}
		err = writePage(file, blockNo, increment, overwriteExisting)
		if err != nil {
			return err
		}
	}
	// at this point, we should have empty increment reader
	if isEmpty := isTarReaderEmpty(increment); !isEmpty {
		return newUnexpectedTarDataError()
	}
	return nil
}

// write page to local file
func writePage(file ReadWriterAt, blockNo int64, content io.Reader, overwrite bool) error {
	page := make([]byte, DatabasePageSize)
	_, err := io.ReadFull(content, page)
	if err != nil {
		return err
	}

	if !overwrite {
		isMissingPage, err := checkIfMissingPage(file, blockNo)
		if err != nil {
			return err
		}
		if !isMissingPage {
			return nil
		}
	}
	_, err = file.WriteAt(page, blockNo*DatabasePageSize)
	if err != nil {
		return err
	}
	return nil
}

// check if page is missing (block of zeros) in local file
func checkIfMissingPage(file ReadWriterAt, blockNo int64) (bool, error) {
	emptyPageHeader := make([]byte, headerSize)
	pageHeader := make([]byte, headerSize)
	_, err := file.ReadAt(pageHeader, blockNo*DatabasePageSize)
	if err != nil {
		return false, err
	}

	return bytes.Equal(pageHeader, emptyPageHeader), nil
}

// check that tar reader is empty
func isTarReaderEmpty(reader io.Reader) bool {
	all, _ := reader.Read(make([]byte, 1))
	return all == 0
}

func getPageCount(file ReadWriterAt) (int64, error) {
	localFileInfo, err := file.Stat()
	if err != nil {
		return 0, errors.Wrap(err, "error getting fileInfo")
	}
	return localFileInfo.Size() / DatabasePageSize, nil
}

func getIncrementHeaderFields(increment io.Reader) (uint64, uint32, []byte, error) {
	err := ReadIncrementFileHeader(increment)
	if err != nil {
		return 0, 0, nil, err
	}

	var fileSize uint64
	var diffBlockCount uint32
	err = parsingutil.ParseMultipleFieldsFromReader([]parsingutil.FieldToParse{
		{Field: &fileSize, Name: "fileSize"},
		{Field: &diffBlockCount, Name: "diffBlockCount"},
	}, increment)
	if err != nil {
		return 0, 0, nil, err
	}

	diffMap := make([]byte, diffBlockCount*sizeofInt32)

	_, err = io.ReadFull(increment, diffMap)
	if err != nil {
		return 0, 0, nil, err
	}
	return fileSize, diffBlockCount, diffMap, nil
}
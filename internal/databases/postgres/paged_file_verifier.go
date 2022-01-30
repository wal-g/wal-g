package postgres

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
	"unsafe"

	"github.com/wal-g/tracelog"
)

// This code is an adaptation of Postgres data page checksum calculation code written in Go.
// It uses a modified version of FNV-1a hash algorithm.
// Full description of the hashing algorithm and original code can be found in origin.
// Origin: https://github.com/postgres/postgres/blob/REL_12_STABLE/src/include/storage/checksum_impl.h

const (
	// number of checksums to calculate in parallel
	NSums int = 32
	// prime multiplier of FNV-1a hash
	FnvPrime uint32 = 16777619
	// page header checksum offset
	PdChecksumOffset = 8
	// page header checksum length (in bytes)
	PdChecksumLen = 2
)

// There is an unsafe pointer logic with PgDatabasePage and PgChecksummablePage.
// Be careful if modifying these data types.

// PgDatabasePage represents single database page
type PgDatabasePage [DatabasePageSize]byte

// PgChecksummablePage represents single database page divided by NSums blocks
// for checksum calculation
type PgChecksummablePage [DatabasePageSize / int64(NSums*sizeofInt32)][NSums]uint32

// Base offsets to initialize each of the parallel FNV hashes into a different initial state
var checksumBaseOffsets [NSums]uint32

// ignoredFileNames contains filenames of files that can not be verified
var ignoredFileNames map[string]bool

func init() {
	checksumBaseOffsets = [NSums]uint32{
		0x5B1F36E9, 0xB8525960, 0x02AB50AA, 0x1DE66D2A,
		0x79FF467A, 0x9BB9F8A3, 0x217E7CD2, 0x83E13D2C,
		0xF8D4474F, 0xE39EB970, 0x42C6AE16, 0x993216FA,
		0x7B093B5D, 0x98DAFF3C, 0xF718902A, 0x0B1C9CDB,
		0xE58F764B, 0x187636BC, 0x5D7B3BB1, 0xE73DE7DE,
		0x92BEC979, 0xCCA6C0B2, 0x304A0979, 0x85AA43D4,
		0x783125BB, 0x6CA8EAA2, 0xE407EAC6, 0x4B5CFC3E,
		0x9FBF8C76, 0x15CA20BE, 0xF2CA9FD3, 0x959BD756,
	}
	ignoredFileNames = map[string]bool{
		"pg_internal.init": true,
	}
}

// Calculate one round of the checksum.
func calculateSum(checksum, value uint32) uint32 {
	tmp := checksum ^ value
	return (tmp * FnvPrime) ^ (tmp >> 17)
}

// Compute the checksum for a Postgres page.
//
// The page must be adequately aligned (at least on a 4-byte boundary).
//
// The checksum includes the block number (to detect the case where a page is
// somehow moved to a different location), the page header (excluding the
// checksum itself), and the page data.
func pgChecksumPage(blockNo uint32, pageBytes *PgDatabasePage) uint16 {
	// Set pd_checksum to zero, so that the checksum calculation
	// isn't affected by the checksum stored on the page.
	for i := PdChecksumOffset; i < PdChecksumOffset+PdChecksumLen; i++ {
		pageBytes[i] = 0
	}
	checksum := pgChecksumBlock(pageBytes)

	// Mix in the block number to detect transposed pages
	checksum ^= blockNo

	// Reduce to a uint16 (to fit in the pd_checksum field) with an offset of
	// one. That avoids checksums of zero, which seems like a good idea.
	return uint16((checksum % 65535) + 1)
}

// Block checksum algorithm. The page must be adequately aligned (at least on 4-byte boundary).
func pgChecksumBlock(page *PgDatabasePage) uint32 {
	// Initialize partial checksums to their corresponding offsets
	sums := checksumBaseOffsets
	var pageForChecksum = *(*PgChecksummablePage)(unsafe.Pointer(page))
	hashIterationsCount := DatabasePageSize / int64(NSums*sizeofInt32)

	// main checksum calculation
	for i := int64(0); i < hashIterationsCount; i++ {
		for j := 0; j < NSums; j++ {
			sums[j] = calculateSum(sums[j], pageForChecksum[i][j])
		}
	}

	// finally add in two rounds of zeroes for additional mixing
	for i := 0; i < 2; i++ {
		for j := 0; j < NSums; j++ {
			sums[j] = calculateSum(sums[j], 0)
		}
	}

	result := uint32(0)
	// xor fold partial checksums together
	for i := 0; i < NSums; i++ {
		result ^= sums[i]
	}

	return result
}

// This function is an adaptation of is_page_corrupted() from
// https://github.com/google/pg_page_verification/blob/master/pg_page_verification.c

// Function checks a page header checksum value against the current
// checksum value of a page.  NewPage checksums will be zero until they
// are set.  There is a similar function PageIsVerified responsible for
// checking pages before they are loaded into buffer pool.
//
// see:  src/backend/storage/page/bufpage.info
func isPageCorrupted(path string, blockNo uint32, page *PgDatabasePage) (bool, error) {
	pageHeader, err := parsePostgresPageHeader(bytes.NewReader(page[:]))
	if err != nil {
		return false, err
	}
	valid := pageHeader.isValid()
	if !valid {
		tracelog.WarningLogger.Printf("Invalid page header encountered: blockNo %d, path %s", blockNo, path)
	}

	// We only calculate the checksum for properly-initialized pages
	isNew := pageHeader.isNew()
	if isNew {
		return false, nil
	}

	if pageHeader.pdChecksum == 0 {
		// Zero value means that there is no checksum calculated for this page.
		// Probably checksums are disabled in the cluster
		return false, nil
	}

	// calculating blkno needs to be absolute so that subsequent segment files
	// have the blkno calculated based on all segment files and not relative to
	// the current segment file. see: https://goo.gl/qRTn46

	// Number of current segment
	relFileID, err := GetRelFileIDFrom(path)
	if err != nil {
		return false, err
	}

	// segmentBlockOffset is the absolute blockNumber of the block when taking
	// into account any previous segment files.
	segmentBlockOffset := uint32(relFileID * BlocksInRelFile)

	checksum := pgChecksumPage(segmentBlockOffset+blockNo, page)

	corrupted := checksum != pageHeader.pdChecksum
	if corrupted {
		tracelog.WarningLogger.Printf("Corruption found in %s/[%d], expected %x, found %x\n",
			path, blockNo, pageHeader.pdChecksum, checksum)
	}

	return corrupted, nil
}

// VerifyPagedFileIncrement verifies pages of an increment
func VerifyPagedFileIncrement(path string, fileInfo os.FileInfo, increment io.Reader) ([]uint32, error) {
	_, diffBlockCount, diffMap, err := GetIncrementHeaderFields(increment)
	if err != nil {
		return nil, err
	}
	blockNumbers := make([]uint32, 0, diffBlockCount)
	for i := uint32(0); i < diffBlockCount; i++ {
		blockNo := binary.LittleEndian.Uint32(diffMap[i*sizeofInt32 : (i+1)*sizeofInt32])
		blockNumbers = append(blockNumbers, blockNo)
	}
	return verifyPageBlocks(path, fileInfo, increment, blockNumbers)
}

// VerifyPagedFileBase verifies pages of a standard paged file
func VerifyPagedFileBase(path string, fileInfo os.FileInfo, pagedFile io.Reader) ([]uint32, error) {
	size := fileInfo.Size()
	// Round up filePageCount so last block will return io.ErrUnexpectedEOF if size isn't multiple of DatabasePageSize
	filePageCount := uint32((size + DatabasePageSize - 1) / DatabasePageSize)
	blockNumbers := make([]uint32, 0, filePageCount)
	for i := uint32(0); i < filePageCount; i++ {
		blockNumbers = append(blockNumbers, i)
	}
	return verifyPageBlocks(path, fileInfo, pagedFile, blockNumbers)
}

// verifyPageBlocks verifies provided page blocks from the pagedBlocks reader
func verifyPageBlocks(path string, fileInfo os.FileInfo, pageBlocks io.Reader,
	blockNumbers []uint32) (corruptBlockNumbers []uint32, err error) {
	if _, ignored := ignoredFileNames[fileInfo.Name()]; ignored {
		_, err = io.Copy(ioutil.Discard, pageBlocks)
		return nil, err
	}
	for _, blockNo := range blockNumbers {
		corrupted, err := verifySinglePage(path, blockNo, pageBlocks)
		if corrupted {
			corruptBlockNumbers = append(corruptBlockNumbers, blockNo)
		}
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF {
			tracelog.WarningLogger.Printf("verifyPageBlocks: %s invalid file size %d\n", path, fileInfo.Size())
			break
		}
		if err != nil {
			return nil, err
		}
	}
	// check if some extra delta blocks left in increment
	if isEmpty := isTarReaderEmpty(pageBlocks); !isEmpty {
		tracelog.WarningLogger.Printf("verifyPageBlocks: Unexpected extra bytes: %s\n", path)
	}
	tracelog.DebugLogger.Printf("verifyPageBlocks: %s, checked %d blocks, found %d corrupt\n",
		path, len(blockNumbers), len(corruptBlockNumbers))
	return corruptBlockNumbers, nil
}

// verifySinglePage reads and verifies single paged file block
func verifySinglePage(path string, blockNo uint32, pageBlocks io.Reader) (bool, error) {
	page := PgDatabasePage{}
	_, err := io.ReadFull(pageBlocks, page[:])
	if err != nil {
		return err == io.ErrUnexpectedEOF, err
	}
	return isPageCorrupted(path, blockNo, &page)
}

package innodb

import "encoding/binary"

const (
	// minimal possible compressed page size
	InnoDBMinPageSize     = 1024
	InnoDBMaxPageSize     = 64 * 1024
	InnoDBDefaultPageSize = 16 * 1024
)

type PageType uint16

type SpaceID uint32

// Each page within a space is assigned a 32-bit integer page number, often called “offset”,
// which is actually just the page’s offset from the beginning of the space
// (not necessarily the file, for multi-file spaces).
type PageNumber uint32

type LSN uint64

const (
	SpaceIDSystem = 0
)

const (
	PageTypeAllocated            PageType = 0 // Freshly allocated page
	PageTypeUnused               PageType = 1
	PageTypeUndoLog              PageType = 2
	PageTypeINode                PageType = 3 // index node
	PageTypeIBufFreeList         PageType = 4 // Insert buffer free list
	PageTypeIBufBitmap           PageType = 5
	PageTypeSys                  PageType = 6
	PageTypeTrxSys               PageType = 7 // Transaction system data
	PageTypeFileSpaceHeader      PageType = 8
	PageTypeXDES                 PageType = 9  // Extent descriptor page
	PageTypeBlob                 PageType = 10 // Uncompressed BLOB page
	PageTypeZBlob                PageType = 11 // First compressed BLOB page
	PageTypeZBlob2               PageType = 12 //
	PageTypeUnknown              PageType = 13
	PageTypeCompressed           PageType = 14
	PageTypeEncrypted            PageType = 15
	PageTypeCompressedEncrypted  PageType = 16
	PageTypeRTree                PageType = 17
	PageTypeSDIBlob              PageType = 18 // Uncompressed SDI BLOB page
	PageTypeZSDIBlob             PageType = 19 // Compressed SDI BLOB page
	PageTypeDoubleWrite          PageType = 20 // Legacy doublewrite buffer page
	PageTypeRollbackSegmentArray PageType = 21
	PageTypeLOBIndex             PageType = 22
	PageTypeLOBData              PageType = 23
	PageTypeLOBFirstPage         PageType = 24
	PageTypeZLOBFirstPage        PageType = 25
	PageTypeZLOBData             PageType = 26
	PageTypeZLOBIndex            PageType = 27
	PageTypeZLOBFragment         PageType = 28
	PageTypeZLOBFragmentEntry    PageType = 29
	PageTypeSDI                  PageType = 17853 // Tablespace SDI Index page
	PageTypeRtree                PageType = 17854
	PageTypeIndex                PageType = 17855
)

// Documentation: https://dev.mysql.com/doc/internals/en/innodb-fil-header.html
// ruby: https://blog.jcole.us/innodb/
// java: https://github.com/alibaba/innodb-java-reader/blob/master/innodb-java-reader
// go:   https://github.com/jemuelmiao/parseibd

type FILHeader struct {
	Checksum   uint32
	PageNumber PageNumber
	// logical link to same-level Index pages
	// SRV_VERSION for 0-page
	PreviousPage uint32
	// logical link to same-level Index pages
	// SPACE_VERSION for 0-page
	NextPage        uint32
	LastModifiedLSN LSN
	PageType        PageType
	// LSN in 0-page (0:0)
	// set of flags in FIL_PAGE_COMPRESSED pages
	// something values IL_RTREE_SPLIT_SEQ_NUM
	// 0/unused for all other pages
	FlushLSN LSN
	SpaceID  SpaceID
}

const FILHeaderSize = 38

func readHeader(page []byte) FILHeader {
	return FILHeader{
		Checksum:        binary.BigEndian.Uint32(page[0:4]),
		PageNumber:      PageNumber(binary.BigEndian.Uint32(page[4:8])),
		PreviousPage:    binary.BigEndian.Uint32(page[8:12]),
		NextPage:        binary.BigEndian.Uint32(page[12:16]),
		LastModifiedLSN: LSN(binary.BigEndian.Uint64(page[16:24])),
		PageType:        PageType(binary.BigEndian.Uint16(page[24:26])),
		FlushLSN:        LSN(binary.BigEndian.Uint64(page[26:34])),
		SpaceID:         SpaceID(binary.BigEndian.Uint32(page[34:38])),
	}
}

type CompressedMeta struct {
	Version         uint8 // values 1 and 2 are supported by Innodb
	CompressionAlgo uint8
	OrigPageType    PageType
	OrigDataSize    uint16
	CompressedSize  uint16
}

func (header FILHeader) GetCompressedData() CompressedMeta {
	if header.PageType != PageTypeCompressed {
		return CompressedMeta{}
	}
	// write it to 64-bit as it was on disk
	raw := [8]byte{}
	binary.BigEndian.PutUint64(raw[:], uint64(header.FlushLSN))
	// read it 'from disk':
	return CompressedMeta{
		Version:         raw[0],
		CompressionAlgo: raw[1],
		OrigPageType:    PageType(binary.BigEndian.Uint16(raw[2:])),
		OrigDataSize:    binary.BigEndian.Uint16(raw[4:]),
		CompressedSize:  binary.BigEndian.Uint16(raw[6:]),
	}
}

type FILTrailer struct {
	OldStyleChecksum uint32 // deprecated
	LowLSN           uint32 // low 32 bytes of LastModifiedLSN
}

const FILTrailerSize = 8

func readTrailer(page []byte) FILTrailer {
	// FILTrailer is used to detect page corruptions. InnoDB checks it on every read from disk.
	// We don't
	return FILTrailer{}
}

// Space is a container for pages (up to 2**32 pages). PageNumber just an offset in Space (measured in pages, not bytes).

// For more efficient management pages are grouped into extents. Each extent consist of:
// 256 *  4kb = 1Mb
// 128 *  8kb = 1Mb
//  64 * 16kb = 1Mb
//  64 * 32kb = 2Mb
//  64 * 64kb = 4Mb

// One (Table)Space can contain multiple 'files' that called 'segments'
// Segment grows: 32pages at first, then by 1-4 extents.

// System (Table)Space always starts from following pages:
// page #0 - FSP_HDR (File Space Header)
// page #1 - IBUF_BITMAP
// page #2 - INODE page - lists of related segments (files)
// page #3 - SYS
// page #4 - INDEX
// page #5 - TRX_SYS
// page #6 - SYS
// page #7 - SYS
// ...
// page  #64..$192 DoubleWrite Buffer blocks (1st and 2nd)
// ...

// Per-Table Space:
// page  #1 - FSP_HDR
// page  #2 - IBUF_BITMAP
// ...

// FSPFlags is a bit set:
// 1 bit  at offset  0: POST_ANTELOPE flag
// 4 bits at offset  1: ZIP page size
// 1 bit  at offset  5: width of ATOMIC_BLOBS
// 4 bits at offset  6: page size
// 1 bit  at offset 10: data dir
// 1 bit  at offset 11: shared tablespace
// 1 bit  at offset 12: temporary (should be deleted on start)
// 1 bit  at offset 13: encrypted
// 1 bit  at offset 14: SDI
type FSPFlags uint32

// nolint:unused
func (flags FSPFlags) compressedPageSize() uint16 {
	return 512 * uint16((uint32(flags)&uint32(0b00000000_00011110))>>1)
}

// nolint:unused
func (flags FSPFlags) pageSize() uint16 {
	return 512 * uint16((uint32(flags)&uint32(0b00011_11000000))>>6)
}

// nolint:unused
func (flags FSPFlags) isDataDir() bool {
	return (uint32(flags) & uint32(0b00000010_00000000)) != 0
}

// nolint:unused
func (flags FSPFlags) isShared() bool {
	return (uint32(flags) & uint32(0b00000100_00000000)) != 0
}

// nolint:unused
func (flags FSPFlags) isTemporary() bool {
	return (uint32(flags) & uint32(0b00001000_00000000)) != 0
}

// nolint:unused
func (flags FSPFlags) isEncrypted() bool {
	return (uint32(flags) & uint32(0b00010000_00000000)) != 0
}

// FSP_HDR - PageTypeFileSpaceHeader
// 112 bytes
type FileSpaceHeader struct {
	SpaceID                      SpaceID
	HighestPageNumberInFile      PageNumber // size
	HighestPageNumberInitialized PageNumber // free_limit
	Flags                        FSPFlags
	// other fields
}

func readFileSpaceHeader(page []byte) FileSpaceHeader {
	return FileSpaceHeader{
		SpaceID: SpaceID(binary.BigEndian.Uint32(page[38:42])),
		// unused 4 bytes
		HighestPageNumberInFile:      PageNumber(binary.BigEndian.Uint32(page[46:50])),
		HighestPageNumberInitialized: PageNumber(binary.BigEndian.Uint32(page[50:54])),
		Flags:                        FSPFlags(binary.BigEndian.Uint32(page[54:58])),
		// other fields
	}
}

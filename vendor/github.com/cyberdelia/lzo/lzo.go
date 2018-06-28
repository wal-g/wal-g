package lzo

/*
#cgo LDFLAGS: -llzo2
#include <lzo/lzo1x.h>

static int lzo_initialize(void) { return lzo_init(); }
static int lzo1x_1_mem_compress() { return LZO1X_1_MEM_COMPRESS; }
static int lzo1x_999_mem_compress() { return LZO1X_999_MEM_COMPRESS; }
*/
import "C"

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/adler32"
	"hash/crc32"
	"io"
	"strconv"
	"time"
	"unsafe"
)

const (
	BestSpeed          = 3
	BestCompression    = 9
	DefaultCompression = -1
	Version            = 0x1030
	flagAdler32D       = 1 << 0
	flagAdler32C       = 1 << 1
	flagStdin          = 1 << 2
	flagStdout         = 1 << 3
	flagNameDefault    = 1 << 4
	flagDosish         = 1 << 5
	flagExtra          = 1 << 6
	flagGmtDiff        = 1 << 7
	flagCRC32D         = 1 << 8
	flagCRC32C         = 1 << 9
	flagMultipart      = 1 << 10
	flagFilter         = 1 << 11
	flagCRC32          = 1 << 12
	flagPath           = 1 << 13
	flagMask           = 1 << 14
)

var (
	lzoMagic   = []byte{0x89, 0x4c, 0x5a, 0x4f, 0x00, 0x0d, 0x0a, 0x1a, 0x0a}
	ErrHeader  = errors.New("lzo: invalid header")
	ErrVersion = errors.New("lzo: incompatible version")
	ErrMethod  = errors.New("lzo: incompatible method")
	ErrCorrupt = errors.New("lzo: data corruption")
)

var lzoErrors = []string{
	1: "data corrupted",
	2: "out of memory",
	4: "input overrun",
	5: "output overrun",
	6: "data corrupted",
	7: "eof not found",
	8: "input not consumed",
}

func init() {
	if err := C.lzo_initialize(); err != 0 {
		panic("lzo: can't initialize")
	}
}

type errno int

func (e errno) Error() string {
	if 0 <= int(e) && int(e) < len(lzoErrors) {
		s := lzoErrors[e]
		if s != "" {
			return "lzo: " + s
		}
	}
	return "lzo: errno " + strconv.Itoa(int(e))
}

// Lzop file stores a header giving metadata about the compressed file.
// That header is exposed as the fields of the Writer and Reader structs.
type Header struct {
	ModTime time.Time
	Name    string
	flags   uint32
}

// A Reader is an io.Reader that can be read to retrieve
// uncompressed data from a lzop-format compressed file.
type Reader struct {
	Header
	r       io.Reader
	buf     [512]byte
	hist    []byte
	adler32 hash.Hash32
	crc32   hash.Hash32
	err     error
}

// NewReader creates a new Reader reading the given reader.
func NewReader(r io.Reader) (*Reader, error) {
	z := new(Reader)
	z.r = r
	z.adler32 = adler32.New()
	z.crc32 = crc32.NewIEEE()
	if err := z.readHeader(); err != nil {
		return nil, err
	}
	return z, nil
}

func (z *Reader) readHeader() error {
	// Read and check magic
	_, err := io.ReadFull(z.r, z.buf[0:len(lzoMagic)])
	if err != nil {
		return err
	}
	if !bytes.Equal(z.buf[0:len(lzoMagic)], lzoMagic) {
		return ErrHeader
	}
	// Read version
	var version uint16
	version, err = z.readUint16()
	if err != nil {
		return err
	}
	if version < 0x0900 {
		return ErrHeader
	}
	// Read library version needed to extract
	var libraryVersion uint16
	libraryVersion, err = z.readUint16()
	if err != nil {
		return err
	}
	if version >= 0x0940 {
		libraryVersion, err = z.readUint16()
		if err != nil {
			return err
		}
		if libraryVersion > version {
			return ErrVersion
		}
		if libraryVersion < 0x0900 {
			return ErrHeader
		}
	}
	// Read method
	var method byte
	method, err = z.readByte()
	if err != nil {
		return err
	}
	// Read level
	if version >= 0x0940 {
		_, err = z.readByte()
		if err != nil {
			return err
		}
	}
	// Read flags
	z.flags, err = z.readUint32()
	if err != nil {
		return err
	}
	// Read filters
	if z.flags&flagFilter != 0 {
		_, err = z.readUint32()
		if err != nil {
			return err
		}
	}
	// Read mode
	_, err = z.readUint32()
	if err != nil {
		return err
	}
	// Read modification times
	modTime, err := z.readUint32()
	if err != nil {
		return err
	}
	z.ModTime = time.Unix(int64(modTime), 0)
	// Read mod time high
	if version >= 0x0940 {
		_, err := z.readUint32()
		if err != nil {
			return err
		}
	}
	if version < 0x0120 {
		z.ModTime = time.Unix(0, 0)
	}
	// Read name
	l, err := z.readByte()
	if err != nil {
		return err
	}
	if l > 0 {
		_, err := io.ReadFull(z.r, z.buf[0:l])
		if err != nil {
			return err
		}
		z.adler32.Write(z.buf[0:l])
		z.crc32.Write(z.buf[0:l])
		z.Name = string(z.buf[0:l])
	}
	// Read and check header checksum
	var checksum uint32
	if z.flags&flagCRC32 != 0 {
		checksum = z.crc32.Sum32()
		z.crc32.Reset()
	} else {
		checksum = z.adler32.Sum32()
		z.adler32.Reset()
	}
	var checksumHeader uint32
	checksumHeader, err = z.readUint32()
	if err != nil {
		return err
	}
	if checksumHeader != checksum {
		return ErrHeader
	}
	if method <= 0 {
		return ErrMethod
	}
	return nil
}

func (z *Reader) readByte() (byte, error) {
	_, err := io.ReadFull(z.r, z.buf[0:1])
	if err != nil {
		return 0, err
	}
	z.adler32.Write(z.buf[0:1])
	z.crc32.Write(z.buf[0:1])
	return z.buf[0], nil
}

func (z *Reader) readUint16() (uint16, error) {
	_, err := io.ReadFull(z.r, z.buf[0:2])
	if err != nil {
		return 0, err
	}
	z.adler32.Write(z.buf[0:2])
	z.crc32.Write(z.buf[0:2])
	return binary.BigEndian.Uint16(z.buf[0:2]), nil
}

func (z *Reader) readUint32() (uint32, error) {
	_, err := io.ReadFull(z.r, z.buf[0:4])
	if err != nil {
		return 0, err
	}
	z.adler32.Write(z.buf[0:4])
	z.crc32.Write(z.buf[0:4])
	return binary.BigEndian.Uint32(z.buf[0:4]), nil
}

func (z *Reader) nextBlock() {
	// Read uncompressed block size
	var dstLen uint32
	dstLen, z.err = z.readUint32()
	if z.err != nil {
		return
	}
	if dstLen == 0 {
		z.err = io.EOF
		return
	}
	// Read compressed block size
	var srcLen uint32
	srcLen, z.err = z.readUint32()
	if z.err != nil {
		return
	}
	if srcLen <= 0 || srcLen > dstLen {
		z.err = ErrCorrupt
		return
	}
	// Read checksum of uncompressed block
	var dstChecksum uint32
	if z.flags&flagAdler32D != 0 {
		dstChecksum, z.err = z.readUint32()
		if z.err != nil {
			return
		}
	}
	if z.flags&flagCRC32D != 0 {
		dstChecksum, z.err = z.readUint32()
		if z.err != nil {
			return
		}
	}
	// Read checksum of compressed block
	var srcChecksum uint32
	if z.flags&flagAdler32C != 0 {
		if srcLen < dstLen {
			srcChecksum, z.err = z.readUint32()
			if z.err != nil {
				return
			}
		} else {
			srcChecksum = dstChecksum
		}
	}
	if z.flags&flagCRC32C != 0 {
		if srcLen < dstLen {
			srcChecksum, z.err = z.readUint32()
			if z.err != nil {
				return
			}
		} else {
			srcChecksum = dstChecksum
		}
	}
	// Read block
	block := make([]byte, srcLen)
	_, z.err = io.ReadFull(z.r, block)
	if z.err != nil {
		return
	}
	// Verify compressed block checksum
	if z.flags&flagAdler32C != 0 {
		z.adler32.Reset()
		z.adler32.Write(block)
		if srcChecksum != z.adler32.Sum32() {
			z.err = ErrCorrupt
			return
		}
	}
	if z.flags&flagCRC32C != 0 {
		z.crc32.Reset()
		z.crc32.Write(block)
		if srcChecksum != z.crc32.Sum32() {
			z.err = ErrCorrupt
			return
		}
	}
	// Decompress
	data := make([]byte, dstLen)
	if srcLen < dstLen {
		_, z.err = lzoDecompress(block, data)
		if z.err != nil {
			return
		}
	} else {
		copy(data, block)
	}
	// Verify uncompressed block checksum
	if z.flags&flagAdler32D != 0 {
		z.adler32.Reset()
		z.adler32.Write(data)
		if dstChecksum != z.adler32.Sum32() {
			z.err = ErrCorrupt
			return
		}
	}
	if z.flags&flagCRC32D != 0 {
		z.crc32.Reset()
		z.crc32.Write(data)
		if dstChecksum != z.crc32.Sum32() {
			z.err = ErrCorrupt
			return
		}
	}
	// Add block to our history
	z.hist = append(z.hist, data...)
}

func (z *Reader) Read(p []byte) (int, error) {
	for {
		if len(z.hist) > 0 {
			n := copy(p, z.hist)
			z.hist = z.hist[n:]
			return n, nil
		}
		if z.err != nil {
			return 0, z.err
		}
		z.nextBlock()
	}
}

// Close closes the Reader. It does not close the underlying io.Reader.
func (z *Reader) Close() error {
	if z.err == io.EOF {
		return nil
	}
	return z.err
}

func lzoDecompress(src []byte, dst []byte) (int, error) {
	dstLen := len(dst)
	err := C.lzo1x_decompress((*C.uchar)(unsafe.Pointer(&src[0])), C.lzo_uint(len(src)),
		(*C.uchar)(unsafe.Pointer(&dst[0])), (*C.lzo_uint)(unsafe.Pointer(&dstLen)), nil)
	if err != 0 {
		return 0, errno(err)
	}
	return dstLen, nil
}

// A Writer is an io.Write that satisfies writes by compressing data written
// to its wrapped io.Writer.
type Writer struct {
	Header
	w          io.Writer
	level      int
	err        error
	compressor func([]byte) ([]byte, error)
	adler32    hash.Hash32
	crc32      hash.Hash32
}

// NewWriter creates a new Writer that satisfies writes by compressing data
// written to w.
func NewWriter(w io.Writer) *Writer {
	z, _ := NewWriterLevel(w, DefaultCompression)
	return z
}

// NewWriterLevel is like NewWriter but specifies the compression level instead
// of assuming DefaultCompression.
func NewWriterLevel(w io.Writer, level int) (*Writer, error) {
	if level < DefaultCompression || level > BestCompression {
		return nil, fmt.Errorf("lzo: invalid compression level: %d", level)
	}
	z := new(Writer)
	z.w = w
	z.ModTime = time.Now()
	z.level = level
	z.adler32 = adler32.New()
	z.crc32 = crc32.NewIEEE()
	return z, nil
}

func (z *Writer) writeHeader() error {
	// Write magic numbers
	_, err := z.w.Write(lzoMagic)
	if err != nil {
		return err
	}
	// Write version
	_, err = z.writeUint16(Version & 0xffff)
	if err != nil {
		return err
	}
	// Write library version
	_, err = z.writeUint16(lzoVersion() & 0xffff)
	if err != nil {
		return err
	}
	// Write library version needed to extract
	_, err = z.writeUint16(0x0940)
	if err != nil {
		return err
	}
	// Write method
	var method, level byte
	if z.level == BestCompression {
		method = 3
		level = 9
	} else {
		method = 1
		level = 3
	}
	_, err = z.writeByte(method)
	if err != nil {
		return err
	}
	// Write level
	_, err = z.writeByte(byte(level))
	if err != nil {
		return err
	}
	// Write flags
	z.flags = 0
	z.flags |= flagAdler32D
	z.flags |= flagAdler32C
	if z.Name == "" {
		z.flags |= flagStdin
		z.flags |= flagStdout
	}
	_, err = z.writeUint32(z.flags)
	if err != nil {
		return err
	}
	// Write mode
	_, err = z.writeUint32(0)
	if err != nil {
		return err
	}
	// Write modification time
	_, err = z.writeUint32(uint32(z.ModTime.Unix()))
	if err != nil {
		return err
	}
	_, err = z.writeUint32(uint32(z.ModTime.Unix()) >> 16 >> 16)
	if err != nil {
		return err
	}
	// Write file name
	name := []byte(z.Name)
	z.writeByte(byte(len(name)))
	if z.Name != "" {
		_, err := z.w.Write(name)
		if err != nil {
			return err
		}
		z.adler32.Write(name)
	}
	// Write header checksum
	_, err = z.writeUint32(z.adler32.Sum32())
	if err != nil {
		return err
	}
	z.adler32.Reset()
	z.crc32.Reset()
	return nil
}

func (z *Writer) writeByte(v byte) (int, error) {
	buf := []byte{v}
	z.adler32.Write(buf)
	z.crc32.Write(buf)
	return z.w.Write(buf)
}

func (z *Writer) writeUint16(v uint16) (int, error) {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, v)
	z.adler32.Write(buf)
	z.crc32.Write(buf)
	return z.w.Write(buf)
}

func (z *Writer) writeUint32(v uint32) (int, error) {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, v)
	z.adler32.Write(buf)
	z.crc32.Write(buf)
	return z.w.Write(buf)
}

// Write writes a compressed form of p to the underlying io.Writer.
func (z *Writer) Write(p []byte) (int, error) {
	if z.err != nil {
		return 0, z.err
	}
	// Write headers
	if z.compressor == nil {
		if z.level == BestCompression {
			z.compressor = func(src []byte) ([]byte, error) {
				return lzoCompress(src, lzoCompressBest)
			}
		} else {
			z.compressor = func(src []byte) ([]byte, error) {
				return lzoCompress(src, lzoCompressSpeed)
			}
		}
		z.err = z.writeHeader()
		if z.err != nil {
			return 0, z.err
		}
	}
	srcLen := len(p)
	// Write uncompressed block size
	_, z.err = z.writeUint32(uint32(srcLen))
	if z.err != nil {
		return 0, z.err
	}
	// Last block?
	if srcLen == 0 {
		return 0, z.err
	}
	// Compute uncompressed block checksum
	z.adler32.Reset()
	z.adler32.Write(p)
	srcChecksum := z.adler32.Sum32()
	// Compress
	var compressed []byte
	compressed, z.err = z.compressor(p)
	if z.err != nil {
		return 0, z.err
	}
	// Write compressed block size
	if len(compressed) >= srcLen {
		compressed = p
	}
	dstLen := len(compressed)
	_, z.err = z.writeUint32(uint32(dstLen))
	if z.err != nil {
		return 0, z.err
	}
	// Write uncompressed block checksum
	_, z.err = z.writeUint32(srcChecksum)
	if z.err != nil {
		return 0, z.err
	}
	// Write compressed block checksum
	z.adler32.Reset()
	z.adler32.Write(compressed)
	dstChecksum := z.adler32.Sum32()
	if dstLen < srcLen {
		_, z.err = z.writeUint32(dstChecksum)
		if z.err != nil {
			return 0, z.err
		}
	}
	// Write compressed block data
	_, z.err = z.w.Write(compressed)
	if z.err != nil {
		return 0, z.err
	}
	return srcLen, z.err
}

// Close closes the Writer. It does not close the underlying io.Writer.
func (z *Writer) Close() error {
	_, z.err = z.writeUint32(uint32(0))
	return z.err
}

func lzoVersion() uint16 {
	return uint16(C.lzo_version())
}

func lzoCompress(src []byte, compress func([]byte, []byte, *int) C.int) ([]byte, error) {
	dst_size := 0
	dst := make([]byte, lzoDestinationSize(len(src)))
	err := compress(src, dst, &dst_size)
	if err != 0 {
		return nil, fmt.Errorf("lzo: errno %d", err)
	}
	return dst[0:dst_size], nil
}

func lzoDestinationSize(n int) int {
	return (n + n/16 + 64 + 3)
}

func lzoCompressSpeed(src []byte, dst []byte, dst_size *int) C.int {
	wrkmem := make([]byte, int(C.lzo1x_1_mem_compress()))
	return C.lzo1x_1_compress((*C.uchar)(unsafe.Pointer(&src[0])), C.lzo_uint(len(src)),
		(*C.uchar)(unsafe.Pointer(&dst[0])), (*C.lzo_uint)(unsafe.Pointer(dst_size)),
		unsafe.Pointer(&wrkmem[0]))
}

func lzoCompressBest(src []byte, dst []byte, dst_size *int) C.int {
	wrkmem := make([]byte, int(C.lzo1x_999_mem_compress()))
	return C.lzo1x_999_compress((*C.uchar)(unsafe.Pointer(&src[0])), C.lzo_uint(len(src)),
		(*C.uchar)(unsafe.Pointer(&dst[0])), (*C.lzo_uint)(unsafe.Pointer(dst_size)),
		unsafe.Pointer(&wrkmem[0]))
}

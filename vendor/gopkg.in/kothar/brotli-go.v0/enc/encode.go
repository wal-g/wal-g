// Package enc provides Brotli encoder bindings
package enc // import "gopkg.in/kothar/brotli-go.v0/enc"

/*
// for memcpy
#include <string.h>

// Parts of original C++ header
#include "./encode_go.h"

typedef uint8_t dict[122784];
dict* kBrotliDictionary;

// Based on BrotliCompressBufferParallel
// https://github.com/google/brotli/blob/24469b81d604ddf1976c3e4b633523bd8f6f631c/enc/encode_parallel.cc#L233
size_t BrotliMaxOutputSize(CBrotliParams params, size_t input_size) {
  // Sanitize params.
  if (params.lgwin < kMinWindowBits) {
    params.lgwin = kMinWindowBits;
  } else if (params.lgwin > kMaxWindowBits) {
    params.lgwin = kMaxWindowBits;
  }
  if (params.lgblock == 0) {
    params.lgblock = 16;
    if (params.quality >= 9 && params.lgwin > params.lgblock) {
      params.lgblock = params.lgwin < 21 ? params.lgwin : 21;
    }
  } else if (params.lgblock < kMinInputBlockBits) {
    params.lgblock = kMinInputBlockBits;
  } else if (params.lgblock > kMaxInputBlockBits) {
    params.lgblock = kMaxInputBlockBits;
  }

  size_t input_block_size = 1 << params.lgblock;
  size_t output_block_size = input_block_size + (input_block_size >> 3) + 1024;

  size_t blocks = (input_size / input_block_size) + 1;

  size_t max_output_size = blocks * output_block_size;

  return max_output_size;
}
*/
import "C"

import (
	"errors"
	"io"
	"runtime"
	"unsafe"

	"gopkg.in/kothar/brotli-go.v0/shared"
)

// Errors which may be returned when encoding
var (
	errInputLargerThanBlockSize = errors.New("data copied to ring buffer larger than brotli compressor block size")
	errBrotliCompression        = errors.New("brotli compression error")
)

func init() {
	// Set up the default dictionary from the data in the shared package
	C.kBrotliDictionary = (*C.dict)(shared.GetDictionary())
}

// Mode defines the operation mode of the compressor
type Mode int

const (
	// GENERIC is the default compression mode. The compressor does not know anything in
	// advance about the properties of the input.
	GENERIC Mode = iota
	// TEXT is a compression mode for UTF-8 format text input.
	TEXT
	// FONT is a compression mode used in WOFF 2.0.
	FONT
)

// BrotliParams describes the settings used when encoding using Brotli
type BrotliParams struct {
	c C.struct_CBrotliParams
}

// NewBrotliParams instantiates the compressor parameters with the default settings
func NewBrotliParams() *BrotliParams {
	params := &BrotliParams{C.struct_CBrotliParams{
		mode:    C.MODE_GENERIC,
		quality: 11,
		lgwin:   22,
		lgblock: 0,

		// Deprecated according to header
		enable_dictionary:       true,
		enable_transforms:       false,
		greedy_block_split:      false,
		enable_context_modeling: true,
	}}

	return params
}

// Mode returns the current operating mode of the compressor
func (p *BrotliParams) Mode() Mode {
	return Mode(p.c.mode)
}

// SetMode controls the operating mode of the compressor (GENERIC, TEXT or FONT)
func (p *BrotliParams) SetMode(value Mode) {
	p.c.mode = C.enum_Mode(value)
}

// Quality returns the quality setting of the compressor
func (p *BrotliParams) Quality() int {
	return int(p.c.quality)
}

// SetQuality controls the compression-speed vs compression-density tradeoffs. The higher
// the quality, the slower the compression. Range is 0 to 11. Default is 11.
func (p *BrotliParams) SetQuality(value int) {
	p.c.quality = C.int(value)
}

// Lgwin returns the current sliding window size setting.
func (p *BrotliParams) Lgwin() int {
	return int(p.c.lgwin)
}

// SetLgwin sets the base 2 logarithm of the sliding window size. Range is 10 to 24. Default is 22.
func (p *BrotliParams) SetLgwin(value int) {
	p.c.lgwin = C.int(value)
}

// Lgblock returns the current maximum input block size setting.
func (p *BrotliParams) Lgblock() int {
	return int(p.c.lgblock)
}

// SetLgblock sets the base 2 logarithm of the maximum input block size. Range is 16 to 24.
// If set to 0 (default), the value will be set based on the quality.
func (p *BrotliParams) SetLgblock(value int) {
	p.c.lgblock = C.int(value)
}

// Maximum output size based on
// https://github.com/google/brotli/blob/24469b81d604ddf1976c3e4b633523bd8f6f631c/enc/encode_parallel.cc#L233
// There doesn't appear to be any documentation of what this calculation is based on.
func (p *BrotliParams) maxOutputSize(inputLength int) int {
	return int(C.BrotliMaxOutputSize(p.c, C.size_t(inputLength)))
}

// CompressBuffer compresses a single block of data. It uses encodedBuffer as
// the destination buffer unless it is too small, in which case a new buffer
// is allocated.
// Default parameters are used if params is nil.
// Returns the slice of the encodedBuffer containing the output, or an error.
func CompressBuffer(params *BrotliParams, inputBuffer []byte, encodedBuffer []byte) ([]byte, error) {

	if params == nil {
		params = NewBrotliParams()
	}

	inputLength := len(inputBuffer)
	maxOutSize := params.maxOutputSize(inputLength)

	if len(encodedBuffer) < maxOutSize {
		encodedBuffer = make([]byte, maxOutSize)
	}

	encodedLength := C.size_t(len(encodedBuffer))
	result := C.CBrotliCompressBuffer(params.c, C.size_t(inputLength), toC(inputBuffer), &encodedLength, toC(encodedBuffer))
	if result == 0 {
		return nil, errBrotliCompression
	}
	return encodedBuffer[0:encodedLength], nil
}

// CompressBufferDict compresses a single block of data using a custom dictionary. It uses encodedBuffer as
// the destination buffer unless it is too small, in which case a new buffer
// is allocated.
// Default parameters are used if params is nil.
// Returns the slice of the encodedBuffer containing the output, or an error.
func CompressBufferDict(params *BrotliParams, inputBuffer []byte, inputDict []byte, encodedBuffer []byte) ([]byte, error) {
	if params == nil {
		params = NewBrotliParams()
	}

	dictLength := len(inputDict)
	inputLength := len(inputBuffer)
	maxOutSize := params.maxOutputSize(inputLength)

	if len(encodedBuffer) < maxOutSize {
		encodedBuffer = make([]byte, maxOutSize)
	}

	encodedLength := C.size_t(len(encodedBuffer))
	result := C.CBrotliCompressBufferDict(params.c,
		C.size_t(inputLength), toC(inputBuffer),
		C.size_t(dictLength), toC(inputDict),
		&encodedLength, toC(encodedBuffer))
	if result == 0 {
		return nil, errBrotliCompression
	}
	return encodedBuffer[0:encodedLength], nil
}

type brotliCompressor struct {
	c            C.CBrotliCompressor
	outputBuffer []byte
}

// An instance can not be reused for multiple brotli streams.
func newBrotliCompressor(params *BrotliParams) *brotliCompressor {
	if params == nil {
		params = NewBrotliParams()
	}

	cbp := C.CBrotliCompressorNew(params.c)
	bp := &brotliCompressor{c: cbp}
	// cf. https://github.com/kothar/brotli-go/blob/467303b7ca58bb7417af1fc35b22933e8f344344/enc/encode_parallel.cc#L154
	bp.outputBuffer = make([]byte, bp.getInputBlockSize()*2+500)

	runtime.SetFinalizer(bp, brotliCompressorFinalizer)
	return bp
}

// The maximum input size that can be processed at once.
func (bp *brotliCompressor) getInputBlockSize() int {
	return int(C.CBrotliCompressorGetInputBlockSize(bp.c))
}

// Copies the given input data to the internal ring buffer of the compressor.
// No processing of the data occurs at this time and this function can be
// called multiple times before calling WriteBrotliData() to process the
// accumulated input. At most getInputBlockSize() bytes of input data can be
// copied to the ring buffer, otherwise the next WriteBrotliData() will fail.
func (bp *brotliCompressor) copyInputToRingBuffer(input []byte) {
	C.CBrotliCompressorCopyInputToRingBuffer(bp.c, C.size_t(len(input)), toC(input))
}

// Processes the accumulated input data and returns the new output meta-block,
// or zero if no new output meta-block was created (in this case the processed
// input data is buffered internally).
// Returns ErrInputLargerThanBlockSize if more data was copied to the ring buffer
// than the block sized.
// If isLast or forceFlush is true, an output meta-block is always created
func (bp *brotliCompressor) writeBrotliData(isLast bool, forceFlush bool) ([]byte, error) {
	var outSize C.size_t
	var output *C.uint8_t
	success := C.CBrotliCompressorWriteBrotliData(bp.c, C.bool(isLast), C.bool(forceFlush), &outSize, &output)
	if success == false {
		return nil, errInputLargerThanBlockSize
	}

	// resize buffer if output is larger than we've anticipated
	if int(outSize) > cap(bp.outputBuffer) {
		bp.outputBuffer = make([]byte, int(outSize))
	}

	C.memcpy(unsafe.Pointer(&bp.outputBuffer[0]), unsafe.Pointer(output), outSize)
	return bp.outputBuffer[:outSize], nil
}

func (bp *brotliCompressor) free() {
	if bp.c == nil {
		return
	}
	C.CBrotliCompressorFree(bp.c)
	bp.c = nil
}

func brotliCompressorFinalizer(bp *brotliCompressor) {
	bp.free()
}

// BrotliWriter implements the io.Writer interface, compressing the stream
// to an output Writer using Brotli.
type BrotliWriter struct {
	compressor *brotliCompressor
	writer     io.Writer

	// amount of data already copied into ring buffer
	inRingBuffer int
}

// NewBrotliWriter instantiates a new BrotliWriter with the provided compression
// parameters and output Writer
func NewBrotliWriter(params *BrotliParams, writer io.Writer) *BrotliWriter {
	return &BrotliWriter{
		compressor:   newBrotliCompressor(params),
		writer:       writer,
		inRingBuffer: 0,
	}
}

func (w *BrotliWriter) Write(buffer []byte) (int, error) {
	comp := w.compressor
	blockSize := int(comp.getInputBlockSize())
	roomFor := blockSize - w.inRingBuffer
	copied := 0

	for len(buffer) >= roomFor {
		comp.copyInputToRingBuffer(buffer[:roomFor])
		copied += roomFor

		compressedData, err := comp.writeBrotliData(false, false)
		if err != nil {
			return copied, err
		}

		_, err = w.writer.Write(compressedData)
		if err != nil {
			return copied, err
		}

		w.inRingBuffer = 0
		buffer = buffer[roomFor:]
		roomFor = blockSize
	}

	remaining := len(buffer)
	if remaining > 0 {
		comp.copyInputToRingBuffer(buffer)
		w.inRingBuffer += remaining
		copied += remaining
	}

	return copied, nil
}

// Close cleans up the resources used by the Brotli encoder for this
// stream. If the output buffer is an io.Closer, it will also be closed.
func (w *BrotliWriter) Close() error {
	compressedData, err := w.compressor.writeBrotliData(true, false)
	if err != nil {
		return err
	}
	w.compressor.free()

	_, err = w.writer.Write(compressedData)
	if err != nil {
		return err
	}

	if v, ok := w.writer.(io.Closer); ok {
		return v.Close()
	}

	return nil
}

// internal cgo utilities

func toC(array []byte) *C.uint8_t {
	return (*C.uint8_t)(unsafe.Pointer(&array[0]))
}

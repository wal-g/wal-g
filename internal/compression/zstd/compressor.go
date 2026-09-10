package zstd

import (
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/wal-g/wal-g/internal/ioextensions"
)

const (
	AlgorithmName = "zstd"
	FileExtension = "zst"
)

// encoderPools holds one pool of encoders per encoder level. Building an encoder
// allocates its match tables and block buffers, and wal-g asks for a new writer
// for every tar part and every WAL segment, so without reuse that setup cost is
// paid once per uploaded member. Pools are keyed by level because a process can
// hold both the registry default and a configured Compressor.
//
// sync.Pool is drained on every GC cycle, so the retained encoder state stays
// bounded by the number of writers actually in flight.
var encoderPools sync.Map // zstd.EncoderLevel -> *sync.Pool

func encoderPool(level zstd.EncoderLevel) *sync.Pool {
	if pool, ok := encoderPools.Load(level); ok {
		return pool.(*sync.Pool)
	}
	pool, _ := encoderPools.LoadOrStore(level, &sync.Pool{
		New: func() any {
			encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(level))
			if err != nil {
				panic(err)
			}
			return encoder
		},
	})
	return pool.(*sync.Pool)
}

// pooledWriter encodes a single stream and hands its encoder back on Close.
//
// The encoder must be returned exactly once. CloseTar can reach Close a second
// time, because a closed archive/tar.Writer reports success when closed again
// and the cascade then closes the writer underneath it as well. An encoder
// returned twice would be handed to two writers at once and both streams would
// be encoded into whichever destination was reset last.
type pooledWriter struct {
	*zstd.Encoder
	pool   *sync.Pool
	once   sync.Once
	closed bool
}

// Write and Flush refuse a closed writer with the same error the bare encoder
// used to return. Without the check they would reach an encoder that may
// already be reset onto another writer's destination and silently encode into
// that stream instead of failing.

func (writer *pooledWriter) Write(data []byte) (int, error) {
	if writer.closed {
		return 0, zstd.ErrEncoderClosed
	}
	return writer.Encoder.Write(data)
}

func (writer *pooledWriter) Flush() error {
	if writer.closed {
		return zstd.ErrEncoderClosed
	}
	return writer.Encoder.Flush()
}

func (writer *pooledWriter) Close() error {
	var err error
	writer.once.Do(func() {
		writer.closed = true
		err = writer.Encoder.Close()
		// An encoder that failed to close is left out of the pool rather than
		// recycled into the next backup member.
		if err == nil {
			writer.pool.Put(writer.Encoder)
		}
	})
	return err
}

// Compressor writes zstd-compressed streams. A zero Level keeps the historical
// default (zstd.SpeedDefault), so an unconfigured Compressor behaves as before.
type Compressor struct {
	Level zstd.EncoderLevel
}

func (compressor Compressor) NewWriter(writer io.Writer) ioextensions.WriteFlushCloser {
	level := compressor.Level
	if level == 0 { // level not set: preserve the previous default
		level = zstd.SpeedDefault
	}
	pool := encoderPool(level)
	encoder := pool.Get().(*zstd.Encoder)
	encoder.Reset(writer)

	return &pooledWriter{Encoder: encoder, pool: pool}
}

func (compressor Compressor) FileExtension() string {
	return FileExtension
}

// EncoderLevelFromName resolves a WALG_ZSTD_LEVEL value ("fastest", "default",
// "better", "best") to a zstd encoder level. The match ignores case; the
// returned bool is false when the name is not recognized.
func EncoderLevelFromName(name string) (zstd.EncoderLevel, bool) {
	ok, level := zstd.EncoderLevelFromString(name)
	return level, ok
}

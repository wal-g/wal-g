package zstd

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompressDecompress(t *testing.T) {
	type testCase struct {
		name  string
		input string
	}

	seed := int64(0x1337c0deb357beef)
	if rand, ok := os.LookupEnv("WALG_RANDOMIZE_TEST"); ok {
		if rand != "" && rand != "0" {
			seed = time.Now().UnixNano()
			t.Logf("randomized seed: %x", seed)
		}
	}

	var buff = make([]byte, 4096)
	rand.New(rand.NewSource(seed)).Read(buff)

	testcases := []*testCase{
		{
			name:  "simple input",
			input: "How much wood could a woodchuck chuck if a woodchuck could chuck wood ?",
		},
		{
			name:  "random data",
			input: string(buff),
		},
	}

	for _, tc := range testcases {
		in := []byte(tc.input)

		var comp bytes.Buffer
		wc := Compressor{}.NewWriter(&comp)

		var err error
		_, err = wc.Write(in)
		require.NoError(t, err, tc.name)

		err = wc.Close()
		require.NoError(t, err, tc.name)

		rdr, err := Decompressor{}.Decompress(&comp)
		require.NoError(t, err, tc.name)

		var decomp bytes.Buffer
		_, err = io.Copy(&decomp, rdr)
		require.NoError(t, err)

		err = rdr.Close()
		require.NoError(t, err)

		if !bytes.Equal(in, decomp.Bytes()) {
			assert.Fail(t, "decompressed data doesn't match expected input", "testCase: %s", tc.name)
			if tc.name == "random data" {
				t.Log("random seed =", seed)
			}
		}
	}
}

func TestCompressDecompressLevels(t *testing.T) {
	levels := []zstd.EncoderLevel{
		zstd.SpeedFastest,
		zstd.SpeedDefault,
		zstd.SpeedBetterCompression,
		zstd.SpeedBestCompression,
	}

	buff := make([]byte, 1<<16)
	rand.New(rand.NewSource(0x1337c0deb357beef)).Read(buff)

	for _, level := range levels {
		var comp bytes.Buffer
		wc := Compressor{Level: level}.NewWriter(&comp)
		_, err := wc.Write(buff)
		require.NoError(t, err, level.String())
		require.NoError(t, wc.Close(), level.String())

		rdr, err := Decompressor{}.Decompress(&comp)
		require.NoError(t, err, level.String())

		var decomp bytes.Buffer
		_, err = io.Copy(&decomp, rdr)
		require.NoError(t, err, level.String())
		require.NoError(t, rdr.Close(), level.String())

		assert.True(t, bytes.Equal(buff, decomp.Bytes()), "roundtrip mismatch at level %s", level.String())
	}
}

func TestEncoderLevelFromName(t *testing.T) {
	level, ok := EncoderLevelFromName("best")
	require.True(t, ok)
	assert.Equal(t, zstd.SpeedBestCompression, level)

	_, ok = EncoderLevelFromName("nonsense")
	assert.False(t, ok)
}

// benchPayload imitates a postgres WAL segment: mostly structured and
// repetitive, with pockets of incompressible data.
func benchPayload(n int) []byte {
	buf := make([]byte, n)
	rnd := rand.New(rand.NewSource(0x1337c0deb357beef))
	for i := 0; i < n; i += 512 {
		end := i + 512
		if end > n {
			end = n
		}
		if (i/512)%4 == 0 {
			rnd.Read(buf[i:end])
		} else {
			for j := i; j < end; j++ {
				buf[j] = byte(j % 71)
			}
		}
	}
	return buf
}

func roundtrip(t *testing.T, compressed *bytes.Buffer) []byte {
	t.Helper()
	reader, err := Decompressor{}.Decompress(compressed)
	require.NoError(t, err)
	decompressed, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	return decompressed
}

// A writer can be closed twice: archive/tar.Writer reports success when it is
// closed again, so CascadeWriteCloser goes on to close the compressing writer
// underneath a second time. The encoder must reach the pool only once, or two
// later writers share it and one stream lands in the other's destination.
func TestCloseTwiceKeepsEncodersIndependent(t *testing.T) {
	var closedTwice bytes.Buffer
	first := Compressor{}.NewWriter(&closedTwice)
	_, err := first.Write([]byte("first stream"))
	require.NoError(t, err)
	require.NoError(t, first.Close())
	require.NoError(t, first.Close())

	payloads := [][]byte{
		bytes.Repeat([]byte("aaaa"), 1<<14),
		bytes.Repeat([]byte("bbbb"), 1<<14),
	}
	buffers := make([]bytes.Buffer, len(payloads))
	writers := make([]io.WriteCloser, len(payloads))
	for i := range writers {
		writers[i] = Compressor{}.NewWriter(&buffers[i])
	}
	for i := range writers {
		_, err := writers[i].Write(payloads[i])
		require.NoError(t, err, "writer %d", i)
	}
	for i := range writers {
		require.NoError(t, writers[i].Close(), "writer %d", i)
	}
	for i := range writers {
		assert.True(t, bytes.Equal(payloads[i], roundtrip(t, &buffers[i])),
			"writer %d did not get its own data back", i)
	}

	assert.Equal(t, "first stream", string(roundtrip(t, &closedTwice)))
}

// Tar parts are compressed concurrently, so pooled encoders must never be
// handed to two writers at once. Worth running under -race.
func TestConcurrentWritersProduceIndependentStreams(t *testing.T) {
	const writerCount = 8

	payloads := make([][]byte, writerCount)
	buffers := make([]bytes.Buffer, writerCount)
	errs := make([]error, writerCount)
	for i := range payloads {
		payloads[i] = bytes.Repeat([]byte{byte('a' + i)}, 1<<18)
	}

	var wg sync.WaitGroup
	for i := 0; i < writerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			writer := Compressor{}.NewWriter(&buffers[i])
			if _, err := writer.Write(payloads[i]); err != nil {
				errs[i] = err
				return
			}
			errs[i] = writer.Close()
		}()
	}
	wg.Wait()

	for i := range payloads {
		require.NoError(t, errs[i], "writer %d", i)
		assert.True(t, bytes.Equal(payloads[i], roundtrip(t, &buffers[i])),
			"writer %d did not get its own data back", i)
	}
}

// Each level keeps its own pool, so a configured Compressor cannot draw an
// encoder that was built for another level. A wrong-level encoder still
// produces a valid stream, which is why this compares the bytes against a
// reference rather than only checking that the roundtrip succeeds.
func TestLevelsDoNotShareEncoders(t *testing.T) {
	payload := benchPayload(1 << 16)
	levels := []zstd.EncoderLevel{zstd.SpeedFastest, zstd.SpeedBestCompression}

	want := make(map[zstd.EncoderLevel][]byte, len(levels))
	for _, level := range levels {
		var out bytes.Buffer
		encoder, err := zstd.NewWriter(&out, zstd.WithEncoderLevel(level))
		require.NoError(t, err)
		_, err = encoder.Write(payload)
		require.NoError(t, err)
		require.NoError(t, encoder.Close())
		want[level] = out.Bytes()
	}

	// The first round fills each pool, the second draws from it.
	for round := range 2 {
		for _, level := range levels {
			var out bytes.Buffer
			writer := Compressor{Level: level}.NewWriter(&out)
			_, err := writer.Write(payload)
			require.NoError(t, err)
			require.NoError(t, writer.Close())
			assert.True(t, bytes.Equal(want[level], out.Bytes()),
				"round %d: level %s produced the output of another level", round, level)
		}
	}
}

func BenchmarkCompressor(b *testing.B) {
	for _, size := range []int{1 << 20, 16 << 20} {
		payload := benchPayload(size)
		b.Run(fmt.Sprintf("%dMiB", size>>20), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(size))
			for i := 0; i < b.N; i++ {
				writer := Compressor{}.NewWriter(io.Discard)
				if _, err := writer.Write(payload); err != nil {
					b.Fatal(err)
				}
				if err := writer.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// After Close the encoder may already serve another writer, so a late Write or
// Flush must fail with the encoder's own closed error instead of reaching it.
func TestWriteAfterCloseDoesNotTouchNextStream(t *testing.T) {
	var first bytes.Buffer
	writer := Compressor{}.NewWriter(&first)
	_, err := writer.Write([]byte("first"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	var second bytes.Buffer
	next := Compressor{}.NewWriter(&second)
	_, err = writer.Write([]byte("intruder"))
	assert.ErrorIs(t, err, zstd.ErrEncoderClosed)
	assert.ErrorIs(t, writer.Flush(), zstd.ErrEncoderClosed)

	_, err = next.Write([]byte("second"))
	require.NoError(t, err)
	require.NoError(t, next.Close())
	assert.Equal(t, "second", string(roundtrip(t, &second)))
	assert.Equal(t, "first", string(roundtrip(t, &first)))
}

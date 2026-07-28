package zstd

import (
	"bytes"
	"io"
	"math/rand"
	"os"
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

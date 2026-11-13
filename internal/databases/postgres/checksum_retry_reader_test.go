package postgres

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
)

func checksumRetryPage(blockNo uint32) []byte {
	page := PgDatabasePage{}
	binary.LittleEndian.PutUint64(page[:8], 42)
	binary.LittleEndian.PutUint16(page[12:14], 24)
	binary.LittleEndian.PutUint16(page[14:16], uint16(DatabasePageSize))
	binary.LittleEndian.PutUint16(page[16:18], uint16(DatabasePageSize))
	binary.LittleEndian.PutUint16(page[18:20], uint16(DatabasePageSize)|4)
	sum := pgChecksumPage(blockNo, &page)
	binary.LittleEndian.PutUint16(page[8:10], sum)
	return append([]byte(nil), page[:DatabasePageSize]...)
}

func TestChecksumRetryReader(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		retries, succeedsAt, wantReads int
		corrupt                        bool
	}{
		{"healthy", 3, 0, 0, false},
		{"retry disabled", 0, 1, 0, true},
		{"first retry", 3, 1, 1, false},
		{"last retry", 3, 3, 3, false},
		{"exhausted", 3, 4, 3, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			good := checksumRetryPage(0)
			bad := bytes.Clone(good)
			bad[len(bad)-1] ^= 1
			initial := bad
			if tc.succeedsAt == 0 {
				initial = good
			}
			path := filepath.Join(t.TempDir(), "base", "123")
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
			require.NoError(t, os.WriteFile(path, good, 0600))
			info, err := os.Stat(path)
			require.NoError(t, err)
			reader, err := newChecksumRetryReader(t.Context(), path, info, io.NopCloser(bytes.NewReader(initial)), false, tc.retries)
			require.NoError(t, err)
			defer reader.Close()
			reads := 0
			reader.reread = func(blockNo uint32, page []byte) error {
				require.Equal(t, uint32(0), blockNo)
				reads++
				data := bad
				if reads >= tc.succeedsAt {
					data = good
				}
				copy(page, data)
				return nil
			}
			actual, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, tc.wantReads, reads)
			if tc.corrupt {
				require.Equal(t, []uint32{0}, reader.corruptBlocks)
				require.Equal(t, bad, actual)
			} else {
				require.Empty(t, reader.corruptBlocks)
				require.Equal(t, good, actual, "the output must contain the verified reread, including its checksum")
			}
		})
	}
}

func TestChecksumRetryReaderIncrement(t *testing.T) {
	// The increment's first page is block 7 in relation segment .1, not block 0.
	const blockNo uint32 = 7
	path := filepath.Join(t.TempDir(), "base", "123.1")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
	file, err := os.Create(path)
	require.NoError(t, err)
	good := checksumRetryPage(uint32(BlocksInRelFile) + blockNo)
	_, err = file.WriteAt(good, int64(blockNo)*DatabasePageSize)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	info, err := os.Stat(path)
	require.NoError(t, err)
	var header bytes.Buffer
	header.Write([]byte{'w', 'i', '1', SignatureMagicNumber})
	require.NoError(t, binary.Write(&header, binary.LittleEndian, uint64(info.Size())))
	require.NoError(t, binary.Write(&header, binary.LittleEndian, uint32(1)))
	require.NoError(t, binary.Write(&header, binary.LittleEndian, blockNo))
	bad := bytes.Clone(good)
	bad[len(bad)-1] ^= 1
	source := io.NopCloser(io.MultiReader(bytes.NewReader(header.Bytes()), bytes.NewReader(bad)))
	reader, err := newChecksumRetryReader(t.Context(), path, info, source, true, 2)
	require.NoError(t, err)
	defer reader.Close()
	actual, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, append(bytes.Clone(header.Bytes()), good...), actual)
	require.Empty(t, reader.corruptBlocks)
}

func TestChecksumRetryReaderErrors(t *testing.T) {
	for _, mode := range []string{"read error", "cancelled", "replaced", "truncated", "partial page"} {
		t.Run(mode, func(t *testing.T) {
			good := checksumRetryPage(0)
			bad := bytes.Clone(good)
			bad[len(bad)-1] ^= 1
			path := filepath.Join(t.TempDir(), "base", "123")
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
			require.NoError(t, os.WriteFile(path, good, 0600))
			info, err := os.Stat(path)
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			reader, err := newChecksumRetryReader(ctx, path, info, io.NopCloser(bytes.NewReader(bad)), false, 3)
			require.NoError(t, err)
			defer reader.Close()
			switch mode {
			case "read error":
				reader.reread = func(uint32, []byte) error { return io.ErrUnexpectedEOF }
			case "cancelled":
				reader.reread = func(_ uint32, page []byte) error { cancel(); copy(page, bad); return nil }
			case "replaced":
				replacement := path + ".new"
				require.NoError(t, os.WriteFile(replacement, good, 0600))
				require.NoError(t, os.Rename(replacement, path))
			case "truncated":
				require.NoError(t, os.Truncate(path, 0))
			case "partial page":
				require.NoError(t, os.Truncate(path, DatabasePageSize/2))
			}
			actual, err := io.ReadAll(reader)
			if mode == "truncated" {
				require.NoError(t, err)
				require.Equal(t, make([]byte, DatabasePageSize), actual)
				require.Empty(t, reader.corruptBlocks)
				return
			}
			require.Error(t, err)
			require.Empty(t, actual, "a failed reread must not release a page into the archive")
			if mode == "cancelled" {
				require.ErrorIs(t, err, context.Canceled)
			}
		})
	}
}

type checksumRetryTarBall struct {
	internal.TarBall
	writer *tar.Writer
}

func (t *checksumRetryTarBall) TarWriter() *tar.Writer { return t.writer }
func (t *checksumRetryTarBall) AddSize(int64)          {}

type checksumRepairingReader struct {
	io.ReadCloser
	repair func()
}

func (r *checksumRepairingReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if r.repair != nil && n > 0 {
		r.repair()
		r.repair = nil
	}
	return n, err
}

func TestPackFileWithChecksumRetries(t *testing.T) {
	for _, directIO := range []bool{false, true} {
		for _, repair := range []bool{false, true} {
			t.Run(fmt.Sprintf("directIO=%t/repair=%t", directIO, repair), func(t *testing.T) {
				old := viper.Get(conf.DirectIO)
				viper.Set(conf.DirectIO, directIO)
				t.Cleanup(func() { viper.Set(conf.DirectIO, old) })
				path := filepath.Join(t.TempDir(), "base", "123")
				good := checksumRetryPage(0)
				bad := bytes.Clone(good)
				bad[len(bad)-1] ^= 1
				require.NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
				require.NoError(t, os.WriteFile(path, bad, 0600))
				info, err := os.Stat(path)
				require.NoError(t, err)
				cfi := internal.NewComposeFileInfo(path, info, false, false,
					&tar.Header{Name: "/base/123", Mode: 0600, Size: info.Size()})
				files := &internal.RegularBundleFiles{}
				packer := NewTarBallFilePacker(nil, nil, files, NewTarBallFilePackerOptions(true, false))
				source, err := packer.createFileReadCloser(t.Context(), cfi)
				require.NoError(t, err)
				if repair {
					source = &checksumRepairingReader{ReadCloser: source, repair: func() {
						require.NoError(t, os.WriteFile(path, good, 0600))
					}}
				}
				var archive bytes.Buffer
				writer := tar.NewWriter(&archive)
				require.NoError(t, packer.packFileWithChecksumRetries(t.Context(), cfi, &checksumRetryTarBall{writer: writer}, source, 3))
				require.NoError(t, writer.Close())
				reader := tar.NewReader(&archive)
				_, err = reader.Next()
				require.NoError(t, err)
				actual, err := io.ReadAll(reader)
				require.NoError(t, err)
				description, ok := files.Load(cfi.Header.Name)
				require.True(t, ok)
				corruption := description.(internal.BackupFileDescription).CorruptBlocks
				if repair {
					require.Equal(t, good, actual)
					require.Nil(t, corruption)
				} else {
					require.Equal(t, bad, actual)
					require.Equal(t, &internal.CorruptBlocksInfo{CorruptBlocksCount: 1, SomeCorruptBlocks: []uint32{0}}, corruption)
				}
			})
		}
	}
}

func TestChecksumRetryReaderRejectsUnverifiableReread(t *testing.T) {
	for _, mode := range []string{"invalid header", "zero checksum", "new page header"} {
		t.Run(mode, func(t *testing.T) {
			good := checksumRetryPage(0)
			bad := bytes.Clone(good)
			bad[len(bad)-1] ^= 1
			path := filepath.Join(t.TempDir(), "base", "123")
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
			require.NoError(t, os.WriteFile(path, good, 0600))
			info, err := os.Stat(path)
			require.NoError(t, err)
			reader, err := newChecksumRetryReader(t.Context(), path, info, io.NopCloser(bytes.NewReader(bad)), false, 3)
			require.NoError(t, err)
			defer reader.Close()
			reads := 0
			reader.reread = func(_ uint32, page []byte) error {
				reads++
				copy(page, good)
				switch mode {
				case "invalid header":
					binary.LittleEndian.PutUint16(page[12:14], 0)
				case "zero checksum":
					binary.LittleEndian.PutUint16(page[8:10], 0)
				case "new page header":
					binary.LittleEndian.PutUint16(page[14:16], 0)
				}
				return nil
			}
			_, err = io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, 3, reads)
			require.Equal(t, []uint32{0}, reader.corruptBlocks)
		})
	}
}

func TestChecksumRetryLimitIsPerPage(t *testing.T) {
	good := append(checksumRetryPage(0), checksumRetryPage(1)...)
	bad := bytes.Clone(good)
	bad[DatabasePageSize-1] ^= 1
	bad[2*DatabasePageSize-1] ^= 1
	path := filepath.Join(t.TempDir(), "base", "123")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
	require.NoError(t, os.WriteFile(path, good, 0600))
	info, err := os.Stat(path)
	require.NoError(t, err)
	reader, err := newChecksumRetryReader(t.Context(), path, info, io.NopCloser(bytes.NewReader(bad)), false, 2)
	require.NoError(t, err)
	defer reader.Close()
	reads := make(map[uint32]int)
	reader.reread = func(block uint32, page []byte) error {
		reads[block]++
		source := bad
		if reads[block] == 2 {
			source = good
		}
		offset := int64(block) * DatabasePageSize
		copy(page, source[offset:offset+DatabasePageSize])
		return nil
	}
	actual, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, map[uint32]int{0: 2, 1: 2}, reads)
	require.Equal(t, good, actual)
	require.Empty(t, reader.corruptBlocks)
}

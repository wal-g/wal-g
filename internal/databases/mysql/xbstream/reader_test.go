package xbstream

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/testutils"
	"io"
	"testing"
)

func TestXBStreamReader(t *testing.T) {
	// echo "hello" > test1.txt
	// echo "world" > test2.txt
	// xbstream -c test1.txt test2.txt | hexdump -C
	var hexFile = `
	00000000  58 42 53 54 43 4b 30 31  00 50 09 00 00 00 74 65  |XBSTCK01.P....te|
	00000010  73 74 31 2e 74 78 74 06  00 00 00 00 00 00 00 00  |st1.txt.........|
	00000020  00 00 00 00 00 00 00 20  30 3a 36 68 65 6c 6c 6f  |....... 0:6hello|
	00000030  0a 58 42 53 54 43 4b 30  31 00 45 09 00 00 00 74  |.XBSTCK01.E....t|
	00000040  65 73 74 31 2e 74 78 74  58 42 53 54 43 4b 30 31  |est1.txtXBSTCK01|
	00000050  00 50 09 00 00 00 74 65  73 74 32 2e 74 78 74 06  |.P....test2.txt.|
	00000060  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 a8  |................|
	00000070  61 38 dd 77 6f 72 6c 64  0a 58 42 53 54 43 4b 30  |a8.world.XBSTCK0|
	00000080  31 00 45 09 00 00 00 74  65 73 74 32 2e 74 78 74  |1.E....test2.txt|`

	xbBytes := testutils.HexToBytes(hexFile)
	reader := NewReader(bytes.NewReader(xbBytes), true)

	// test1.txt
	chunk, err := reader.Next()
	assert.NoError(t, err)
	assert.Equal(t, ChunkHeader{
		Magic:      chunkMagic,
		Flags:      0,
		Type:       ChunkTypePayload,
		Path:       "test1.txt",
		PayloadLen: 6,
		Offset:     0,
		Checksum:   uint32(0x363a3020),
	}, chunk.ChunkHeader)

	payload, err := io.ReadAll(chunk.Reader)
	assert.NoError(t, err)
	assert.Equal(t, []byte{0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x0a}, payload)
	assert.Nil(t, chunk.ValidateCheckSum())

	// test1.txt EOF
	chunk, err = reader.Next()
	assert.NoError(t, err)
	assert.Equal(t, ChunkHeader{
		Magic:      chunkMagic,
		Flags:      0,
		Type:       ChunkTypeEOF,
		Path:       "test1.txt",
		PayloadLen: 0,
		Offset:     0,
		Checksum:   0,
	}, chunk.ChunkHeader)

	assert.Nil(t, chunk.Reader)
	assert.Nil(t, chunk.ValidateCheckSum())

	// test2.txt
	chunk, err = reader.Next()
	assert.NoError(t, err)
	assert.Equal(t, ChunkHeader{
		Magic:      chunkMagic,
		Flags:      0,
		Type:       ChunkTypePayload,
		Path:       "test2.txt",
		PayloadLen: 6,
		Offset:     0,
		Checksum:   uint32(0xdd3861a8),
	}, chunk.ChunkHeader)

	payload, err = io.ReadAll(chunk.Reader)
	assert.NoError(t, err)
	assert.Equal(t, []byte{0x77, 0x6f, 0x72, 0x6c, 0x64, 0x0a}, payload)
	assert.Nil(t, chunk.ValidateCheckSum())

	// test2.txt EOF
	chunk, err = reader.Next()
	assert.NoError(t, err)
	assert.Equal(t, ChunkHeader{
		Magic:      chunkMagic,
		Flags:      0,
		Type:       ChunkTypeEOF,
		Path:       "test2.txt",
		PayloadLen: 0,
		Offset:     0,
		Checksum:   0,
	}, chunk.ChunkHeader)

	assert.Nil(t, chunk.Reader)
	assert.Nil(t, chunk.ValidateCheckSum())

	_, err = reader.Next()
	assert.Equal(t, err, io.EOF)
}

func TestXBStreamReader_with_sparse(t *testing.T) {
	// There is no way to create sparse file with xbstream CLI - only during backup of database with compressed tables
	// Here an example from test db
	var hexFile = `
		00c701e1  58 42 53 54 43 4b 30 31  00 53 10 00 00 00 73 75  |XBSTCK01.S....su|
		00c701f1  70 65 72 64 62 2f 74 65  73 74 2e 69 62 64 02 00  |perdb/test.ibd..|
		00c70201  00 00 b8 80 01 00 00 00  00 00 00 00 00 00 00 00  |................|
		00c70211  00 00 85 fe 70 73 00 00  00 00 b8 00 01 00 48 3f  |....ps........H?|
		00c70221  00 00 00 80 00 00 a3 83  56 17 00 00 00 00 00 01  |........V.......|
		00c70231  38 a3 00 00 00 01 00 00  00 00 00 00 00 00 00 00  |8...............|
		00c70241  00 00 00 00 00 00 00 00  00 00 00 09 00 00 00 09  |................|
		00c70251  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 21  |...............!|
		00c70261  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |................|
		*
		00c74211  00 00 00 00 00 00 00 00  00 00 00 00 00 00 a3 83  |................|
		00c74221  56 17 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |V...............|
		00c74231  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |................|
		*
		00c80221  00 00 00 00 00 00 a2 ff  75 33 00 00 00 04 ff ff  |........u3......|
		00c80231  ff ff ff ff ff ff 00 00  00 00 2a 4f 36 38 00 0e  |..........*O68..|
		00c80241  02 01 45 bf 3f da 00 92  00 00 00 09 78 9c ed cd  |..E.?.......x...|
		00c80251  31 0e 01 41 14 06 e0 37  ab 12 cd 1e 80 0b 68 25  |1..A...7......h%|
		00c80261  e2 16 ce b0 11 12 05 11  b2 b5 75 06 95 56 a7 15  |..........u..V..|
		00c80271  67 d2 3b c1 9a 2d 34 dc  40 be 2f 79 33 ef 25 33  |g.;..-4.@./y3.%3|
		00c80281  ff 8b 22 2e 4d 2f 3a c7  28 f2 99 e2 db 2d 57 3f  |..".M/:.(....-W?|
		00c80291  57 51 ec 3f 5d ba a7 fc  78 b8 de ae d6 9b 7a 93  |WQ.?]...x.....z.|
		00c802a1  db 41 c4 a1 de ed 97 79  ea c2 ca f6 d5 fd ec f2  |.A.....y........|
		00c802b1  62 74 7e 9e f2 f5 48 65  93 d3 ab aa fa 59 00 00  |bt~...He.....Y..|
		00c802c1  00 00 00 00 00 00 00 00  00 00 00 00 00 00 7f 66  |...............f|
		00c802d1  17 8b 6b 5b 4f c6 f3 e9  ec 0d df e6 14 ae 00 00  |..k[O...........|
		00c802e1  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |................|
		*
		00c882d1  00 00 00 00 00 00 00 00  00 00 00 00 00 00 58 42  |..............XB|
		00c882e1  53 54 43 4b 30 31 00 45  10 00 00 00 73 75 70 65  |STCK01.E....supe|
		00c882f1  72 64 62 2f 74 65 73 74  2e 69 62 64              |rdb/test.ibd|`

	xbBytes := testutils.HexToBytes(hexFile)
	reader := NewReader(bytes.NewReader(xbBytes), true)

	// superdb/test.ibd
	chunk, err := reader.Next()
	assert.NoError(t, err)
	assert.Equal(t, ChunkHeader{
		Magic:      chunkMagic,
		Flags:      0,
		Type:       ChunkTypeSparse,
		Path:       "superdb/test.ibd",
		PayloadLen: 0x000180b8, // 98488
		Offset:     0,
		Checksum:   uint32(0x7370fe85),
		SparseMap: []SparseChunk{
			{SkipBytes: 0x00000000, WriteBytes: 0x000100b8},
			{SkipBytes: 0x00003f48, WriteBytes: 0x00008000},
		},
	}, chunk.ChunkHeader)

	payload, err := io.ReadAll(chunk.Reader)
	assert.NoError(t, err)
	assert.Equal(t, 0x000180b8, len(payload))
	assert.Nil(t, chunk.ValidateCheckSum())

	// superdb/test.ibd EOF
	chunk, err = reader.Next()
	assert.NoError(t, err)
	assert.Equal(t, ChunkHeader{
		Magic:      chunkMagic,
		Flags:      0,
		Type:       ChunkTypeEOF,
		Path:       "superdb/test.ibd",
		PayloadLen: 0,
		Offset:     0,
		Checksum:   0,
	}, chunk.ChunkHeader)

	assert.Nil(t, chunk.Reader)
	assert.Nil(t, chunk.ValidateCheckSum())

	_, err = reader.Next()
	assert.Equal(t, err, io.EOF)
}

package walg

import (
	"archive/tar"
	"bytes"
	"encoding/binary"
	"github.com/rasky/go-lzo"
	"io"
	"math/rand"
	"net/http"
	"regexp"
	"strconv"
	"sync/atomic"
	"time"
)

var counter int32

const LzopPrefix = "\x89\x4c\x5a\x4f\x00\x0d\x0a\x1a\x0a\x10\x30\x20\xa0\x09\x40" +
	"\x01\x05\x03\x00\x00\x01\x00\x00\x81\xa4\x59\x43\x06\xd0\x00" +
	"\x00\x00\x00\x06\x70\x32\x2e\x74\x61\x72\x51\xf8\x06\x08"

const LzopBlockSize = 256 * 1024

type StrideByteReader struct {
	stride    int
	counter   int
	randBytes []byte
}

type LzopReader struct {
	Uncompressed io.Reader
	slice        []byte
	err          error
}

func (lz *LzopReader) Read(p []byte) (n int, err error) {
	if len(lz.slice) == 0 {
		if lz.err == nil {
			lz.slice = make([]byte, LzopBlockSize+12)
			sum := 12
			i := 0
			for {
				if sum >= len(lz.slice) {
					break
				}

				i, lz.err = lz.Uncompressed.Read(lz.slice[sum:])
				sum += i

				if lz.err != nil {
					break
				}

			}

			lz.slice = lz.slice[:sum]

			out := lzo.Compress1X(lz.slice[12:])

			if (len(lz.slice) - 12) <= len(out) {
				binary.BigEndian.PutUint32(lz.slice[:4], uint32(sum-12))
				binary.BigEndian.PutUint32(lz.slice[4:8], uint32(sum-12))
			} else {
				binary.BigEndian.PutUint32(lz.slice[:4], uint32(len(lz.slice)-12))
				binary.BigEndian.PutUint32(lz.slice[4:8], uint32(len(out)))
				copy(lz.slice[12:], out)

				lz.slice = lz.slice[:len(out)+12]
			}

			binary.BigEndian.PutUint32(lz.slice[8:12], 0xFFFFFFFF)
		} else {
			return 0, lz.err
		}
	}

	n = copy(p, lz.slice)
	lz.slice = lz.slice[n:]
	return n, nil
}

func NewStrideByteReader(s int) *StrideByteReader {
	sb := StrideByteReader{
		stride:    s,
		randBytes: make([]byte, s),
	}

	rand.Seed(0)
	//rand.Seed(time.Now().UTC().UnixNano())
	rand.Read(sb.randBytes)
	return &sb
}

func (sb *StrideByteReader) Read(p []byte) (int, error) {
	l := len(sb.randBytes)

	n := 0
	for start := 0; start < len(p); n = copy(p[start:], sb.randBytes[sb.counter:]) {
		sb.counter = (sb.counter + n) % l
		start += n
	}

	return len(p), nil
}

func CreateTar(w io.Writer, r *io.LimitedReader) {
	//defer TimeTrack(time.Now(), "CREATE TAR")
	counter = atomic.AddInt32(&counter, 1)
	tw := tar.NewWriter(w)

	hdr := &tar.Header{
		Name: strconv.Itoa(int(counter)),
		Size: int64(r.N),
		Mode: 0600,
	}

	if err := tw.WriteHeader(hdr); err != nil {
		panic(err)
	}

	if _, err := io.Copy(tw, r); err != nil {
		panic(err)
	}

	if err := tw.Close(); err != nil {
		panic(err)
	}

}

func Handler(w http.ResponseWriter, r *http.Request) {
	matcher := regexp.MustCompile("/stride-(\\d+).bytes-(\\d+).tar(.lzo)?")
	str := matcher.FindStringSubmatch(r.URL.Path)
	stride, err := strconv.Atoi(str[1])

	if err != nil {
		panic(err)
	}

	nBytes, err := strconv.Atoi(str[2])
	if err != nil {
		panic(err)
	}

	lzoFlag := str[3]

	sb := NewStrideByteReader(stride)
	lr := io.LimitedReader{sb, int64(nBytes)}

	defer TimeTrack(time.Now(), "HANDLER")

	switch lzoFlag {
	case "":
		CreateTar(w, &lr)
	case ".lzo":
		io.Copy(w, bytes.NewBufferString(LzopPrefix))

		pr, pw := io.Pipe()

		go func() {
			CreateTar(pw, &lr)
			defer pw.Close()
		}()

		compressedReader := LzopReader{Uncompressed: pr}

		io.Copy(w, &compressedReader)
		n, err := w.Write(make([]byte, 12))
		if n != 12 {
			panic("Did not write empty signal bytes.")
		}
		if err != nil {
			panic(err)
		}
	default:
		panic("bug")
	}
}

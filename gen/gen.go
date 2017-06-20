package main

import (
	"archive/tar"
	_ "bytes"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
)

type StrideByteReader struct {
	stride    int
	counter   int
	randBytes []byte
}

func newStrideByteReader(s int) *StrideByteReader {
	sb := StrideByteReader{
		stride:    s,
		randBytes: make([]byte, s),
	}
	rand.Read(sb.randBytes)
	return &sb
}

func (sb *StrideByteReader) Read(p []byte) (n int, err error) {

	for i := 0; i < len(p); i++ {
		p[i] = sb.randBytes[sb.counter]
		sb.counter = (sb.counter + 1) % len(sb.randBytes)
	}

	return len(p), nil
}

func createTar(w http.ResponseWriter, r *io.LimitedReader) {
	tw := tar.NewWriter(w)

	hdr := &tar.Header{
		Name: "test",
		Size: int64(r.N),
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

// func readTar(b *bytes.Buffer) (a []int64){
// 	r := bytes.NewReader(b.Bytes())
// 	tr := tar.NewReader(r)
// 	//fmt.Println("READER: ", tr)
// 	archive := make([]int64, 10)
// 	counter := 0

// 	for {
// 		hdr, err := tr.Next()
// 		//fmt.Println("HEADER: ", hdr.Size)
// 		if err == io.EOF {
// 			break
// 		}
// 		if err != nil {
// 			panic(err)
// 		}

// 		archive[counter] = hdr.Size
// 		counter++
// 	}
// 	return archive
// }

func handler(w http.ResponseWriter, r *http.Request) {
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

	sb := newStrideByteReader(stride)
	lr := io.LimitedReader{sb, int64(nBytes)}

	createTar(w, &lr)

	fmt.Fprintf(w, "LZO: %s\n", lzoFlag)
	fmt.Fprintf(w, "Type of lzoFlag: %s\n", reflect.TypeOf(lzoFlag))
	fmt.Fprintf(w, "Length of s: %d\n", len(str))
	fmt.Fprintf(w, "Vector: %s\n", str)

	//fmt.Fprintf(w, "Data: %v\n", v)

}

func main() {

	http.HandleFunc("/", handler)
	http.ListenAndServe("localhost:8080", nil)
}

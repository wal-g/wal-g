package tools

// import (
// 	"fmt"
// 	"os"
// 	"path/filepath"
// 	//"runtime"
// 	"io"
// 	"time"
// )

// type Empty struct{}

// func (bb *BufTarBall) divide(bundle *Bundle) {
// 	defer TimeTrack(time.Now(), "DIVIDE")
// 	tarMember := bb.Buffer().Bytes()
// 	total := len(tarMember)
// 	chunks := total / bb.byteInterval
// 	remainder := total % bb.byteInterval

// 	// fmt.Println("CHUNKS:", chunks)
// 	// fmt.Println("REMAINDER:", remainder)

// 	concurrency := 50
// 	sem := make(chan Empty, concurrency)

// 	for i := 0; i < concurrency; i++ {
// 		sem <- Empty{}
// 	}

// 	done := make(chan bool)
// 	wait := make(chan bool)

// 	go func() {
// 		for i := 0; i < chunks; i++ {
// 			<-done
// 			sem <- Empty{}
// 		}
// 		wait <- true
// 	}()

// 	for i := 0; i < chunks; i++ {
// 		<-sem
// 		go func(i int) {
// 			c := make([]byte, bb.byteInterval)
// 			c = tarMember[i*bb.byteInterval : (i+1)*bb.byteInterval]
// 			n := filepath.Join(bb.Out(), "part_"+fmt.Sprintf("%0.3d", bb.number)+"_"+fmt.Sprintf("%0.9d", i)+".tar.lz4")

// 			bz := &S3LzWriter{
// 				chunk: c,
// 				name:  n,
// 			}
// 			bz.Compress()
// 			bundle.Upload(bz)
// 			//writeToFile(bz.pr, bz.name)

// 			/*** FILE COMPRESSOR ***/
// 			// fz := &FileLzWriter{
// 			// 	chunk: c,
// 			// 	name:  n,
// 			// }
// 			// fz.Compress()

// 			//writeToFile(chunk, name)
// 			//fmt.Println(chunk)

// 			done <- true
// 		}(i)

// 	}
// 	//num := runtime.NumGoroutine()
// 	//fmt.Println("GO ROUTINES:", num)
// 	<-wait

// 	if remainder != 0 {
// 		c := tarMember[chunks*bb.byteInterval:]
// 		n := filepath.Join(bb.Out(), "part_"+fmt.Sprintf("%0.3d", bb.number)+"_"+fmt.Sprintf("%0.9d", chunks)+".tar.lz4")

// 		bz := &S3LzWriter{
// 			chunk: c,
// 			name:  n,
// 		}
// 		bz.Compress()
// 		bundle.Upload(bz)
// 		//writeToFile(bz.pr, bz.name)

// 		/*** FILE COMPRESSOR ***/
// 		// fz := &FileLzWriter{
// 		// 	chunk: c,
// 		// 	name:  n,
// 		// }
// 		// fz.Compress()

// 		//writeToFile(chunk, name)
// 		//fmt.Println(chunk)

// 	}

// }

// func writeToFile(r io.Reader, name string) {
// 	f, err := os.Create(name)
// 	if err != nil {
// 		panic(err)
// 	}

// 	_, err = io.Copy(f, r)
// 	if err != nil {
// 		panic(err)
// 	}

// 	f.Close()
// }

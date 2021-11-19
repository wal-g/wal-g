package splitmerge

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"math/rand"
	"testing"
	"golang.org/x/sync/errgroup"

)

//               ┌─> copy data per 1 byte    ─>┐
// data ─> split ├─> copy data per ... bytes ─>├─> merge
//               └─> copy data per 42 bytes  ─>┘
func TestSplitMerge(t *testing.T) {
	const blockSize = 128
	const dataSize = 115249 // some prime number
	var bufSizes = []int{1, blockSize + 1, blockSize - 1, 2*blockSize + 1, 4, 8, 15, 16, 23, 42}
	var partitions = len(bufSizes)

	// in:
	inputData := generateDataset(dataSize)
	dataReader := bytes.NewReader(inputData)
	var readers = SplitReader(dataReader, partitions, blockSize)

	// out:
	var sink bytes.Buffer
	writers := MergeWriter(&sink, partitions, blockSize)

	errGroup := new(errgroup.Group)
	for i := 0; i < partitions; i++ {
		//idx := i
		reader := readers[i]
		writer := writers[i]
		buffSize:= bufSizes[i%len(bufSizes)]
		
		errGroup.Go(func() error {
			defer writer.Close()
			// read _all_ data first and only then send it to MergeWriter:
			allData, err := io.ReadAll(reader)
			if err != nil {
				return err
			}
			
			offset := 0
			for {
				data := make([]byte, buffSize, buffSize)
				rbytes := copy(data, allData[offset:])
				offset += rbytes
				//tracelog.InfoLogger.Printf("goroutine #%d: %d bytes fetched, err=%v", idx, rbytes, rerr)
				if rbytes == 0 {
					return nil
				}
				_, werr := writer.Write(data[:rbytes])
				if werr != nil {
					return werr
				} else {
					//tracelog.InfoLogger.Printf("goroutine #%d: %d bytes copied", idx, rbytes)
				}
			}
		})
	}

	// Wait for upload finished:
	assert.NoError(t, errGroup.Wait())
	
	fmt.Printf("%d\n", len(inputData))
	fmt.Printf("%d\n", sink.Len())

	assert.ElementsMatch(t, inputData, sink.Bytes())
}

func generateDataset(size int) []byte {
	result := make([]byte, size, size)
	rand.Read(result)
	return result
}

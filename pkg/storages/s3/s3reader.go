package s3

import (
	"fmt"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"hash/fnv"
	"io"
	"math"
	"math/rand"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

var (
	DebugLogBufferCounter = 0
)

type s3Reader struct {
	lastBody      io.ReadCloser
	folder        *Folder
	maxRetries    int
	retryNum      int
	objectPath    string
	storageCursor int64
	maxRetryDelay time.Duration
	minRetryDelay time.Duration
	reconnectId   int
	to            int64
	logDebugId    string // hash from filename and logDebugId - unique logDebugId used only for debug
}

func (reader *s3Reader) getObjectRange(from, to int64) (*s3.GetObjectOutput, error) {
	// FIXME: use ' ?partNumber=N' ??
	// Typical sizes for byte-range requests are 8 MB or 16 MB. If objects are PUT using a multipart upload,
	// it’s a good practice to GET them in the same part sizes (or at least aligned to part boundaries) for
	// best performance. GET requests can directly address individual parts; for example, GET ?partNumber=N.
	bytesRange := fmt.Sprintf("bytes=%d-", from)
	if to != 0 {
		bytesRange += strconv.Itoa(int(to))
	}
	input := &s3.GetObjectInput{
		Bucket: reader.folder.Bucket,
		Key:    aws.String(reader.objectPath),
		Range:  aws.String(bytesRange),
	}
	reader.debugLog("GetObject with range %s", bytesRange)
	return reader.folder.S3API.GetObject(input)
}

func (reader *s3Reader) Read(p []byte) (n int, err error) {
	reconnect := false
	if reader.lastBody == nil { // initial connect, if lastBody wasn't provided
		reconnect = true
	}
	for {
		if reconnect {
			reconnect = false
			connErr := reader.reconnect()
			if connErr != nil {
				reader.debugLog("reconnect failed %s", connErr)
				return 0, connErr
			}
		}

		n, err = reader.lastBody.Read(p)
		if err != nil && err != io.EOF {
			reader.debugLog("read(%d) to cursor(%d) failed: %+v", n, reader.storageCursor, err)
			reconnect = true
			continue
		}
		reader.storageCursor += int64(n)
		return n, err
	}
}

func (reader *s3Reader) getDebugLogLine(format string, v ...interface{}) string {
	prefix := fmt.Sprintf("s3Reader [%s] ", reader.logDebugId)
	message := fmt.Sprintf(format, v...)
	return prefix + message
}

func (reader *s3Reader) debugLog(format string, v ...interface{}) {
	tracelog.DebugLogger.Print(reader.getDebugLogLine(format, v...))
}

func (reader *s3Reader) reconnect() error {
	failed := 0

	for {
		reader.reconnectId++
		object, err := reader.getObjectRange(reader.storageCursor, reader.to)
		if err != nil {
			failed += 1
			reader.debugLog("reconnect failed [%d/%d]: %s", failed, reader.maxRetries, err)
			if failed >= reader.maxRetries {
				return errors.Wrap(err, reader.getDebugLogLine("Too much reconnecting retries"))
			}
			sleepTime := reader.getIncrSleep(failed)
			reader.debugLog("sleep: %s", sleepTime)
			time.Sleep(sleepTime)
			continue
		}
		failed = 0
		if reader.lastBody != nil {
			err = reader.lastBody.Close()
			if err != nil {
				msg := reader.getDebugLogLine("We have problems with closing previous connection")
				tracelog.DebugLogger.Print(msg)
				return errors.Wrap(err, msg)
			}
		}
		reader.lastBody = object.Body
		reader.debugLog("reconnect #%d succeeded", reader.reconnectId)
		break
	}
	return nil
}

// THIS COde stolen from s3 lib, from vendor/github.com/aws/aws-sdk-go/aws/client/default_retryer.go
// func (d DefaultRetryer) RetryRules( .. ) time.Duration
// this calculate sleep duration (jitter and exponential backoff)
func (reader *s3Reader) getIncrSleep(retryCount int) time.Duration {
	minDelay := reader.minRetryDelay
	maxDelay := reader.maxRetryDelay
	var delay time.Duration

	actualRetryCount := int(math.Log2(float64(minDelay))) + 1
	if actualRetryCount < 63-retryCount {
		delay = time.Duration(1<<uint64(retryCount)) * getJitterDelay(minDelay)
		if delay > maxDelay {
			delay = getJitterDelay(maxDelay / 2)
		}
	} else {
		delay = getJitterDelay(maxDelay / 2)
	}
	return delay
}

func (reader *s3Reader) Close() (err error) {
	return reader.lastBody.Close()
}

func NewS3Reader(body io.ReadCloser, objectPath string, retriesCount int, folder *Folder,
	minRetryDelay, maxRetryDelay time.Duration) *s3Reader {

	DebugLogBufferCounter++
	reader := &s3Reader{
		lastBody:      body,
		objectPath:    objectPath,
		maxRetries:    retriesCount,
		logDebugId:    getHash(objectPath, DebugLogBufferCounter),
		folder:        folder,
		minRetryDelay: minRetryDelay,
		maxRetryDelay: maxRetryDelay,
	}

	reader.debugLog("Init s3reader path %s", objectPath)
	return reader
}

func NewS3RangeReader(objectPath string, retriesCount int, folder *Folder,
	minRetryDelay, maxRetryDelay time.Duration, offset, size int64) *s3Reader {
	DebugLogBufferCounter++
	reader := &s3Reader{
		objectPath:    objectPath,
		maxRetries:    retriesCount,
		logDebugId:    getHash(objectPath, DebugLogBufferCounter),
		folder:        folder,
		minRetryDelay: minRetryDelay,
		maxRetryDelay: maxRetryDelay,
		storageCursor: offset,            // aka from
		to:            offset + size - 1, // rfc2616: the byte positions specified are inclusive.
		// Byte offsets start at zero.
	}

	reader.debugLog("Init s3reader path %s", objectPath)
	return reader
}

func NewS3ConcurrentReader(objectPath string, retriesCount int, folder *Folder, minRetryDelay, maxRetryDelay time.Duration, totalSize int64, concurrencyFactor int) io.ReadCloser {
	in := make(chan storage.Runnable, 0)
	out := make(chan []byte, 0)
	storage.Process(in, out, concurrencyFactor)

	go func() {
		// generate Readers:
		chunkSize := int64(64 * 1024 * 1024)
		chunksCnt := int(totalSize / chunkSize)
		if totalSize%chunkSize != 0 {
			chunksCnt = chunksCnt + 1
		}
		for i := 0; i < chunksCnt; i++ {
			i := i // save `i` to function closure
			in <- func() []byte {
				DebugLogBufferCounter++ // FIXME: WTF?
				reader := NewS3RangeReader(objectPath, retriesCount, folder, minRetryDelay, maxRetryDelay,
					int64(i)*chunkSize, chunkSize)

				data, err := _ReadAll(reader, int(chunkSize))
				tracelog.ErrorLogger.Printf("NewS3ConcurrentReader range read finished: %d-%d", int64(i)*chunkSize, reader.to)
				if err != nil {
					tracelog.ErrorLogger.Printf("%v", err)
				}
				return data
			}
		}
		close(in)
	}()
	return storage.NewChannelReader(out)
}

func _ReadAll(r io.Reader, initSize int) ([]byte, error) {
	b := make([]byte, 0, initSize)
	for {
		if len(b) == cap(b) {
			// Add more capacity (let append pick how much).
			b = append(b, 0)[:len(b)]
		}
		n, err := r.Read(b[len(b):cap(b)])
		b = b[:len(b)+n]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return b, err
		}
	}
}

func getHash(objectPath string, id int) string {
	hash := fnv.New32a()
	_, err := hash.Write([]byte(objectPath))
	tracelog.ErrorLogger.FatalfOnError("Fatal, can't write buffer to hash %v", err)

	return fmt.Sprintf("%x_%d", hash.Sum32(), id)
}

// getJitterDelay returns a jittered delay for retry
func getJitterDelay(duration time.Duration) time.Duration {
	return time.Duration(rand.Int63n(int64(duration)) + int64(duration))
}

package s3

import (
	"fmt"
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

const (
	minRetryDelay = 30 * time.Millisecond
	maxRetryDelay = 300 * time.Second
)

var DebugLogBufferCounter = 0

type RangeReader struct {
	lastBody      io.ReadCloser
	folder        *Folder
	maxRetries    int
	objectPath    string
	storageCursor int64
	reconnectID   int
	logDebugID    string // hash from filename and logDebugID - unique logDebugID used only for debug
}

func (reader *RangeReader) getObjectRange(from, to int64) (*s3.GetObjectOutput, error) {
	bytesRange := fmt.Sprintf("bytes=%d-", from)
	if to != 0 {
		bytesRange += strconv.Itoa(int(to))
	}
	input := &s3.GetObjectInput{
		Bucket: reader.folder.bucket,
		Key:    aws.String(reader.objectPath),
		Range:  aws.String(bytesRange),
	}
	reader.debugLog("GetObject with range %s", bytesRange)
	return reader.folder.s3API.GetObject(input)
}

func (reader *RangeReader) Read(p []byte) (n int, err error) {
	reconnect := false
	if reader.lastBody == nil { // initial connect, if lastBody wasn't provided
		reconnect = true
	}
	for {
		if reconnect {
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
func (reader *RangeReader) getDebugLogLine(format string, v ...interface{}) string {
	prefix := fmt.Sprintf("RangeReader [%s] ", reader.logDebugID)
	message := fmt.Sprintf(format, v...)
	return prefix + message
}

func (reader *RangeReader) debugLog(format string, v ...interface{}) {
	tracelog.DebugLogger.Print(reader.getDebugLogLine(format, v...))
}

func (reader *RangeReader) reconnect() error {
	failed := 0

	for {
		reader.reconnectID++
		object, err := reader.getObjectRange(reader.storageCursor, 0)
		if err != nil {
			failed++
			reader.debugLog("reconnect failed [%d/%d]: %s", failed, reader.maxRetries, err)
			if failed >= reader.maxRetries {
				return errors.Wrap(err, reader.getDebugLogLine("Too much reconnecting retries"))
			}
			sleepTime := reader.getIncrSleep(failed)
			reader.debugLog("sleep: %s", sleepTime)
			time.Sleep(sleepTime)
			continue
		}
		if reader.lastBody != nil {
			err = reader.lastBody.Close()
			if err != nil {
				msg := reader.getDebugLogLine("We have problems with closing previous connection")
				tracelog.DebugLogger.Print(msg)
				return errors.Wrap(err, msg)
			}
		}
		reader.lastBody = object.Body
		reader.debugLog("reconnect #%d succeeded", reader.reconnectID)
		break
	}
	return nil
}

// THIS COde stolen from s3 lib, from vendor/github.com/aws/aws-sdk-go/aws/client/default_retryer.go
// func (d DefaultRetryer) RetryRules( .. ) time.Duration
// this calculate sleep duration (jitter and exponential backoff)
func (reader *RangeReader) getIncrSleep(retryCount int) time.Duration {
	minDelay := minRetryDelay
	maxDelay := maxRetryDelay
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

func (reader *RangeReader) Close() (err error) {
	return reader.lastBody.Close()
}

func NewRangeReader(body io.ReadCloser, objectPath string, retriesCount int, folder *Folder) *RangeReader {
	DebugLogBufferCounter++
	reader := &RangeReader{
		lastBody:   body,
		objectPath: objectPath,
		maxRetries: retriesCount,
		logDebugID: getHash(objectPath, DebugLogBufferCounter),
		folder:     folder,
	}

	reader.debugLog("Init s3reader path %s", objectPath)
	return reader
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

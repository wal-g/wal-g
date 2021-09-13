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
	logDebugId    string // hash from filename and logDebugId - unique logDebugId used only for debug
}

func (reader *s3Reader) getObjectRange(from, to int64) (*s3.GetObjectOutput, error) {
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
	reader.debugLog("Read to buffer [%d] bytes", len(p))
	reconnect := false
	if reader.lastBody == nil { // initial connect
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
		reader.debugLog("read %d, err %s", n, err)
		if err != nil && err != io.EOF {
			reconnect = true
			continue
		}
		reader.storageCursor += int64(n)
		reader.debugLog("success read")
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
		object, err := reader.getObjectRange(reader.storageCursor, 0)
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

func NewS3Reader(objectPath string, retriesCount int, folder *Folder,
	minRetryDelay, maxRetryDelay time.Duration) *s3Reader {

	DebugLogBufferCounter++
	reader := &s3Reader{objectPath: objectPath, maxRetries: retriesCount, logDebugId: getHash(objectPath, DebugLogBufferCounter),
		folder: folder, minRetryDelay: minRetryDelay, maxRetryDelay: maxRetryDelay}

	reader.debugLog("Init s3reader path %s", objectPath)
	return reader
}

func getHash(objectPath string, id int) string {
	hash := fnv.New32a()
	_, err := hash.Write([]byte(objectPath))
	tracelog.ErrorLogger.FatalfOnError("Fatal, can't write buffer to hash", err)

	return fmt.Sprintf("%x_%d", hash.Sum32(), id)
}

// getJitterDelay returns a jittered delay for retry
func getJitterDelay(duration time.Duration) time.Duration {
	return time.Duration(rand.Int63n(int64(duration)) + int64(duration))
}

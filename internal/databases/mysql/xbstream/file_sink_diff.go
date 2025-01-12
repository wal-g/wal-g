package xbstream

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
	"io"
	"os"
	"slices"
	"strings"

	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/databases/mysql/innodb"
	"github.com/wal-g/wal-g/internal/splitmerge"
)

type diffFileSink struct {
	file             *os.File
	dataDir          string
	incrementalDir   string
	meta             *diffMetadata
	readHere         io.ReadCloser
	writeHere        chan []byte
	fileCloseChan    chan struct{}
	spaceIDCollector innodb.SpaceIDCollector
	strategy         diffFileStrategy
}

type diffStrategyType int

const (
	applyDiffStrategy diffStrategyType = iota
	simpleCopyStrategy
)

type diffFileStrategy struct {
	destinationDir      string
	destinationFilePath string
	strategy            diffStrategyType
}

var _ fileSink = &diffFileSink{}

func newDiffFileSink(
	dataDir string,
	incrementalDir string,
	decompressor compression.Decompressor,
	spaceIDCollector innodb.SpaceIDCollector,
) fileSink {
	// xbstream is a simple archive format. Compression / encryption / delta-files are xtrabackup features.
	// so, all chunks of one compressed file is a _single_ stream
	// we should combine data from all file chunks in a single io.Reader before passing to Decompressor:
	sink := diffFileSink{
		dataDir:          dataDir,
		incrementalDir:   incrementalDir,
		meta:             nil,
		writeHere:        make(chan []byte),
		fileCloseChan:    make(chan struct{}),
		spaceIDCollector: spaceIDCollector,
	}

	if decompressor != nil {
		readHere, err := decompressor.Decompress(splitmerge.NewChannelReader(sink.writeHere))
		tracelog.ErrorLogger.FatalfOnError("Cannot decompress: %v", err)
		sink.readHere = readHere
	} else {
		sink.readHere = splitmerge.NewChannelReader(sink.writeHere)
	}

	return &sink
}

func (sink *diffFileSink) Process(chunk *Chunk) error {
	if chunk.Type == ChunkTypeEOF && strings.HasSuffix(chunk.Path, ".meta") {
		return nil // skip
	}
	if chunk.Type == ChunkTypeEOF && strings.HasSuffix(chunk.Path, ".delta") {
		close(sink.writeHere)
		<-sink.fileCloseChan // file will be closed in goroutine, wait for it...
		return ErrSinkEOF
	}

	if strings.HasSuffix(chunk.Path, ".meta") {
		return sink.ProcessMeta(chunk)
	}
	if strings.HasSuffix(chunk.Path, ".delta") {
		// synchronously read data & send it to writer
		buffer := make([]byte, chunk.PayloadLen)
		_, err := io.ReadFull(chunk, buffer)
		tracelog.ErrorLogger.FatalfOnError(fmt.Sprintf("ReadFull on file %v", chunk.Path), err)
		sink.writeHere <- buffer
		return nil
	}

	return fmt.Errorf("unexpected file extension for diff-sink %v", chunk.Path)
}

func (sink *diffFileSink) ProcessMeta(chunk *Chunk) error {
	if sink.meta != nil {
		return fmt.Errorf("unexpected '.meta' file %v - we already seen it", chunk.Path)
	}
	rawMeta, err := io.ReadAll(chunk.Reader)
	if err != nil {
		return err
	}
	meta, err := parseDiffMetadata(rawMeta)
	if err != nil {
		return err
	}
	sink.meta = &meta

	err = sink.writeToFile(sink.incrementalDir, chunk.Path, rawMeta)
	if err != nil {
		return err
	}

	strategy, err := sink.getHandlingStrategy(chunk)
	if err != nil {
		tracelog.ErrorLogger.Printf("No handling strategy found for chunk %v", chunk.Path)
		return err
	}
	sink.strategy = strategy

	file, err := safeFileCreate(sink.strategy.destinationDir, sink.strategy.destinationFilePath)
	tracelog.ErrorLogger.FatalfOnError("Cannot create new file: %v", err)
	sink.file = file

	switch strategy.strategy {
	case simpleCopyStrategy:
		sink.startSimpleCopyStrategy()
	case applyDiffStrategy:
		sink.startApplyDiffStrategy()
	default:
		return fmt.Errorf("unknown diff-handling strategy %v for file %v", strategy.strategy, chunk.Path)
	}

	return nil
}

func (sink *diffFileSink) startSimpleCopyStrategy() {
	go func() {
		_, err := io.Copy(sink.file, sink.readHere)
		tracelog.ErrorLogger.FatalfOnError("Cannot copy data: %v", err)
		// copying to INCR dir - we don't need to fix Sparse files
		utility.LoggedClose(sink.file, "datasink.Close()")
		close(sink.fileCloseChan)
	}()
}

func (sink *diffFileSink) startApplyDiffStrategy() {
	go func() {
		err := sink.applyDiff()
		tracelog.ErrorLogger.FatalfOnError("Cannot handle diff: %v", err)
		err = innodb.RepairSparse(sink.file)
		if err != nil {
			tracelog.WarningLogger.Printf("Error during repairSparse(): %v", err)
		}
		utility.LoggedClose(sink.file, "sink.Close()")
		close(sink.fileCloseChan)
	}()
}

func (sink *diffFileSink) getHandlingStrategy(chunk *Chunk) (diffFileStrategy, error) {
	// xbstream instructs us to store file at this path:
	newFilePath := strings.TrimSuffix(chunk.Path, ".meta")

	// we observed this SpaceID at the following path:
	oldFilePath, err := sink.spaceIDCollector.GetFileForSpaceID(sink.meta.SpaceID)
	if err != nil && !errors.Is(err, innodb.ErrSpaceIDNotFound) {
		return diffFileStrategy{}, err
	}
	if errors.Is(err, innodb.ErrSpaceIDNotFound) {
		checkErr := sink.spaceIDCollector.CheckFileForSpaceID(sink.meta.SpaceID, newFilePath)
		if checkErr != nil && !errors.Is(checkErr, innodb.ErrSpaceIDNotFound) {
			tracelog.ErrorLogger.Printf("CheckFileForSpaceID: %v %v: %v", sink.meta.SpaceID, newFilePath, checkErr)
			return diffFileStrategy{}, err // return original ErrSpaceIDNotFound
		}
		if errors.Is(checkErr, innodb.ErrSpaceIDNotFound) {
			// we had tried twice and still haven't found Tablespace in datadir. Highly likely that this a new Tablespace.
			// let xtrabackup to decide what to do with it - send it too incremental dir:
			tracelog.InfoLogger.Printf("New file for SpaceID %v will be created at %s", sink.meta.SpaceID, newFilePath)
			return diffFileStrategy{
				destinationDir:      sink.incrementalDir,
				destinationFilePath: newFilePath + ".delta",
				strategy:            simpleCopyStrategy,
			}, nil
		}
		// we have found Tablespace at `newFilePath` path.
		// send it to dataDir
		tracelog.DebugLogger.Printf("Our spaceId collector failed to find SpaceID %v, however it is at %v", sink.meta.SpaceID, newFilePath)
		return diffFileStrategy{
			destinationDir:      sink.dataDir,
			destinationFilePath: newFilePath,
			strategy:            applyDiffStrategy,
		}, nil
	}

	// We have found Tablespace - use it:
	if oldFilePath != newFilePath {
		tracelog.InfoLogger.Printf("File path for SpaceID %v changed from %s to %s", sink.meta.SpaceID, oldFilePath, newFilePath)
	}
	return diffFileStrategy{
		destinationDir:      sink.dataDir,
		destinationFilePath: oldFilePath,
		strategy:            applyDiffStrategy,
	}, nil
}

func (sink *diffFileSink) applyDiff() error {
	// check stream format in README.md
	header := make([]byte, sink.meta.PageSize)
	_, err := sink.readHere.Read(header)
	if err != nil {
		return err
	}
	if !slices.Equal(header[0:4], []byte("XTRA")) && !slices.Equal(header[0:4], []byte("xtra")) {
		return errors.New("unexpected header in diff file")
	}
	isLast := slices.Equal(header[0:4], []byte("XTRA"))
	isFirst := true

	pageNums := make([]innodb.PageNumber, 0, sink.meta.PageSize/4)
	for i := uint32(1); i < sink.meta.PageSize/4; i++ {
		pageNum := innodb.PageNumber(binary.BigEndian.Uint32(header[i*4 : i*4+4]))
		if pageNum == 0xFFFFFFFF {
			break
		}
		pageNums = append(pageNums, pageNum)
	}

	if uint32(len(pageNums)) != sink.meta.PageSize/4 && !isLast {
		return fmt.Errorf("invalid '.delta' format: number of pages %v doesn't match delta-header type %v", len(pageNums), header[0:4])
	}

	// copy pages:
	for _, pageNum := range pageNums {
		_, err = sink.file.Seek(int64(pageNum)*int64(sink.meta.PageSize), io.SeekStart)
		if err != nil {
			return err
		}

		// we are trying to leave as much work as possible to xtrabackup (e.g. files renaming)
		// so, we are writing minimal possible `delta` file to incremental dir in order to trigger xtrabackup
		// to do its work:
		if isFirst {
			firstPage := make([]byte, sink.meta.PageSize)
			_, err = sink.readHere.Read(firstPage)
			if err != nil {
				return err
			}
			// write to data dir:
			_, err = sink.file.Write(firstPage)
			if err != nil {
				return err
			}
			tracelog.DebugLogger.Printf("[DATA]/%v: %v bytes applied", sink.file.Name(), len(firstPage))

			// write to incremental dir:
			raw := sink.buildFakeDelta(header, firstPage)
			err = sink.writeToFile(sink.incrementalDir, sink.strategy.destinationFilePath+".delta", raw)
			if err != nil {
				return err
			}
			tracelog.DebugLogger.Printf("[INCR]/%v: %v bytes copied", sink.strategy.destinationFilePath+".delta", len(raw))
		} else {
			_, err = io.CopyN(sink.file, sink.readHere, int64(sink.meta.PageSize))
			if err != nil {
				return err
			}
			tracelog.DebugLogger.Printf("[DATA]/%v: %v bytes applied", sink.file.Name(), sink.meta.PageSize)
		}

		isFirst = false
	}

	tracelog.DebugLogger.Printf("%v pages applied to file %v", len(pageNums), sink.file.Name())

	return nil
}

func (sink *diffFileSink) writeToFile(dir string, relFilePath string, bytes []byte) error {
	file, err := safeFileCreate(dir, relFilePath)
	tracelog.ErrorLogger.FatalfOnError("Cannot open new file for write: %v", err)

	_, err = file.Write(bytes)
	if err != nil {
		return err
	}

	err = file.Close()
	if err != nil {
		return err
	}
	return nil
}

func (sink *diffFileSink) buildFakeDelta(header []byte, page []byte) []byte {
	// here we are writing fake diff-file to incrementalDir:
	// it consists of:
	// * Header - page_size bytes (page_size - from '.meta' file)
	//   (4 bytes) 'XTRA' (as it last block for this delta file)
	//   (4 byte) page_number
	//   (4 bytes) 0xFFFFFFFF - as page list termination symbol
	//   (page_size - N) 0x0 - filler
	// * Body
	//   1 * <page content>
	//
	// xtrabackup will re-apply this page and do all its magic for us
	raw := make([]byte, 2*sink.meta.PageSize)
	binary.BigEndian.PutUint32(raw[0:4], 0x58545241)
	binary.BigEndian.PutUint32(raw[4:8], binary.BigEndian.Uint32(header[4:8]))
	binary.BigEndian.PutUint32(raw[8:12], 0xFFFFFFFF)
	copy(raw[sink.meta.PageSize:], page)
	return raw
}

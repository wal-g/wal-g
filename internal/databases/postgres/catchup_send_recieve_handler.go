package postgres

import (
	"encoding/gob"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/postgres/errors"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/utility"
)

func HandleCatchupSend(pgDataDirectory string, destination string) {
	pgDataDirectory = utility.ResolveSymlink(pgDataDirectory)
	tracelog.InfoLogger.Printf("Sending %v to %v\n", pgDataDirectory, destination)
	info, runner, err := GetPgServerInfo(true)
	if info.systemIdentifier == nil {
		tracelog.ErrorLogger.Fatal("Our system lacks System Identifier, cannot proceed")
	}
	tracelog.ErrorLogger.FatalOnError(err)

	// Coordinator connection (worker 0): handshake only.
	writer0, decoder0, encoder0 := startSendConnection(destination)

	var control PgControlData
	err = decoder0.Decode(&control)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.InfoLogger.Printf("Destination control file %v", control)
	tracelog.InfoLogger.Printf("Our system id %v", *info.systemIdentifier)
	if *info.systemIdentifier != control.SystemIdentifier {
		tracelog.ErrorLogger.Fatal("System identifiers do not match")
	}
	if control.CurrentTimeline != info.Timeline {
		tracelog.ErrorLogger.Fatalf("Destination is on timeline %v, but we are on %v",
			control.CurrentTimeline, info.Timeline)
	}
	var fileList internal.BackupFileList
	err = decoder0.Decode(&fileList)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.InfoLogger.Printf("Received file list of %v files", len(fileList))

	_, lsnStr, _, err := runner.StartBackup("")
	tracelog.ErrorLogger.FatalOnError(err)
	lsn, err := ParseLSN(lsnStr)
	tracelog.ErrorLogger.FatalOnError(err)
	if lsn <= control.Checkpoint {
		tracelog.ErrorLogger.Fatalf("Catchup destination is already ahead (our LSN %v, destination LSN %v).",
			lsn, control.Checkpoint)
	}

	numWorkers, err := conf.GetMaxUploadDiskConcurrency()
	tracelog.ErrorLogger.FatalOnError(err)
	if numWorkers < 1 {
		numWorkers = 1
	}
	tracelog.InfoLogger.Printf("Sending with %v parallel workers", numWorkers)

	// Tell receiver how many worker connections will follow.
	err = encoder0.Encode(CatchupCommandDto{IsStart: true, NumWorkers: numWorkers})
	tracelog.ErrorLogger.FatalOnError(err)
	err = writer0.Flush()
	tracelog.ErrorLogger.FatalOnError(err)

	// Open worker connections (worker 0 reuses the coordinator).
	encoders := make([]*gob.Encoder, numWorkers)
	writers := make([]ioextensions.WriteFlushCloser, numWorkers)
	encoders[0] = encoder0
	writers[0] = writer0
	for i := 1; i < numWorkers; i++ {
		w, _, enc := startSendConnection(destination)
		encoders[i] = enc
		writers[i] = w
	}

	// Walk the source tree in a goroutine; N workers consume from the channel.
	fileChan := make(chan fileTask, numWorkers*10)
	seenFiles := make(map[string]bool)
	go func() {
		walkIntoChannel(pgDataDirectory, fileList, control.Checkpoint, fileChan, seenFiles)
		close(fileChan)
	}()

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(enc *gob.Encoder) {
			defer wg.Done()
			for task := range fileChan {
				sendOneFile(task.path, task.info, task.wasInBase, task.checkpoint, enc, task.fullFileName)
			}
		}(encoders[i])
	}
	// wg.Wait() also guarantees the walk goroutine has finished (fileChan is closed
	// only after the full walk), so seenFiles is complete when we proceed.
	wg.Wait()

	sendDeletedFiles(encoder0, fileList, seenFiles)

	label, offsetMap, _, err := runner.StopBackup()
	tracelog.ErrorLogger.FatalOnError(err)

	// Final binary files and IsDone always go through the coordinator (worker 0).
	err = encoder0.Encode(
		CatchupCommandDto{BinaryContents: []byte(label), FileName: BackupLabelFilename, IsBinContents: true})
	tracelog.ErrorLogger.FatalOnError(err)
	err = encoder0.Encode(
		CatchupCommandDto{BinaryContents: []byte(offsetMap), FileName: TablespaceMapFilename, IsBinContents: true})
	tracelog.ErrorLogger.FatalOnError(err)
	ourPgControl, err := os.ReadFile(path.Join(pgDataDirectory, PgControlPath))
	tracelog.ErrorLogger.FatalOnError(err)
	err = encoder0.Encode(
		CatchupCommandDto{
			BinaryContents: ourPgControl, FileName: utility.SanitizePath(PgControlPath), IsBinContents: true,
		})
	tracelog.ErrorLogger.FatalOnError(err)

	for i := 0; i < numWorkers; i++ {
		err = encoders[i].Encode(CatchupCommandDto{IsDone: true})
		tracelog.ErrorLogger.FatalOnError(err)
		err = writers[i].Flush()
		tracelog.ErrorLogger.FatalOnError(err)
	}
	tracelog.InfoLogger.Printf("Send done")
}

func startSendConnection(destination string) (ioextensions.WriteFlushCloser, *gob.Decoder, *gob.Encoder) {
	dial, err := net.Dial("tcp", destination)
	tracelog.ErrorLogger.FatalOnError(err)

	crypter := internal.ConfigureCrypter()

	cmpr, decmpr := chooseCompression()

	writer := cmpr.NewWriter(dial)
	reader, err := decmpr.Decompress(dial)
	tracelog.ErrorLogger.FatalOnError(err)
	var decoder *gob.Decoder
	var encoder *gob.Encoder
	if crypter != nil {
		decrypt, err := crypter.Decrypt(reader)
		tracelog.ErrorLogger.FatalOnError(err)
		decoder = gob.NewDecoder(decrypt)
		encrypt, err := crypter.Encrypt(writer)
		tracelog.ErrorLogger.FatalOnError(err)
		encoder = gob.NewEncoder(encrypt)
	} else {
		decoder = gob.NewDecoder(reader)
		encoder = gob.NewEncoder(writer)
	}
	return writer, decoder, encoder
}

func chooseCompression() (compression.Compressor, compression.Decompressor) {
	var c compression.Compressor
	var ok bool
	if c, ok = compression.Compressors["br"]; !ok {
		c = compression.Compressors["lz4"]
	}

	d := compression.GetDecompressorByCompressor(c)
	return c, d
}

type fileTask struct {
	path         string
	info         fs.FileInfo
	wasInBase    bool
	checkpoint   LSN
	fullFileName string
}

func walkIntoChannel(directory string, list internal.BackupFileList, checkpoint LSN,
	tasks chan<- fileTask, seenFiles map[string]bool) {
	extendExcludedFiles()
	err := filepath.Walk(directory, func(filePath string, info fs.FileInfo, err error) error {
		fullFileName := utility.GetSubdirectoryRelativePath(filePath, directory)
		seenFiles[fullFileName] = true
		if err != nil {
			if os.IsNotExist(err) {
				tracelog.WarningLogger.Println(filePath, " deleted during filepath walk")
				return nil
			}
			return err
		}
		if info.Name() == PgControl {
			return nil
		}
		fileName := info.Name()
		_, excluded := ExcludedFilenames[fileName]
		isDir := info.IsDir()
		if isDir && excluded {
			return filepath.SkipDir
		}
		if isDir {
			return nil
		}
		if excluded {
			return nil
		}

		wasInBase := false
		if fdto, ok := list[fullFileName]; ok {
			if fdto.MTime.Equal(info.ModTime()) {
				return nil
			}
			wasInBase = true
		}

		tasks <- fileTask{
			path:         filePath,
			info:         info,
			wasInBase:    wasInBase,
			checkpoint:   checkpoint,
			fullFileName: fullFileName,
		}
		return nil
	})
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.DebugLogger.Printf("Filepath walk done")
}

func sendDeletedFiles(encoder *gob.Encoder, list internal.BackupFileList, seenFiles map[string]bool) {
	var filesToDelete []string
	for k := range list {
		if _, ok := seenFiles[k]; ok {
			continue
		}
		excluded := false
		for _, e := range strings.Split(k, string(os.PathSeparator)) {
			if _, ok := ExcludedFilenames[e]; ok {
				excluded = true
				break
			}
		}
		if excluded {
			continue
		}
		filesToDelete = append(filesToDelete, k)
	}
	if len(filesToDelete) > 0 {
		err := encoder.Encode(CatchupCommandDto{IsDelete: true, FilesToDelete: filesToDelete})
		tracelog.ErrorLogger.FatalOnError(err)
	}
}

func sendOneFile(path string, info fs.FileInfo, wasInBase bool, checkpoint LSN,
	encoder *gob.Encoder, fullFileName string) {
	increment := isPagedFile(info, path) && wasInBase
	var err error

	var fd io.ReadCloser
	var size int64
	if !increment {
		fd, err = os.Open(path)
		if os.IsNotExist(err) {
			return
		}
		tracelog.ErrorLogger.FatalOnError(err)
		size = info.Size()
	} else {
		fd, size, err = ReadIncrementalFile(path, info.Size(), checkpoint, nil)

		if _, ok := err.(*errors.InvalidBlockError); ok {
			fd, err = os.Open(path)
			if os.IsNotExist(err) {
				return
			}
			tracelog.ErrorLogger.FatalOnError(err)
			size = info.Size()
			increment = false
		} else {
			tracelog.ErrorLogger.FatalOnError(err)
		}
	}

	err = encoder.Encode(
		CatchupCommandDto{FileName: fullFileName, IsFull: !increment, FileSize: uint64(size), IsIncremental: increment})
	tracelog.ErrorLogger.FatalOnError(err)
	reader := io.MultiReader(fd, &ioextensions.ZeroReader{})

	for size != 0 {
		var bytes = make([]byte, int(min(size, 8192)))
		_, err := io.ReadFull(reader, bytes)
		tracelog.ErrorLogger.FatalOnError(err)
		size -= int64(len(bytes))
		err = encoder.Encode(bytes)
		tracelog.ErrorLogger.FatalOnError(err)
	}

	tracelog.InfoLogger.Printf("Sent %v, %v bytes", fullFileName, info.Size())
	tracelog.ErrorLogger.FatalOnError(err)
	err = fd.Close()
	tracelog.ErrorLogger.FatalOnError(err)
}

func setupReceiverConn(conn net.Conn) (*gob.Encoder, *gob.Decoder, ioextensions.WriteFlushCloser) {
	cmpr, decmpr := chooseCompression()
	writer := cmpr.NewWriter(conn)
	reader, err := decmpr.Decompress(conn)
	tracelog.ErrorLogger.FatalOnError(err)
	crypter := internal.ConfigureCrypter()
	if crypter != nil {
		decrypt, err := crypter.Decrypt(reader)
		tracelog.ErrorLogger.FatalOnError(err)
		encrypt, err := crypter.Encrypt(writer)
		tracelog.ErrorLogger.FatalOnError(err)
		return gob.NewEncoder(encrypt), gob.NewDecoder(decrypt), writer
	}
	return gob.NewEncoder(writer), gob.NewDecoder(reader), writer
}

func HandleCatchupReceive(pgDataDirectory string, port int) {
	pgDataDirectory = utility.ResolveSymlink(pgDataDirectory)
	tracelog.InfoLogger.Printf("Receiving %v on port %v\n", pgDataDirectory, port)
	listen, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	tracelog.ErrorLogger.FatalOnError(err)

	// Coordinator connection: handshake (send control + file list, read IsStart).
	conn0, err := listen.Accept()
	tracelog.ErrorLogger.FatalOnError(err)
	encoder0, decoder0, writer0 := setupReceiverConn(conn0)

	sendControlAndFileList(pgDataDirectory, encoder0)
	err = writer0.Flush()
	tracelog.ErrorLogger.FatalOnError(err)

	var startCmd CatchupCommandDto
	err = decoder0.Decode(&startCmd)
	tracelog.ErrorLogger.FatalOnError(err)
	if !startCmd.IsStart {
		tracelog.ErrorLogger.Fatal("Expected IsStart command from sender")
	}
	numWorkers := startCmd.NumWorkers
	if numWorkers < 1 {
		numWorkers = 1
	}
	tracelog.InfoLogger.Printf("Receiving with %v parallel workers", numWorkers)

	// Accept the remaining worker connections on the same port.
	decoders := make([]*gob.Decoder, numWorkers)
	decoders[0] = decoder0
	for i := 1; i < numWorkers; i++ {
		conn, err := listen.Accept()
		tracelog.ErrorLogger.FatalOnError(err)
		_, dec, _ := setupReceiverConn(conn)
		decoders[i] = dec
	}
	listen.Close()

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(dec *gob.Decoder) {
			defer wg.Done()
			for {
				var cmd CatchupCommandDto
				err := dec.Decode(&cmd)
				tracelog.ErrorLogger.FatalOnError(err)
				if cmd.IsDone {
					return
				}
				doRcvCommand(cmd, pgDataDirectory, dec)
			}
		}(decoders[i])
	}
	wg.Wait()
	tracelog.InfoLogger.Printf("Receive done")
}

type DecoderReader struct {
	*gob.Decoder
	buf  []byte
	size int64
}

func (d *DecoderReader) Read(bytes []byte) (n int, err error) {
	if d.size <= 0 {
		return 0, io.EOF
	}
	if len(d.buf) == 0 {
		err := d.Decode(&d.buf)
		tracelog.ErrorLogger.FatalOnError(err)
	}
	i := copy(bytes, d.buf)
	i = utility.Min(i, int(d.size))
	d.buf = d.buf[i:]
	d.size -= int64(i)
	return i, err
}

func doRcvCommand(cmd CatchupCommandDto, directory string, decoder *gob.Decoder) {
	if cmd.IsBinContents {
		tracelog.InfoLogger.Printf("Writing file %v", cmd.FileName)
		err := os.WriteFile(path.Join(directory, cmd.FileName), cmd.BinaryContents, 0666)
		tracelog.ErrorLogger.FatalOnError(err)
		return
	}

	if cmd.IsFull {
		tracelog.InfoLogger.Printf("Full file %v", cmd.FileName)
		fd, err := os.Create(path.Join(directory, cmd.FileName))
		tracelog.ErrorLogger.FatalOnError(err)
		size := int64(cmd.FileSize)
		for size != 0 {
			var bytes []byte
			err := decoder.Decode(&bytes)
			tracelog.ErrorLogger.FatalOnError(err)
			_, err = fd.Write(bytes)
			tracelog.ErrorLogger.FatalOnError(err)
			size -= int64(len(bytes))
		}
		tracelog.InfoLogger.Printf("Received %v bytes", cmd.FileSize)
		tracelog.ErrorLogger.FatalOnError(err)
		err = fd.Close()
		tracelog.ErrorLogger.FatalOnError(err)
		return
	}

	if cmd.IsIncremental {
		tracelog.InfoLogger.Printf("Incremental file %v", cmd.FileName)

		err := ApplyFileIncrement(path.Join(directory, cmd.FileName),
			&DecoderReader{decoder, nil, int64(cmd.FileSize)}, true, false)
		tracelog.ErrorLogger.FatalOnError(err)
		return
	}
	if cmd.IsDelete {
		tracelog.InfoLogger.Printf("Deleting files %v", cmd.FilesToDelete)
		for _, f := range cmd.FilesToDelete {
			err := os.Remove(path.Join(directory, f))
			tracelog.ErrorLogger.FatalOnError(err)
		}
		return
	}
	tracelog.ErrorLogger.Fatal("Unknown command")
}

type CatchupCommandDto struct {
	IsStart        bool
	NumWorkers     int
	IsDone         bool
	IsIncremental  bool
	IsFull         bool
	IsDelete       bool
	IsBinContents  bool
	FileSize       uint64
	FileName       string
	BinaryContents []byte
	FilesToDelete  []string
}

func sendControlAndFileList(pgDataDirectory string, encoder *gob.Encoder) {
	control, err := ExtractPgControl(pgDataDirectory)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.InfoLogger.Printf("Our system id %v, need catchup from %v",
		control.SystemIdentifier, control.Checkpoint)
	tracelog.ErrorLogger.FatalOnError(err)
	err = encoder.Encode(control)
	tracelog.ErrorLogger.FatalOnError(err)
	rcvFileList := receiveFileList(pgDataDirectory)
	err = encoder.Encode(rcvFileList)
	tracelog.ErrorLogger.FatalOnError(err)
}

func receiveFileList(directory string) internal.BackupFileList {
	var result = make(internal.BackupFileList)
	err := filepath.Walk(directory, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			tracelog.WarningLogger.Println("Apparent concurrent modification")
			return err
		}
		if info.Name() == PgControl {
			return nil
		}
		fileName := info.Name()
		_, excluded := ExcludedFilenames[fileName]
		isDir := info.IsDir()
		if isDir && excluded {
			return filepath.SkipDir
		}
		if excluded {
			return nil
		}
		result[utility.GetSubdirectoryRelativePath(path, directory)] =
			internal.BackupFileDescription{MTime: info.ModTime(), IsSkipped: false, IsIncremented: false}

		return nil
	})
	tracelog.ErrorLogger.FatalOnError(err)
	return result
}

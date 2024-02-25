package postgres

import (
	"encoding/gob"
	"fmt"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/utility"
	"io"
	"io/fs"
	"net"
	"os"
	"path"
	"path/filepath"
)

func HandleCatchupSend(pgDataDirectory string, destination string) {
	pgDataDirectory = utility.ResolveSymlink(pgDataDirectory)
	tracelog.InfoLogger.Printf("Sending %v to %v\n", pgDataDirectory, destination)
	info, runner, err := GetPgServerInfo(true)
	if info.systemIdentifier == nil {
		tracelog.ErrorLogger.Fatal("Our system lacks System Identifier, cannot proceed")
	}
	tracelog.ErrorLogger.PanicOnError(err)
	dial, err := net.Dial("tcp", destination)
	tracelog.ErrorLogger.PanicOnError(err)
	decoder := gob.NewDecoder(dial)
	var control PgControlData
	err = decoder.Decode(&control)
	tracelog.ErrorLogger.PanicOnError(err)
	tracelog.InfoLogger.Printf("Destination control file %v", control)
	tracelog.InfoLogger.Printf("Our system id %v", *info.systemIdentifier)
	if *info.systemIdentifier != control.SystemIdentifier {
		tracelog.ErrorLogger.Fatal("System identifiers do not match")
	}
	if control.CurrentTimeline != info.Timeline {
		tracelog.ErrorLogger.Fatal("Destination is on timeline %v, but we are on %v", control.CurrentTimeline, info.Timeline)
	}
	var fileList internal.BackupFileList
	err = decoder.Decode(&fileList)
	tracelog.ErrorLogger.PanicOnError(err)
	tracelog.InfoLogger.Printf("Received file list of %v failes", len(fileList))
	_, lsnStr, _, err := runner.StartBackup("")
	tracelog.ErrorLogger.PanicOnError(err)
	lsn, err := ParseLSN(lsnStr)
	tracelog.ErrorLogger.PanicOnError(err)
	if lsn <= control.Checkpoint {
		tracelog.ErrorLogger.Fatal("Catchup destination is already ahead (our LSN %v, destination LSN %v).", lsn, control.Checkpoint)
	}

	encoder := gob.NewEncoder(dial)

	label, offsetMap, _, err := runner.StopBackup()
	tracelog.ErrorLogger.PanicOnError(err)

	sendFileCommands(encoder, pgDataDirectory, fileList, control.Checkpoint)

	err = encoder.Encode(CatchupCommandDto{BinaryContents: []byte(label), FileName: BackupLabelFilename, IsBinContents: true})
	tracelog.ErrorLogger.PanicOnError(err)
	err = encoder.Encode(CatchupCommandDto{BinaryContents: []byte(offsetMap), FileName: TablespaceMapFilename, IsBinContents: true})
	tracelog.ErrorLogger.PanicOnError(err)
	ourPgControl, err := os.ReadFile(path.Join(pgDataDirectory, PgControlPath))
	tracelog.ErrorLogger.PanicOnError(err)
	err = encoder.Encode(CatchupCommandDto{BinaryContents: ourPgControl, FileName: utility.SanitizePath(PgControlPath), IsBinContents: true})
	tracelog.ErrorLogger.PanicOnError(err)

	err = encoder.Encode(CatchupCommandDto{IsDone: true})
	tracelog.ErrorLogger.PanicOnError(err)
	tracelog.InfoLogger.Printf("Send done")
}

func sendFileCommands(encoder *gob.Encoder, directory string, list internal.BackupFileList, checkpoint LSN) {
	extendExcludedFiles()
	filepath.Walk(directory, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				tracelog.WarningLogger.Println(path, " deleted during filepath walk")
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
		fullFileName := utility.GetSubdirectoryRelativePath(path, directory)

		wasInBase := false
		if fdto, ok := list[fullFileName]; ok {
			if fdto.MTime.Equal(info.ModTime()) {
				// No need to catchup
				return nil
			}
			wasInBase = true
		}

		increment := isPagedFile(info, path) && wasInBase

		var fd io.ReadCloser
		var size int64
		if !increment {
			fd, err = os.Open(path)
			tracelog.ErrorLogger.PanicOnError(err)
			size = info.Size()
		} else {
			fd, size, err = ReadIncrementalFile(path, info.Size(), checkpoint, nil)

			if _, ok := err.(*InvalidBlockError); ok {
				fd, err = os.Open(path)
				tracelog.ErrorLogger.PanicOnError(err)
				size = info.Size()
				increment = false
			} else {
				tracelog.ErrorLogger.PanicOnError(err)
			}
		}

		//size := info.Size()
		err = encoder.Encode(CatchupCommandDto{FileName: fullFileName, IsFull: !increment, FileSize: uint64(size), IsIncremental: increment})
		tracelog.ErrorLogger.PanicOnError(err)
		reader := io.MultiReader(fd, &ioextensions.ZeroReader{})

		for size != 0 {
			min := 8192
			if int64(min) > size {
				min = int(size)
			}
			var bytes = make([]byte, min)
			io.ReadFull(reader, bytes)
			size -= int64(len(bytes))
			encoder.Encode(bytes)
		}

		tracelog.InfoLogger.Printf("Sent %v, %v bytes", fullFileName, info.Size())
		tracelog.ErrorLogger.PanicOnError(err)
		err = fd.Close()
		tracelog.ErrorLogger.PanicOnError(err)

		return nil
	})
	tracelog.DebugLogger.Printf("Filepath walk done")
}

func HandleCatchupReceive(pgDataDirectory string, port int) {
	pgDataDirectory = utility.ResolveSymlink(pgDataDirectory)
	tracelog.InfoLogger.Printf("Receiving %v on port %v\n", pgDataDirectory, port)
	listen, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	tracelog.ErrorLogger.PanicOnError(err)
	conn, err := listen.Accept()
	sendControlAndFileList(pgDataDirectory, err, conn)
	decoder := gob.NewDecoder(conn)
	for {
		var cmd CatchupCommandDto
		err := decoder.Decode(&cmd)
		if io.EOF == err {
			break
		}
		tracelog.ErrorLogger.PanicOnError(err)
		if cmd.IsDone {
			break
		}
		doRcvCommand(cmd, pgDataDirectory, decoder)
	}
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
		tracelog.ErrorLogger.PanicOnError(err)
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
		tracelog.ErrorLogger.PanicOnError(err)
		return
	}

	if cmd.IsFull {
		tracelog.InfoLogger.Printf("Full file %v", cmd.FileName)
		fd, err := os.Create(path.Join(directory, cmd.FileName))
		tracelog.ErrorLogger.PanicOnError(err)
		size := int64(cmd.FileSize)
		for size != 0 {
			var bytes []byte
			err := decoder.Decode(&bytes)
			tracelog.ErrorLogger.PanicOnError(err)
			_, err = fd.Write(bytes)
			tracelog.ErrorLogger.PanicOnError(err)
			size -= int64(len(bytes))
		}
		tracelog.InfoLogger.Printf("Received %v bytes", cmd.FileSize)
		tracelog.ErrorLogger.PanicOnError(err)
		err = fd.Close()
		tracelog.ErrorLogger.PanicOnError(err)
		return
	}

	if cmd.IsIncremental {
		tracelog.InfoLogger.Printf("Incremental file %v", cmd.FileName)

		err := ApplyFileIncrement(path.Join(directory, cmd.FileName), &DecoderReader{decoder, nil, int64(cmd.FileSize)}, true, false)
		tracelog.ErrorLogger.PanicOnError(err)
		return
	}
}

type CatchupCommandDto struct {
	IsDone         bool
	IsIncremental  bool
	IsFull         bool
	IsDelete       bool
	IsBinContents  bool
	FileSize       uint64
	FileName       string
	BinaryContents []byte
}

func sendControlAndFileList(pgDataDirectory string, err error, conn net.Conn) {
	tracelog.ErrorLogger.PanicOnError(err)
	control, err := ExtractPgControl(pgDataDirectory)
	tracelog.InfoLogger.Printf("Our system id %v, need catchup from %v", control.SystemIdentifier, control.Checkpoint)
	tracelog.ErrorLogger.PanicOnError(err)
	encoder := gob.NewEncoder(conn)
	err = encoder.Encode(control)
	tracelog.ErrorLogger.PanicOnError(err)
	rcvFileList := receiveFileList(pgDataDirectory)
	err = encoder.Encode(rcvFileList)
	tracelog.ErrorLogger.PanicOnError(err)
}

func receiveFileList(directory string) internal.BackupFileList {
	var result = make(internal.BackupFileList)
	filepath.Walk(directory, func(path string, info fs.FileInfo, err error) error {
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
		result[utility.GetSubdirectoryRelativePath(path, directory)] = internal.BackupFileDescription{MTime: info.ModTime(), IsSkipped: false, IsIncremented: false}

		return nil
	})
	return result
}

package postgres

/*
This object represents a base backup object.
A base backup object can connect to Postgres, issue a BASE_BACKUP command, and receive the backup data from Postgres.
*/

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/wal-g/wal-g/internal"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

var (
	errTbsOutOfRange     = errors.New("requesting next tablespace after all tablespaces are streamed")
	errTbsStillStreaming = errors.New("cannot move to next table. Current tablespace is not yet fully streamed")
)

// The StreamingBaseBackup object represents a Postgres BASE_BACKUP, connecting to Postgres, and streaming backup data.
// For every tablespace, all files are combined in a tar format and streamed in a CopyData stream.
type StreamingBaseBackup struct {
	TimeLine         uint32
	StartLSN         pglogrepl.LSN
	EndLSN           pglogrepl.LSN
	buffer           []byte
	readIndex        int
	tablespaces      []pglogrepl.BaseBackupTablespace
	tbsPointer       int
	tbsStreaming     bool
	pgConn           *pgconn.PgConn
	UncompressedSize int64
	maxTarSize       int64
	dataDir          string
	Files            internal.BackupFileList
	uploader         *WalUploader
	streamer         *TarballStreamer
	fileNo           int
}

// NewStreamingBaseBackup will define a new StreamingBaseBackup object
func NewStreamingBaseBackup(pgDataDir string, maxTarSize int64, pgConn *pgconn.PgConn) (bb *StreamingBaseBackup) {
	bb = &StreamingBaseBackup{
		dataDir:    pgDataDir,
		maxTarSize: maxTarSize,
		pgConn:     pgConn,
	}
	return
}

// Start will start a base_backup read the backup info, and prepare for uploading tar files
func (bb *StreamingBaseBackup) Start(verifyChecksum bool, diskLimit int32) (err error) {
	options := pglogrepl.BaseBackupOptions{
		// Following implementation for local backup.
		Fast:              true,
		TablespaceMap:     true,
		Label:             "wal-g",
		NoVerifyChecksums: !verifyChecksum,
		MaxRate:           diskLimit,
	}
	result, err := pglogrepl.StartBaseBackup(context.Background(), bb.pgConn, options)
	if err != nil {
		return
	}
	bb.tablespaces = result.Tablespaces
	bb.StartLSN = result.LSN
	bb.TimeLine = uint32(result.TimelineID)
	bb.Files = make(internal.BackupFileList)
	return
}

// Finish will wrap up a backup after finalizing upload.
func (bb *StreamingBaseBackup) Finish() (err error) {
	result, err := pglogrepl.FinishBaseBackup(context.Background(), bb.pgConn)
	if err != nil {
		return
	}
	bb.EndLSN = result.LSN
	return
}

// nextTbs will internally switch to the next tablespace
func (bb *StreamingBaseBackup) nextTbs() (err error) {
	if bb.tbsStreaming {
		return errTbsStillStreaming
	}
	if bb.tbsPointer > len(bb.tablespaces) {
		return errTbsOutOfRange
	}
	err = pglogrepl.NextTableSpace(context.Background(), bb.pgConn)
	if err != nil {
		return err
	}
	var tee []string
	remaps := TarballStreamerRemaps{}
	if bb.tbsPointer == len(bb.tablespaces) {
		tee = append(tee, "global/pg_control")
		tracelog.InfoLogger.Printf("Adding data directory")
	} else {
		curTbs := bb.tablespaces[bb.tbsPointer]
		tsr, err := NewTarballStreamerRemap("^", fmt.Sprintf("pg_tblspc/%d/", curTbs.OID))
		if err != nil {
			return err
		}
		remaps = append(remaps, *tsr)
		tracelog.InfoLogger.Printf("Adding tablespace %d (%s)", curTbs.OID, curTbs.Location)
	}
	bb.streamer.Tee = tee
	bb.streamer.Remaps = remaps

	bb.tbsStreaming = true
	bb.tbsPointer++
	return nil
}

// Upload will read all tar files from Postgres, and use the uploader to upload to the backup location
func (bb *StreamingBaseBackup) Upload(uploader *WalUploader, bundleFiles internal.BundleFiles) (err error) {
	// Upload the tar
	bb.uploader = uploader
	bb.streamer = NewTarballStreamer(bb, bb.maxTarSize, bundleFiles)
	for {
		tbsTar := ioextensions.NewNamedReaderImpl(bb.streamer, bb.FileName())
		compressedFile := internal.CompressAndEncrypt(tbsTar, bb.uploader.Compressor, internal.ConfigureCrypter())
		dstPath := fmt.Sprintf("%s.%s", bb.Path(), bb.uploader.Compressor.FileExtension())
		err = bb.uploader.Upload(dstPath, compressedFile)
		if err != nil {
			return err
		}
		if bb.readIndex == 0 && !bb.tbsStreaming && bb.tbsPointer > len(bb.tablespaces) {
			// No data in buffer, not streaming anymore, and no Table spaces left. We are done here...
			break
		}
		bb.fileNo++
	}

	// Update file info
	bb.streamer.Files.GetUnderlyingMap().Range(func(k, v interface{}) bool {
		fileName := k.(string)
		description := v.(internal.BackupFileDescription)
		bb.Files[fileName] = description
		return true
	})

	// Upload the extra tar
	if len(bb.streamer.Tee) > 0 {
		teeTar := ioextensions.NewNamedReaderImpl(bb.streamer.TeeIo, bb.FileName())
		teeCompressedFile := internal.CompressAndEncrypt(teeTar, bb.uploader.Compressor, internal.ConfigureCrypter())
		teeFileName := fmt.Sprintf("pg_control.tar.%s", bb.uploader.Compressor.FileExtension())
		teeFilePath := storage.JoinPath(bb.BackupName(), internal.TarPartitionFolderName, teeFileName)
		err = bb.uploader.Upload(teeFilePath, teeCompressedFile)
		if err != nil {
			return err
		}
	}

	return nil
}

// BackupName returns the name of the folder where the backup should be stored.
func (bb *StreamingBaseBackup) BackupName() string {
	return "base_" + formatWALFileName(bb.TimeLine, uint64(bb.StartLSN)/WalSegmentSize)
}

// FileName returns the filename of a tablespace backup file.
// This is used by the WalUploader to set the name of the destination file during upload of the backup file.
func (bb *StreamingBaseBackup) FileName() string {
	// Example LSN -> Name:
	// '0/2A33FE00' -> '00000001000000000000002A'
	return fmt.Sprintf("part_%03d.tar", bb.fileNo+1)
}

// Path returns the name of the folder where the backup should be stored.
func (bb *StreamingBaseBackup) Path() string {
	return storage.JoinPath(bb.BackupName(), internal.TarPartitionFolderName, bb.FileName())
}

// GetTablespaceSpec returns the tablespace specifications.
func (bb *StreamingBaseBackup) GetTablespaceSpec() *TablespaceSpec {
	spec := NewTablespaceSpec(bb.dataDir)
	for _, tbs := range bb.tablespaces {
		spec.addTablespace(fmt.Sprintf("%d", tbs.OID), tbs.Location)
	}
	return &spec
}

// streamFromPostgres is the lower level function to receive a portion of the backup data from Postgres.
// The data is stored in the buffer where the Read function can retrieve it.
func (bb *StreamingBaseBackup) streamFromPostgres() (err error) {
	var message pgproto3.BackendMessage
	if !bb.tbsStreaming {
		err = bb.nextTbs()
		if err != nil {
			return err
		}
	}

	nextStandbyMessageDeadline := time.Now().Add(StandbyMessageTimeout)
	if bb.readIndex != 0 {
		// Protection against overwriting a partially read buffer
		return nil
	}
	for {
		ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
		message, err = bb.pgConn.ReceiveMessage(ctx)
		cancel()
		if pgconn.Timeout(err) {
			continue
		}
		tracelog.ErrorLogger.FatalOnError(err)
		switch msg := message.(type) {
		case *pgproto3.CopyData:
			bb.buffer = msg.Data
			return nil
		case *pgproto3.CopyDone:
			bb.tbsStreaming = false
			return nil
		default:
			return errors.Errorf("Received unexpected message: %#v\n", msg)
		}
	}
}

// Read makes the StreamingBaseBackup an io.Reader, to be handled by WalUploader.UploadWalFile written to a file.
func (bb *StreamingBaseBackup) Read(p []byte) (n int, err error) {
	if bb.readIndex == 0 {
		err = bb.streamFromPostgres()
		if err == errTbsOutOfRange {
			// Buffer is empty, and we have streamed all tablespaces. We are done here.
			return 0, io.EOF
		}
		if err != nil {
			return
		}
	}
	n = copy(p, bb.buffer[bb.readIndex:])
	bb.UncompressedSize += int64(n)
	bb.readIndex += n
	if bb.readIndex == len(bb.buffer) {
		//The entire buffer is returned. Empty buffer.
		bb.buffer = []byte{}
		bb.readIndex = 0
	}
	return n, nil
}

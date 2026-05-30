package postgres

/*
This object represents a base backup object.
A base backup object can connect to Postgres, issue a BASE_BACKUP command, and receive the backup data from Postgres.
*/

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"iter"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// The StreamingBaseBackup object represents a Postgres BASE_BACKUP, connecting to Postgres, and streaming backup data.
// On PG14 and earlier, every tablespace is sent in its own CopyOut session.
// On PG15+, all archives plus optional manifest live in a single CopyOut session
// with one-byte-tagged CopyData payloads
type StreamingBaseBackup struct {
	TimeLine         uint32
	StartLSN         pglogrepl.LSN
	EndLSN           pglogrepl.LSN
	tablespaces      []pglogrepl.BaseBackupTablespace
	pgConn           *pgconn.PgConn
	UncompressedSize int64
	maxTarSize       int64
	dataDir          string
	Files            internal.BackupFileList
	uploader         internal.Uploader
	fileNo           int
	pgVersion        int
}

// NewStreamingBaseBackup will define a new StreamingBaseBackup object
func NewStreamingBaseBackup(pgDataDir string, maxTarSize int64, pgVersion int, pgConn *pgconn.PgConn) *StreamingBaseBackup {
	return &StreamingBaseBackup{
		dataDir:    pgDataDir,
		maxTarSize: maxTarSize,
		pgConn:     pgConn,
		pgVersion:  pgVersion,
	}
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

// archive describes one BASE_BACKUP archive (data dir or one tablespace).
// reader yields the archive's tar bytes (including its 1024-byte trailer)
// and is valid only during the iteration that produced it.
type archive struct {
	name   string // "base.tar" or "<oid>.tar"
	oid    int32  // 0 for data dir
	reader io.Reader
}

func (a *archive) isDataDir() bool { return a.oid == 0 }

// Archives streams the archives produced by the running BASE_BACKUP command,
// dispatching on bb.pgVersion. PG14- yields one archive per tablespace driven
// by per-tablespace CopyOut framing; PG15+ parses tagged CopyData payloads
// out of the singleton CopyOut session.
//
// The yielded archive's reader is valid only until the loop body completes
// for that iteration. Errors are surfaced in-band; on error the iterator
// yields once with (nil, err) and stops.
func (bb *StreamingBaseBackup) Archives(ctx context.Context) iter.Seq2[*archive, error] {
	if bb.pgVersion < 150000 {
		return bb.compatArchives(ctx)
	}
	return bb.streamArchives(ctx)
}

func remapsForArchive(arch *archive) (TarballStreamerRemaps, []string, error) {
	if arch.isDataDir() {
		return nil, []string{"global/pg_control"}, nil
	}
	tsr, err := NewTarballStreamerRemap("^", fmt.Sprintf("pg_tblspc/%d/", arch.oid))
	if err != nil {
		return nil, nil, err
	}
	return TarballStreamerRemaps{*tsr}, nil, nil
}

// Upload streams every archive produced by BASE_BACKUP through a per-archive
// TarballStreamer (and therefore a fresh inner tar.Reader per archive),
// rotating into wal-g part files when maxTarSize is exceeded. The Tee tar
// (pg_control) is uploaded at the end from the streamer that produced it.
func (bb *StreamingBaseBackup) Upload(ctx context.Context, uploader internal.Uploader, bundleFiles internal.BundleFiles) error {
	bb.uploader = uploader

	var teeStreamer *TarballStreamer

	for arch, err := range bb.Archives(ctx) {
		if err != nil {
			return err
		}
		streamer := NewTarballStreamer(arch.reader, bb.maxTarSize, bundleFiles)
		remaps, tee, err := remapsForArchive(arch)
		if err != nil {
			return err
		}
		streamer.Remaps = remaps
		streamer.Tee = tee
		if len(tee) > 0 {
			teeStreamer = streamer
		}

		for {
			tbsTar := ioextensions.NewNamedReaderImpl(streamer, bb.FileName())
			compressedFile := internal.CompressAndEncrypt(tbsTar, uploader.Compression(), internal.ConfigureCrypter())
			dstPath := utility.AddFileExtension(bb.Path(), uploader.Compression().FileExtension())
			if err := uploader.Upload(ctx, dstPath, compressedFile); err != nil {
				return err
			}
			bb.fileNo++
			if streamer.ArchiveDone() {
				break
			}
		}

		streamer.Files.GetUnderlyingMap().Range(func(k, v interface{}) bool {
			fileName := k.(string)
			description := v.(internal.BackupFileDescription)
			bb.Files[fileName] = description
			return true
		})
	}

	if teeStreamer != nil {
		teeTar := ioextensions.NewNamedReaderImpl(teeStreamer.TeeIo, bb.FileName())
		teeCompressedFile := internal.CompressAndEncrypt(teeTar, bb.uploader.Compression(), internal.ConfigureCrypter())
		teeFileName := utility.AddFileExtension("pg_control.tar", bb.uploader.Compression().FileExtension())
		teeFilePath := storage.JoinPath(bb.BackupName(), internal.TarPartitionFolderName, teeFileName)
		if err := bb.uploader.Upload(ctx, teeFilePath, teeCompressedFile); err != nil {
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

// recvMessage receives the next backend message, refreshing StandbyMessageTimeout
// per attempt and honoring ctx cancellation.
func recvMessage(ctx context.Context, conn *pgconn.PgConn) (pgproto3.BackendMessage, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		deadline := time.Now().Add(StandbyMessageTimeout)
		rctx, cancel := context.WithDeadline(ctx, deadline)
		msg, err := conn.ReceiveMessage(rctx)
		cancel()
		if pgconn.Timeout(err) && ctx.Err() == nil {
			continue
		}
		return msg, err
	}
}

// base backup protocol up to PG14
func (bb *StreamingBaseBackup) compatArchives(ctx context.Context) iter.Seq2[*archive, error] {
	return func(yield func(*archive, error) bool) {
		for tbsIdx := 0; tbsIdx <= len(bb.tablespaces); tbsIdx++ {
			if err := pglogrepl.NextTableSpace(ctx, bb.pgConn); err != nil {
				yield(nil, err)
				return
			}
			arch := bb.compatArchiveForIdx(tbsIdx)
			r := &compatReader{bb: bb, ctx: ctx}
			arch.reader = r
			if !yield(arch, nil) {
				return
			}
			if err := r.drain(); err != nil {
				yield(nil, err)
				return
			}
		}
	}
}

func (bb *StreamingBaseBackup) compatArchiveForIdx(idx int) *archive {
	if idx == len(bb.tablespaces) {
		tracelog.InfoLogger.Printf("Adding data directory")
		return &archive{name: "base.tar"}
	}
	tbs := bb.tablespaces[idx]
	tracelog.InfoLogger.Printf("Adding tablespace %d (%s)", tbs.OID, tbs.Location)
	return &archive{name: fmt.Sprintf("%d.tar", tbs.OID), oid: tbs.OID}
}

// compatReader yields raw CopyData payloads for one tablespace until CopyDone.
type compatReader struct {
	bb       *StreamingBaseBackup
	ctx      context.Context
	chunk    []byte
	chunkPos int
	done     bool
}

func (r *compatReader) Read(p []byte) (int, error) {
	for r.chunkPos == len(r.chunk) {
		if r.done {
			return 0, io.EOF
		}
		if err := r.pump(); err != nil {
			return 0, err
		}
	}
	n := copy(p, r.chunk[r.chunkPos:])
	r.chunkPos += n
	r.bb.UncompressedSize += int64(n)
	return n, nil
}

func (r *compatReader) pump() error {
	msg, err := recvMessage(r.ctx, r.bb.pgConn)
	if err != nil {
		return err
	}
	switch m := msg.(type) {
	case *pgproto3.CopyData:
		r.chunk = m.Data
		r.chunkPos = 0
	case *pgproto3.CopyDone:
		r.done = true
	case *pgproto3.NoticeResponse:
		// drop
	case *pgproto3.ErrorResponse:
		return pgconn.ErrorResponseToPgError(m)
	default:
		return errors.Errorf("BASE_BACKUP: unexpected message: %#v", msg)
	}
	return nil
}

// drain consumes any remaining CopyData chunks and the closing CopyDone so
// that the next call to NextTableSpace lands on the next CopyOutResponse.
func (r *compatReader) drain() error {
	for !r.done {
		if err := r.pump(); err != nil {
			return err
		}
	}
	return nil
}

// base backup protocol for PG15+
func (bb *StreamingBaseBackup) streamArchives(ctx context.Context) iter.Seq2[*archive, error] {
	return func(yield func(*archive, error) bool) {
		// Consume the singleton CopyOutResponse. Tolerate intervening
		// NoticeResponse / ParameterStatus.
		for {
			msg, err := recvMessage(ctx, bb.pgConn)
			if err != nil {
				yield(nil, err)
				return
			}
			done := false
			switch m := msg.(type) {
			case *pgproto3.CopyOutResponse:
				done = true
			case *pgproto3.NoticeResponse, *pgproto3.ParameterStatus:
				// drop
			case *pgproto3.ErrorResponse:
				yield(nil, pgconn.ErrorResponseToPgError(m))
				return
			default:
				yield(nil, errors.Errorf("BASE_BACKUP: expected CopyOutResponse, got %#v", msg))
				return
			}
			if done {
				break
			}
		}
		(&streamPump{bb: bb, ctx: ctx, yield: yield}).run()
	}
}

// streamPump drives the PG15+ tagged CopyData stream and yields one archive
// at a time. State is shared between the pump's own drain loop and the
// streamReader handed to each yielded archive; only one goroutine pumps at a
// time (Upload's BG compression goroutine reads bytes via streamReader during
// yield, then exits before the main goroutine resumes the drain loop).
type streamPump struct {
	bb         *StreamingBaseBackup
	ctx        context.Context
	yield      func(*archive, error) bool
	chunk      []byte // current 'd' payload (after stripping tag)
	chunkPos   int
	archiveEnd bool     // current archive done (boundary tag seen on wire)
	streamEnd  bool     // CopyDone seen
	pendingArc *archive // 'n' parsed but not yet yielded
	inManifest bool     // 'm' seen, swallowing 'd' until CopyDone
}

func (s *streamPump) run() {
	for !s.streamEnd {
		// Pump until we have a pending archive or stream end.
		for s.pendingArc == nil && !s.streamEnd {
			if err := s.advance(); err != nil {
				s.yield(nil, err)
				return
			}
		}
		if s.streamEnd {
			return
		}
		arch := s.pendingArc
		s.pendingArc = nil
		s.archiveEnd = false
		arch.reader = s
		if !s.yield(arch, nil) {
			return
		}
		// Caller broke out of streaming; drain any remaining wire events for
		// this archive (e.g. trailing 'p' before next 'n'/'m'/CopyDone).
		for !s.archiveEnd && !s.streamEnd {
			if err := s.advance(); err != nil {
				s.yield(nil, err)
				return
			}
		}
	}
}

// advance reads exactly one wire message and updates pump state.
func (s *streamPump) advance() error {
	msg, err := recvMessage(s.ctx, s.bb.pgConn)
	if err != nil {
		return err
	}
	switch m := msg.(type) {
	case *pgproto3.CopyData:
		return s.handleCopyData(m.Data)
	case *pgproto3.CopyDone:
		s.streamEnd = true
		s.archiveEnd = true
		return nil
	case *pgproto3.NoticeResponse:
		return nil
	case *pgproto3.ErrorResponse:
		return pgconn.ErrorResponseToPgError(m)
	default:
		return errors.Errorf("BASE_BACKUP: unexpected message: %#v", msg)
	}
}

func (s *streamPump) handleCopyData(data []byte) error {
	if len(data) == 0 {
		return errors.New("BASE_BACKUP: empty CopyData payload")
	}
	tag := data[0]
	body := data[1:]
	switch tag {
	case 'd':
		if s.inManifest {
			return nil
		}
		s.chunk = body
		s.chunkPos = 0
	case 'p':
		// 8-byte BE counter; ignored (Progress not requested)
	case 'n':
		if s.inManifest {
			return errors.New("BASE_BACKUP: unexpected 'n' inside manifest stream")
		}
		name, path, err := parseArchiveHeader(body)
		if err != nil {
			return err
		}
		arch, err := s.bb.makeArchive(name, path)
		if err != nil {
			return err
		}
		s.archiveEnd = true
		s.pendingArc = arch
	case 'm':
		tracelog.WarningLogger.Print("BASE_BACKUP: manifest stream received but not requested; dropping")
		s.inManifest = true
		s.archiveEnd = true
	default:
		return errors.Errorf("BASE_BACKUP: unexpected CopyData tag %q", tag)
	}
	return nil
}

// parseArchiveHeader parses the body of an 'n' message: cstring archive_name
// followed by cstring path.
func parseArchiveHeader(body []byte) (name, path string, err error) {
	nameEnd := bytes.IndexByte(body, 0)
	if nameEnd < 0 {
		return "", "", errors.New("BASE_BACKUP: archive header missing NUL after name")
	}
	rest := body[nameEnd+1:]
	pathEnd := bytes.IndexByte(rest, 0)
	if pathEnd < 0 {
		return "", "", errors.New("BASE_BACKUP: archive header missing NUL after path")
	}
	return string(body[:nameEnd]), string(rest[:pathEnd]), nil
}

func (bb *StreamingBaseBackup) makeArchive(name, path string) (*archive, error) {
	if name == "base.tar" {
		tracelog.InfoLogger.Printf("Adding data directory")
		return &archive{name: name}, nil
	}
	oidStr := strings.TrimSuffix(name, ".tar")
	if oidStr == name {
		return nil, errors.Errorf("BASE_BACKUP: unrecognized archive name %q", name)
	}
	oid64, err := strconv.ParseInt(oidStr, 10, 32)
	if err != nil {
		return nil, errors.Wrapf(err, "BASE_BACKUP: parsing OID from archive %q", name)
	}
	oid := int32(oid64)
	if !bb.knownTablespace(oid) {
		return nil, errors.Errorf("BASE_BACKUP: archive %q for unknown tablespace OID %d", name, oid)
	}
	tracelog.InfoLogger.Printf("Adding tablespace %d (%s)", oid, path)
	return &archive{name: name, oid: oid}, nil
}

func (bb *StreamingBaseBackup) knownTablespace(oid int32) bool {
	return slices.ContainsFunc(bb.tablespaces, func(ts pglogrepl.BaseBackupTablespace) bool {
		return ts.OID == oid
	})
}

// Read yields concatenated 'd' payloads for the currently-yielded archive and
// EOFs at the wire-side boundary marker ('n'/'m'/CopyDone). Caller must not
// retain a reference past the iteration that produced the archive.
func (s *streamPump) Read(p []byte) (int, error) {
	for s.chunkPos == len(s.chunk) {
		if s.archiveEnd {
			return 0, io.EOF
		}
		if err := s.advance(); err != nil {
			return 0, err
		}
	}
	n := copy(p, s.chunk[s.chunkPos:])
	s.chunkPos += n
	s.bb.UncompressedSize += int64(n)
	return n, nil
}

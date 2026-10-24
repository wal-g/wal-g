package postgres

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// This PoC accepts the options used by pg_basebackup --no-manifest -X stream
// (or -X none). Other options fail explicitly rather than changing the backup.
var walSendBaseBackupQuery = func() *regexp.Regexp {
	option := `(?:LABEL\s+'(?:[^']|'')*'|PROGRESS|TABLESPACE_MAP|FAST|NOWAIT|NOVERIFY_CHECKSUMS|` +
		`CHECKPOINT\s+'(?:fast|spread)'|WAIT\s+[01]|VERIFY_CHECKSUMS\s+0|TARGET\s+'client')`
	return regexp.MustCompile(`(?is)^BASE_BACKUP\s*(?:\(\s*(?:` + option + `(?:\s*,\s*` + option + `)*)?\s*\)|` +
		`(?:` + option + `\s*)*)$`)
}()

func loadWalSendBaseBackup(ctx context.Context, folder storage.Folder, name string) (*Backup, error) {
	source, err := internal.GetBackupByName(ctx, name, utility.BaseBackupPath, folder)
	if err != nil {
		return nil, err
	}
	backup := ToPgBackup(source)
	sentinel, err := backup.GetSentinel(ctx)
	if err != nil {
		return nil, err
	}
	if sentinel.IncrementFrom != nil || (sentinel.TablespaceSpec != nil && !sentinel.TablespaceSpec.empty()) {
		return nil, fmt.Errorf("BASE_BACKUP PoC requires a full backup without external tablespaces")
	}
	if sentinel.BackupStartLSN == nil || sentinel.BackupFinishLSN == nil ||
		*sentinel.BackupStartLSN == 0 || *sentinel.BackupFinishLSN < *sentinel.BackupStartLSN ||
		sentinel.PgVersion < 100000 || sentinel.SystemIdentifier == nil {
		return nil, fmt.Errorf("BASE_BACKUP PoC requires backup LSNs, PostgreSQL version and system identifier")
	}
	return &backup, nil
}

func (c *walSendClient) handleBaseBackup(ctx context.Context, query string) {
	if c.server.baseBackup == nil {
		_ = c.writeQueryError("0A000", "enable BASE_BACKUP PoC with wal-send --backup-name LATEST")
		return
	}
	if !walSendBaseBackupQuery.MatchString(query) {
		_ = c.writeQueryError("0A000", "BASE_BACKUP PoC supports pg_basebackup --no-manifest -X stream (or -X none)")
		return
	}
	dir, err := os.MkdirTemp("", "walg-basebackup-")
	if err != nil {
		_ = c.writeQueryError("XX000", err.Error())
		return
	}
	defer func() { _ = os.RemoveAll(dir) }()
	// Use the normal restore path to preserve metadata filtering, compression,
	// encryption and backup_label/pg_control ordering. Disk usage is a full copy.
	backup := *c.server.baseBackup
	files, err := backup.GetFilesToUnwrap(ctx, "")
	if err == nil {
		err = backup.unwrapToEmptyDirectory(ctx, dir, files, false, ExtractProviderImpl{})
	}
	if err == nil {
		err = os.MkdirAll(filepath.Join(dir, "pg_wal", "archive_status"), 0700)
	}
	if err == nil {
		err = c.sendBaseBackup(ctx, dir, backup)
	}
	if err != nil {
		_ = c.writeQueryError("XX000", err.Error())
	}
}

func (c *walSendClient) baseBackupPosition(lsn LSN, timeline uint32) error {
	return c.writeMessages(pgRowDescription("recptr", "tli"),
		&pgproto3.DataRow{Values: [][]byte{[]byte(lsn.String()), []byte(strconv.FormatUint(uint64(timeline), 10))}},
		&pgproto3.CommandComplete{CommandTag: []byte("SELECT")})
}

func (c *walSendClient) sendBaseBackup(ctx context.Context, dir string, backup Backup) error {
	sentinel := backup.SentinelDto
	timeline, err := ParseTimelineFromBackupName(backup.Name)
	if err != nil {
		return err
	}
	if err := c.baseBackupPosition(*sentinel.BackupStartLSN, timeline); err != nil {
		return err
	}
	if err := c.writeMessages(pgRowDescription("spcoid", "spclocation", "size"),
		&pgproto3.DataRow{Values: [][]byte{nil, nil, []byte(strconv.FormatInt(sentinel.UncompressedSize/1024, 10))}},
		&pgproto3.CommandComplete{CommandTag: []byte("SELECT")}, &pgproto3.CopyOutResponse{}); err != nil {
		return err
	}
	modern := sentinel.PgVersion >= 150000
	if modern {
		if err := c.writeMessages(&pgproto3.CopyData{Data: []byte("nbase.tar\x00\x00")}); err != nil {
			return err
		}
	}
	if err := writeWalSendBaseBackupTar(ctx, dir, walSendBaseBackupWriter{c, modern}, modern); err != nil {
		return err
	}
	if err := c.writeMessages(&pgproto3.CopyDone{}); err != nil {
		return err
	}
	if err := c.baseBackupPosition(*sentinel.BackupFinishLSN, timeline); err != nil {
		return err
	}
	return c.writeMessages(&pgproto3.CommandComplete{CommandTag: []byte("BASE_BACKUP")}, &pgproto3.ReadyForQuery{TxStatus: 'I'})
}

type walSendBaseBackupWriter struct {
	client *walSendClient
	modern bool
}

func (w walSendBaseBackupWriter) Write(data []byte) (int, error) {
	n := len(data)
	if w.modern {
		data = append([]byte{'d'}, data...)
	}
	if err := w.client.writeMessages(&pgproto3.CopyData{Data: data}); err != nil {
		return 0, err
	}
	return n, nil
}

func writeWalSendBaseBackupTar(ctx context.Context, dir string, dst io.Writer, modern bool) error {
	tw := tar.NewWriter(dst)
	// One traversal of the restored backup per BASE_BACKUP request. No archive
	// enumeration or local directory scan is added to the WAL streaming loop.
	err := filepath.WalkDir(dir, func(name string, entry fs.DirEntry, err error) error {
		if err != nil || ctx.Err() != nil {
			return errors.Join(err, ctx.Err())
		}
		if name == dir {
			return nil
		}
		rel, err := filepath.Rel(dir, name)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			return fmt.Errorf("BASE_BACKUP PoC cannot send special file %s", rel)
		}
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name, header.Format = filepath.ToSlash(rel), tar.FormatUSTAR
		header.AccessTime, header.ChangeTime = time.Time{}, time.Time{}
		if info.IsDir() {
			header.Name += "/" // Older pg_basebackup clients recognize directories by this suffix.
		}
		if err := tw.WriteHeader(header); err != nil || info.IsDir() {
			return err
		}
		file, err := os.Open(name)
		if err != nil {
			return err
		}
		_, err = io.Copy(tw, file)
		return errors.Join(err, file.Close())
	})
	if err != nil {
		return err
	}
	if !modern {
		return tw.Flush() // Pre-15 clients use CopyDone as EOF and cannot consume the tar trailer.
	}
	return tw.Close()
}

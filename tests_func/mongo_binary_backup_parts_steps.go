package functests

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/cucumber/godog"
	"github.com/pierrec/lz4/v4"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/crypto/openpgp"
	"github.com/wal-g/wal-g/tests_func/helpers"
	"github.com/wal-g/wal-g/utility"
)

func setupMongodbBinaryBackupPartsSteps(ctx *godog.ScenarioContext, tctx *TestContext) {
	ctx.Step(`^([^\s]+) has (\d+) indexed large mongodb documents$`, tctx.addMongoBinaryBackupPartsData)
	ctx.Step(`^([^\s]+) has (\d+) valid indexed mongodb documents$`, tctx.validateMongoBinaryBackupPartsData)
	ctx.Step(`^we create binary mongo-backup on ([^\s]+) with file parts of (\d+) bytes$`,
		tctx.createMongoBinaryBackupWithFileParts)
	ctx.Step(`^binary mongo-backup #(\d+) on ([^\s]+) stores collection and indexes in parts of (\d+) bytes$`,
		tctx.checkMongoBinaryBackupFileParts)
}

func (tctx *TestContext) addMongoBinaryBackupPartsData(container string, documentCount int) error {
	mc, err := MongoCtlFromTestContext(tctx, container)
	if err != nil {
		return err
	}
	return mc.WriteBinaryBackupPartsData(documentCount)
}

func (tctx *TestContext) validateMongoBinaryBackupPartsData(container string, documentCount int) error {
	mc, err := MongoCtlFromTestContext(tctx, container)
	if err != nil {
		return err
	}
	return mc.ValidateBinaryBackupPartsData(documentCount)
}

func (tctx *TestContext) createMongoBinaryBackupWithFileParts(container string, partSize int) error {
	return WalgUtilFromTestContext(tctx, container).PushBinaryBackupWithFileParts(partSize)
}

type binaryBackupFilePaths struct {
	DBPath   string   `json:"db_path"`
	TarPaths []string `json:"tar_paths"`
}

func (tctx *TestContext) checkMongoBinaryBackupFileParts(backupNumber int, container string, partSize int) error {
	backupName, err := WalgUtilFromTestContext(tctx, container).GetBackupByNumber(backupNumber)
	if err != nil {
		return err
	}
	client, err := S3StorageFromTestContext(tctx, tctx.S3Host()).Client()
	if err != nil {
		return err
	}
	backupPrefix, err := helpers.ObjectKeyFromS3Prefix(
		tctx.Env["WALG_S3_PREFIX"], tctx.Env["S3_BUCKET"], utility.BaseBackupPath, backupName,
	)
	if err != nil {
		return err
	}
	metadata, err := client.FileContents(path.Join(backupPrefix, utility.MetadataFileName))
	if err != nil {
		return err
	}
	var routes struct {
		Databases map[string]map[string]struct {
			Paths     binaryBackupFilePaths            `json:"paths"`
			IndexInfo map[string]binaryBackupFilePaths `json:"index_info"`
		} `json:"databases"`
	}
	if err = json.Unmarshal(metadata, &routes); err != nil {
		return err
	}
	collection, ok := routes.Databases["file_parts"]["documents"]
	if !ok {
		return fmt.Errorf("test collection is absent from backup metadata")
	}
	files := []binaryBackupFilePaths{collection.Paths}
	if !strings.HasPrefix(path.Base(collection.Paths.DBPath), "collection-") {
		return fmt.Errorf("expected a WiredTiger collection file, got %q", collection.Paths.DBPath)
	}
	for _, index := range []string{"_id_", "unique_key", "group_key", "tags"} {
		file, ok := collection.IndexInfo[index]
		if !ok || !strings.HasPrefix(path.Base(file.DBPath), "index-") {
			return fmt.Errorf("missing WiredTiger file for index %q: %+v", index, file)
		}
		files = append(files, file)
	}

	crypter := openpgp.CrypterFromKeyPath(path.Join("images", "mongodb", "config", "gpg-key.armor"),
		func() (string, bool) { return "", false })
	seenObjects := make(map[string]bool)
	for _, file := range files {
		if len(file.TarPaths) < 2 {
			return fmt.Errorf("expected multiple independent objects for %s, got %v", file.DBPath, file.TarPaths)
		}
		type filePart struct {
			offset, size, fileSize int64
		}
		parts := make([]filePart, 0, len(file.TarPaths))
		for _, tarPath := range file.TarPaths {
			if seenObjects[tarPath] {
				return fmt.Errorf("file part object %s appears more than once", tarPath)
			}
			seenObjects[tarPath] = true
			objectKey := path.Join(backupPrefix, "tar_partitions", tarPath)
			contents, err := client.FileContents(objectKey)
			if err != nil {
				return err
			}
			decrypted, err := crypter.Decrypt(tctx.Context, bytes.NewReader(contents))
			if err != nil {
				return fmt.Errorf("decrypt %s: %w", objectKey, err)
			}
			decompressed := lz4.NewReader(decrypted)
			tarReader := tar.NewReader(decompressed)
			header, err := tarReader.Next()
			if err != nil {
				return fmt.Errorf("read header from %s: %w", objectKey, err)
			}
			if strings.TrimPrefix(header.Name, "/") != strings.TrimPrefix(file.DBPath, "/") {
				return fmt.Errorf("object %s contains %q, expected %q", objectKey, header.Name, file.DBPath)
			}
			offset, err := strconv.ParseInt(header.PAXRecords["WAL-G.MongoDB.FilePartOffset"], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid file part offset in %s: %w", objectKey, err)
			}
			fileSize, err := strconv.ParseInt(header.PAXRecords["WAL-G.MongoDB.FileSize"], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid full file size in %s: %w", objectKey, err)
			}
			if header.Size <= 0 || header.Size > int64(partSize) {
				return fmt.Errorf("object %s payload size %d exceeds part size %d", objectKey, header.Size, partSize)
			}
			parts = append(parts, filePart{offset: offset, size: header.Size, fileSize: fileSize})
			if _, err = io.Copy(io.Discard, tarReader); err != nil {
				return err
			}
			if _, err = tarReader.Next(); err != io.EOF {
				return fmt.Errorf("object %s must contain exactly one file part, got %v", objectKey, err)
			}
			if _, err = io.Copy(io.Discard, decompressed); err != nil {
				return fmt.Errorf("read complete encrypted object %s: %w", objectKey, err)
			}
		}
		sort.Slice(parts, func(i, j int) bool { return parts[i].offset < parts[j].offset })
		var nextOffset int64
		for _, part := range parts {
			if part.offset != nextOffset || part.fileSize != parts[0].fileSize {
				return fmt.Errorf("file %s has inconsistent or noncontiguous parts: %+v", file.DBPath, parts)
			}
			if part.offset+part.size < part.fileSize && part.size != int64(partSize) {
				return fmt.Errorf("file %s contains a short nonfinal part: %+v", file.DBPath, part)
			}
			nextOffset += part.size
		}
		if nextOffset != parts[0].fileSize {
			return fmt.Errorf("file %s has %d backed up bytes, expected %d", file.DBPath, nextOffset, parts[0].fileSize)
		}
		tracelog.InfoLogger.Printf("Verified %d independently downloadable parts for %s (%d bytes)",
			len(parts), file.DBPath, nextOffset)
	}
	return nil
}

package walg

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime/pprof"
	"strconv"
	"text/tabwriter"
	"time"
)

type InvalidWalFileMagicError struct {
	error
}

func NewInvalidWalFileMagicError() InvalidWalFileMagicError {
	return InvalidWalFileMagicError{errors.New("WAL-G: WAL file magic is invalid ")}
}

type CantOverwriteWalFileError struct {
	error
}

func NewCantOverwriteWalFileError(walFilePath string) CantOverwriteWalFileError {
	return CantOverwriteWalFileError{errors.Errorf("WAL file '%s' already archived, contents differ, unable to overwrite\n", walFilePath)}
}

const DefaultDataFolderPath = "/tmp"

type ArchiveNonExistenceError struct {
	error
}

func NewArchiveNonExistenceError(archiveName string) ArchiveNonExistenceError {
	return ArchiveNonExistenceError{errors.Errorf("Archive '%s' does not exist.\n", archiveName)}
}

// TODO : unit tests
// HandleDelete is invoked to perform wal-g delete
func HandleDelete(folder *S3Folder, args []string) {
	arguments := ParseDeleteArguments(args, printDeleteUsageAndFail)

	if arguments.Before {
		if arguments.BeforeTime == nil {
			deleteBeforeTarget(NewBackup(folder, arguments.Target), arguments.FindFull, nil, arguments.dryrun)
		} else {
			backups, err := getBackups(folder)
			if err != nil {
				log.Fatal(err)
			}
			for _, b := range backups {
				if b.Time.Before(*arguments.BeforeTime) {
					deleteBeforeTarget(NewBackup(folder, b.Name), arguments.FindFull, backups, arguments.dryrun)
					return
				}
			}
			log.Println("No backups before ", *arguments.BeforeTime)
		}
	}
	if arguments.Retain {
		backupCount, err := strconv.Atoi(arguments.Target)
		if err != nil {
			log.Fatal("Unable to parse number of backups: ", err)
		}
		backups, err := getBackups(folder)
		if err != nil {
			log.Fatal(err)
		}
		if arguments.Full {
			if len(backups) <= backupCount {
				fmt.Printf("Have only %v backups.\n", backupCount)
			}
			left := backupCount
			for _, b := range backups {
				if left == 1 {
					deleteBeforeTarget(NewBackup(folder, b.Name), true, backups, arguments.dryrun)
					return
				}
				backup := NewBackup(folder, b.Name)
				dto := backup.fetchSentinel()
				if !dto.isIncremental() {
					left--
				}
			}
			fmt.Printf("Scanned all backups but didn't have %v full.", backupCount)
		} else {
			if len(backups) <= backupCount {
				fmt.Printf("Have only %v backups.\n", backupCount)
			} else {
				deleteBeforeTarget(NewBackup(folder, backups[backupCount-1].Name), arguments.FindFull, nil, arguments.dryrun)
			}
		}
	}
}

// TODO : unit tests
// HandleBackupList is invoked to perform wal-g backup-list
func HandleBackupList(folder *S3Folder) {
	backups, err := getBackups(folder)
	if err != nil {
		log.Fatal(err)
	}

	writer := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
	defer writer.Flush()
	fmt.Fprintln(writer, "name\tlast_modified\twal_segment_backup_start")

	for i := len(backups) - 1; i >= 0; i-- {
		b := backups[i]
		fmt.Fprintln(writer, fmt.Sprintf("%v\t%v\t%v", b.Name, b.Time.Format(time.RFC3339), b.WalFileName))
	}
}

// TODO : unit tests
// HandleBackupFetch is invoked to perform wal-g backup-fetch
func HandleBackupFetch(backupName string, folder *S3Folder, archiveDirectory string, mem bool) (lsn *uint64) {
	archiveDirectory = ResolveSymlink(archiveDirectory)
	lsn = deltaFetchRecursion(backupName, folder, archiveDirectory)

	if mem {
		memProfileLog, err := os.Create("mem.prof")
		if err != nil {
			log.Fatal(err)
		}

		pprof.WriteHeapProfile(memProfileLog)
		defer memProfileLog.Close()
	}
	return
}

// TODO : unit tests
// deltaFetchRecursion function composes Backup object and recursively searches for necessary base backup
func deltaFetchRecursion(backupName string, folder *S3Folder, archiveDirectory string) (lsn *uint64) {
	var backup *Backup
	// Check if backup exists and if it does extract to archiveDirectory.
	if backupName != "LATEST" {
		backup = NewBackup(folder, backupName)

		exists, err := backup.CheckExistence()
		if err != nil {
			log.Fatalf("%+v\n", err)
		}
		if !exists {
			log.Fatalf("Backup '%s' does not exist.\n", backup.Name)
		}

		// Find the LATEST valid backup (checks against JSON file and grabs backup name) and extract to archiveDirectory.
	} else {
		latest, err := GetLatestBackupKey(folder)
		if err != nil {
			log.Fatalf("%+v\n", err)
		}

		backup = NewBackup(folder, latest)
	}
	sentinelDto := backup.fetchSentinel()

	if sentinelDto.isIncremental() {
		fmt.Printf("Delta from %v at LSN %x \n", *sentinelDto.IncrementFrom, *sentinelDto.IncrementFromLSN)
		deltaFetchRecursion(*sentinelDto.IncrementFrom, folder, archiveDirectory)
		fmt.Printf("%v fetched. Upgrading from LSN %x to LSN %x \n", *sentinelDto.IncrementFrom, *sentinelDto.IncrementFromLSN, sentinelDto.BackupStartLSN)
	}

	unwrapBackup(backup, archiveDirectory, sentinelDto)

	lsn = sentinelDto.BackupStartLSN
	return
}

// TODO : unit tests
func extractPgControl(folder *S3Folder, fileTarInterpreter *FileTarInterpreter, name string) error {
	sentinel := make([]ReaderMaker, 1)
	sentinel[0] = NewS3ReaderMaker(folder, name)

	err := ExtractAll(fileTarInterpreter, sentinel)
	if err != nil {
		return err
	}

	if serr, ok := err.(UnsupportedFileTypeError); ok {
		return serr
	}
	return nil
}

// TODO : unit tests
// Do the job of unpacking Backup object
func unwrapBackup(backup *Backup, archiveDirectory string, sentinelDto S3TarBallSentinelDto) {

	incrementBase := path.Join(archiveDirectory, "increment_base")
	if !sentinelDto.isIncremental() {
		var empty = true
		searchLambda := func(path string, info os.FileInfo, err error) error {
			if path != archiveDirectory {
				empty = false
			}
			return nil
		}
		filepath.Walk(archiveDirectory, searchLambda)

		if !empty {
			log.Fatalf("Directory %v for delta base must be empty", archiveDirectory)
		}
	} else {
		defer func() {
			err := os.RemoveAll(incrementBase)
			if err != nil {
				log.Fatal(err)
			}
		}()

		err := os.MkdirAll(incrementBase, os.FileMode(os.ModePerm))
		if err != nil {
			log.Fatal(err)
		}

		files, err := ioutil.ReadDir(archiveDirectory)
		if err != nil {
			log.Fatal(err)
		}

		for _, f := range files {
			objName := f.Name()
			if objName != "increment_base" {
				err := os.Rename(path.Join(archiveDirectory, objName), path.Join(incrementBase, objName))
				if err != nil {
					log.Fatal(err)
				}
			}
		}

		for fileName, fd := range sentinelDto.Files {
			if !fd.IsSkipped {
				continue
			}
			fmt.Printf("Skipped file %v\n", fileName)
			targetPath := path.Join(archiveDirectory, fileName)
			// this path is only used for increment restoration
			incrementalPath := path.Join(incrementBase, fileName)
			err = moveFileAndCreateDirs(incrementalPath, targetPath, fileName)
			if err != nil {
				log.Fatal(err, "Failed to move skipped file for "+targetPath+" "+fileName)
			}
		}

	}

	keys, err := backup.GetKeys()
	if err != nil {
		log.Fatalf("%+v\n", err)
	}

	fileTarInterpreter := &FileTarInterpreter{
		NewDir:             archiveDirectory,
		Sentinel:           sentinelDto,
		IncrementalBaseDir: incrementBase,
	}
	out := make([]ReaderMaker, 0, len(keys))

	var pgControlKey *string
	pgControlRe := regexp.MustCompile(`^.*?/tar_partitions/pg_control\.tar(\..+$|$)`)
	for _, key := range keys {
		// Separate the pg_control key from the others to
		// extract it at the end, as to prevent server startup
		// with incomplete backup restoration.  But only if it
		// exists: it won't in the case of WAL-E backup
		// backwards compatibility.
		if pgControlRe.MatchString(key) {
			if pgControlKey != nil {
				panic("expect only one pg_control key match")
			}
			pgControlKey = &key
			continue
		}
		s := NewS3ReaderMaker(backup.Folder, key)
		out = append(out, s)
	}

	// Extract all compressed tar members except `pg_control.tar.lz4` if WALG version backup.
	err = ExtractAll(fileTarInterpreter, out)
	if serr, ok := err.(UnsupportedFileTypeError); ok {
		log.Fatalf("%v\n", serr)
	} else if err != nil {
		log.Fatalf("%+v\n", err)
	}
	// Check name for backwards compatibility. Will check for `pg_control` if WALG version of backup.
	re := regexp.MustCompile(`^([^_]+._{1}[^_]+._{1})`)
	match := re.FindString(backup.Name)
	if match == "" || sentinelDto.isIncremental() {
		if pgControlKey == nil {
			log.Fatal("Expect pg_control archive, but not found")
		}

		err = extractPgControl(backup.Folder, fileTarInterpreter, *pgControlKey)
		if err != nil {
			log.Fatalf("%+v\n", err)
		}
	}

	fmt.Print("\nBackup extraction complete.\n")
}

// TODO : unit tests
func getDeltaConfig() (maxDeltas int, fromFull bool) {
	stepsStr, hasSteps := os.LookupEnv("WALG_DELTA_MAX_STEPS")
	var err error
	if hasSteps {
		maxDeltas, err = strconv.Atoi(stepsStr)
		if err != nil {
			log.Fatal("Unable to parse WALG_DELTA_MAX_STEPS ", err)
		}
	}
	origin, hasOrigin := os.LookupEnv("WALG_DELTA_ORIGIN")
	if hasOrigin {
		switch origin {
		case "LATEST":
		case "LATEST_FULL":
			fromFull = true
		default:
			log.Fatal("Unknown WALG_DELTA_ORIGIN:", origin)
		}
	}
	return
}

// TODO : unit tests
// HandleBackupPush is invoked to perform a wal-g backup-push
func HandleBackupPush(archiveDirectory string, uploader *Uploader) {
	archiveDirectory = ResolveSymlink(archiveDirectory)
	maxDeltas, fromFull := getDeltaConfig()

	var previousBackupSentinelDto S3TarBallSentinelDto
	var previousBackupName string
	var err error
	incrementCount := 1

	if maxDeltas > 0 {
		previousBackupName, err = GetLatestBackupKey(uploader.uploadingFolder)
		if _, ok := err.(NoBackupsFoundError); !ok {
			if err != nil {
				log.Fatalf("%+v\n", err)
			}
			previousBackup := NewBackup(uploader.uploadingFolder, previousBackupName)
			previousBackupSentinelDto = previousBackup.fetchSentinel()
			if previousBackupSentinelDto.IncrementCount != nil {
				incrementCount = *previousBackupSentinelDto.IncrementCount + 1
			}

			if incrementCount > maxDeltas {
				fmt.Println("Reached max delta steps. Doing full backup.")
				previousBackupSentinelDto = S3TarBallSentinelDto{}
			} else if previousBackupSentinelDto.BackupStartLSN == nil {
				fmt.Println("LATEST backup was made without support for delta feature. Fallback to full backup with LSN marker for future deltas.")
			} else {
				if fromFull {
					fmt.Println("Delta will be made from full backup.")
					previousBackupName = *previousBackupSentinelDto.IncrementFullName
					previousBackup := NewBackup(uploader.uploadingFolder, previousBackupName)
					previousBackupSentinelDto = previousBackup.fetchSentinel()
				}
				fmt.Printf("Delta backup from %v with LSN %x. \n", previousBackupName, *previousBackupSentinelDto.BackupStartLSN)
			}
		}
	}

	bundle := NewBundle(archiveDirectory, previousBackupSentinelDto.BackupStartLSN, previousBackupSentinelDto.Files)

	// Connect to postgres and start/finish a nonexclusive backup.
	conn, err := Connect()
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	backupName, backupStartLSN, pgVersion, err := bundle.StartBackup(conn, time.Now().String())
	if err != nil {
		log.Fatalf("%+v\n", err)
	}

	if len(previousBackupName) > 0 && previousBackupSentinelDto.BackupStartLSN != nil {
		if uploader.useWalDelta {
			err = bundle.DownloadDeltaMap(uploader.uploadingFolder, backupStartLSN)
			if err == nil {
				fmt.Println("Successfully loaded delta map, delta backup will be made with provided delta map")
			} else {
				fmt.Printf("Error during loading delta map: '%v'. Fallback to full scan delta backup\n", err)
			}
		}
		backupName = backupName + "_D_" + stripWalFileName(previousBackupName)
	}

	bundle.TarBallMaker = NewS3TarBallMaker(backupName, uploader)

	// Start a new tar bundle, walk the archiveDirectory and upload everything there.
	bundle.StartQueue()
	fmt.Println("Walking ...")
	err = filepath.Walk(archiveDirectory, bundle.HandleWalkedFSObject)
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	err = bundle.FinishQueue()
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	err = bundle.UploadPgControl(uploader.compressor.FileExtension())
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	// Stops backup and write/upload postgres `backup_label` and `tablespace_map` Files
	finishLsn, err := bundle.UploadLabelFiles(conn)
	if err != nil {
		log.Fatalf("%+v\n", err)
	}

	timelineChanged := bundle.checkTimelineChanged(conn)
	var currentBackupSentinelDto *S3TarBallSentinelDto

	if !timelineChanged {
		currentBackupSentinelDto = &S3TarBallSentinelDto{
			BackupStartLSN:   &backupStartLSN,
			IncrementFromLSN: previousBackupSentinelDto.BackupStartLSN,
			PgVersion:        pgVersion,
		}
		if previousBackupSentinelDto.BackupStartLSN != nil {
			currentBackupSentinelDto.IncrementFrom = &previousBackupName
			if previousBackupSentinelDto.isIncremental() {
				currentBackupSentinelDto.IncrementFullName = previousBackupSentinelDto.IncrementFullName
			} else {
				currentBackupSentinelDto.IncrementFullName = &previousBackupName
			}
			currentBackupSentinelDto.IncrementCount = &incrementCount
		}

		currentBackupSentinelDto.setFiles(bundle.GetFiles())
		currentBackupSentinelDto.BackupFinishLSN = &finishLsn
	}

	// Wait for all uploads to finish.
	err = bundle.TarBall.Finish(currentBackupSentinelDto)
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
}

// TODO : unit tests
// HandleWALFetch is invoked to performa wal-g wal-fetch
func HandleWALFetch(folder *S3Folder, walFileName string, location string, triggerPrefetch bool) {
	location = ResolveSymlink(location)
	if triggerPrefetch {
		defer forkPrefetch(walFileName, location)
	}

	_, _, running, prefetched := GetPrefetchLocations(path.Dir(location), walFileName)
	seenSize := int64(-1)

	for {
		if stat, err := os.Stat(prefetched); err == nil {
			if stat.Size() != int64(WalSegmentSize) {
				log.Println("WAL-G: Prefetch error: wrong file size of prefetched file ", stat.Size())
				break
			}

			err = os.Rename(prefetched, location)
			if err != nil {
				log.Fatalf("%+v\n", err)
			}

			err := checkWALFileMagic(location)
			if err != nil {
				log.Println("Prefetched file contain errors", err)
				os.Remove(location)
				break
			}

			return
		} else if !os.IsNotExist(err) {
			log.Fatalf("%+v\n", err)
		}

		// We have race condition here, if running is renamed here, but it's OK

		if runStat, err := os.Stat(running); err == nil {
			observedSize := runStat.Size() // If there is no progress in 50 ms - start downloading myself
			if observedSize <= seenSize {
				defer func() {
					os.Remove(running) // we try to clean up and ignore here any error
					os.Remove(prefetched)
				}()
				break
			}
			seenSize = observedSize
		} else if os.IsNotExist(err) {
			break // Normal startup path
		} else {
			break // Abnormal path. Permission denied etc. Yes, I know that previous 'else' can be eliminated.
		}
		time.Sleep(50 * time.Millisecond)
	}

	err := downloadWALFileTo(folder, walFileName, location)
	if err != nil {
		log.Fatalf("%v+\n", err)
	}
}

// TODO : unit tests
func checkWALFileMagic(prefetched string) error {
	file, err := os.Open(prefetched)
	if err != nil {
		return err
	}
	defer file.Close()
	magic := make([]byte, 4)
	file.Read(magic)
	if binary.LittleEndian.Uint32(magic) < 0xD061 {
		return NewInvalidWalFileMagicError()
	}

	return nil
}

func TryDownloadWALFile(folder *S3Folder, walPath string) (archiveReader io.ReadCloser, exists bool, err error) {
	archive := &Archive{
		Folder:  folder,
		Archive: aws.String(sanitizePath(walPath)),
	}
	archiveReader, err = archive.GetArchive()
	if err != nil {
		if IsAwsNotExist(errors.Cause(err)) {
			err = nil
		}
	} else {
		exists = true
	}
	return
}

// TODO : unit tests
func decompressWALFile(dst io.Writer, archiveReader io.ReadCloser, decompressor Decompressor) error {
	crypter := OpenPGPCrypter{}
	if crypter.IsUsed() {
		reader, err := crypter.Decrypt(archiveReader)
		if err != nil {
			return err
		}
		archiveReader = ReadCascadeCloser{reader, archiveReader}
	}

	err := decompressor.Decompress(dst, archiveReader)
	return err
}

// TODO : unit tests
func downloadAndDecompressWALFile(folder *S3Folder, walFileName string) (io.ReadCloser, error) {
	for _, decompressor := range Decompressors {
		archiveReader, exists, err := TryDownloadWALFile(folder, folder.Server+WalPath+walFileName+"."+decompressor.FileExtension())
		if err != nil {
			return nil, err
		}
		if !exists {
			continue
		}
		reader, writer := io.Pipe()
		go func() {
			err = decompressWALFile(&EmptyWriteIgnorer{writer}, archiveReader, decompressor)
			writer.CloseWithError(err)
		}()
		return reader, nil
	}
	return nil, NewArchiveNonExistenceError(walFileName)
}

// TODO : unit tests
// downloadWALFileTo downloads a file and writes it to local file
func downloadWALFileTo(folder *S3Folder, walFileName string, dstPath string) error {
	reader, err := downloadAndDecompressWALFile(folder, walFileName)
	if err != nil {
		return err
	}
	defer reader.Close()
	return CreateFileWith(dstPath, reader)
}

// TODO : unit tests
// HandleWALPush is invoked to perform wal-g wal-push
func HandleWALPush(uploader *Uploader, walFilePath string) {
	bgUploader := NewBgUploader(walFilePath, int32(getMaxUploadConcurrency(16)-1), uploader)
	// Look for new WALs while doing main upload
	bgUploader.Start()
	err := uploadWALFile(uploader, walFilePath)
	if err != nil {
		panic(err)
	}

	bgUploader.Stop()
	if uploader.deltaFileManager != nil {
		uploader.deltaFileManager.FlushFiles(uploader.Clone())
	}
} //

// TODO : unit tests
// uploadWALFile from FS to the cloud
func uploadWALFile(uploader *Uploader, walFilePath string) error {
	if uploader.uploadingFolder.preventWalOverwrite {
		overwriteAttempt, err := checkWALOverwrite(uploader, walFilePath)
		if err != nil {
			return errors.Wrap(err, "Couldn't check whether there is an overwrite attempt due to inner error")
		} else if overwriteAttempt {
			return NewCantOverwriteWalFileError(walFilePath)
		}
	}
	walFile, err := os.Open(walFilePath)
	if err != nil {
		return errors.Wrapf(err, "upload: could not open '%s'\n", walFilePath)
	}
	err = uploader.UploadWalFile(walFile)
	return errors.Wrapf(err, "upload: could not upload '%s'\n", walFilePath)
}

func checkWALOverwrite(uploader *Uploader, walFilePath string) (overwriteAttempt bool, err error) {
	walFileReader, err := downloadAndDecompressWALFile(uploader.uploadingFolder, uploader.uploadingFolder.Server+WalPath+filepath.Base(walFilePath)+"."+uploader.compressor.FileExtension())
	if err != nil {
		if _, ok := err.(ArchiveNonExistenceError); ok {
			err = nil
		}
		return false, err
	}

	archived, err := ioutil.ReadAll(walFileReader)
	if err != nil {
		return false, err
	}

	localBytes, err := ioutil.ReadFile(walFilePath)
	if err != nil {
		return false, err
	}

	if !bytes.Equal(archived, localBytes) {
		return true, nil
	} else {
		log.Printf("WARNING: WAL file '%s' already archived, archived content equals\n", walFilePath)
		return false, nil
	}
}

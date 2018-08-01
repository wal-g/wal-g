package walg

import (
	"encoding/binary"
	"fmt"
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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/pkg/errors"
)

var PgControlMissingError = errors.New("Corrupted backup: missing pg_control")
var InvalidWalFileMagicError = errors.New("WAL-G: WAL file magic is invalid ")

type ArchiveNonExistenceError struct {
	archiveName string
}

func (err ArchiveNonExistenceError) Error() string {
	return fmt.Sprintf("Archive '%s' does not exist.\n", err.archiveName)
}

// HandleDelete is invoked to perform wal-g delete
func HandleDelete(folder *S3Folder, args []string) {
	arguments := ParseDeleteArguments(args, printDeleteUsageAndFail)

	var backup = &Backup{
		Folder: folder,
		Path:   GetBackupPath(folder),
	}

	if arguments.Before {
		if arguments.BeforeTime == nil {
			deleteBeforeTarget(arguments.Target, backup, folder, arguments.FindFull, nil, arguments.dryrun)
		} else {
			backups, err := backup.getBackups()
			if err != nil {
				log.Fatal(err)
			}
			for _, b := range backups {
				if b.Time.Before(*arguments.BeforeTime) {
					deleteBeforeTarget(b.Name, backup, folder, arguments.FindFull, backups, arguments.dryrun)
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
		backups, err := backup.getBackups()
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
					deleteBeforeTarget(b.Name, backup, folder, true, backups, arguments.dryrun)
					return
				}
				dto := fetchSentinel(b.Name, backup, folder)
				if !dto.isIncremental() {
					left--
				}
			}
			fmt.Printf("Scanned all backups but didn't have %v full.", backupCount)
		} else {
			if len(backups) <= backupCount {
				fmt.Printf("Have only %v backups.\n", backupCount)
			} else {
				arguments.Target = backups[backupCount-1].Name
				deleteBeforeTarget(arguments.Target, backup, folder, arguments.FindFull, nil, arguments.dryrun)
			}
		}
	}
}

// HandleBackupList is invoked to perform wal-g backup-list
func HandleBackupList(folder *S3Folder) {
	var backup = &Backup{
		Folder: folder,
		Path:   GetBackupPath(folder),
	}
	backups, err := backup.getBackups()
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

// deltaFetchRecursion function composes Backup object and recursively searches for necessary base backup
func deltaFetchRecursion(backupName string, folder *S3Folder, archiveDirectory string) (lsn *uint64) {
	var backup *Backup
	// Check if backup exists and if it does extract to archiveDirectory.
	if backupName != "LATEST" {
		backup = &Backup{
			Folder: folder,
			Path:   GetBackupPath(folder),
			Name:   aws.String(backupName),
		}
		backup.Js = aws.String(*backup.Path + *backup.Name + "_backup_stop_sentinel.json")

		exists, err := backup.CheckExistence()
		if err != nil {
			log.Fatalf("%+v\n", err)
		}
		if !exists {
			log.Fatalf("Backup '%s' does not exist.\n", *backup.Name)
		}

		// Find the LATEST valid backup (checks against JSON file and grabs backup name) and extract to archiveDirectory.
	} else {
		backup = &Backup{
			Folder: folder,
			Path:   GetBackupPath(folder),
		}

		latest, err := backup.GetLatest()
		if err != nil {
			log.Fatalf("%+v\n", err)
		}
		backup.Name = aws.String(latest)
	}
	sentinelDto := fetchSentinel(*backup.Name, backup, folder)

	if sentinelDto.isIncremental() {
		fmt.Printf("Delta from %v at LSN %x \n", *sentinelDto.IncrementFrom, *sentinelDto.IncrementFromLSN)
		deltaFetchRecursion(*sentinelDto.IncrementFrom, folder, archiveDirectory)
		fmt.Printf("%v fetched. Upgrading from LSN %x to LSN %x \n", *sentinelDto.IncrementFrom, *sentinelDto.IncrementFromLSN, sentinelDto.BackupStartLSN)
	}

	unwrapBackup(backup, archiveDirectory, folder, sentinelDto)

	lsn = sentinelDto.BackupStartLSN
	return
}

func extractPgControl(backup *Backup, folder *S3Folder, fileTarInterpreter *FileTarInterpreter) error {
	// Extract pg_control last. If pg_control does not exist, program exits with error code 1.
	for _, decompressor := range Decompressors {
		name := *backup.Path + *backup.Name + "/tar_partitions/pg_control.tar." + decompressor.FileExtension()
		pgControl := &Archive{
			Folder:  folder,
			Archive: aws.String(name),
		}

		exists, err := pgControl.CheckExistence()
		if err != nil {
			return err
		}

		if !exists {
			continue
		}
		sentinel := make([]ReaderMaker, 1)
		sentinel[0] = &S3ReaderMaker{
			Backup:     backup,
			Key:        aws.String(name),
			FileFormat: GetFileExtension(name),
		}
		err = ExtractAll(fileTarInterpreter, sentinel)
		if serr, ok := err.(*UnsupportedFileTypeError); ok {
			return serr
		}
		return err
	}
	return PgControlMissingError
}

// Do the job of unpacking Backup object
func unwrapBackup(backup *Backup, archiveDirectory string, folder *S3Folder, sentinelDto S3TarBallSentinelDto) {

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

		err := os.MkdirAll(incrementBase, os.FileMode(0777))
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

	var allKeys []string
	var keys []string
	allKeys, err := backup.GetKeys()
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	keys = allKeys[:len(allKeys)-1] // TODO: WTF is going on?
	fileTarInterpreter := &FileTarInterpreter{
		NewDir:             archiveDirectory,
		Sentinel:           sentinelDto,
		IncrementalBaseDir: incrementBase,
	}
	out := make([]ReaderMaker, len(keys))
	for i, key := range keys {
		s := &S3ReaderMaker{
			Backup:     backup,
			Key:        aws.String(key),
			FileFormat: GetFileExtension(key),
		}
		out[i] = s
	}
	// Extract all compressed tar members except `pg_control.tar.lz4` if WALG version backup.
	err = ExtractAll(fileTarInterpreter, out)
	if serr, ok := err.(*UnsupportedFileTypeError); ok {
		log.Fatalf("%v\n", serr)
	} else if err != nil {
		log.Fatalf("%+v\n", err)
	}
	// Check name for backwards compatibility. Will check for `pg_control` if WALG version of backup.
	re := regexp.MustCompile(`^([^_]+._{1}[^_]+._{1})`)
	match := re.FindString(*backup.Name)
	if match == "" || sentinelDto.isIncremental() { // TODO: extract pg_control
		err = extractPgControl(backup, folder, fileTarInterpreter)
		if err != nil {
			log.Fatalf("%+v\n", err)
		} else {
			fmt.Print("\nBackup extraction complete.\n")
		}
	}
}

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
			fromFull = false
		default:
			log.Fatal("Unknown WALG_DELTA_ORIGIN:", origin)
		}
	}
	return
}

// HandleBackupPush is invoked to perform a wal-g backup-push
func HandleBackupPush(archiveDirectory string, uploader *Uploader) {
	archiveDirectory = ResolveSymlink(archiveDirectory)
	maxDeltas, fromFull := getDeltaConfig()

	var backup = &Backup{
		Folder: uploader.UploadingFolder,
		Path:   GetBackupPath(uploader.UploadingFolder),
	}

	var previousBackupSentinelDto S3TarBallSentinelDto
	var latest string
	var err error
	incrementCount := 1

	if maxDeltas > 0 {
		latest, err = backup.GetLatest()
		if err != ErrLatestNotFound {
			if err != nil {
				log.Fatalf("%+v\n", err)
			}
			previousBackupSentinelDto = fetchSentinel(latest, backup, uploader.UploadingFolder)
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
					latest = *previousBackupSentinelDto.IncrementFullName
					previousBackupSentinelDto = fetchSentinel(latest, backup, uploader.UploadingFolder)
				}
				fmt.Printf("Delta backup from %v with LSN %x. \n", latest, *previousBackupSentinelDto.BackupStartLSN)
			}
		}
	}

	bundle := NewBundle(previousBackupSentinelDto.BackupStartLSN, previousBackupSentinelDto.Files)

	// Connect to postgres and start/finish a nonexclusive backup.
	conn, err := Connect()
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	backupName, backupStartLSN, pgVersion, err := bundle.StartBackup(conn, time.Now().String())
	if err != nil {
		log.Fatalf("%+v\n", err)
	}

	if len(latest) > 0 && previousBackupSentinelDto.BackupStartLSN != nil {
		if uploader.useWalDelta {
			err = bundle.loadDeltaMap(uploader.UploadingFolder, backupStartLSN)
			if err == nil {
				fmt.Println("Successfully loaded delta map, delta backup will be made with provided delta map")
			} else {
				bundle.DeltaMap = nil
				fmt.Printf("Error during loading delta map: '%v'. Fallback to full scan delta backup\n", err)
			}
		}
		backupName = backupName + "_D_" + stripWalFileName(latest)
	}

	// Start a new tar bundle and walk the archiveDirectory directory and upload to S3.
	bundle.TarBallMaker = &S3TarBallMaker{
		ArchiveDirectory: archiveDirectory,
		BackupName:       backupName,
		Uploader:         uploader,
	}

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
	// Upload `pg_control`.
	err = bundle.HandleSentinel()
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	// Stops backup and write/upload postgres `backup_label` and `tablespace_map` Files
	finishLsn, err := bundle.HandleLabelFiles(conn)
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
			currentBackupSentinelDto.IncrementFrom = &latest
			currentBackupSentinelDto.IncrementFullName = &latest
			if previousBackupSentinelDto.isIncremental() {
				currentBackupSentinelDto.IncrementFullName = previousBackupSentinelDto.IncrementFullName
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

func checkWALFileMagic(prefetched string) error {
	file, err := os.Open(prefetched)
	if err != nil {
		return err
	}
	defer file.Close()
	magic := make([]byte, 4)
	file.Read(magic)
	if binary.LittleEndian.Uint32(magic) < 0xD061 {
		return InvalidWalFileMagicError
	}

	return nil
}

func tryDownloadWALFile(folder *S3Folder, walFullPath string) (archiveReader io.ReadCloser, exists bool, err error) {
	archive := &Archive{
		Folder:  folder,
		Archive: aws.String(sanitizePath(walFullPath)),
	}

	exists, err = archive.CheckExistence()
	if err != nil || !exists {
		return
	}

	archiveReader, err = archive.GetArchive()
	return
}

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

func downloadAndDecompressWALFile(folder *S3Folder, walFileName string) (io.ReadCloser, error) {
	for _, decompressor := range Decompressors {
		archiveReader, exists, err := tryDownloadWALFile(folder, *folder.Server+WalPath+walFileName+"."+decompressor.FileExtension())
		if err != nil {
			return nil, err
		}
		if !exists {
			continue
		}
		reader, writer := io.Pipe()
		defer writer.Close()
		err = decompressWALFile(writer, archiveReader, decompressor)
		if err != nil {
			return nil, err
		}
		return reader, nil
	}
	return nil, ArchiveNonExistenceError{walFileName}
}

// downloadWALFileTo downloads a file and writes it to local file
func downloadWALFileTo(folder *S3Folder, walFileName string, dstPath string) error {
	reader, err := downloadAndDecompressWALFile(folder, walFileName)
	defer reader.Close()
	if err != nil {
		return err
	}
	return createFileAndWriteToIt(dstPath, reader)
}

// HandleWALPush is invoked to perform wal-g wal-push
func HandleWALPush(uploader *Uploader, walFilePath string, verify bool) {
	bgUploader := BgUploader{}
	// Look for new WALs while doing main upload
	bgUploader.Start(walFilePath, int32(getMaxUploadConcurrency(16)-1), uploader, verify)

	uploadWALFile(uploader, walFilePath, verify)

	bgUploader.Stop()
}

// uploadWALFile from FS to the cloud
func uploadWALFile(tarUploader *Uploader, walFilePath string, verify bool) {
	walFile, err := os.Open(walFilePath)
	if err != nil {
		log.Fatalf("upload: could not open '%s'\n", walFilePath)
	}
	path, err := tarUploader.UploadWal(walFile, verify)
	if compressionError, ok := err.(CompressingPipeWriterError); ok {
		log.Fatalf("FATAL: could not upload '%s' due to compression error.\n%+v\n", path, compressionError)
	} else if err != nil {
		log.Printf("upload: could not upload '%s'\n", path)
		log.Fatalf("FATAL%+v\n", err)
	}
}

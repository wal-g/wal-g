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
	"sync"
)

type PgControlMissingError struct{}

func (err PgControlMissingError) Error() string {
	return "Corrupted backup: missing pg_control"
}

// HandleDelete is invoked to perform wal-g delete
func HandleDelete(pre *S3Prefix, args []string) {
	arguments := ParseDeleteArguments(args, printDeleteUsageAndFail)

	var backup = &Backup{
		Prefix: pre,
		Path:   GetBackupPath(pre),
	}

	if arguments.before {
		if arguments.beforeTime == nil {
			deleteBeforeTarget(arguments.target, backup, pre, arguments.findFull, nil, arguments.dryrun)
		} else {
			backups, err := backup.GetBackups()
			if err != nil {
				log.Fatal(err)
			}
			for _, b := range backups {
				if b.Time.Before(*arguments.beforeTime) {
					deleteBeforeTarget(b.Name, backup, pre, arguments.findFull, backups, arguments.dryrun)
					return
				}
			}
			log.Println("No backups before ", *arguments.beforeTime)
		}
	}
	if arguments.retain {
		backupCount, err := strconv.Atoi(arguments.target)
		if err != nil {
			log.Fatal("Unable to parse number of backups: ", err)
		}
		backups, err := backup.GetBackups()
		if err != nil {
			log.Fatal(err)
		}
		if arguments.full {
			if len(backups) <= backupCount {
				fmt.Printf("Have only %v backups.\n", backupCount)
			}
			left := backupCount
			for _, b := range backups {
				if left == 1 {
					deleteBeforeTarget(b.Name, backup, pre, true, backups, arguments.dryrun)
					return
				}
				dto := fetchSentinel(b.Name, backup, pre)
				if !dto.IsIncremental() {
					left--
				}
			}
			fmt.Printf("Scanned all backups but didn't have %v full.", backupCount)
		} else {
			if len(backups) <= backupCount {
				fmt.Printf("Have only %v backups.\n", backupCount)
			} else {
				arguments.target = backups[backupCount-1].Name
				deleteBeforeTarget(arguments.target, backup, pre, arguments.findFull, nil, arguments.dryrun)
			}
		}
	}
}

// HandleBackupList is invoked to perform wal-g backup-list
func HandleBackupList(pre *S3Prefix) {
	var backup = &Backup{
		Prefix: pre,
		Path:   GetBackupPath(pre),
	}
	backups, err := backup.GetBackups()
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
func HandleBackupFetch(backupName string, pre *S3Prefix, archiveDirectory string, mem bool) (lsn *uint64) {
	archiveDirectory = ResolveSymlink(archiveDirectory)
	lsn = deltaFetchRecursion(backupName, pre, archiveDirectory)

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
func deltaFetchRecursion(backupName string, pre *S3Prefix, archiveDirectory string) (lsn *uint64) {
	var backup *Backup
	// Check if backup exists and if it does extract to archiveDirectory.
	if backupName != "LATEST" {
		backup = &Backup{
			Prefix: pre,
			Path:   GetBackupPath(pre),
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
			Prefix: pre,
			Path:   GetBackupPath(pre),
		}

		latest, err := backup.GetLatest()
		if err != nil {
			log.Fatalf("%+v\n", err)
		}
		backup.Name = aws.String(latest)
	}
	var dto = fetchSentinel(*backup.Name, backup, pre)

	if dto.IsIncremental() {
		fmt.Printf("Delta from %v at LSN %x \n", *dto.IncrementFrom, *dto.IncrementFromLSN)
		deltaFetchRecursion(*dto.IncrementFrom, pre, archiveDirectory)
		fmt.Printf("%v fetched. Upgrading from LSN %x to LSN %x \n", *dto.IncrementFrom, *dto.IncrementFromLSN, dto.LSN)
	}

	unwrapBackup(backup, archiveDirectory, pre, dto)

	lsn = dto.LSN
	return
}

func extractPgControl(backup *Backup, pre *S3Prefix, fileTarInterpreter *FileTarInterpreter) error {
	// Extract pg_control last. If pg_control does not exist, program exits with error code 1.
	for _, decompressor := range Decompressors {
		name := *backup.Path + *backup.Name + "/tar_partitions/pg_control.tar." + decompressor.FileExtension()
		pgControl := &Archive{
			Prefix:  pre,
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
	return PgControlMissingError{}
}

// Do the job of unpacking Backup object
func unwrapBackup(backup *Backup, archiveDirectory string, pre *S3Prefix, sentinelDto S3TarBallSentinelDto) {

	incrementBase := path.Join(archiveDirectory, "increment_base")
	if !sentinelDto.IsIncremental() {
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
			err = MoveFileAndCreateDirs(incrementalPath, targetPath, fileName)
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
	if match == "" || sentinelDto.IsIncremental() { // TODO: extract pg_control
		err = extractPgControl(backup, pre, fileTarInterpreter)
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
func HandleBackupPush(archiveDirectory string, tarUploader *TarUploader, pre *S3Prefix) {
	archiveDirectory = ResolveSymlink(archiveDirectory)
	maxDeltas, fromFull := getDeltaConfig()

	var backup = &Backup{
		Prefix: pre,
		Path:   GetBackupPath(pre),
	}

	var sentinelDto S3TarBallSentinelDto
	var latest string
	var err error
	incrementCount := 1

	if maxDeltas > 0 {
		latest, err = backup.GetLatest()
		if err != ErrLatestNotFound {
			if err != nil {
				log.Fatalf("%+v\n", err)
			}
			sentinelDto = fetchSentinel(latest, backup, pre)
			if sentinelDto.IncrementCount != nil {
				incrementCount = *sentinelDto.IncrementCount + 1
			}

			if incrementCount > maxDeltas {
				fmt.Println("Reached max delta steps. Doing full backup.")
				sentinelDto = S3TarBallSentinelDto{}
			} else if sentinelDto.LSN == nil {
				fmt.Println("LATEST backup was made without support for delta feature. Fallback to full backup with LSN marker for future deltas.")
			} else {
				if fromFull {
					fmt.Println("Delta will be made from full backup.")
					latest = *sentinelDto.IncrementFullName
					sentinelDto = fetchSentinel(latest, backup, pre)
				}
				fmt.Printf("Delta backup from %v with LSN %x. \n", latest, *sentinelDto.LSN)
			}
		}
	}

	bundle := &Bundle{
		MinSize:            int64(1000000000), //MINSIZE = 1GB
		IncrementFromLsn:   sentinelDto.LSN,
		IncrementFromFiles: sentinelDto.Files,
		Files:              &sync.Map{},
	}
	if sentinelDto.Files == nil {
		bundle.IncrementFromFiles = make(map[string]BackupFileDescription)
	}

	// Connect to postgres and start/finish a nonexclusive backup.
	conn, err := Connect()
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
	backupName, lsn, pgVersion, err := bundle.StartBackup(conn, time.Now().String())
	if err != nil {
		log.Fatalf("%+v\n", err)
	}

	if len(latest) > 0 && sentinelDto.LSN != nil {
		backupName = backupName + "_D_" + stripWalFileName(latest)
	}

	// Start a new tar bundle and walk the archiveDirectory directory and upload to S3.
	bundle.TarBallMaker = &S3TarBallMaker{
		ArchiveDirectory: archiveDirectory,
		BackupName:       backupName,
		TarUploader:      tarUploader,
		Lsn:              &lsn,
		IncrementFromLsn: sentinelDto.LSN,
		IncrementFrom:    latest,
	}

	bundle.StartQueue()
	fmt.Println("Walking ...")
	err = filepath.Walk(archiveDirectory, bundle.TarWalk)
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

	timelineChanged := bundle.CheckTimelineChanged(conn)
	var sentinel *S3TarBallSentinelDto

	if !timelineChanged {
		sentinel = &S3TarBallSentinelDto{
			LSN:              &lsn,
			IncrementFromLSN: sentinelDto.LSN,
			PgVersion:        pgVersion,
		}
		if sentinelDto.LSN != nil {
			sentinel.IncrementFrom = &latest
			sentinel.IncrementFullName = &latest
			if sentinelDto.IsIncremental() {
				sentinel.IncrementFullName = sentinelDto.IncrementFullName
			}
			sentinel.IncrementCount = &incrementCount
		}

		sentinel.SetFiles(bundle.GetFiles())
		sentinel.FinishLSN = &finishLsn
	}

	// Wait for all uploads to finish.
	err = bundle.TarBall.Finish(sentinel)
	if err != nil {
		log.Fatalf("%+v\n", err)
	}
}

// HandleWALFetch is invoked to performa wal-g wal-fetch
func HandleWALFetch(pre *S3Prefix, walFileName string, location string, triggerPrefetch bool) {
	location = ResolveSymlink(location)
	if triggerPrefetch {
		defer forkPrefetch(walFileName, location)
	}

	_, _, running, prefetched := getPrefetchLocations(path.Dir(location), walFileName)
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

	DownloadAndDecompressWALFile(pre, walFileName, location)
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
		return errors.New("WAL-G: WAL file magic is invalid ")
	}

	return nil
}

func tryDownloadWALFile(pre *S3Prefix, walFullPath string) (archiveReader io.ReadCloser, exists bool, err error) {
	archive := &Archive{
		Prefix:  pre,
		Archive: aws.String(sanitizePath(walFullPath)),
	}

	exists, err = archive.CheckExistence()
	if err != nil || !exists {
		return
	}

	archiveReader, err = archive.GetArchive()
	return
}

func decompressWALFile(archiveReader io.ReadCloser, dstLocation string, decompressor Decompressor) error {
	crypter := OpenPGPCrypter{}
	if crypter.IsUsed() {
		reader, err := crypter.Decrypt(archiveReader)
		if err != nil {
			return err
		}
		archiveReader = ReadCascadeCloser{reader, archiveReader}
	}

	file, err := os.OpenFile(dstLocation, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_EXCL, 0666)
	if err != nil {
		return err
	}

	err = decompressor.Decompress(file, archiveReader)
	if err != nil {
		return err
	}
	return file.Close()
}

// DownloadAndDecompressWALFile downloads a file and writes it to local file
func DownloadAndDecompressWALFile(pre *S3Prefix, walFileName string, dstLocation string) {
	for _, decompressor := range Decompressors {
		archiveReader, exists, err := tryDownloadWALFile(pre, *pre.Server+WalPath+walFileName+"."+decompressor.FileExtension())
		if err != nil {
			log.Fatalf("%+v\n", err)
		}
		if !exists {
			continue
		}
		err = decompressWALFile(archiveReader, dstLocation, decompressor)
		if err != nil {
			log.Fatalf("%+v\n", err)
		}
		return
	}
	log.Fatalf("Archive '%s' does not exist.\n", walFileName)
}

// HandleWALPush is invoked to perform wal-g wal-push
func HandleWALPush(tarUploader *TarUploader, archiveDirectory string, pre *S3Prefix, verify bool) {
	bgUploader := BgUploader{}
	// Look for new WALs while doing main upload
	bgUploader.Start(archiveDirectory, int32(getMaxUploadConcurrency(16)-1), tarUploader, pre, verify)

	UploadWALFile(tarUploader, archiveDirectory, pre, verify)

	bgUploader.Stop()
}

// UploadWALFile from FS to the cloud
func UploadWALFile(tarUploader *TarUploader, archiveDirectory string, pre *S3Prefix, verify bool) {
	path, err := tarUploader.UploadWal(archiveDirectory, pre, verify)
	if compressionError, ok := err.(CompressingPipeWriterError); ok {
		log.Fatalf("FATAL: could not upload '%s' due to compression error.\n%+v\n", path, compressionError)
	} else if err != nil {
		log.Printf("upload: could not upload '%s'\n", path)
		log.Fatalf("FATAL%+v\n", err)
	}
}

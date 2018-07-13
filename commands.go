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
	cfg := ParseDeleteArguments(args, printDeleteUsageAndFail)

	var bk = &Backup{
		Prefix: pre,
		Path:   GetBackupPath(pre),
	}

	if cfg.before {
		if cfg.beforeTime == nil {
			deleteBeforeTarget(cfg.target, bk, pre, cfg.findFull, nil, cfg.dryrun)
		} else {
			backups, err := bk.GetBackups()
			if err != nil {
				log.Fatal(err)
			}
			for _, b := range backups {
				if b.Time.Before(*cfg.beforeTime) {
					deleteBeforeTarget(b.Name, bk, pre, cfg.findFull, backups, cfg.dryrun)
					return
				}
			}
			log.Println("No backups before ", *cfg.beforeTime)
		}
	}
	if cfg.retain {
		number, err := strconv.Atoi(cfg.target)
		if err != nil {
			log.Fatal("Unable to parse number of backups: ", err)
		}
		backups, err := bk.GetBackups()
		if err != nil {
			log.Fatal(err)
		}
		if cfg.full {
			if len(backups) <= number {
				fmt.Printf("Have only %v backups.\n", number)
			}
			left := number
			for _, b := range backups {
				if left == 1 {
					deleteBeforeTarget(b.Name, bk, pre, true, backups, cfg.dryrun)
					return
				}
				dto := fetchSentinel(b.Name, bk, pre)
				if !dto.IsIncremental() {
					left--
				}
			}
			fmt.Printf("Scanned all backups but didn't have %v full.", number)
		} else {
			if len(backups) <= number {
				fmt.Printf("Have only %v backups.\n", number)
			} else {
				cfg.target = backups[number-1].Name
				deleteBeforeTarget(cfg.target, bk, pre, cfg.findFull, nil, cfg.dryrun)
			}
		}
	}
}

// HandleBackupList is invoked to perform wal-g backup-list
func HandleBackupList(pre *S3Prefix) {
	var bk = &Backup{
		Prefix: pre,
		Path:   GetBackupPath(pre),
	}
	backups, err := bk.GetBackups()
	if err != nil {
		log.Fatal(err)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
	defer w.Flush()
	fmt.Fprintln(w, "name\tlast_modified\twal_segment_backup_start")

	for i := len(backups) - 1; i >= 0; i-- {
		b := backups[i]
		fmt.Fprintln(w, fmt.Sprintf("%v\t%v\t%v", b.Name, b.Time.Format(time.RFC3339), b.WalFileName))
	}
}

// HandleBackupFetch is invoked to perform wal-g backup-fetch
func HandleBackupFetch(backupName string, pre *S3Prefix, dirArc string, mem bool) (lsn *uint64) {
	dirArc = ResolveSymlink(dirArc)
	lsn = deltaFetchRecursion(backupName, pre, dirArc)

	if mem {
		f, err := os.Create("mem.prof")
		if err != nil {
			log.Fatal(err)
		}

		pprof.WriteHeapProfile(f)
		defer f.Close()
	}
	return
}

// deltaFetchRecursion function composes Backup object and recursively searches for necessary base backup
func deltaFetchRecursion(backupName string, pre *S3Prefix, dirArc string) (lsn *uint64) {
	var bk *Backup
	// Check if BACKUPNAME exists and if it does extract to DIRARC.
	if backupName != "LATEST" {
		bk = &Backup{
			Prefix: pre,
			Path:   GetBackupPath(pre),
			Name:   aws.String(backupName),
		}
		bk.Js = aws.String(*bk.Path + *bk.Name + "_backup_stop_sentinel.json")

		exists, err := bk.CheckExistence()
		if err != nil {
			log.Fatalf("%+v\n", err)
		}
		if !exists {
			log.Fatalf("Backup '%s' does not exist.\n", *bk.Name)
		}

		// Find the LATEST valid backup (checks against JSON file and grabs backup name) and extract to DIRARC.
	} else {
		bk = &Backup{
			Prefix: pre,
			Path:   GetBackupPath(pre),
		}

		latest, err := bk.GetLatest()
		if err != nil {
			log.Fatalf("%+v\n", err)
		}
		bk.Name = aws.String(latest)
	}
	var dto = fetchSentinel(*bk.Name, bk, pre)

	if dto.IsIncremental() {
		fmt.Printf("Delta from %v at LSN %x \n", *dto.IncrementFrom, *dto.IncrementFromLSN)
		deltaFetchRecursion(*dto.IncrementFrom, pre, dirArc)
		fmt.Printf("%v fetched. Upgrading from LSN %x to LSN %x \n", *dto.IncrementFrom, *dto.IncrementFromLSN, dto.LSN)
	}

	unwrapBackup(bk, dirArc, pre, dto)

	lsn = dto.LSN
	return
}

// Extract pg_control separately, and last.
func extractPgControl(backup *Backup, pre *S3Prefix, fileTarInterpreter *FileTarInterpreter, name string) error {
	sentinel := make([]ReaderMaker, 1)
	sentinel[0] = &S3ReaderMaker{
		Backup:     backup,
		Key:        aws.String(name),
		FileFormat: GetFileExtension(name),
	}

	err := ExtractAll(fileTarInterpreter, sentinel)
	if err != nil {
		return err
	}

	if serr, ok := err.(*UnsupportedFileTypeError); ok {
		return serr
	}

	return PgControlMissingError{}
}

// Do the job of unpacking Backup object
func unwrapBackup(backup *Backup, dirArc string, pre *S3Prefix, sentinelDto S3TarBallSentinelDto) {

	incrementBase := path.Join(dirArc, "increment_base")
	if !sentinelDto.IsIncremental() {
		var empty = true
		searchLambda := func(path string, info os.FileInfo, err error) error {
			if path != dirArc {
				empty = false
			}
			return nil
		}
		filepath.Walk(dirArc, searchLambda)

		if !empty {
			log.Fatalf("Directory %v for delta base must be empty", dirArc)
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

		files, err := ioutil.ReadDir(dirArc)
		if err != nil {
			log.Fatal(err)
		}

		for _, f := range files {
			objName := f.Name()
			if objName != "increment_base" {
				err := os.Rename(path.Join(dirArc, objName), path.Join(incrementBase, objName))
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
			targetPath := path.Join(dirArc, fileName)
			// this path is only used for increment restoration
			incrementalPath := path.Join(incrementBase, fileName)
			err = MoveFileAndCreateDirs(incrementalPath, targetPath, fileName)
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
		NewDir:             dirArc,
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

		s := &S3ReaderMaker{
			Backup:     backup,
			Key:        aws.String(key),
			FileFormat: GetFileExtension(key),
		}
		out = append(out, s)
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
	if match == "" || sentinelDto.IsIncremental() {
		if pgControlKey == nil {
			log.Fatal("Expect pg_control archive, but not found")
		}

		err = extractPgControl(backup, pre, fileTarInterpreter, *pgControlKey)
		if err != nil {
			log.Fatalf("%+v\n", err)
		}
	}

	fmt.Print("\nBackup extraction complete.\n")
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

// HandleBackupPush is invoked to performa wal-g backup-push
func HandleBackupPush(dirArc string, tu *TarUploader, pre *S3Prefix) {
	dirArc = ResolveSymlink(dirArc)
	maxDeltas, fromFull := getDeltaConfig()

	var bk = &Backup{
		Prefix: pre,
		Path:   GetBackupPath(pre),
	}

	var sentinelDto S3TarBallSentinelDto
	var latest string
	var err error
	incrementCount := 1

	if maxDeltas > 0 {
		latest, err = bk.GetLatest()
		if err != ErrLatestNotFound {
			if err != nil {
				log.Fatalf("%+v\n", err)
			}
			sentinelDto = fetchSentinel(latest, bk, pre)
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
					sentinelDto = fetchSentinel(latest, bk, pre)
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
	name, lsn, pgVersion, err := bundle.StartBackup(conn, time.Now().String())
	if err != nil {
		log.Fatalf("%+v\n", err)
	}

	if len(latest) > 0 && sentinelDto.LSN != nil {
		name = name + "_D_" + stripWalFileName(latest)
	}

	// Start a new tar bundle and walk the DIRARC directory and upload to S3.
	bundle.TarBallMaker = &S3TarBallMaker{
		Trim:             dirArc,
		BkupName:         name,
		TarUploader:      tu,
		Lsn:              &lsn,
		IncrementFromLsn: sentinelDto.LSN,
		IncrementFrom:    latest,
	}

	bundle.StartQueue()
	fmt.Println("Walking ...")
	err = filepath.Walk(dirArc, bundle.TarWalk)
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
func HandleWALPush(tarUploader *TarUploader, dirArc string, pre *S3Prefix, verify bool) {
	bgUploader := BgUploader{}
	// Look for new WALs while doing main upload
	bgUploader.Start(dirArc, int32(getMaxUploadConcurrency(16)-1), tarUploader, pre, verify)

	UploadWALFile(tarUploader, dirArc, pre, verify)

	bgUploader.Stop()
}

// UploadWALFile from FS to the cloud
func UploadWALFile(tarUploader *TarUploader, dirArc string, pre *S3Prefix, verify bool) {
	path, err := tarUploader.UploadWal(dirArc, pre, verify)
	if re, ok := err.(CompressingPipeWriterError); ok {
		log.Fatalf("FATAL: could not upload '%s' due to compression error.\n%+v\n", path, re)
	} else if err != nil {
		log.Printf("upload: could not upload '%s'\n", path)
		log.Fatalf("FATAL%+v\n", err)
	}
}

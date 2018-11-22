package walg

import (
	"github.com/wal-g/wal-g/tracelog"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime/pprof"
)

// TODO : unit tests
// HandleBackupFetch is invoked to perform wal-g backup-fetch
func HandleBackupFetch(backupName string, folder *S3Folder, archiveDirectory string, mem bool) (lsn *uint64) {
	archiveDirectory = ResolveSymlink(archiveDirectory)
	lsn = deltaFetchRecursion(backupName, folder, archiveDirectory)

	if mem {
		memProfileLog, err := os.Create("mem.prof")
		if err != nil {
			tracelog.ErrorLogger.FatalError(err)
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
			tracelog.ErrorLogger.FatalError(err)
		}
		if !exists {
			tracelog.ErrorLogger.Fatalf("Backup '%s' does not exist.\n", backup.Name)
		}

		// Find the LATEST valid backup (checks against JSON file and grabs backup name) and extract to archiveDirectory.
	} else {
		latest, err := GetLatestBackupKey(folder)
		if err != nil {
			tracelog.ErrorLogger.FatalError(err)
		}

		backup = NewBackup(folder, latest)
	}
	sentinelDto, err := backup.fetchSentinel()
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}

	if sentinelDto.isIncremental() {
		tracelog.InfoLogger.Printf("Delta from %v at LSN %x \n", *sentinelDto.IncrementFrom, *sentinelDto.IncrementFromLSN)
		deltaFetchRecursion(*sentinelDto.IncrementFrom, folder, archiveDirectory)
		tracelog.InfoLogger.Printf("%v fetched. Upgrading from LSN %x to LSN %x \n", *sentinelDto.IncrementFrom, *sentinelDto.IncrementFromLSN, sentinelDto.BackupStartLSN)
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
			tracelog.ErrorLogger.Fatalf("Directory %v for delta base must be empty", archiveDirectory)
		}
	} else {
		defer func() {
			err := os.RemoveAll(incrementBase)
			if err != nil {
				tracelog.ErrorLogger.FatalError(err)
			}
		}()

		err := os.MkdirAll(incrementBase, os.FileMode(os.ModePerm))
		if err != nil {
			tracelog.ErrorLogger.FatalError(err)
		}

		files, err := ioutil.ReadDir(archiveDirectory)
		if err != nil {
			tracelog.ErrorLogger.FatalError(err)
		}
		tracelog.DebugLogger.Println("Archive directory before increment:")
		filepath.Walk(archiveDirectory,
			func(path string, info os.FileInfo, err error) error {
				if !info.IsDir() {
					tracelog.DebugLogger.Println(path)
				}
				return nil
			})

		for _, f := range files {
			objName := f.Name()
			if objName != "increment_base" {
				err := os.Rename(path.Join(archiveDirectory, objName), path.Join(incrementBase, objName))
				if err != nil {
					tracelog.ErrorLogger.FatalError(err)
				}
			}
		}

		for fileName, fd := range sentinelDto.Files {
			if !fd.IsSkipped {
				continue
			}
			tracelog.InfoLogger.Printf("Skipped file %v\n", fileName)
			targetPath := path.Join(archiveDirectory, fileName)
			// this path is only used for increment restoration
			incrementalPath := path.Join(incrementBase, fileName)
			err = moveFileAndCreateDirs(incrementalPath, targetPath, fileName)
			if err != nil {
				tracelog.ErrorLogger.Fatal(err, "Failed to move skipped file for "+targetPath+" "+fileName)
			}
		}

	}

	keys, err := backup.GetKeys()
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
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
		tracelog.ErrorLogger.FatalError(serr)
	} else if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}
	// Check name for backwards compatibility. Will check for `pg_control` if WALG version of backup.
	re := regexp.MustCompile(`^([^_]+._{1}[^_]+._{1})`)
	match := re.FindString(backup.Name)
	if match == "" || sentinelDto.isIncremental() {
		if pgControlKey == nil {
			tracelog.ErrorLogger.Fatal("Expect pg_control archive, but not found")
		}

		err = extractPgControl(backup.Folder, fileTarInterpreter, *pgControlKey)
		if err != nil {
			tracelog.ErrorLogger.FatalError(err)
		}
	}

	tracelog.DebugLogger.Println("Archive directory after unwrap:")
	filepath.Walk(archiveDirectory,
		func(path string, info os.FileInfo, err error) error {
			if !info.IsDir() {
				tracelog.DebugLogger.Println(path)
			}
			return nil
		})
	tracelog.InfoLogger.Print("\nBackup extraction complete.\n")
}

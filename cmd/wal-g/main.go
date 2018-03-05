package main

import (
	"flag"
	"fmt"
	"github.com/wal-g/wal-g"
	"log"
	"os"
	"runtime/pprof"
)

var profile bool
var mem bool
var help bool
var l *log.Logger
var helpMsg = "  backup-fetch\tfetch a backup from S3\n" +
	"  backup-push\tstarts and uploads a finished backup to S3\n" +
	"  backup-list\tprints available backups\n" +
	"  wal-fetch\tfetch a WAL file from S3\n" +
	"  wal-push\tupload a WAL file to S3\n" +
	"  extract\textract a local WAL file\n" +
	"  delete\tclear old backups and WALs\n"

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of WAL-G:\n")
		fmt.Fprintf(os.Stderr, "%s", helpMsg)
		flag.PrintDefaults()
	}
	flag.BoolVar(&profile, "p", false, "\tProfiler (false by default)")
	flag.BoolVar(&mem, "m", false, "\tMemory profiler (false by default)")

	flag.BoolVar(&walg.DeleteConfirmed, "confirm", false, "\tConfirm deletion")
	flag.BoolVar(&walg.DeleteDryrun, "dry-run", false, "\tDry-run deletion")
	l = log.New(os.Stderr, "", 0)
}

func main() {
	flag.Parse()
	all := flag.Args()
	if len(all) < 1 {
		l.Fatalf("Please choose a command:\n%s", helpMsg)
	}
	command := all[0]
	firstArgument := ""
	if len(all) > 1 {
		firstArgument = all[1]
	}

	// Usage strings for supported commands
	// TODO: refactor arg parsing towards gloang flag usage and more helpful messages
	if firstArgument == "-h" || firstArgument == "--help" || (firstArgument == "" && command != "backup-list") {
		switch command {
		case "backup-fetch":
			fmt.Printf("usage:\twal-g backup-fetch output_directory backup_name\n\twal-g backup-fetch output_directory LATEST\n\n")
			os.Exit(0)
		case "backup-push":
			fmt.Printf("usage:\twal-g backup-push backup_directory\n\n")
			os.Exit(0)
		case "backup-list":
			fmt.Printf("usage:\twal-g backup-list\n\n")
			os.Exit(0)
		case "wal-fetch":
			fmt.Printf("usage:\twal-g wal-fetch wal_name file_name\n\t   wal_name: name of WAL archive\n\t   file_name: name of file to be written to\n\n")
			os.Exit(0)
		case "wal-push":
			fmt.Printf("usage:\twal-g wal-push archive_path\n\n")
			os.Exit(0)
		case "extract":
			fmt.Printf("usage:\twal-g extract file_to_extract extract_to\n\n")
		default:
			l.Fatalf("Command '%s' is unsupported by WAL-G.\n\n", command)
		}
	}

	var backupName string
	if len(all) == 3 {
		backupName = all[2]
	}

	// Various profiling options
	if profile {
		f, err := os.Create("cpu.prof")
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	// Configure and start S3 session with bucket, region, and path names.
	// Checks that environment variables are properly set.
	tu, pre, err := walg.Configure()
	if err != nil {
		log.Fatalf("FATAL: %+v\n", err)
	}

	fmt.Println("BUCKET:", *pre.Bucket)
	fmt.Println("SERVER:", *pre.Server)

	if command == "wal-fetch" {
		// Fetch and decompress a WAL file from S3.
		walg.HandleWALFetch(pre, firstArgument, backupName, true)
	} else if command == "wal-prefetch" {
		walg.HandleWALPrefetch(pre, firstArgument, backupName)
	} else if command == "wal-push" {
		// Upload a WAL file to S3.
		walg.HandleWALPush(tu, firstArgument)
	} else if command == "backup-push" {
		walg.HandleBackupPush(firstArgument, tu, pre)
	} else if command == "backup-fetch" {
		walg.HandleBackupFetch(backupName, pre, firstArgument, mem)
	} else if command == "backup-list" {
		walg.HandleBackupList(pre)
	} else if command == "extract" {
		walg.HandleExtract(firstArgument, backupName)
	} else if command == "delete" {
		walg.HandleDelete(pre, all)
	} else {
		l.Fatalf("Command '%s' is unsupported by WAL-G.", command)
	}
}

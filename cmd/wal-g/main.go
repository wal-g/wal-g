package main

import (
	"flag"
	"fmt"
	"github.com/wal-g/wal-g/internal"
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
	"  delete\tclear old backups and WALs\n"

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of WAL-G:\n")
		fmt.Fprintf(os.Stderr, "%s", helpMsg)
		flag.PrintDefaults()
	}
	flag.BoolVar(&profile, "p", false, "\tProfiler (false by default)")
	flag.BoolVar(&mem, "m", false, "\tMemory profiler (false by default)")

	// this is temp solution to pass everything through flag. Will remove it when using CLI like cobra or cli
	flag.BoolVar(&showVersion, "version", false, "\tversion")
	flag.BoolVar(&showVersion, "v", false, "\tversion")
	flag.BoolVar(&showVersionVerbose, "version-verbose", false, "\tLong version")
	flag.BoolVar(&showVersionVerbose, "vv", false, "\tLong version")

	l = log.New(os.Stderr, "", 0)
}

var WalgVersion = "devel"
var GitRevision = "devel"
var BuildDate = "devel"

var showVersion bool
var showVersionVerbose bool

func main() {
	flag.Parse()

	if WalgVersion == "" {
		WalgVersion = "devel"
	}

	if showVersionVerbose {
		fmt.Println(WalgVersion, "\t", GitRevision, "\t", BuildDate)
		return
	}
	if showVersion {
		fmt.Println(WalgVersion)
		return
	}

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
	// TODO: refactor arg parsing towards golang flag usage and more helpful messages
	if firstArgument == "-h" || firstArgument == "--help" || (firstArgument == "" && command != "backup-list") {
		switch command {
		case "backup-fetch":
			fmt.Printf("usage:\twal-g backup-fetch output_directory backup_name\n\twal-g backup-fetch output_directory LATEST\n\n")
			os.Exit(1)
		case "backup-push":
			fmt.Printf("usage:\twal-g backup-push backup_directory\n\n")
			os.Exit(1)
		case "backup-list":
			fmt.Printf("usage:\twal-g backup-list\n\n")
			os.Exit(1)
		case "wal-fetch":
			fmt.Printf("usage:\twal-g wal-fetch wal_name file_name\n\t   wal_name: name of WAL archive\n\t   file_name: name of file to be written to\n\n")
			os.Exit(1)
		case "wal-push":
			fmt.Printf("usage:\twal-g wal-push archive_path\n\n")
			os.Exit(1)
		case "delete":
			fmt.Println(internal.DeleteUsageText)
			os.Exit(1)
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
	uploader, folder, err := internal.Configure()
	if err != nil {
		log.Fatalf("FATAL: %+v\n", err)
	}

	fmt.Println("Path: ", folder.GetPath())

	if command == "wal-fetch" {
		// Fetch and decompress a WAL file from S3.
		internal.HandleWALFetch(folder, firstArgument, backupName, true)
	} else if command == "wal-prefetch" {
		internal.HandleWALPrefetch(folder, firstArgument, backupName, uploader)
	} else if command == "wal-push" {
		// Upload a WAL file to S3.
		internal.HandleWALPush(uploader, firstArgument)
	} else if command == "backup-push" {
		internal.HandleBackupPush(firstArgument, uploader)
	} else if command == "backup-fetch" {
		internal.HandleBackupFetch(backupName, folder, firstArgument, mem)
	} else if command == "backup-list" {
		internal.HandleBackupList(folder)
	} else if command == "delete" {
		internal.HandleDelete(folder, all)
	} else {
		l.Fatalf("Command '%s' is unsupported by WAL-G.", command)
	}
}

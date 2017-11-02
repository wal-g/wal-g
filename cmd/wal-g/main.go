package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"github.com/wal-g/wal-g"
)

var profile bool
var mem bool
var help bool
var l *log.Logger
var helpMsg = "  backup-fetch\tfetch a backup from S3\n" +
	"  backup-push\tstarts and uploads a finished backup to S3\n" +
	"  wal-fetch\tfetch a WAL file from S3\n" +
	"  wal-push\tupload a WAL file to S3\n"

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of WAL-G:\n")
		fmt.Fprintf(os.Stderr, "%s", helpMsg)
		flag.PrintDefaults()
	}
	flag.BoolVar(&profile, "p", false, "\tProfiler (false by default)")
	flag.BoolVar(&mem, "m", false, "\tMemory profiler (false by default)")
	l = log.New(os.Stderr, "", 0)
}

func main() {
	// Configure and start S3 session with bucket, region, and path names.
	// Checks that environment variables are properly set.
	flag.Parse()
	all := flag.Args()
	if len(all) < 2 {
		l.Fatalf("Please choose a command:\n%s", helpMsg)
	}
	command := all[0]
	dirArc := all[1]

	// Usage strings for supported commands
	if dirArc == "-h" {
		switch command {
		case "backup-fetch":
			fmt.Printf("usage:\twal-g backup-fetch output_directory backup_name\n\twal-g backup-fetch output_directory LATEST\n\n")
			os.Exit(0)
		case "backup-push":
			fmt.Printf("usage:\twal-g backup-push backup_directory\n\n")
			os.Exit(0)
		case "wal-fetch":
			fmt.Printf("usage:\twal-g wal-fetch wal_name file_name\n\t   wal_name: name of WAL archive\n\t   file_name: name of file to be written to\n\n")
			os.Exit(0)
		case "wal-push":
			fmt.Printf("usage:\twal-g wal-push archive_path\n\n")
			os.Exit(0)
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

	tu, pre, err := walg.Configure()
	if err != nil {
		log.Fatalf("FATAL: %+v\n", err)
	}

	fmt.Println("BUCKET:", *pre.Bucket)
	fmt.Println("SERVER:", *pre.Server)

	if command == "wal-fetch" {
		// Fetch and decompress a WAL file from S3.
		walg.HandleWALFetch(pre, dirArc, backupName)
	} else if command == "wal-push" {
		// Upload a WAL file to S3.
		walg.HandleWALPush(tu, dirArc)
	} else if command == "backup-push" {
		walg.HandleBackupPush(dirArc, tu, pre)
	} else if command == "backup-fetch" {
		walg.HandleBackupFetch(backupName, pre, dirArc, mem)
	} else {
		l.Fatalf("Command '%s' is unsupported by WAL-G.", command)
	}
}

package main

import (
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/katie31/wal-g"
	"os"
	"path/filepath"
	"time"
)

var help bool
var helpMsg = "\tbackup-fetch\tfetch a backup from S3\n" +
	"\tbackup-push\tstarts and uploads a finished backup to S3\n" +
	"\twal-fetch\tfetch a WAL file from S3\n" +
	"\twal-push\tupload a WAL file to S3\n"

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of WAL-G:\n")
		fmt.Fprintf(os.Stderr, "%s", helpMsg)
		flag.PrintDefaults()
	}
}

func main() {
	/**
	 *  Configure and start session with bucket, region, and path names. Checks that environment variables
	 *  are properly set.
	 */
	flag.Parse()
	all := flag.Args()
	if len(all) == 0 {
		fmt.Println("Please choose a command:")
		fmt.Println(helpMsg)
		os.Exit(1)
	}
	command := all[0]
	dirArc := all[1]

	var backupName string
	if len(all) == 3 {
		backupName = all[2]
	}

	tu, pre, err := walg.Configure()
	if err != nil {
		fmt.Printf("FATAL: \t%+v\n", err)
		os.Exit(1)
	}

	fmt.Println("BUCKET:", *pre.Bucket)
	fmt.Println("PATH:", *pre.Server)

	/*** OPTION: BACKUP-FETCH ***/
	if command == "backup-fetch" {
		var allKeys []string
		var keys []string
		var bk *walg.Backup
		/*** Check if BACKUPNAME exists and if it does extract to DIRARC. ***/
		if backupName != "LATEST" {
			bk = &walg.Backup{
				Prefix: pre,
				Path:   aws.String(*pre.Server + "/basebackups_005/"),
				Name:   aws.String(backupName),
			}

			bk.Js = aws.String(*bk.Path + *bk.Name + "_backup_stop_sentinel.json")

			fmt.Println("NEWDIR:", dirArc)
			fmt.Println("PATH:", *bk.Path)
			fmt.Println("NAME:", *bk.Name)
			fmt.Println("JSON:", *bk.Js)
			fmt.Println(bk.CheckExistence())

			if bk.CheckExistence() {
				allKeys, err := bk.GetKeys()
				if err != nil {
					fmt.Printf("%+v\n", err)
					os.Exit(1)
				}
				keys = allKeys[:len(allKeys)-1]

			} else {
				fmt.Printf("Backup '%s' does not exist.\n", *bk.Name)
				os.Exit(1)
			}

			/*** Find the LATEST valid backup (checks against JSON file and grabs name from there) and extract to DIRARC. ***/
		} else if backupName == "LATEST" {
			bk = &walg.Backup{
				Prefix: pre,
				Path:   aws.String(*pre.Server + "/basebackups_005/"),
			}

			latest, err := bk.GetLatest()
			if err != nil {
				fmt.Printf("%+v\n", err)
				os.Exit(1)
			}
			bk.Name = aws.String(latest)
			allKeys, err = bk.GetKeys()
			if err != nil {
				fmt.Printf("%+v\n", err)
				os.Exit(1)
			}
			keys = allKeys[:len(allKeys)-1]

			fmt.Println("NEWDIR", dirArc)
			fmt.Println("PATH:", *bk.Path)
			fmt.Println("NAME:", *bk.Name)

		}

		f := &walg.FileTarInterpreter{
			NewDir: dirArc,
		}

		out := make([]walg.ReaderMaker, len(keys))
		for i, key := range keys {
			s := &walg.S3ReaderMaker{
				Backup:     bk,
				Key:        aws.String(key),
				FileFormat: walg.CheckType(key),
			}
			out[i] = s
		}

		/*** Extract all except pg_control. ***/
		err = walg.ExtractAll(f, out)
		if serr, ok := err.(*walg.UnsupportedFileTypeError); ok {
			fmt.Println(serr.Error())
			os.Exit(1)
		} else if err != nil {
			panic(err)
		}

		/*** Extract pg_control last. If pg_control does not exist, program exits with error code 1. ***/
		name := *bk.Path + *bk.Name + "/tar_partitions/pg_control.tar.lz4"
		pgControl := &walg.Archive{
			Prefix:  pre,
			Archive: aws.String(name),
		}

		if pgControl.CheckExistence() {
			sentinel := make([]walg.ReaderMaker, 1)
			sentinel[0] = &walg.S3ReaderMaker{
				Backup:     bk,
				Key:        aws.String(name),
				FileFormat: walg.CheckType(name),
			}
			err := walg.ExtractAll(f, sentinel)
			if serr, ok := err.(*walg.UnsupportedFileTypeError); ok {
				fmt.Println(serr.Error())
				os.Exit(1)
			} else if err != nil {
				panic(err)
			}
			fmt.Println("Extract complete.")
		} else {
			fmt.Println("Corrupt backup: missing pg_control")
			os.Exit(1)
		}
	} else if command == "wal-fetch" {
		/*** Fetch and decompress a WAL file from S3. ***/
		a := &walg.Archive{
			Prefix:  pre,
			Archive: aws.String(*pre.Server + "/wal_005/" + dirArc + ".lzo"),
		}

		if a.CheckExistence() {
			arch, err := a.GetArchive()
			if err != nil {
				fmt.Printf("%+v\n", err)
				os.Exit(1)
			}
			f, err := os.Create(backupName)
			if err != nil {
				panic(err)
			}

			err = walg.DecompressLzo(f, arch)
			if err != nil {
				fmt.Printf("FATAL: %+v\n", err)
				os.Exit(1)
			}
			f.Close()
		} else if a.Archive = aws.String(*pre.Server + "/wal_005/" + dirArc + ".lz4"); a.CheckExistence() {
			arch, err := a.GetArchive()
			if err != nil {
				fmt.Printf("%+v\n", err)
				os.Exit(1)
			}
			f, err := os.Create(backupName)
			if err != nil {
				panic(err)
			}

			err = walg.DecompressLz4(f, arch)
			if err != nil {
				fmt.Printf("FATAL: %+v\n", err)
				os.Exit(1)
			}
			f.Close()
		} else {
			fmt.Printf("Archive '%s' does not exist.\n", dirArc)
			os.Exit(1)
		}

	} else if command == "wal-push" {
		_, err := tu.UploadWal(dirArc)
		if err != nil {
			fmt.Printf("%+v\n", err)
			os.Exit(1)
		}

		tu.Finish()

	} else if command == "backup-push" {
		bundle := &walg.Bundle{
			MinSize: int64(1000000000), //MINSIZE = 1GB
		}
		c, err := walg.Connect()
		if err != nil {
			fmt.Printf("%+v\n", err)
			os.Exit(1)
		}
		lbl, sc, err := walg.QueryFile(c, time.Now().String())
		if err != nil {
			fmt.Printf("%+v\n", err)
			os.Exit(1)
		}

		n, err := walg.FormatName(lbl)
		if err != nil {
			panic(err)
		}

		bundle.Tbm = &walg.S3TarBallMaker{
			BaseDir:  filepath.Base(dirArc),
			Trim:     dirArc,
			BkupName: n,
			Tu:       tu,
		}

		/*** WALK the DIRARC directory and upload to S3. ***/
		bundle.NewTarBall()
		fmt.Println("Walking ...")
		err = filepath.Walk(dirArc, bundle.TarWalker)
		if err != nil {
			panic(err)
		}
		err = bundle.Tb.CloseTar()
		if err != nil {
			panic(err)
		}

		/*** UPLOAD label files. ***/
		err = bundle.HandleLabelFiles(lbl, sc)
		if err != nil {
			fmt.Println("%+v\n", err)
			os.Exit(1)
		}

		/*** UPLOAD `pg_control`. ***/
		err = bundle.HandleSentinel()
		if err != nil {
			fmt.Println("%+v\n", err)
			os.Exit(1)
		}
		err = bundle.Tb.Finish()
		if err != nil {
			panic(err)
		}
	}

}

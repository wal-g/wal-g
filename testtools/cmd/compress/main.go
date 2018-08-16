package main

import (
	"flag"
	"fmt"
	"github.com/wal-g/wal-g"
	"github.com/wal-g/wal-g/testtools"
	"log"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"time"
)

var profile bool
var mem bool
var nop bool
var s3 bool
var outDir string

func init() {
	flag.BoolVar(&profile, "p", false, "\tProfiler (false by default)")
	flag.BoolVar(&mem, "m", false, "\tMemory profiler (false by default)")
	flag.BoolVar(&nop, "n", false, "\tUse a NOP writer (false by default).")
	flag.BoolVar(&s3, "s", false, "\tUpload compressed tar files to s3 (write to disk by default)")
	flag.StringVar(&outDir, "out", "", "\tDirectory compressed tar files will be written to (unset by default)")
}

func main() {
	flag.Parse()
	all := flag.Args()
	part, err := strconv.Atoi(all[0])
	if err != nil {
		panic(err)
	}
	in := all[1]

	bundle := &walg.Bundle{
		ArchiveDirectory: in,
		TarSizeThreshold: int64(part),
	}

	if nop {
		bundle.TarBallMaker = &testtools.NOPTarBallMaker{}
	} else if !s3 && outDir == "" {
		fmt.Printf("Please provide a directory to write to.\n")
		os.Exit(1)
	} else if !s3 {
		if profile {
			f, err := os.Create("cpu.prof")
			if err != nil {
				log.Fatal(err)
			}
			pprof.StartCPUProfile(f)
			defer pprof.StopCPUProfile()
		}

		if mem {
			f, err := os.Create("mem.prof")
			if err != nil {
				log.Fatal(err)
			}

			pprof.WriteHeapProfile(f)
			f.Close()
		}

		bundle.TarBallMaker = &testtools.FileTarBallMaker{
			Out:              outDir,
		}
		os.MkdirAll(outDir, 0766)

	} else if s3 {
		tu, _, _ := walg.Configure()
		c, err := walg.Connect()
		if err != nil {
			panic(err)
		}

		n, _, _, err := bundle.StartBackup(c, time.Now().String())
		if err != nil {
			fmt.Printf("%+v\n", err)
			os.Exit(1)
		}

		bundle.TarBallMaker = &walg.S3TarBallMaker{
			BackupName:       n,
			Uploader:         tu,
		}

		bundle.NewTarBall(false)
		bundle.HandleLabelFiles(c)

	}

	bundle.StartQueue()
	defer testtools.TimeTrack(time.Now(), "MAIN")
	fmt.Println("Walking ...")
	err = filepath.Walk(in, bundle.HandleWalkedFSObject)
	if err != nil {
		panic(err)
	}
	err = bundle.FinishQueue()
	if err != nil {
		panic(err)
	}
	err = bundle.TarBall.Finish(&walg.S3TarBallSentinelDto{})
	if err != nil {
		panic(err)
	}

}

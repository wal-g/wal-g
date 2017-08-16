package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/katie31/wal-g"
	"github.com/katie31/wal-g/testTools"
	"log"
	"net/http"
	"os"
	"runtime/pprof"
	"strings"
)

var profile bool
var mem bool
var noOp bool
var data []string

func init() {
	flag.BoolVar(&profile, "p", false, "Profiler (false on default)")
	flag.BoolVar(&mem, "m", false, "Memory profiler (false on default)")
	flag.BoolVar(&noOp, "n", false, "NOP extractor (write on default)")

}

func main() {
	flag.Parse()
	all := flag.Args()
	dir := all[0]
	data := all[1:]

	if mem {
		f, err := os.Create("mem.prof")
		if err != nil {
			log.Fatal(err)
		}

		pprof.WriteHeapProfile(f)
		f.Close()
	}
	out := make([]walg.ReaderMaker, len(data))
	for i, val := range data {
		if strings.HasPrefix(val, "https://") {
			tls := &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			}

			h := &tools.HttpReaderMaker{
				Client:     &http.Client{Transport: tls},
				Key:        val,
				FileFormat: walg.CheckType(val),
			}

			out[i] = h
		} else {
			f := &tools.FileReaderMaker{
				Key:        val,
				FileFormat: walg.CheckType(val),
			}
			out[i] = f
		}
	}

	if profile {
		f, err := os.Create("cpu.prof")
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if !noOp {
		ft := &walg.FileTarInterpreter{
			NewDir: dir,
		}

		tools.MakeDir(ft.NewDir)
		err := walg.ExtractAll(ft, out)
		if serr, ok := err.(*walg.UnsupportedFileTypeError); ok {
			fmt.Println(serr.Error())
			os.Exit(1)
		} else if err != nil {
			panic(err)
		}

	} else {
		np := &tools.NOPTarInterpreter{}
		err := walg.ExtractAll(np, out)
		if serr, ok := err.(*walg.UnsupportedFileTypeError); ok {
			fmt.Println(serr.Error())
			os.Exit(1)
		} else if err != nil {
			panic(err)
		}

	}

	// Only prints compression data for lzo.
	fmt.Printf("Uncompressed: %v\n", walg.Uncompressed)
	fmt.Printf("Compressed: %v\n", walg.Compressed)
	fmt.Printf("Ratio: %.2f%%\n", (float64(walg.Compressed)/float64(walg.Uncompressed))*float64(100))

}

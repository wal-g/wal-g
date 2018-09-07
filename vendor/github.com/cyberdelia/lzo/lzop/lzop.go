package main

import (
	"flag"
	"fmt"
	"github.com/cyberdelia/lzo"
	"io"
	"os"
)

var (
	uncompress = flag.Bool("decompress", false, "Decompress.")
	level      = flag.Int("level", 3, "Compression level.")
)

func decompress(path string) error {
	input, err := os.Open(path)
	if err != nil {
		return err
	}
	decompressor, err := lzo.NewReader(input)
	if err != nil {
		return err
	}
	output, err := os.Create(decompressor.Name)
	if err != nil {
		return err
	}
	_, err = io.Copy(output, decompressor)
	if err != nil {
		return err
	}
	return nil
}

func compress(level int, path string) error {
	if level > lzo.BestCompression {
		level = lzo.BestCompression
	} else if level < lzo.BestSpeed {
		level = lzo.BestSpeed
	} else {
		level = lzo.DefaultCompression
	}
	input, err := os.Open(path)
	if err != nil {
		return err
	}
	output, err := os.Create(path + ".lzo")
	if err != nil {
		return err
	}
	compressor, err := lzo.NewWriterLevel(output, level)
	defer compressor.Close()
	compressor.Name = input.Name()
	if err != nil {
		return err
	}
	_, err = io.Copy(compressor, input)
	if err != nil {
		return err
	}
	return nil
}

func main() {
	flag.Parse()
	path := flag.Arg(0)
	var err error
	if *uncompress == true {
		err = decompress(path)
	} else {
		err = compress(*level, path)
	}
	if err != nil {
		fmt.Println("lzop:", err)
	}
}

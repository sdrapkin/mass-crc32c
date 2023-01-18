package main

import (
	"encoding/base64"
	"encoding/binary"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

var wg sync.WaitGroup

var limiter chan struct{}
var token struct{}
var readSize int

func printErr(a ...any) {
	fmt.Fprintln(os.Stderr, a)
}

func CRCReader(reader io.Reader) (string, error) {
	table := crc32.MakeTable(crc32.Castagnoli)
	checksum := crc32.Checksum([]byte(""), table)
	buf := make([]byte, 1024*1024*readSize)
	for {
		switch n, err := reader.Read(buf); err {
		case nil:
			checksum = crc32.Update(checksum, table, buf[:n])
		case io.EOF:
			b := make([]byte, 4)
			binary.BigEndian.PutUint32(b, checksum)
			str := base64.StdEncoding.EncodeToString(b)
			return str, nil
		default:
			return "", err
		}
	}
}

func fileHandler(path string) error {
	defer wg.Done()              // register that we finish a job at the end of the task
	defer func() { <-limiter }() // pop a token out of the queue when the task is done
	file, err := os.Open(path)
	defer file.Close()
	if err != nil {
		printErr(err)
		return nil
	}
	crc, err := CRCReader(file)
	if err != nil {
		printErr(err)
		return nil
	}
	fmt.Printf("%s %s\n", crc, path)
	return nil
}

func walkHandler(path string, info os.FileInfo, err error) error {
	if err != nil {
		printErr(err)
		return nil
	}
	if info.IsDir() {
		fmt.Fprintf(os.Stderr, "entering dir: %s\n", path)
		return nil
	}
	limiter <- token // add a token to the queue (blocking when queue is full)
	wg.Add(1)        // register that we start a new job
	go fileHandler(path)
	return nil
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage of %s: [options] path [path ...]\n\nOptions:\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	p := flag.Int("p", 1, "# of cpu used")
	j := flag.Int("j", 1, "# of parallel reads")
	r := flag.Int("s", 8, "size of reads in Mbytes")
	flag.Usage = printUsage

	flag.Parse()
	if flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "error: missing paths")
		printUsage()
		os.Exit(1)
	}

	runtime.GOMAXPROCS(*p)            // limit number of kernel threads (CPUs used)
	limiter = make(chan struct{}, *j) // use a channel with a size to limit the number of parallel jobs
	readSize = *r
	for _, arg := range flag.Args() {
		err := filepath.Walk(arg, walkHandler)
		if err != nil {
			log.Fatal(err)
		}
	}
	wg.Wait()
}

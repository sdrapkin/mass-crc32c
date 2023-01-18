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
	"sync"
	"time"
)

var wg sync.WaitGroup

var limiter chan struct{}
var token struct{}
var readSize int
var global_crc32cTable *crc32.Table

func printErr(path string, err error, isDir bool) {
	node_type := "file"
	if isDir {
		node_type = "dir"
	}
	fmt.Fprintf(os.Stderr, "%s error: '%s': %v\n", node_type, path, err)
}

func CRCReader(reader io.Reader) (string, error) {
	checksum := uint32(0)
	buf := make([]byte, 1024*1024*readSize)
	for {
		switch n, err := reader.Read(buf); err {
		case nil:
			checksum = crc32.Update(checksum, global_crc32cTable, buf[:n])
		case io.EOF:
			binary.BigEndian.PutUint32(buf, checksum)
			str := base64.StdEncoding.EncodeToString(buf[:4])
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
	defer func() { file.Close() }()
	if err != nil {
		printErr(path, err, false)
		return nil
	}
	crc, err := CRCReader(file)
	if err != nil {
		printErr(path, err, false)
		return nil
	}
	fmt.Printf("%s %s\n", crc, path)
	return nil
}

func walkHandler(path string, info os.FileInfo, err error) error {
	if err != nil {
		printErr(path, err, info.IsDir())
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

func sanityCheck() {
	const data = "861844d6704e8573fec34d967e20bcfef3d424cf48be04e6dc08f2bd58c729743371015ead891cc3cf1c9d34b49264b510751b1ff9e537937bc46b5d6ff4ecc8" // sha512("Hello World!")
	const expectedCorrectChecksum = "C7DdPQ=="

	calculatedChecksum := crc32.Update(uint32(0), global_crc32cTable, []byte(data))
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, calculatedChecksum)
	calculatedChecksumBase64 := base64.StdEncoding.EncodeToString(buf)
	if expectedCorrectChecksum != calculatedChecksumBase64 {
		fmt.Fprintf(os.Stderr, "Sanity Check failed! Terminating.\n")
		os.Exit(-1)
	}
}

func main() {
	start := time.Now()
	p := flag.Int("p", 1, "# of cpu used")
	j := flag.Int("j", 1, "# of parallel reads")
	s := flag.Int("s", 8, "size of reads in Mbytes")
	flag.Usage = printUsage

	flag.Parse()
	if flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "error: missing paths")
		printUsage()
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "Flags (p j s): %d %d %d\n", *p, *j, *s)

	global_crc32cTable = crc32.MakeTable(crc32.Castagnoli)

	sanityCheck()

	//runtime.GOMAXPROCS(*p)            // limit number of kernel threads (CPUs used)
	limiter = make(chan struct{}, *j) // use a channel with a size to limit the number of parallel jobs
	readSize = *s
	for _, arg := range flag.Args() {
		err := filepath.Walk(arg, walkHandler)
		if err != nil {
			log.Fatal(err)
		}
	}
	wg.Wait()
	fmt.Fprintln(os.Stderr, time.Since(start))
}

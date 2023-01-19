package main

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
) //import

var wg sync.WaitGroup

type job struct {
	path string
	size int64
} //job struct

var jobQueue chan job
var global_crc32cTable *crc32.Table

func printErr(path string, err error) {
	fmt.Fprintf(os.Stderr, "error: '%s': %v\n", path, err)
} //printErr()

func CRCReader(path string, fileInfoSize int64, buffer *[]byte) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		printErr(path, err)
		return "", err
	}
	defer file.Close()

	checksum := uint32(0)
	processedSize := int64(0)
	for {
		switch n, err := file.Read(*buffer); err {
		case nil:
			checksum = crc32.Update(checksum, global_crc32cTable, (*buffer)[:n])
			processedSize += int64(n)
		case io.EOF:
			binary.BigEndian.PutUint32(*buffer, checksum)
			str := base64.StdEncoding.EncodeToString((*buffer)[:4])

			if fileInfoSize != processedSize {
				return "", errors.New("fileInfoSize != processedSize")
			}
			return str, nil
		default:
			return "", err
		}
	}
} //CRCReader()

func fileHandler(bufferSizeKB int) error {
	wg.Add(1)
	defer wg.Done()
	buffer := make([]byte, 1024*bufferSizeKB)
	for work := range jobQueue { // consume the messages in the queue

		crc, err := CRCReader(work.path, work.size, &buffer)
		if err != nil {
			printErr(work.path, err)
			continue
		}
		fmt.Printf("%s %016x %s\n", crc, work.size, work.path)
	}
	return nil
} //fileHandler()

func enqueueJob(path string, info os.FileInfo, err error) error {
	if err != nil {
		nodeType := "file"
		if info.IsDir() {
			nodeType = "dir"
		}
		fmt.Fprintf(os.Stderr, "%s error: '%s': %v\n", nodeType, path, err)
		return nil
	}
	if info.IsDir() {
		fmt.Fprintf(os.Stderr, "entering dir: %s\n", path)
		return nil
	}
	if !info.Mode().IsRegular() {
		fmt.Fprintf(os.Stderr, "ignoring: %s\n", path)
		return nil
	}
	jobQueue <- job{path: path, size: info.Size()} // add new file job to the queue (blocking when queue is full)
	return nil
} //enqueueJob()

func sanityCheck() {
	const data = "861844d6704e8573fec34d967e20bcfef3d424cf48be04e6dc08f2bd58c729743371015ead891cc3cf1c9d34b49264b510751b1ff9e537937bc46b5d6ff4ecc8" // sha512("Hello World!")
	const expectedCorrectChecksum = "C7DdPQ=="

	calculatedChecksum := crc32.Update(uint32(0), global_crc32cTable, []byte(data))
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, calculatedChecksum)
	calculatedChecksumBase64 := base64.StdEncoding.EncodeToString(buf)
	if expectedCorrectChecksum != calculatedChecksumBase64 {
		fmt.Fprintf(os.Stderr, "Sanity Check failed! Terminating.\n")
		os.Exit(2)
	}
} //sanityCheck()

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage of %s: [options] path [path ...]\n\nOptions:\n", os.Args[0])
	flag.PrintDefaults()
} //printUsage()

func main() {
	p := flag.Int("p", runtime.NumCPU(), "# of cpu used")
	j := flag.Int("j", runtime.NumCPU()*4, "# of parallel reads")
	l := flag.Int("l", runtime.NumCPU()*4*4, "size of list ahead queue")
	s := flag.Int("s", 1024, "size of reads in kbytes")
	flag.Usage = printUsage

	flag.Parse()
	if flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "error: missing paths")
		printUsage()
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "Flags: [p=%d j=%d l=%d s=%d)]\n", *p, *j, *l, *s)

	global_crc32cTable = crc32.MakeTable(crc32.Castagnoli)
	sanityCheck()

	runtime.GOMAXPROCS(*p)        // limit number of kernel threads (CPUs used)
	jobQueue = make(chan job, *l) // use a channel with a size to limit the number of list ahead path

	start := time.Now()

	// create the coroutines
	for i := 1; i < *j; i++ {
		go fileHandler(*s)
	}
	for _, arg := range flag.Args() {
		err := filepath.Walk(arg, enqueueJob)
		if err != nil {
			log.Fatal(err)
		}
	}
	close(jobQueue)
	wg.Wait()

	fmt.Fprintln(os.Stderr, time.Since(start))
} //main()

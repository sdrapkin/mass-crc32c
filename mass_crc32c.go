package main

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/text/language"
	"golang.org/x/text/message"
) //import

type job struct {
	path string
	size int64
} //job struct

type jobStat struct {
	bytesProcessed int64
	filesProcessed int64
}

var (
	g_jobQueue    chan job
	g_crc32cTable *crc32.Table
)

func printErr(path string, err error) {
	os.Stderr.WriteString("CRC error: '" + path + "' : " + err.Error() + "\n")
} //printErr()

func CRCReader(j job, buffer []byte, bufferSize int) (string, error) {
	file, err := os.Open(j.path)
	if err != nil {
		printErr(j.path, err)
		return "", err
	}
	defer file.Close()

	checksum := uint32(0)
	processedSize := int64(0)

	for {
		switch n, err := file.Read(buffer); err {
		case nil: // runs many times
			if n == bufferSize {
				processedSize += int64(bufferSize)
				checksum = crc32.Update(checksum, g_crc32cTable, buffer)
			} else {
				processedSize += int64(n)
				checksum = crc32.Update(checksum, g_crc32cTable, buffer[:n])
			}
		case io.EOF: // runs once
			if j.size != processedSize {
				return "", errors.New("fileInfoSize != processedSize")
			}

			const checksumByteSize = crc32.Size
			const checksumBase64Size = (checksumByteSize + 2) / 3 * 4

			checksumBufferSlice := buffer[0:checksumByteSize]
			encodedBufferSlice := buffer[checksumByteSize : checksumByteSize+checksumBase64Size]

			binary.BigEndian.PutUint32(checksumBufferSlice, checksum)
			base64.StdEncoding.Encode(encodedBufferSlice, checksumBufferSlice)

			return string(encodedBufferSlice), nil
		default: // should never run
			return "ERROR!", err
		} //switch
	} //for
} //CRCReader()

func fileHandler(jobId int, bufferSizeKB int, jobStats []jobStat) error {
	fileReadBufferSize := 1024 * bufferSizeKB
	fileReadBuffer := make([]byte, fileReadBufferSize)
	stdoutBuffer := bytes.Buffer{}
	batchCounter := uint8(0) // batches of 256
	localJobStat := jobStat{}

	for j := range g_jobQueue { // consume the messages in the queue
		crc, err := CRCReader(j, fileReadBuffer, fileReadBufferSize)
		if err != nil {
			printErr(j.path, err)
			continue
		}
		batchCounter++
		jobFileSize := j.size
		localJobStat.bytesProcessed += jobFileSize

		stdoutBuffer.WriteString(crc + fmt.Sprintf(" %016x ", jobFileSize) + j.path + "\n")

		if batchCounter == 0 { // byte wrap-around
			os.Stdout.Write(stdoutBuffer.Bytes())
			stdoutBuffer.Reset()
			localJobStat.filesProcessed += (math.MaxUint8 + 1)
		}
	} //for

	if batchCounter > 0 {
		os.Stdout.Write(stdoutBuffer.Bytes())
		localJobStat.filesProcessed += int64(batchCounter)
	}

	jobStats[jobId] = localJobStat
	return nil
} //fileHandler()

func enqueueJob(path string, info os.FileInfo, err error) error {
	if err != nil {
		nodeType := ""
		if info != nil && info.Mode().IsDir() {
			nodeType = "dir: "
		} else {
			nodeType = "file: "
		}
		os.Stderr.WriteString(nodeType + "error: '" + path + "': " + err.Error() + "\n")
		return nil
	}

	fileMode := info.Mode()
	if fileMode.IsDir() {
		str := "entering dir: " + path + "\n"
		os.Stderr.Write(unsafe.Slice(unsafe.StringData(str), len(str)))
		return nil
	}
	if !fileMode.IsRegular() {
		os.Stderr.WriteString("ignoring: " + path + "\n")
		return nil
	}
	g_jobQueue <- job{path: path, size: info.Size()} // add new file job to the queue (blocking when queue is full)
	return nil
} //enqueueJob()

func init() {
	g_crc32cTable = crc32.MakeTable(crc32.Castagnoli)
	sanityCheck()
}

func sanityCheck() {
	const data = "861844d6704e8573fec34d967e20bcfef3d424cf48be04e6dc08f2bd58c729743371015ead891cc3cf1c9d34b49264b510751b1ff9e537937bc46b5d6ff4ecc8" // sha512("Hello World!")
	const expectedCorrectChecksum = "C7DdPQ=="

	calculatedChecksum := crc32.Update(uint32(0), g_crc32cTable, []byte(data))
	slice4 := make([]byte, 4)
	binary.BigEndian.PutUint32(slice4, calculatedChecksum)
	calculatedChecksumBase64 := base64.StdEncoding.EncodeToString(slice4)
	if expectedCorrectChecksum != calculatedChecksumBase64 {
		fmt.Fprintf(os.Stderr, "Sanity Check failed! [expected: %s, calculated: %s]. Terminating.\n",
			expectedCorrectChecksum, calculatedChecksumBase64)
		os.Exit(2)
	}
} //sanityCheck()

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage of %s: [options] path [path ...]\n\nOptions:\n", os.Args[0])
	flag.PrintDefaults()
} //printUsage()

func main() {
	var cpuCount int
	var workerCount int
	var bufferSizeKB int
	var listAheadSize int

	const DEFAULT_BUFFER_SIZE_KB = 1024
	numCPU := runtime.NumCPU()

	flag.IntVar(&cpuCount, "p", numCPU, "# of cpu used")
	flag.IntVar(&workerCount, "j", numCPU*4, "# of parallel reads")
	flag.IntVar(&listAheadSize, "l", workerCount, "size of list ahead queue")
	flag.IntVar(&bufferSizeKB, "s", DEFAULT_BUFFER_SIZE_KB, "size of reads in kbytes")
	flag.Usage = printUsage

	flag.Parse()
	if flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "error: missing paths")
		printUsage()
		os.Exit(1)
	}

	printFlags := func() {
		fmt.Fprintf(os.Stderr, "Flags: [p=%d j=%d l=%d s=%d)]\n", cpuCount, workerCount, listAheadSize, bufferSizeKB)
	}

	runtime.GOMAXPROCS(cpuCount)               // limit number of kernel threads (CPUs used)
	g_jobQueue = make(chan job, listAheadSize) // use a channel with a size to limit the number of list ahead path

	jobStats := make([]jobStat, workerCount)
	waitGroup := sync.WaitGroup{}

	start := time.Now()

	// create the coroutines
	waitGroup.Add(workerCount)
	for jobId := 0; jobId < workerCount; jobId++ {
		go func(jobId int) {
			defer waitGroup.Done()
			fileHandler(jobId, bufferSizeKB, jobStats)
		}(jobId)
	}

	// enqueue jobs
	for _, arg := range flag.Args() {
		err := filepath.Walk(arg, enqueueJob)
		if err != nil {
			log.Fatal(err)
		}
	} //for
	close(g_jobQueue) // safe to close, since all jobs have already been channel-received by now

	waitGroup.Wait()
	duration := time.Since(start)

	var totalFilesProcessed, totalBytesProcessed int64
	for _, item := range jobStats {
		totalFilesProcessed += item.filesProcessed
		totalBytesProcessed += item.bytesProcessed
	}

	var mbPerSecond float64 = (float64(totalBytesProcessed) / (1024 * 1024)) / duration.Seconds()
	printer := message.NewPrinter(language.English)

	printFlags()
	printer.Fprintf(os.Stderr, "[Duration: %v] [Files processed: %v] [Bytes processed: %v] [%.2f MiB/second]\n",
		duration, totalFilesProcessed, totalBytesProcessed, mbPerSecond)
} //main()

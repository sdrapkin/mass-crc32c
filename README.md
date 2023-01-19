# mass-crc32c
Computes Google GCS compatible CRC32C of local files with configurable multithreading and parallel file reads 

The initial usecase was to compute an inventory with CRC32C of a billion files on a local storage prior to upload to GCS.

# Usage
```
$ mass-crc32c --help
Usage of mass-crc32c: [options] [path ...]

Options:
  -j int
        # of parallel reads (default <#CPU>*4)
  -l int
        size of list ahead queue (default <#CPU>*16)
  -p int
        # of cpu used (default <#CPU>)
  -s int
        size of reads in kbytes (default 1024)
```
# Output

Each line of output contains:
- Base64-encoded `crc32c`
- Base16-encoded `int64` file size
- file path

Example: 
```
0vVTfA== 0000000000000039 go.mod
PcDXzA== 00000000000002fd README.md
C7DdPQ== 0000000000000080 hashtest.txt
nXpcgw== 0000000000000fc0 mass_crc32c.go
QO07FQ== 0000000000016000 .vs\slnx.sqlite
MPgmSg== 000000000023a800 mass-crc32c.exe
```

# Release
This project uses [goreleaser](https://goreleaser.com/)
You can follow this [quick start guide](https://goreleaser.com/quick-start/) to create a new release

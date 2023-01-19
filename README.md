# mass-crc32c
Computes Google GCS compatible CRC32C of local files with configurable multithreading and parallel file reads 

The initial usecase was to compute an inventory with CRC32C of a billion files on a local storage prior to upload to GCS.

# usage
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

# Release
This project uses [goreleaser](https://goreleaser.com/)
You can follow this [quick start guide](https://goreleaser.com/quick-start/) to create a new release

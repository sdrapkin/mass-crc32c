# mass-crc32c
Computes GCS compatible CRC32C of local files with configurable multithreading and parallel file reads 

# usage
```
$ mass-crc32c --help
Usage of mass-crc32c: [options] [path ...]

Options:
  -j int
    	# of parallel reads (default 1)
  -p int
    	# of cpu used (default 1)
  -s int
    	size of reads in Mbytes (default 8)
```

# Release
This project uses [goreleaser](https://goreleaser.com/)
You can follow this [quick start guide](https://goreleaser.com/quick-start/) to create a new release

package main

import (
	"flag"

	"powersight/data-load/internal/dataset"
)

func main() {
	root := flag.String("root", "..", "project root with data/raw and data/processed")
	workers := flag.Int("workers", 4, "number of parallel workers")
	total := flag.Int("total", 2100000, "initial capacity for clean records")
	chunkSize := flag.Int("chunk-size", 0, "reserved chunk size parameter")
	flag.Parse()

	dataset.LoadData(*root, *workers, *total, *chunkSize)
}

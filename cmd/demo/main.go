package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"time"

	"reliablewriter"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	uw, err := reliablewriter.NewFileUnreliableWriter("output.bin", 0.30)
	if err != nil {
		panic(err)
	}

	rw := reliablewriter.NewReliableWriter(
		ctx,
		uw,
		8*1024,  // minChunk
		32*1024, // maxChunk
	)

	dataSize := 200 * 1024
	src := make([]byte, dataSize)
	for i := range src {
		src[i] = byte(i % 251)
	}

	off := int64(0)
	step := 5000 // специально не кратно minChunk, чтобы были разные остатки
	for off < int64(len(src)) {
		end := off + int64(step)
		if end > int64(len(src)) {
			end = int64(len(src))
		}
		if err := rw.WriteAt(ctx, src[off:end], off); err != nil {
			panic(err)
		}
		off = end
	}

	if err := rw.Complete(ctx); err != nil {
		panic(err)
	}

	out, err := os.ReadFile("output.bin")
	if err != nil {
		panic(err)
	}

	if !bytes.Equal(out, src) {
		fmt.Printf("FAIL: data mismatch (out=%d, src=%d)\n", len(out), len(src))
		// маленький хелп для дебага: найти первое отличие
		n := min(len(out), len(src))
		for i := 0; i < n; i++ {
			if out[i] != src[i] {
				fmt.Printf("first diff at %d: out=%02x src=%02x\n", i, out[i], src[i])
				break
			}
		}
		return
	}

	fmt.Printf("OK: wrote %d bytes reliably into output.bin (failProb=%.2f)\n", len(src), 0.10)
}

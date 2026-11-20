// Command verify runs the pinned ltx library's Decoder.Verify over each file named on the
// command line: the reference reader that every Rust-written file must satisfy. It prints
// "ok <path>" or "FAIL <path>: <error>" per file and exits non-zero if any failed.
package main

import (
	"fmt"
	"os"

	"github.com/superfly/ltx"
)

func verify(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return ltx.NewDecoder(f).Verify()
}

func main() {
	failed := 0
	for _, path := range os.Args[1:] {
		if err := verify(path); err != nil {
			fmt.Printf("FAIL %s: %v\n", path, err)
			failed++
			continue
		}
		fmt.Printf("ok %s\n", path)
	}
	if failed > 0 {
		os.Exit(1)
	}
}

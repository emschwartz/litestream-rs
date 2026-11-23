// lsrcompact.go is copied into the pinned upstream cmd/litestream package beside lsrdump.go by
// scripts/build-go-litestream.sh. It runs Go's own level compactor, litestream.Compactor's
// Compact, once over a file replica: what `replicate` does on its compaction schedule, without a
// database. It runs only when LSR_COMPACT is set, from an init function, so the rest of the
// binary is upstream's unchanged. It prints one JSON object on stdout: the file written, or the
// error Go returned.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
)

type lsrCompactOut struct {
	Error   string `json:"error,omitempty"`
	Level   int    `json:"level"`
	MinTXID string `json:"min_txid,omitempty"`
	MaxTXID string `json:"max_txid,omitempty"`
}

func init() {
	if os.Getenv("LSR_COMPACT") == "" {
		return
	}
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: LSR_COMPACT=1 litestream-dump <replica dir> <level>")
		os.Exit(2)
	}
	level, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	out := lsrCompactOut{Level: level}
	compactor := litestream.NewCompactor(file.NewReplicaClient(os.Args[1]), slog.Default())
	if info, err := compactor.Compact(context.Background(), level); err != nil {
		out.Error = err.Error()
	} else {
		out.MinTXID = info.MinTXID.String()
		out.MaxTXID = info.MaxTXID.String()
	}
	text, err := json.Marshal(out)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	fmt.Println(string(text))
	os.Exit(0)
}

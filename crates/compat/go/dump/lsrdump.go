// lsrdump.go is copied into the pinned upstream cmd/litestream package by
// scripts/build-go-litestream.sh to build litestream-dump-<name>. Go Litestream has no command
// that prints the configuration it would run, so this file adds one built only from Go's own
// functions: ReadConfig, NewDBsFromDirectoryConfig and NewDBFromConfig, the calls `replicate`
// makes before it starts the store. It runs only when LSR_DUMP is set, from an init function, so
// the rest of the binary is upstream's unchanged. It prints one JSON object on stdout: the
// databases with their meta paths and replicas, or the first error Go returned. The Rust side
// of the differential (lsr_compat::dump) prints the same shape from lsr_config.
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/s3"
)

type lsrDumpReplica struct {
	Type   string `json:"type"`
	Path   string `json:"path"`
	Bucket string `json:"bucket,omitempty"`
}

type lsrDumpDB struct {
	Path    string         `json:"path"`
	Meta    string         `json:"meta"`
	Replica lsrDumpReplica `json:"replica"`
}

type lsrDumpOut struct {
	Error string      `json:"error,omitempty"`
	DBs   []lsrDumpDB `json:"dbs"`
}

func init() {
	if os.Getenv("LSR_DUMP") == "" {
		return
	}
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: LSR_DUMP=1 litestream-dump <config>")
		os.Exit(2)
	}
	out := lsrDump(os.Args[1])
	text, err := json.Marshal(out)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	fmt.Println(string(text))
	os.Exit(0)
}

func lsrDump(path string) lsrDumpOut {
	out := lsrDumpOut{DBs: []lsrDumpDB{}}
	config, err := ReadConfig(path, true)
	if err != nil {
		return lsrDumpOut{Error: err.Error(), DBs: []lsrDumpDB{}}
	}
	for _, dbc := range config.DBs {
		var dbs []*litestream.DB
		if dbc.Dir != "" {
			dbs, err = NewDBsFromDirectoryConfig(dbc)
		} else {
			var db *litestream.DB
			db, err = NewDBFromConfig(dbc)
			dbs = []*litestream.DB{db}
		}
		if err != nil {
			return lsrDumpOut{Error: err.Error(), DBs: []lsrDumpDB{}}
		}
		for _, db := range dbs {
			out.DBs = append(out.DBs, lsrDumpDB{
				Path:    db.Path(),
				Meta:    db.MetaPath(),
				Replica: lsrDumpReplicaOf(db.Replica),
			})
		}
	}
	return out
}

func lsrDumpReplicaOf(r *litestream.Replica) lsrDumpReplica {
	if r == nil || r.Client == nil {
		return lsrDumpReplica{}
	}
	switch client := r.Client.(type) {
	case *file.ReplicaClient:
		return lsrDumpReplica{Type: "file", Path: client.Path()}
	case *s3.ReplicaClient:
		return lsrDumpReplica{Type: "s3", Path: client.Path, Bucket: client.Bucket}
	default:
		return lsrDumpReplica{Type: r.Client.Type()}
	}
}

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
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/s3"
)

type lsrDumpReplica struct {
	Type   string `json:"type"`
	Path   string `json:"path"`
	Bucket string `json:"bucket,omitempty"`
	// Credentials, only when LSR_DUMP_CREDENTIALS is set: they are resolved as the S3
	// client's Init resolves them, which reads the shared files and may reach for IMDS.
	Credentials *lsrDumpCredentials `json:"credentials,omitempty"`
}

// lsrDumpCredentials is what the AWS SDK's chain gave, or why it gave nothing. The tests
// that ask for it use fake keys only.
type lsrDumpCredentials struct {
	Error           string `json:"error,omitempty"`
	AccessKeyID     string `json:"access_key_id,omitempty"`
	SecretAccessKey string `json:"secret_access_key,omitempty"`
	SessionToken    string `json:"session_token,omitempty"`
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
	// Run copies LITESTREAM_* into AWS_* before any command; this init runs before Run.
	applyLitestreamEnv()
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
		out := lsrDumpReplica{Type: "s3", Path: client.Path, Bucket: client.Bucket}
		if os.Getenv("LSR_DUMP_CREDENTIALS") != "" {
			out.Credentials = lsrDumpResolve(client)
		}
		return out
	default:
		return lsrDumpReplica{Type: r.Client.Type()}
	}
}

// lsrDumpResolve resolves credentials with the options s3.ReplicaClient.Init gives
// config.LoadDefaultConfig that bear on them: static keys when the client holds both, else
// the SDK's default chain.
func lsrDumpResolve(c *s3.ReplicaClient) *lsrDumpCredentials {
	ctx := context.Background()
	opts := []func(*config.LoadOptions) error{config.WithRegion(c.Region)}
	if c.AccessKeyID != "" && c.SecretAccessKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(c.AccessKeyID, c.SecretAccessKey, c.SessionToken),
		))
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return &lsrDumpCredentials{Error: err.Error()}
	}
	v, err := cfg.Credentials.Retrieve(ctx)
	if err != nil {
		return &lsrDumpCredentials{Error: err.Error()}
	}
	return &lsrDumpCredentials{
		AccessKeyID:     v.AccessKeyID,
		SecretAccessKey: v.SecretAccessKey,
		SessionToken:    v.SessionToken,
	}
}

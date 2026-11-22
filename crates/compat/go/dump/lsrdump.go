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
	"time"

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
	// The parsed configuration, only when LSR_DUMP_CONFIG is set: Go's Config after
	// ReadConfig (defaults applied, global replica settings copied down), before any
	// database is built, so it is printed even when building one fails.
	Config *lsrDumpConfig `json:"config,omitempty"`
}

// lsrDumpConfig is the part of Go's Config that litestream-rs keeps. Durations are
// nanoseconds; a nil pointer is null.
type lsrDumpConfig struct {
	Addr                string            `json:"addr"`
	Exec                string            `json:"exec"`
	Levels              []int64           `json:"levels"`
	SnapshotInterval    *int64            `json:"snapshot_interval"`
	SnapshotRetention   *int64            `json:"snapshot_retention"`
	L0Retention         *int64            `json:"l0_retention"`
	L0RetentionCheck    *int64            `json:"l0_retention_check_interval"`
	RetentionEnabled    *bool             `json:"retention_enabled"`
	ShutdownSyncTimeout *int64            `json:"shutdown_sync_timeout"`
	DBs                 []lsrDumpDBConfig `json:"dbs"`
}

type lsrDumpDBConfig struct {
	Path                 string                `json:"path"`
	Dir                  string                `json:"dir"`
	Pattern              string                `json:"pattern"`
	Recursive            bool                  `json:"recursive"`
	MetaPath             *string               `json:"meta_path"`
	MetaDir              *string               `json:"meta_dir"`
	MonitorInterval      *int64                `json:"monitor_interval"`
	CheckpointInterval   *int64                `json:"checkpoint_interval"`
	MinCheckpointPageN   *int64                `json:"min_checkpoint_page_count"`
	TruncatePageN        *int64                `json:"truncate_page_n"`
	MaxSyncWALBytes      *int64                `json:"max_sync_wal_bytes"`
	RestoreIfDBNotExists bool                  `json:"restore_if_db_not_exists"`
	SnapshotInterval     *int64                `json:"snapshot_interval"`
	SnapshotRetention    *int64                `json:"snapshot_retention"`
	Replica              *lsrDumpReplicaConfig `json:"replica"`
}

type lsrDumpReplicaConfig struct {
	Type            string `json:"type"`
	URL             string `json:"url"`
	Path            string `json:"path"`
	SyncInterval    *int64 `json:"sync_interval"`
	MaxSyncLTXFiles *int64 `json:"max_sync_ltx_files"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	Region          string `json:"region"`
	Bucket          string `json:"bucket"`
	Endpoint        string `json:"endpoint"`
	ForcePathStyle  *bool  `json:"force_path_style"`
	PartSize        *int64 `json:"part_size"`
	Concurrency     *int64 `json:"concurrency"`
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
	config, err := ReadConfig(path, os.Getenv("LSR_DUMP_NO_EXPAND") == "")
	if err != nil {
		return lsrDumpOut{Error: err.Error(), DBs: []lsrDumpDB{}}
	}
	if os.Getenv("LSR_DUMP_CONFIG") != "" {
		out.Config = lsrDumpConfigOf(&config)
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
			return lsrDumpOut{Error: err.Error(), DBs: []lsrDumpDB{}, Config: out.Config}
		}
		for _, db := range dbs {
			// The one field check DB.Open makes before it touches a file (db.go:872 at
			// 041a8c3); replicate's start fails on it.
			if db.MinCheckpointPageN <= 0 {
				return lsrDumpOut{Error: "minimum checkpoint page count required", DBs: []lsrDumpDB{}, Config: out.Config}
			}
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

func lsrDur(d *time.Duration) *int64 {
	if d == nil {
		return nil
	}
	v := int64(*d)
	return &v
}

func lsrInt(i *int) *int64 {
	if i == nil {
		return nil
	}
	v := int64(*i)
	return &v
}

func lsrDumpConfigOf(c *Config) *lsrDumpConfig {
	out := &lsrDumpConfig{
		Addr:                c.Addr,
		Exec:                c.Exec,
		Levels:              []int64{},
		SnapshotInterval:    lsrDur(c.Snapshot.Interval),
		SnapshotRetention:   lsrDur(c.Snapshot.Retention),
		L0Retention:         lsrDur(c.L0Retention),
		L0RetentionCheck:    lsrDur(c.L0RetentionCheckInterval),
		RetentionEnabled:    c.Retention.Enabled,
		ShutdownSyncTimeout: lsrDur(c.ShutdownSyncTimeout),
		DBs:                 []lsrDumpDBConfig{},
	}
	for _, level := range c.Levels {
		out.Levels = append(out.Levels, int64(level.Interval))
	}
	for _, db := range c.DBs {
		d := lsrDumpDBConfig{
			Path:                 db.Path,
			Dir:                  db.Dir,
			Pattern:              db.Pattern,
			Recursive:            db.Recursive,
			MetaPath:             db.MetaPath,
			MetaDir:              db.MetaDir,
			MonitorInterval:      lsrDur(db.MonitorInterval),
			CheckpointInterval:   lsrDur(db.CheckpointInterval),
			MinCheckpointPageN:   lsrInt(db.MinCheckpointPageN),
			TruncatePageN:        lsrInt(db.TruncatePageN),
			MaxSyncWALBytes:      db.MaxSyncWALBytes,
			RestoreIfDBNotExists: db.RestoreIfDBNotExists,
			SnapshotInterval:     lsrDur(db.Snapshot.Interval),
			SnapshotRetention:    lsrDur(db.Snapshot.Retention),
		}
		r := db.Replica
		if r == nil && len(db.Replicas) == 1 {
			r = db.Replicas[0]
		}
		if r != nil {
			rc := &lsrDumpReplicaConfig{
				Type:            r.ReplicaType(),
				URL:             r.URL,
				Path:            r.Path,
				SyncInterval:    lsrDur(r.SyncInterval),
				MaxSyncLTXFiles: lsrInt(r.MaxSyncLTXFiles),
				AccessKeyID:     r.AccessKeyID,
				SecretAccessKey: r.SecretAccessKey,
				Region:          r.Region,
				Bucket:          r.Bucket,
				Endpoint:        r.Endpoint,
				ForcePathStyle:  r.ForcePathStyle,
				Concurrency:     lsrInt(r.Concurrency),
			}
			if r.PartSize != nil {
				v := int64(*r.PartSize)
				rc.PartSize = &v
			}
			d.Replica = rc
		}
		out.DBs = append(out.DBs, d)
	}
	return out
}

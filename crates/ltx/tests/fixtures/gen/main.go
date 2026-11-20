// Command gen writes the LTX fixtures the lsr-ltx tests decode.
//
// The same source builds against two pinned ltx versions (see the module
// directories beside this file): v0.5.1, the newest that wrote LZ4 frames,
// and v0.5.3, which writes LZ4 blocks. Output is deterministic: a fixed seed,
// fixed timestamps, no clock.
//
// For every file it writes a sidecar <name>.expect listing the header and
// trailer fields Go wrote and the SHA-256 of every page Go encoded, which the
// Rust tests compare against what they decode.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"

	"github.com/superfly/ltx"
)

type page struct {
	pgno uint32
	data []byte
}

func fill(r *rand.Rand, size int, kind int) []byte {
	b := make([]byte, size)
	switch kind % 4 {
	case 0: // incompressible: LZ4 stores the block raw
		r.Read(b)
	case 1: // zeros
	case 2: // text-like for the first 1 KiB, then zeros, to keep fixtures small
		for i := range b[:min(len(b), 1024)] {
			b[i] = "abcdefgh sqlite "[r.Intn(16)]
		}
	case 3: // a short repeating record
		for i := range b {
			b[i] = byte(i % 37)
		}
	}
	return b
}

func encode(hdr ltx.Header, pages []page, post ltx.Checksum) ([]byte, ltx.Trailer) {
	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		panic(err)
	}
	if err := enc.EncodeHeader(hdr); err != nil {
		panic(err)
	}
	for _, p := range pages {
		if err := enc.EncodePage(ltx.PageHeader{Pgno: p.pgno}, p.data); err != nil {
			panic(err)
		}
	}
	enc.SetPostApplyChecksum(post)
	if err := enc.Close(); err != nil {
		panic(err)
	}
	return buf.Bytes(), enc.Trailer()
}

func write(dir string, b []byte) {
	hdr, _, err := ltx.DecodeHeader(bytes.NewReader(b))
	if err != nil {
		panic(err)
	}
	name := ltx.FormatFilename(hdr.MinTXID, hdr.MaxTXID)
	if err := os.WriteFile(filepath.Join(dir, name), b, 0o644); err != nil {
		panic(err)
	}

	// The sidecar comes from Go's own decoder reading the file back.
	dec := ltx.NewDecoder(bytes.NewReader(b))
	if err := dec.DecodeHeader(); err != nil {
		panic(err)
	}
	var side bytes.Buffer
	h := dec.Header()
	fmt.Fprintf(&side, "flags %d\npage_size %d\ncommit %d\nmin_txid %d\nmax_txid %d\n", h.Flags, h.PageSize, h.Commit, h.MinTXID, h.MaxTXID)
	fmt.Fprintf(&side, "timestamp %d\npre_apply %d\nwal_offset %d\nwal_size %d\n", h.Timestamp, uint64(h.PreApplyChecksum), h.WALOffset, h.WALSize)
	fmt.Fprintf(&side, "wal_salt1 %d\nwal_salt2 %d\nnode_id %d\n", h.WALSalt1, h.WALSalt2, h.NodeID)
	var ph ltx.PageHeader
	data := make([]byte, h.PageSize)
	var lines bytes.Buffer
	for {
		if err := dec.DecodePage(&ph, data); err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		fmt.Fprintf(&lines, "page %d %x\n", ph.Pgno, sha256.Sum256(data))
	}
	if err := dec.Close(); err != nil {
		panic(err)
	}
	t := dec.Trailer()
	fmt.Fprintf(&side, "post_apply %d\nfile_checksum %d\n", uint64(t.PostApplyChecksum), uint64(t.FileChecksum))
	side.Write(lines.Bytes())
	if err := os.WriteFile(filepath.Join(dir, name+".expect"), side.Bytes(), 0o644); err != nil {
		panic(err)
	}
}

// db is the database the files describe, so tracked checksums are real.
type db struct {
	pageSize uint32
	pages    map[uint32][]byte
}

func (d *db) checksum() ltx.Checksum {
	c := ltx.ChecksumFlag
	for pgno, data := range d.pages {
		c = ltx.ChecksumFlag | (c ^ ltx.ChecksumPage(pgno, data))
	}
	return c
}

func (d *db) apply(commit uint32, pages []page) {
	for _, p := range pages {
		d.pages[p.pgno] = p.data
	}
	for pgno := range d.pages {
		if pgno > commit {
			delete(d.pages, pgno)
		}
	}
}

func generate(dir string, pageSize uint32, tracked bool, r *rand.Rand) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		panic(err)
	}
	var flags uint32
	if !tracked {
		flags = ltx.HeaderFlagNoChecksum
	}
	n := uint32(6)
	if pageSize == 512 {
		n = 12
	}
	state := &db{pageSize: pageSize, pages: map[uint32][]byte{}}
	checksumOr0 := func(c ltx.Checksum) ltx.Checksum {
		if tracked {
			return c
		}
		return 0
	}

	// Snapshot: TXIDs 1..2, every page from 1 to commit.
	var snap []page
	for pgno := uint32(1); pgno <= n; pgno++ {
		snap = append(snap, page{pgno, fill(r, int(pageSize), int(pgno))})
	}
	state.apply(n, snap)
	snapPost := checksumOr0(state.checksum())
	snapBytes, _ := encode(ltx.Header{
		Version: ltx.Version, Flags: flags, PageSize: pageSize, Commit: n,
		MinTXID: 1, MaxTXID: 2, Timestamp: 1700000000000, NodeID: 7,
	}, snap, snapPost)
	write(dir, snapBytes)

	// Incremental: TXID 3, rewrites two pages and grows by two.
	inc := []page{
		{2, fill(r, int(pageSize), 2)},
		{4, fill(r, int(pageSize), 3)},
		{n + 1, fill(r, int(pageSize), 2)},
		{n + 2, fill(r, int(pageSize), 1)},
	}
	state.apply(n+2, inc)
	incPost := checksumOr0(state.checksum())
	incBytes, _ := encode(ltx.Header{
		Version: ltx.Version, Flags: flags, PageSize: pageSize, Commit: n + 2,
		MinTXID: 3, MaxTXID: 3, Timestamp: 1700000005000, PreApplyChecksum: checksumOr0(snapPost),
		WALOffset: 32, WALSize: int64(4 * (24 + pageSize)), WALSalt1: 0xdeadbeef, WALSalt2: 9,
	}, inc, incPost)
	write(dir, incBytes)

	// Shrink: TXIDs 4..5, rewrites page 1 and truncates below the old commit,
	// as VACUUM does.
	shrink := []page{{1, fill(r, int(pageSize), 2)}}
	state.apply(n-1, shrink)
	shrinkPost := checksumOr0(state.checksum())
	shrinkBytes, _ := encode(ltx.Header{
		Version: ltx.Version, Flags: flags, PageSize: pageSize, Commit: n - 1,
		MinTXID: 4, MaxTXID: 5, Timestamp: 1700000009000, PreApplyChecksum: checksumOr0(incPost),
	}, shrink, shrinkPost)
	write(dir, shrinkBytes)

	// Compacted: TXIDs 1..5 through Go's own compactor, which drops the
	// pages above the final commit.
	var out bytes.Buffer
	c, err := ltx.NewCompactor(&out, []io.Reader{
		bytes.NewReader(snapBytes), bytes.NewReader(incBytes), bytes.NewReader(shrinkBytes),
	})
	if err != nil {
		panic(err)
	}
	c.HeaderFlags = flags
	if err := c.Compact(context.Background()); err != nil {
		panic(err)
	}
	write(dir, out.Bytes())
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: gen <output dir>")
		os.Exit(2)
	}
	r := rand.New(rand.NewSource(1))
	for _, pageSize := range []uint32{512, 4096, 16384, 65536} {
		// Litestream writes NoChecksum files; one page size is tracked so
		// the post-apply check on snapshots is exercised too.
		tracked := pageSize == 4096
		generate(filepath.Join(os.Args[1], fmt.Sprint(pageSize)), pageSize, tracked, r)
	}
}

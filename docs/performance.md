# Performance against Go Litestream

On the one machine measured, `lsr` finishes a first start of a 4 GiB database sooner than Go Litestream, with less CPU, less memory and fewer requests, in every row below. The MinIO rows are three paired runs each; the file-replica row is one.

## How it was measured

- **Machine:** AMD EPYC 7713, a 4-core Linux VM with 8 GiB of memory and an ext4 SSD, kernel 6.8.
- **Builds:** `lsr` from main at 27a41bd04, release build. Go Litestream at the pinned 041a8c3, built with Go 1.27.4.
- **Store:** MinIO built from source, on loopback. A file replica on the same disk.
- **Method:** the same `litestream.yml` for both programs, a fresh copy of the database for each run, a warm page cache, and pairs run interleaved with the order alternated. A run ends when the log says the first snapshot is complete. CPU is the process's user plus system time. Memory is the peak of anonymous memory, sampled once a second.
- **Noise:** an A/A run (`lsr` against itself, three pairs) spread 7% in wall time and 5% in CPU, so a difference under about 5% is not claimed.
- **Intervals:** 95% intervals on the paired ratio. With three pairs they are wide: read them as direction and rough size.
- **Databases:** 4 GiB, either random pages or text-like pages that LZ4 compresses to about half.

## First start

The time from start until the first snapshot is on the replica. "Large parts" is a large part size with ten parts in flight; "default parts" leaves both settings unset.

| Replica, database | n | Wall, Go / lsr | Wall change | CPU change | Peak memory, Go / lsr |
|---|---|---|---|---|---|
| MinIO, random, default parts | 3 | 59.6 / 34.6 s | -42% [-48, -35] | -30% [-45, -12] | 171 / 55 MiB (-67% [-82, -41]) |
| MinIO, text-like, default parts | 3 | 61.9 / 28.2 s | -54% [-56, -53] | -49% [-60, -34] | 154 / 46 MiB (-70% [-80, -54]) |
| MinIO, random, large parts | 3 | 60.4 / 41.6 s | -31% [-39, -22] | -32% [-45, -16] | 824 / 215 MiB (-73% [-89, -34]) |
| MinIO, text-like, large parts | 3 | 62.7 / 28.9 s | -54% [-57, -50] | -52% [-66, -32] | 810 / 197 MiB (-76% [-83, -65]) |
| File, random | 1 | 67.6 / 30.5 s | -55% | -74% | 120 / 23 MiB (-81%) |

The two large-parts rows predate `docs/decisions/D309.md`, which removed the 256 MiB cap on the parts a buffered upload holds. On a Mac Studio against local MinIO at 100 MB parts, `lsr`'s peak for the same first start rose from 225 to 320 MiB with ten parts at once and from 225 to 320 MiB with two (a 4 GiB random database, six pairs each, +42% [+41, +43]); Go's 824 MiB was measured on the Linux VM and has not been re-measured with the new code there.

Bytes written to the replica are equal on MinIO (8.5 GB random, 4.4 GB text-like). On the file replica `lsr` wrote 8.5 GB and Go 9.1 GB. A text-like database to a file replica was not measured at this commit.

## Requests at a first start

One counted run per row, through a proxy in front of MinIO, at 27a41bd04.

| Database, parts | Go | lsr | Of which upload parts, Go / lsr |
|---|---|---|---|
| Random, default parts | 3,809 | 1,680 | 3,770 / 1,630 |
| Text-like, default parts | 2,227 | 898 | not split out |
| Random, large parts | 210 | 135 | not split out |
| Text-like, large parts | 147 | 96 | not split out |

`lsr` sends fewer requests in total, but more of some kinds, in every row:

- **LIST:** 40 against Go's 33 or 34. `lsr` lists all ten levels on each full listing, where Go lists only its configured ones (`docs/decisions/D246.md`).
- **Reads at boot:** three GETs and two HEADs that Go does not send. 1314261bd, after this run, drops one of them.
- **Multipart abort:** one DELETE in three of the four rows, from the start's sweep of unfinished uploads.

## Not measured at the current code

- **Restore.** Last measured on 3c384ab27, from a file replica only. 1314261bd since changed how a restore reads each file (one GET per file instead of ranged reads), and a file of 16 MiB or more is now read through a window of 8 MiB ranged GETs side by side (docs/decisions/D312.md), so those numbers are not repeated here.
- **Steady state** (20 commits a second to MinIO). Last measured on c5c8f9817, before later changes to the upload and compaction paths, so its CPU, memory and request counts are not repeated here.
- **Other machines and real providers.** Every row above is one Linux VM against MinIO on loopback. A provider's latency changes the balance: this run hides the cost of a round trip that a real bucket charges.
- **Larger databases**, and an `lsr` that has run for days.

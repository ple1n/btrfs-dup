# btrfs-dup

A minimal Btrfs snapshot scanner and extent deduplicator. The embedded LanceDB
store defaults to `./btrfs-dup.lance`. Use the global `--db <path>` option to
choose another store; the path can appear before or after the command. Existing
SurrealDB data is not migrated or read.

Build with the pinned Nix toolchain (including `protoc` and `libclang`):

```sh
nix develop --command cargo build --release
nix build
```

Run the CLI or build without entering the development shell:

```text
cargo run --                         # guided start (inside nix develop)
cargo run -- dedup /mnt/data
cargo run -- dedup /dev/nvme0n1p2
cargo run -- --db /var/lib/btrfs-dup.lance dedup /mnt/data
cargo run -- list --db /var/lib/btrfs-dup.lance
cargo run -- status <scan-id>
cargo run -- verify <scan-id>
cargo run -- resume <scan-id>
cargo run -- dedup --resume <scan-id>
sudo ./target/release/btrfs-dup install
```

With no command or with `dedup` and no path, the guided flow asks for a mounted
path or `/dev/...` device. A device is matched to its mounted Btrfs path; if
there are multiple matching mounts, the user chooses one. A directory inside
a Btrfs mount resolves to its mount root, and the scan covers that mounted
subvolume. Before indexing an entry or descending into a directory, the scanner
checks the Linux mount ID of its live source path against the selected root mount,
avoiding descendant mount points even when they share a device number. Snapshot
entries whose source paths have disappeared are skipped. The guided flow
defaults to a dry run and asks for a second
confirmation before allowing dedupe changes. `--dry-run` selects dry-run mode
without the prompt.

`dedup` creates a read-only snapshot under `.btrfs-dup-snapshots` in the
mounted filesystem. Its `block_decision_function` indexes files under 10 MiB
as whole-file blocks, uses 10 MiB chunks for files from 10 MiB to under 2 GiB,
and uses 100 MiB chunks for files at least 2 GiB.
XXH3 narrows candidate blocks; BLAKE3 is computed only when blocks with the
same length and XXH3 collide. Matching `(length, XXH3, BLAKE3)` tuples are
submitted to the Btrfs extent-deduplication ioctl against the writable source
subvolume. LanceDB stores scan, block, checkpoint, and duplicate records in
one table. Since LanceDB does not support composite B-tree indexes, block rows
store a `UInt64` `hash_key` made by hashing the `(len, xxh3)` tuple and index
that scalar with a B-tree. The original `len` and XXH3 values remain stored
separately and are checked as the exact tuple; the derived key is not treated
as a unique identifier.
The table also maintains B-tree indexes on `scan` and `id` for scan-scoped
lookups and stable record upserts. When opening an existing database, the CLI
creates any missing indexes over its existing rows; it does not discard or
rewrite saved records.

`--dry-run` still creates the snapshot and persists scan progress/index data,
but does not call the deduplication ioctl. The snapshot is retained so scans
can be resumed. `resume` re-walks the immutable snapshot and uses stable
database record IDs to avoid duplicating indexed block records.
Resume skips files with persisted completion checkpoints and initializes the
progress panel from their saved byte and block counts. New block records and
file checkpoints are buffered and flushed to LanceDB in batches at least
every 30 seconds and at scan completion; an abrupt stop can require redoing
work since the latest flush.
Each worker hashes a group of up to 256 files and commits their block rows in
one `merge_insert` operation. The coordinator later persists BLAKE3 collision
upgrades, duplicate relationships, and completed-file checkpoints in periodic
batches. A file checkpoint is not written until its candidate processing is
complete.

File reads and XXH3 hashing use a bounded worker pool sized to leave one
available CPU idle when possible (up to eight workers). Candidate resolution,
BLAKE3 collision checks, and dedupe operations remain serialized to protect
the shared index.

During a scan, the CLI reports the inventory and saved-index phases, then
updates one in-place progress bar with scanned bytes, remaining bytes,
throughput in MB/s or GB/s, elapsed time, files, blocks processed, and
duplicate blocks found. It logs only XXH3 collisions that BLAKE3 distinguishes;
confirmed duplicates are counted in the progress display instead.
`list` shows per-scan counts and totals across all saved scans.
Confirmed duplicate block relationships are stored per scan, including both
relative paths and offsets, candidate length, whether the scan was a dry run,
and bytes actually reported deduplicated by Btrfs. Resume restores the
persisted duplicate count and matching-byte total.
`verify <scan-id>` rereads each recorded pair from the scan's source filesystem
and compares their bytes in bounded-memory chunks. It displays scan-style
progress, logs mismatches or read errors as anomalies, and exits unsuccessfully
if any pair differs or cannot be read.

Final chunks are indexed, but dedupe is skipped when their length is not
sector-aligned. Snapshot creation and dedupe require a Btrfs filesystem and
appropriate permissions.

`install` copies the running executable to `/usr/local/bin/btrfs-dup` and
installs a completion script for the shell detected from `$SHELL` under
`/usr/local/share`. It supports Bash, Fish, and Zsh and requires permission to
write to `/usr/local` (usually `sudo`).

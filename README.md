# btrfs-dup

A minimal Btrfs snapshot scanner and extent deduplicator. The embedded
SurrealDB store defaults to `./btrfs-dup.db`. Use the global `--db <path>`
option to choose another store; the path can appear before or after the command.

```text
cargo run --                         # guided start
cargo run -- dedup /mnt/data
cargo run -- dedup /dev/nvme0n1p2
cargo run -- --db /var/lib/btrfs-dup dedup /mnt/data
cargo run -- list --db /var/lib/btrfs-dup
cargo run -- status <scan-id>
cargo run -- resume <scan-id>
cargo run -- dedup --resume <scan-id>
sudo ./target/release/btrfs-dup install
```

With no command or with `dedup` and no path, the guided flow asks for a mounted
path or `/dev/...` device. A device is matched to its mounted Btrfs path; if
there are multiple matching mounts, the user chooses one. A directory inside
a Btrfs mount resolves to its mount root, and the scan covers that mounted
subvolume. The guided flow defaults to a dry run and asks for a second
confirmation before allowing dedupe changes. `--dry-run` selects dry-run mode
without the prompt.

`dedup` creates a read-only snapshot under `.btrfs-dup-snapshots` in the
mounted filesystem. Its `block_decision_function` indexes files under 10 MiB
as whole-file blocks, uses 10 MiB chunks for files from 10 MiB to under 2 GiB,
and uses 100 MiB chunks for files at least 2 GiB.
XXH3 narrows candidate blocks; BLAKE3 is computed only when blocks with the
same length and XXH3 collide. Matching `(length, XXH3, BLAKE3)` tuples are
submitted to the Btrfs extent-deduplication ioctl against the writable source
subvolume. Paths and digests are stored as binary values in SurrealDB.

`--dry-run` still creates the snapshot and persists scan progress/index data,
but does not call the deduplication ioctl. The snapshot is retained so scans
can be resumed. `resume` re-walks the immutable snapshot and uses stable
database record IDs to avoid duplicating indexed block records.
Resume skips files with persisted completion checkpoints and initializes the
progress panel from their saved byte and block counts. New block records and
file checkpoints are buffered and flushed to SurrealDB in batches at least
every 30 seconds and at scan completion; an abrupt stop can require redoing
work since the latest flush.
Each worker hashes a group of up to 256 files and commits their block rows in
one array-bound SurrealQL loop. The coordinator later persists BLAKE3 collision
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

Final chunks are indexed, but dedupe is skipped when their length is not
sector-aligned. Snapshot creation and dedupe require a Btrfs filesystem and
appropriate permissions.

`install` copies the running executable to `/usr/local/bin/btrfs-dup` and
installs a completion script for the shell detected from `$SHELL` under
`/usr/local/share`. It supports Bash, Fish, and Zsh and requires permission to
write to `/usr/local` (usually `sudo`).

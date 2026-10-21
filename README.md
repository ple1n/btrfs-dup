# btrfs-dup

A minimal Btrfs snapshot scanner and extent deduplicator. The embedded SQLite
database defaults to `./btrfs-dup.sqlite`. Use the global `--db <path>` option
to choose another database file; the path can appear before or after the
command. 

Build with the pinned Nix toolchain (including `libclang` and the GTK/WebKit
libraries required by the desktop GUI):

```sh
nix develop --command cargo build --release
nix build
```

Run the CLI or build without entering the development shell:

```text
cargo run --                         # guided start (inside nix develop)
cargo run -- dedup /mnt/data
cargo run -- dedup /dev/nvme0n1p2
cargo run -- --db /var/lib/btrfs-dup.sqlite dedup /mnt/data
cargo run -- list --db /var/lib/btrfs-dup.sqlite
cargo run -- status <scan-id>
cargo run -- verify <scan-id>
cargo run -- commit <scan-id>
cargo run -- finish <scan-id>
cargo run -- finish <scan-id> --delete
cargo run -- cleanup --orphans /mnt/data
cargo run -- cleanup --orphans /mnt/data --delete
cargo run -- --db /var/lib/btrfs-dup.sqlite gui
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
mounted filesystem. Its `block_decision_function` excludes files smaller than
2 MiB, indexes files from 2 MiB to under 16 MiB as whole-file blocks, uses
16 MiB chunks for files from 16 MiB to under 2 GiB, and uses 96 MiB chunks
for files at least 2 GiB. It hashes only sector-aligned block ranges, leaving
any final sub-sector tail outside the dedupe index.
XXH3 narrows candidate blocks; BLAKE3 is computed only when blocks with the
same length and XXH3 collide. Matching `(length, XXH3, BLAKE3)` tuples are
submitted to the Btrfs extent-deduplication ioctl against the writable source
subvolume. SQLite stores scan, block, file-checkpoint, and duplicate records
in one `records` table. Its primary-key B-tree on `id` supports stable upserts;
composite B-tree indexes on `(kind, scan, id)` and
`(kind, scan, len, xxh3)` support scan-scoped browsing and exact candidate-key
lookups. Opening the database creates the table and any missing indexes.
Existing LanceDB data is not migrated or read.

`--dry-run` still creates the snapshot and persists scan progress/index data,
but does not call the deduplication ioctl. The snapshot is retained so scans
can be resumed. `resume` re-walks the immutable snapshot and uses stable
database record IDs to avoid duplicating indexed block records.
Resume skips files with persisted completion checkpoints and initializes the
progress panel from their saved byte and block counts. New block records and
file checkpoints are buffered and flushed to SQLite in transactions at least
every 30 seconds and at scan completion; an abrupt stop can require redoing
work since the latest flush.
Each worker hashes a group of up to 256 files and commits their block rows in
one SQLite transaction. The coordinator later persists BLAKE3 collision
upgrades, duplicate relationships, and completed-file checkpoints in periodic
batches. A file checkpoint is not written until its candidate processing is
complete.

File reads and XXH3 hashing use a bounded worker pool sized to leave one
available CPU idle when possible (up to eight workers). BLAKE3 checks for
possible matches also run in parallel; updates to the shared candidate index
remain ordered. `commit` runs up to eight independent duplicate-pair ioctls
concurrently, while splitting each pair into sector-aligned ioctl chunks in
order.

During a scan, the CLI reports the inventory and saved-index phases, then
updates one in-place progress bar with scanned bytes, remaining bytes,
throughput in MB/s or GB/s, elapsed time, files, blocks processed, and
duplicate blocks found. It logs only XXH3 collisions that BLAKE3 distinguishes;
confirmed duplicates are counted in the progress display instead.
`list` shows per-scan counts and totals across all saved scans.
`status <scan-id>` also reports duplicate-pair counts, candidate range bytes,
and successfully deduplicated bytes, split into commited and uncommited
totals.
Confirmed duplicate block relationships are stored per scan, including both
relative paths and offsets, candidate length, whether the scan was a dry run,
whether the range has been commited, and bytes actually reported deduplicated
by Btrfs. Resume restores the persisted duplicate count and matching-byte total.
`verify <scan-id>` rereads each recorded pair from the scan's source filesystem
and compares their bytes in bounded-memory chunks. It displays scan-style
progress, logs mismatches or read errors as anomalies, and exits unsuccessfully
if any pair differs or cannot be read.
`commit <scan-id>` processes persisted, uncommited duplicate pairs even when
the scan is incomplete, against its saved source mount using Btrfs extent
deduplication. It only reads rows where `commited = false`, using a composite
SQLite B-tree index. Ranges are split into sector-aligned requests no larger
than 16 MiB, and a final sub-sector tail is left untouched. Btrfs checks each
range before sharing extents; mismatches and other anomalies are logged, and
successful rows are marked commited with their deduped byte counts. Resume
and commit can be run later for remaining scan work and duplicate pairs.
`finish <scan-id>` is allowed only for a complete scan. It removes that scan's
read-only snapshot and waits for Btrfs to finish cleaning it up, then marks the
scan finished. The scan's database records are retained by default, but the
scan can no longer be resumed. Add `--delete` to also delete all database
records belonging to that scan; this removes its status, duplicate, and index
data as well.
`cleanup --orphans <mount-path>` previews app-named snapshots under that
mount's `.btrfs-dup-snapshots` directory that have no matching scan records in
the selected database. It only considers direct, read-only Btrfs snapshots.
Add `--delete` to submit removal of all listed orphan snapshots to Btrfs. The
command reports each submitted deletion and exits without waiting for the
background cleaner; space reclamation continues asynchronously. The mount path
must be the Btrfs mount root.
`gui` opens a local Dioxus desktop database browser for the selected `--db`
store. It paginates scan and per-scan record rows; command actions are shown as
placeholders in the sidebar.

Final chunks are indexed in full; commit deduplicates their largest complete
sector-aligned prefix. Snapshot creation and dedupe require a Btrfs filesystem
and appropriate permissions.

`install` copies the running executable to `/usr/local/bin/btrfs-dup` and
installs a completion script for the shell detected from `$SHELL` under
`/usr/local/share`. It supports Bash, Fish, and Zsh and requires permission to
write to `/usr/local` (usually `sudo`).

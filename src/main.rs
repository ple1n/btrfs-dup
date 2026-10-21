use std::io::IsTerminal;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    ffi::CString,
    fs::{self, File},
    hash::Hasher,
    io::Read,
    io::{self, Write},
    os::{
        fd::{AsFd, AsRawFd, BorrowedFd},
        unix::{
            ffi::{OsStrExt, OsStringExt},
            fs::{FileTypeExt, MetadataExt, PermissionsExt},
        },
    },
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use btrfs_uapi::subvolume;
use btrfs_uapi::{
    dedupe::{DedupeResult, DedupeTarget, file_extent_same},
    filesystem::filesystem_info,
};
use rusqlite::{Connection, OpenFlags, params};
use serde::{Deserialize, Serialize};
use twox_hash::XxHash3_64;

mod gui;

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;
const MAX_DEDUPE_IOCTL_LEN: u64 = 16 * MIB;
const BLOCK_CHUNK_SIZE: usize = MAX_DEDUPE_IOCTL_LEN as usize;
const LARGE_BLOCK_CHUNK_SIZE: usize = (6 * MAX_DEDUPE_IOCTL_LEN) as usize;
const HASH_WORKER_BATCH_FILES: usize = 256;
const SNAPSHOT_DIR: &str = ".btrfs-dup-snapshots";
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockDecision {
    Exclude,
    WholeFile,
    FixedChunks(usize),
}

fn block_decision_function(file_size: u64) -> BlockDecision {
    if file_size < 2 * MIB {
        BlockDecision::Exclude
    } else if file_size < BLOCK_CHUNK_SIZE as u64 {
        BlockDecision::WholeFile
    } else if file_size < 2 * GIB {
        BlockDecision::FixedChunks(BLOCK_CHUNK_SIZE)
    } else {
        BlockDecision::FixedChunks(LARGE_BLOCK_CHUNK_SIZE)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Scan {
    scan_id: Option<String>,
    source: String,
    snapshot: String,
    state: String,
    dry_run: bool,
    files: u64,
    blocks: u64,
}

impl Scan {
    fn key(&self) -> &str {
        self.scan_id.as_deref().unwrap_or("")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Block {
    scan: String,
    xxh3: u64,
    len: usize,
    path: Vec<u8>,
    offset: u64,
    blake3: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ScannedFile {
    scan: String,
    path: Vec<u8>,
    #[serde(default)]
    bytes: u64,
    #[serde(default)]
    blocks: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Duplicate {
    scan: String,
    duplicate_path: Vec<u8>,
    duplicate_offset: u64,
    source_path: Vec<u8>,
    source_offset: u64,
    len: u64,
    deduped_bytes: u64,
    dry_run: bool,
    commited: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredRecord {
    id: String,
    kind: String,
    scan: String,
    len: u64,
    xxh3: u64,
    commited: bool,
    payload: String,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct DuplicateStats {
    pairs: u64,
    range_bytes: u64,
    deduped_bytes: u64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct ScanDuplicateStats {
    commited: DuplicateStats,
    uncommited: DuplicateStats,
}

#[derive(Clone)]
struct Store {
    connection: Arc<std::sync::Mutex<Connection>>,
}

struct WriteBatch {
    blocks: HashMap<String, Block>,
    files: HashMap<String, ScannedFile>,
    duplicates: HashMap<String, Duplicate>,
    collision_candidates: HashMap<String, Block>,
    last_flush: Instant,
}

impl WriteBatch {
    fn new() -> Self {
        Self {
            blocks: HashMap::new(),
            files: HashMap::new(),
            duplicates: HashMap::new(),
            collision_candidates: HashMap::new(),
            last_flush: Instant::now(),
        }
    }

    async fn flush(&mut self, store: &Store, scan: &Scan) -> Result<()> {
        if self.blocks.is_empty() && self.files.is_empty() && self.duplicates.is_empty() {
            self.last_flush = Instant::now();
            return Ok(());
        }
        let blocks = std::mem::take(&mut self.blocks);
        let files = std::mem::take(&mut self.files);
        let duplicates = std::mem::take(&mut self.duplicates);
        let mut records = blocks
            .iter()
            .map(|(id, block)| {
                stored_record(
                    id,
                    "block",
                    &block.scan,
                    block.len as u64,
                    &block.xxh3.to_be_bytes(),
                    block,
                )
            })
            .collect::<Result<Vec<_>>>()
            .map_err(|error| error.to_string())?;
        records.extend(
            files
                .iter()
                .map(|(id, file)| stored_record(id, "file", &file.scan, 0, &[], file))
                .collect::<Result<Vec<_>>>()?,
        );
        records.extend(
            duplicates
                .iter()
                .map(|(id, duplicate)| stored_duplicate_record(id, duplicate))
                .collect::<Result<Vec<_>>>()?,
        );
        records.push(stored_record(
            &scan_record_id(scan.key()),
            "scan",
            scan.key(),
            0,
            &[],
            scan,
        )?);
        if let Err(error) = write_records_off_thread(store, records).await {
            self.blocks.extend(blocks);
            self.files.extend(files);
            self.duplicates.extend(duplicates);
            return Err(error);
        }
        for (id, block) in blocks {
            if block.blake3.is_none() {
                self.collision_candidates.insert(id, block);
            }
        }
        self.last_flush = Instant::now();
        Ok(())
    }
}

#[derive(Default)]
struct Inventory {
    files: u64,
    blocks: u64,
    bytes: u64,
    last_draw: Option<Instant>,
    paths: Vec<(PathBuf, PathBuf)>,
}

#[derive(Clone)]
struct HashedBlock {
    offset: u64,
    len: usize,
    xxh3: u64,
}

struct HashedFile {
    path: PathBuf,
    relative: PathBuf,
    size: u64,
    blocks: Vec<HashedBlock>,
}

struct ScanProgress {
    total_files: u64,
    total_blocks: u64,
    total_bytes: u64,
    files: u64,
    blocks: u64,
    bytes: u64,
    duplicates: u64,
    duplicate_bytes: u64,
    last_draw: Instant,
    started_at: Instant,
    last_rate_sample_at: Instant,
    rate_samples: VecDeque<(Instant, u64)>,
    bytes_per_second: f64,
    panel_drawn: bool,
    mode: ProgressMode,
    successful_pairs: u64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ProgressMode {
    Scan,
    Verify,
    Commit,
}

impl ScanProgress {
    fn new(
        inventory: &Inventory,
        completed_files: u64,
        completed_blocks: u64,
        completed_bytes: u64,
        duplicates: u64,
        duplicate_bytes: u64,
        mode: ProgressMode,
    ) -> Self {
        let now = Instant::now();
        Self {
            total_files: inventory.files,
            total_blocks: inventory.blocks,
            total_bytes: inventory.bytes,
            files: completed_files,
            blocks: completed_blocks,
            bytes: completed_bytes,
            duplicates,
            duplicate_bytes,
            last_draw: now - Duration::from_secs(1),
            started_at: now,
            last_rate_sample_at: now,
            rate_samples: VecDeque::from([(now, completed_bytes)]),
            bytes_per_second: 0.0,
            panel_drawn: false,
            mode,
            successful_pairs: 0,
        }
    }

    fn draw(&mut self, force: bool) -> Result<()> {
        let now = Instant::now();
        let since_last_draw = now.duration_since(self.last_draw);
        let sample_elapsed = now.duration_since(self.last_rate_sample_at).as_secs_f64();
        if sample_elapsed >= 0.5 {
            self.last_rate_sample_at = now;
            self.rate_samples.push_back((now, self.bytes));
            while self
                .rate_samples
                .front()
                .is_some_and(|(at, _)| now.duration_since(*at) > Duration::from_secs(5))
                && self.rate_samples.len() > 1
            {
                self.rate_samples.pop_front();
            }
            if let Some((start_at, start_bytes)) = self.rate_samples.front() {
                let elapsed = now.duration_since(*start_at).as_secs_f64();
                if elapsed > 0.0 {
                    self.bytes_per_second =
                        self.bytes.saturating_sub(*start_bytes) as f64 / elapsed;
                }
            }
        }
        if !force && self.last_draw.elapsed() < Duration::from_millis(150) {
            return Ok(());
        }
        self.last_draw = now;
        let fraction = if self.mode == ProgressMode::Verify {
            if self.total_files == 0 || self.files == self.total_files {
                1.0
            } else {
                (self.bytes as f64 / self.total_bytes.max(1) as f64).min(1.0)
            }
        } else if self.mode == ProgressMode::Commit {
            if self.total_files == 0 {
                1.0
            } else {
                (self.files as f64 / self.total_files as f64).min(1.0)
            }
        } else if self.total_bytes == 0 {
            1.0
        } else {
            (self.bytes as f64 / self.total_bytes as f64).min(1.0)
        };
        let percent = (fraction * 100.0) as usize;
        let width = 28;
        let filled = width * percent / 100;
        let bar = format!("{}{}", "=".repeat(filled), " ".repeat(width - filled));
        let remaining_bytes = self.total_bytes.saturating_sub(self.bytes);
        let rate = format_byte_rate(self.bytes_per_second);
        let elapsed = format_duration(now.duration_since(self.started_at));
        let last_line = if self.mode == ProgressMode::Verify {
            format!(
                "Pairs: {}/{} | Blocks: {}/{} | Verified: {} | Anomalies: {}",
                self.files,
                self.total_files,
                self.blocks,
                self.total_blocks,
                self.blocks.saturating_sub(self.duplicates),
                self.duplicates
            )
        } else if self.mode == ProgressMode::Commit {
            format!(
                "Pairs: {}/{} | Blocks: {}/{} | Deduped: {} ({}) | Anomalies: {}",
                self.files,
                self.total_files,
                self.blocks,
                self.total_blocks,
                self.successful_pairs,
                format_bytes(self.duplicate_bytes),
                self.duplicates
            )
        } else {
            format!(
                "Files: {}/{} | Blocks: {}/{} | Found dups: {} ({})",
                self.files,
                self.total_files,
                self.blocks,
                self.total_blocks,
                self.duplicates,
                format_bytes(self.duplicate_bytes)
            )
        };
        let lines = [
            format!(
                "{} [{bar}] {percent:3}%",
                match self.mode {
                    ProgressMode::Scan => "Scan",
                    ProgressMode::Verify => "Verify",
                    ProgressMode::Commit => "Commit",
                }
            ),
            format!(
                "{}: {} / {} | {} remaining | {} rate {} | elapsed {elapsed}",
                match self.mode {
                    ProgressMode::Scan => "Bytes read",
                    ProgressMode::Verify => "Bytes compared",
                    ProgressMode::Commit => "Bytes processed",
                },
                format_bytes(self.bytes),
                format_bytes(self.total_bytes),
                if self.mode != ProgressMode::Scan {
                    format!("{} pairs", self.total_files.saturating_sub(self.files))
                } else {
                    format_bytes(remaining_bytes)
                },
                match self.mode {
                    ProgressMode::Scan => "read",
                    ProgressMode::Verify => "compare",
                    ProgressMode::Commit => "commit",
                },
                rate,
            ),
            last_line,
        ];
        if io::stderr().is_terminal() {
            if self.panel_drawn {
                eprint!("\x1b[2A");
            }
            for line in lines {
                eprint!("\r\x1b[2K{line}\n");
            }
            eprint!("\x1b[1A");
            self.panel_drawn = true;
            io::stderr().flush()?;
            if force {
                eprintln!();
                self.panel_drawn = false;
            }
        } else if force || since_last_draw >= Duration::from_secs(5) {
            eprintln!("{}", lines.join("\n"));
        }
        Ok(())
    }

    fn log(&mut self, message: String) -> Result<()> {
        if self.panel_drawn {
            eprint!("\x1b[2A");
            for _ in 0..3 {
                eprint!("\r\x1b[2K\n");
            }
            self.panel_drawn = false;
        }
        eprintln!("{message}");
        self.last_draw = Instant::now() - Duration::from_secs(1);
        self.draw(false)?;
        Ok(())
    }
}

fn format_bytes(bytes: u64) -> String {
    const MB: f64 = 1_000_000.0;
    const GB: f64 = 1_000_000_000.0;
    if bytes >= 1_000_000_000 {
        format!("{:.2} GB", bytes as f64 / GB)
    } else if bytes >= 1_000_000 {
        format!("{:.2} MB", bytes as f64 / MB)
    } else if bytes >= 1_000 {
        format!("{:.2} kB", bytes as f64 / 1_000.0)
    } else {
        format!("{bytes} B")
    }
}

fn format_byte_rate(bytes_per_second: f64) -> String {
    if bytes_per_second >= 1_000_000_000.0 {
        format!("{:.2} GB/s", bytes_per_second / 1_000_000_000.0)
    } else if bytes_per_second >= 1_000_000.0 {
        format!("{:.2} MB/s", bytes_per_second / 1_000_000.0)
    } else if bytes_per_second >= 1_000.0 {
        format!("{:.2} kB/s", bytes_per_second / 1_000.0)
    } else {
        format!("{:.0} B/s", bytes_per_second)
    }
}

fn format_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();
    format!(
        "{:02}:{:02}:{:02}",
        seconds / 3600,
        (seconds / 60) % 60,
        seconds % 60
    )
}

fn stored_record<T: Serialize>(
    id: &str,
    kind: &str,
    scan: &str,
    len: u64,
    hash: &[u8],
    payload: &T,
) -> Result<StoredRecord> {
    let xxh3 = match hash.len() {
        0 => 0,
        8 => u64::from_be_bytes(
            hash.try_into()
                .map_err(|_| "XXH3 hash must contain exactly 8 bytes")?,
        ),
        _ => return Err("XXH3 hash must contain exactly 8 bytes".into()),
    };
    Ok(StoredRecord {
        id: id.to_owned(),
        kind: kind.to_owned(),
        scan: scan.to_owned(),
        len,
        xxh3,
        commited: false,
        payload: serde_json::to_string(payload)?,
    })
}

fn stored_duplicate_record(id: &str, duplicate: &Duplicate) -> Result<StoredRecord> {
    let mut record = stored_record(id, "duplicate", &duplicate.scan, 0, &[], duplicate)?;
    record.commited = duplicate.commited;
    record.len = duplicate.len;
    Ok(record)
}

fn decode_record<T: for<'de> Deserialize<'de>>(record: &StoredRecord) -> Result<T> {
    Ok(serde_json::from_str(&record.payload)?)
}

fn write_records(store: &Store, records: Vec<StoredRecord>) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }
    let mut unique_records = HashMap::with_capacity(records.len());
    for record in records {
        if let Some(existing) = unique_records.get(&record.id) {
            if existing != &record {
                return Err(
                    format!("conflicting database rows share record id {}", record.id).into(),
                );
            }
        } else {
            unique_records.insert(record.id.clone(), record);
        }
    }
    let records = unique_records.into_values().collect::<Vec<_>>();
    let mut connection = store
        .connection
        .lock()
        .map_err(|_| "SQLite connection lock was poisoned")?;
    let transaction = connection.transaction()?;
    {
        let mut statement = transaction.prepare(
            "INSERT INTO records (id, kind, scan, len, xxh3, commited, payload)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(id) DO UPDATE SET
                 kind = excluded.kind,
                 scan = excluded.scan,
                 len = excluded.len,
                 xxh3 = excluded.xxh3,
                 commited = excluded.commited,
                 payload = excluded.payload",
        )?;
        for record in records {
            let len = i64::try_from(record.len).map_err(|_| {
                format!("record {} length exceeds SQLite's integer range", record.id)
            })?;
            statement.execute(params![
                record.id,
                record.kind,
                record.scan,
                len,
                record.xxh3 as i64,
                record.commited,
                record.payload
            ])?;
        }
    }
    transaction.commit()?;
    Ok(())
}

async fn write_records_off_thread(store: &Store, records: Vec<StoredRecord>) -> Result<()> {
    let store = store.clone();
    tokio::task::spawn_blocking(move || {
        write_records(&store, records).map_err(|error| error.to_string())
    })
    .await?
    .map_err(std::io::Error::other)?;
    Ok(())
}

fn collect_record_rows(mut rows: rusqlite::Rows<'_>) -> Result<Vec<StoredRecord>> {
    let mut records = Vec::new();
    while let Some(row) = rows.next()? {
        let len: i64 = row.get(3)?;
        if len < 0 {
            return Err("database record has a negative length".into());
        }
        records.push(StoredRecord {
            id: row.get(0)?,
            kind: row.get(1)?,
            scan: row.get(2)?,
            len: len as u64,
            xxh3: row.get::<_, i64>(4)? as u64,
            commited: row.get(5)?,
            payload: row.get(6)?,
        });
    }
    Ok(records)
}

fn load_records(store: &Store, kind: &str, scan: Option<&str>) -> Result<Vec<StoredRecord>> {
    let connection = store
        .connection
        .lock()
        .map_err(|_| "SQLite connection lock was poisoned")?;
    let sql = match scan {
        Some(_) => {
            "SELECT id, kind, scan, len, xxh3, commited, payload
             FROM records WHERE kind = ?1 AND scan = ?2 ORDER BY id"
        }
        None => {
            "SELECT id, kind, scan, len, xxh3, commited, payload
             FROM records WHERE kind = ?1 ORDER BY id"
        }
    };
    let mut statement = connection.prepare(sql)?;
    let rows = match scan {
        Some(scan) => statement.query(params![kind, scan])?,
        None => statement.query(params![kind])?,
    };
    collect_record_rows(rows)
}

fn load_scan(store: &Store, scan_id: &str) -> Result<Scan> {
    let record = load_records(store, "scan", Some(scan_id))?
        .into_iter()
        .find(|record| record.id == scan_record_id(scan_id))
        .ok_or("scan not found")?;
    decode_record(&record)
}

fn ensure_scan_resumable(scan: &Scan) -> Result<()> {
    if scan.state == "finished" {
        return Err(format!(
            "scan {} has been finished; its snapshot is no longer available for resume",
            scan.key()
        )
        .into());
    }
    if !Path::new(&scan.snapshot).exists() {
        return Err(format!(
            "scan {} cannot be resumed because its snapshot is missing",
            scan.key()
        )
        .into());
    }
    Ok(())
}

fn finish_scan(store: &Store, mut scan: Scan, delete_data: bool) -> Result<()> {
    if scan.state != "complete" && scan.state != "finished" {
        return Err(format!(
            "scan {} is {}; only complete scans can be finished",
            scan.key(),
            scan.state
        )
        .into());
    }

    let source = fs::canonicalize(&scan.source)?;
    if resolve_btrfs_mount(&source)? != source {
        return Err("scan source is no longer the mounted Btrfs root".into());
    }
    let snapshot_parent = source.join(SNAPSHOT_DIR);
    let snapshot_name = format!(".btrfs-dup-{}", scan.key());
    let expected_snapshot = snapshot_parent.join(&snapshot_name);
    let snapshot = PathBuf::from(&scan.snapshot);
    if snapshot != expected_snapshot {
        return Err("refusing to finish: saved snapshot path does not match this scan".into());
    }

    let root_fd = File::open(&source)?;
    let parent_fd = File::open(&snapshot_parent)?;
    match fs::symlink_metadata(&snapshot) {
        Ok(metadata) => {
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                return Err(
                    "refusing to finish: scan snapshot is not a directory subvolume".into(),
                );
            }
            let snapshot_fd = File::open(&snapshot)?;
            let snapshot_info = subvolume::subvolume_info(snapshot_fd.as_fd())?;
            let parent_info = subvolume::subvolume_info(parent_fd.as_fd())?;
            if snapshot_info.id == parent_info.id {
                return Err("refusing to finish: target is not a separate Btrfs subvolume".into());
            }
            let snapshot_flags = subvolume::subvolume_flags_get(snapshot_fd.as_fd())?;
            if !snapshot_flags.contains(subvolume::SubvolumeFlags::RDONLY) {
                return Err(format!(
                    "refusing to finish: scan subvolume {} is not read-only (reported flags: {snapshot_flags:?})",
                    snapshot.display()
                )
                .into());
            }
            drop(snapshot_fd);
            let snapshot_name = CString::new(snapshot_name)?;
            subvolume::subvolume_delete(parent_fd.as_fd(), &snapshot_name)?;
            subvolume::subvol_sync_wait_one(root_fd.as_fd(), snapshot_info.id)?;
            println!(
                "Deleted scan snapshot and waited for Btrfs cleanup to finish: {}",
                snapshot.display()
            );
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            println!("Scan snapshot is already absent: {}", snapshot.display());
        }
        Err(error) => return Err(error.into()),
    }

    if delete_data {
        let deleted = delete_scan_records(store, scan.key())?;
        println!(
            "Deleted {deleted} database records for scan {}.",
            scan.key()
        );
    } else {
        scan.state = "finished".into();
        write_scan(store, &scan)?;
        println!(
            "Scan {} is finished. Its database records were retained; it can no longer be resumed.",
            scan.key()
        );
    }
    Ok(())
}

fn delete_scan_records(store: &Store, scan_id: &str) -> Result<usize> {
    let mut connection = store
        .connection
        .lock()
        .map_err(|_| "SQLite connection lock was poisoned")?;
    let transaction = connection.transaction()?;
    let deleted = transaction.execute("DELETE FROM records WHERE scan = ?1", params![scan_id])?;
    transaction.commit()?;
    Ok(deleted)
}

fn load_record_scan_ids(store: &Store) -> Result<HashSet<String>> {
    let connection = store
        .connection
        .lock()
        .map_err(|_| "SQLite connection lock was poisoned")?;
    let mut statement = connection.prepare("SELECT DISTINCT scan FROM records")?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    rows.collect::<std::result::Result<HashSet<_>, _>>()
        .map_err(Into::into)
}

fn cleanup_orphan_snapshots(store: &Store, mount_path: &Path, delete: bool) -> Result<()> {
    let requested_mount = fs::canonicalize(mount_path)?;
    let source = resolve_btrfs_mount(&requested_mount)?;
    let source = fs::canonicalize(source)?;
    if requested_mount != source {
        return Err(format!(
            "provide the mounted Btrfs root, not a subdirectory: {}",
            requested_mount.display()
        )
        .into());
    }

    let snapshot_dir = source.join(SNAPSHOT_DIR);
    let snapshot_dir_metadata = match fs::symlink_metadata(&snapshot_dir) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            println!(
                "No btrfs-dup snapshot directory exists at {}.",
                snapshot_dir.display()
            );
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };
    if !snapshot_dir_metadata.is_dir() || snapshot_dir_metadata.file_type().is_symlink() {
        return Err(format!(
            "refusing to inspect non-directory snapshot path {}",
            snapshot_dir.display()
        )
        .into());
    }
    let parent_fd = match File::open(&snapshot_dir) {
        Ok(fd) => fd,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            println!(
                "No btrfs-dup snapshot directory exists at {}.",
                snapshot_dir.display()
            );
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };
    let parent_info = subvolume::subvolume_info(parent_fd.as_fd())?;
    let known_scan_ids = load_record_scan_ids(store)?;
    let mut candidates = Vec::new();
    let mut deleted_count = 0usize;
    let mut failures = 0usize;

    for entry in fs::read_dir(&snapshot_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(scan_id) = name.strip_prefix(".btrfs-dup-") else {
            continue;
        };
        if scan_id.is_empty() || known_scan_ids.contains(scan_id) {
            continue;
        }

        let snapshot_path = entry.path();
        let metadata = match fs::symlink_metadata(&snapshot_path) {
            Ok(metadata) => metadata,
            Err(error) => {
                failures += 1;
                eprintln!(
                    "Could not inspect possible orphan {}: {error}",
                    snapshot_path.display()
                );
                continue;
            }
        };
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            failures += 1;
            eprintln!(
                "Refusing to process non-directory snapshot candidate {}.",
                snapshot_path.display()
            );
            continue;
        }

        let snapshot_fd = match File::open(&snapshot_path) {
            Ok(fd) => fd,
            Err(error) => {
                failures += 1;
                eprintln!(
                    "Could not open possible orphan {}: {error}",
                    snapshot_path.display()
                );
                continue;
            }
        };
        let info = match subvolume::subvolume_info(snapshot_fd.as_fd()) {
            Ok(info) => info,
            Err(error) => {
                failures += 1;
                eprintln!(
                    "Could not verify possible orphan {} as a Btrfs subvolume: {error}",
                    snapshot_path.display()
                );
                continue;
            }
        };
        let flags = match subvolume::subvolume_flags_get(snapshot_fd.as_fd()) {
            Ok(flags) => flags,
            Err(error) => {
                failures += 1;
                eprintln!(
                    "Could not read flags for possible orphan {}: {error}",
                    snapshot_path.display()
                );
                continue;
            }
        };
        if info.id == parent_info.id
            || info.parent_id != parent_info.id
            || !flags.contains(subvolume::SubvolumeFlags::RDONLY)
        {
            failures += 1;
            eprintln!(
                "Refusing to process {}: it is not a direct read-only snapshot of this directory (id={}, parent_id={}, flags={flags:?}).",
                snapshot_path.display(),
                info.id,
                info.parent_id
            );
            continue;
        }

        drop(snapshot_fd);
        candidates.push((name.to_owned(), snapshot_path, info.id));
    }

    if delete {
        println!(
            "Found {} orphan snapshots to delete; submitting all removals to Btrfs without waiting for background extent cleanup.",
            candidates.len()
        );
        for (index, (name, snapshot_path, subvolume_id)) in candidates.iter().enumerate() {
            println!(
                "Submitting deletion {}/{}: {} (subvolume id {})",
                index + 1,
                candidates.len(),
                snapshot_path.display(),
                subvolume_id
            );
            io::stdout().flush()?;
            let subvolume_name = CString::new(name.as_str())?;
            if let Err(error) = subvolume::subvolume_delete(parent_fd.as_fd(), &subvolume_name) {
                failures += 1;
                eprintln!(
                    "Could not delete orphan snapshot {}: {error}",
                    snapshot_path.display()
                );
                continue;
            }
            deleted_count += 1;
            println!(
                "Deletion submitted for snapshot {}/{}: {}; Btrfs reclaims extents asynchronously.",
                index + 1,
                candidates.len(),
                snapshot_path.display()
            );
        }
    } else {
        for (_, snapshot_path, subvolume_id) in &candidates {
            println!(
                "Orphan snapshot candidate: {} (subvolume id {})",
                snapshot_path.display(),
                subvolume_id
            );
        }
    }

    if delete {
        println!(
            "Orphan cleanup: submitted={deleted_count}, failed={failures}. Btrfs may continue reclaiming space in the background."
        );
    } else {
        println!(
            "Orphan cleanup preview: candidates={}, failed={failures}. Rerun with --delete to remove candidates.",
            candidates.len()
        );
    }
    if failures > 0 {
        return Err(format!("orphan cleanup encountered {failures} errors").into());
    }
    Ok(())
}

fn load_pending_duplicates(store: &Store, scan_id: &str) -> Result<Vec<Duplicate>> {
    let connection = store
        .connection
        .lock()
        .map_err(|_| "SQLite connection lock was poisoned")?;
    let mut statement = connection.prepare(
        "SELECT id, kind, scan, len, xxh3, commited, payload
         FROM records
         WHERE kind = 'duplicate' AND scan = ?1 AND commited = 0
         ORDER BY id",
    )?;
    collect_record_rows(statement.query(params![scan_id])?)?
        .iter()
        .map(decode_record::<Duplicate>)
        .collect()
}

fn load_duplicate_stats(store: &Store, scan_id: &str) -> Result<ScanDuplicateStats> {
    let connection = store
        .connection
        .lock()
        .map_err(|_| "SQLite connection lock was poisoned")?;
    let mut statement = connection.prepare(
        "SELECT commited, COUNT(*),
                COALESCE(SUM(CAST(json_extract(payload, '$.len') AS INTEGER)), 0),
                COALESCE(SUM(CAST(json_extract(payload, '$.deduped_bytes') AS INTEGER)), 0)
         FROM records
         WHERE kind = 'duplicate' AND scan = ?1
         GROUP BY commited",
    )?;
    let mut rows = statement.query(params![scan_id])?;
    let mut stats = ScanDuplicateStats::default();
    while let Some(row) = rows.next()? {
        let duplicate_stats = DuplicateStats {
            pairs: u64::try_from(row.get::<_, i64>(1)?)
                .map_err(|_| "database returned a negative duplicate count")?,
            range_bytes: u64::try_from(row.get::<_, i64>(2)?)
                .map_err(|_| "database returned a negative duplicate range total")?,
            deduped_bytes: u64::try_from(row.get::<_, i64>(3)?)
                .map_err(|_| "database returned a negative deduped byte total")?,
        };
        if row.get::<_, bool>(0)? {
            stats.commited = duplicate_stats;
        } else {
            stats.uncommited = duplicate_stats;
        }
    }
    Ok(stats)
}

fn write_scan(store: &Store, scan: &Scan) -> Result<()> {
    write_records(
        store,
        vec![stored_record(
            &scan_record_id(scan.key()),
            "scan",
            scan.key(),
            0,
            &[],
            scan,
        )?],
    )
}

fn open_store(path: &Path) -> Result<Store> {
    let connection = Connection::open(path)?;
    connection.busy_timeout(Duration::from_secs(30))?;
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS records (
             id TEXT PRIMARY KEY NOT NULL,
             kind TEXT NOT NULL,
             scan TEXT NOT NULL,
             len INTEGER NOT NULL CHECK (len >= 0),
             xxh3 INTEGER NOT NULL,
             commited INTEGER NOT NULL DEFAULT 0 CHECK (commited IN (0, 1)),
             payload TEXT NOT NULL
         ) WITHOUT ROWID;
         CREATE INDEX IF NOT EXISTS records_kind_scan_id
             ON records(kind, scan, id);
         CREATE INDEX IF NOT EXISTS records_block_match
             ON records(kind, scan, len, xxh3);
         CREATE INDEX IF NOT EXISTS records_pending_duplicates
             ON records(kind, scan, commited, id);",
    )?;
    Ok(Store {
        connection: Arc::new(std::sync::Mutex::new(connection)),
    })
}

fn load_record_page(
    path: &Path,
    scan: Option<&str>,
    kind: &str,
    limit: usize,
    offset: usize,
) -> Result<Vec<StoredRecord>> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    connection.busy_timeout(Duration::from_secs(30))?;
    let has_records: bool = connection.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'records'
         )",
        [],
        |row| row.get(0),
    )?;
    if !has_records {
        return Err("database does not contain a records table".into());
    }
    let limit = i64::try_from(limit)?;
    let offset = i64::try_from(offset)?;
    let mut statement = match (scan, kind) {
        (Some(_), "all") => connection.prepare(
            "SELECT id, kind, scan, len, xxh3, commited, payload
             FROM records WHERE scan = ?1 ORDER BY id LIMIT ?2 OFFSET ?3",
        )?,
        (Some(_), _) => connection.prepare(
            "SELECT id, kind, scan, len, xxh3, commited, payload
             FROM records WHERE scan = ?1 AND kind = ?2 ORDER BY id LIMIT ?3 OFFSET ?4",
        )?,
        (None, _) => connection.prepare(
            "SELECT id, kind, scan, len, xxh3, commited, payload
             FROM records WHERE kind = 'scan' ORDER BY id LIMIT ?1 OFFSET ?2",
        )?,
    };
    let rows = match (scan, kind) {
        (Some(scan), "all") => statement.query(params![scan, limit, offset])?,
        (Some(scan), _) => statement.query(params![scan, kind, limit, offset])?,
        (None, _) => statement.query(params![limit, offset])?,
    };
    collect_record_rows(rows)
}

fn main() -> Result<()> {
    let mut arguments = std::env::args_os().skip(1).collect::<Vec<_>>();
    let db_path = take_db_path(&mut arguments)?;
    let command = arguments.first().cloned().unwrap_or_else(|| "dedup".into());
    if command == "verify" || command == "commit" {
        let scan_id = arguments
            .get(1)
            .map(|id| id.to_string_lossy())
            .unwrap_or_else(|| "<missing scan id>".into());
        eprintln!(
            "Starting {} for scan {scan_id}; opening database...",
            if command == "verify" {
                "verification"
            } else {
                "commit"
            }
        );
    }
    if command == "install" {
        if arguments.len() > 1 {
            return Err("usage: btrfs-dup install".into());
        }
        return install();
    }
    if command == "gui" {
        if arguments.len() > 1 {
            return Err("usage: btrfs-dup [--db <path>] gui".into());
        }
        return gui::launch(db_path);
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run_cli(arguments, db_path))
}

async fn run_cli(arguments: Vec<std::ffi::OsString>, db_path: PathBuf) -> Result<()> {
    let command = arguments.first().cloned().unwrap_or_else(|| "dedup".into());
    let mut args = arguments.into_iter().skip(1);
    let store = open_store(&db_path)?;
    if command == "verify" || command == "commit" {
        eprintln!("Database opened; loading scan records...");
    }
    match command.to_string_lossy().as_ref() {
        "dedup" => {
            let remaining = args.collect::<Vec<_>>();
            let first = remaining.first().cloned();
            if first.as_deref() == Some(std::ffi::OsStr::new("--resume")) {
                let id = remaining
                    .get(1)
                    .cloned()
                    .ok_or("missing scan id after --resume")?;
                let id = id.to_string_lossy().into_owned();
                let mut scan = load_scan(&store, &id)?;
                ensure_scan_resumable(&scan)?;
                scan.scan_id = Some(id.clone());
                scan.state = "running".into();
                write_scan(&store, &scan)?;
                return run_scan(&store, scan).await;
            }
            let dry_run_flag = remaining.iter().any(|arg| arg == "--dry-run");
            let input = remaining
                .into_iter()
                .find(|arg| arg != "--dry-run")
                .map(PathBuf::from);
            let input = match input {
                Some(path) => path,
                None => PathBuf::from(prompt("Enter a mounted path or Btrfs device (/dev/...): ")?),
            };
            let source = resolve_btrfs_mount(&input)?;
            println!(
                "Step 1/3: Found mounted Btrfs filesystem at {}",
                source.display()
            );
            println!("Step 2/3: A read-only snapshot will be created there.");
            let dry_run = if dry_run_flag {
                true
            } else {
                prompt_yes_no("Start with a dry run? [Y/n] ", true)?
            };
            if !dry_run
                && !prompt_yes_no("Allow dedupe changes to the mounted files? [y/N] ", false)?
            {
                println!("Cancelled; no snapshot or scan was created.");
                return Ok(());
            }
            println!(
                "Step 3/3: Starting {} scan.",
                if dry_run { "dry-run" } else { "deduplication" }
            );
            let scan = create_scan(&store, &source, dry_run).await?;
            run_scan(&store, scan).await
        }
        "resume" => {
            let id = args
                .next()
                .ok_or("usage: btrfs_dup resume <scan-id>")?
                .to_string_lossy()
                .into_owned();
            let mut scan = load_scan(&store, &id)?;
            ensure_scan_resumable(&scan)?;
            scan.scan_id = Some(id.clone());
            scan.state = "running".into();
            write_scan(&store, &scan)?;
            run_scan(&store, scan).await
        }
        "list" => {
            let scans = load_records(&store, "scan", None)?
                .iter()
                .map(decode_record::<Scan>)
                .collect::<Result<Vec<_>>>()?;
            let duplicates = load_records(&store, "duplicate", None)?
                .iter()
                .map(decode_record::<Duplicate>)
                .collect::<Result<Vec<_>>>()?;
            let mut duplicate_totals: HashMap<String, (u64, u64)> = HashMap::new();
            for duplicate in duplicates {
                let totals = duplicate_totals.entry(duplicate.scan).or_default();
                totals.0 += 1;
                totals.1 += duplicate.len;
            }
            let mut total_files = 0;
            let mut total_blocks = 0;
            let mut total_duplicates = 0;
            let mut total_duplicate_bytes = 0;
            for s in scans {
                let id = s.key().to_owned();
                let (duplicates, duplicate_bytes) =
                    duplicate_totals.get(&id).copied().unwrap_or_default();
                total_files += s.files;
                total_blocks += s.blocks;
                total_duplicates += duplicates;
                total_duplicate_bytes += duplicate_bytes;
                println!(
                    "{}\t{}\t{}\tfiles={} blocks={} dups={} ({}) dry-run={}",
                    id,
                    s.state,
                    s.source,
                    s.files,
                    s.blocks,
                    duplicates,
                    format_bytes(duplicate_bytes),
                    s.dry_run
                );
            }
            println!(
                "Totals across saved scans: files={}, blocks={}, dups={} ({})",
                total_files,
                total_blocks,
                total_duplicates,
                format_bytes(total_duplicate_bytes)
            );
            Ok(())
        }
        "status" => {
            let id = args
                .next()
                .ok_or("usage: btrfs_dup status <scan-id>")?
                .to_string_lossy()
                .into_owned();
            let s = load_scan(&store, &id)?;
            println!("{s:#?}");
            let stats = load_duplicate_stats(&store, &id)?;
            let total_pairs = stats.commited.pairs.saturating_add(stats.uncommited.pairs);
            let total_range_bytes = stats
                .commited
                .range_bytes
                .saturating_add(stats.uncommited.range_bytes);
            let total_deduped_bytes = stats
                .commited
                .deduped_bytes
                .saturating_add(stats.uncommited.deduped_bytes);
            println!("Duplicate stats:");
            println!(
                "  Total: {} pairs, {} candidate, {} deduped",
                total_pairs,
                format_bytes(total_range_bytes),
                format_bytes(total_deduped_bytes)
            );
            println!(
                "  Commited: {} pairs, {} candidate, {} deduped",
                stats.commited.pairs,
                format_bytes(stats.commited.range_bytes),
                format_bytes(stats.commited.deduped_bytes)
            );
            println!(
                "  Not commited: {} pairs, {} candidate, {} deduped",
                stats.uncommited.pairs,
                format_bytes(stats.uncommited.range_bytes),
                format_bytes(stats.uncommited.deduped_bytes)
            );
            Ok(())
        }
        "verify" => {
            let id = args
                .next()
                .ok_or("usage: btrfs-dup verify <scan-id>")?
                .to_string_lossy()
                .into_owned();
            if args.next().is_some() {
                return Err("usage: btrfs-dup verify <scan-id>".into());
            }
            let scan = load_scan(&store, &id)?;
            eprintln!("Loading duplicate relationships for scan {id}...");
            let duplicates = load_records(&store, "duplicate", Some(&id))?
                .iter()
                .map(decode_record::<Duplicate>)
                .collect::<Result<Vec<_>>>()?;
            verify_duplicates(&scan, &duplicates)
        }
        "commit" => {
            let id = args
                .next()
                .ok_or("usage: btrfs-dup commit <scan-id>")?
                .to_string_lossy()
                .into_owned();
            if args.next().is_some() {
                return Err("usage: btrfs-dup commit <scan-id>".into());
            }
            let scan = load_scan(&store, &id)?;
            eprintln!("Loading uncommited duplicate relationships for scan {id}...");
            let duplicates = load_pending_duplicates(&store, &id)?;
            commit_duplicates(&store, &scan, &duplicates).await
        }
        "finish" => {
            let arguments = args.collect::<Vec<_>>();
            let mut delete_data = false;
            let mut scan_id = None;
            for argument in arguments {
                match argument.to_str() {
                    Some("--delete") if !delete_data => delete_data = true,
                    Some(value) if value.starts_with("--") => {
                        return Err(format!("unknown or repeated finish option: {value}").into());
                    }
                    Some(value) if scan_id.is_none() => scan_id = Some(value.to_owned()),
                    _ => return Err("usage: btrfs-dup finish <scan-id> [--delete]".into()),
                }
            }
            let id = scan_id.ok_or("usage: btrfs-dup finish <scan-id> [--delete]")?;
            let scan = load_scan(&store, &id)?;
            finish_scan(&store, scan, delete_data)
        }
        "cleanup" => {
            if args.next().as_deref() != Some(std::ffi::OsStr::new("--orphans")) {
                return Err("usage: btrfs-dup cleanup --orphans <mount-path> [--delete]".into());
            }
            let mount_path = PathBuf::from(
                args.next()
                    .ok_or("usage: btrfs-dup cleanup --orphans <mount-path> [--delete]")?,
            );
            let delete = match args.next() {
                Some(flag) if flag == "--delete" => true,
                Some(_) => {
                    return Err("usage: btrfs-dup cleanup --orphans <mount-path> [--delete]".into());
                }
                None => false,
            };
            if args.next().is_some() {
                return Err("usage: btrfs-dup cleanup --orphans <mount-path> [--delete]".into());
            }
            cleanup_orphan_snapshots(&store, &mount_path, delete)
        }
        _ => usage(),
    }
}

fn usage<T>() -> Result<T> {
    Err("usage: btrfs-dup [--db <path>] [dedup [<mount-path|/dev/device>] [--dry-run] | dedup --resume <scan-id> | list | status <scan-id> | verify <scan-id> | commit <scan-id> | finish <scan-id> [--delete] | cleanup --orphans <mount-path> [--delete] | resume <scan-id> | gui | install]".into())
}

fn take_db_path(arguments: &mut Vec<std::ffi::OsString>) -> Result<PathBuf> {
    let mut db_path = None;
    let mut index = 0;
    while index < arguments.len() {
        if arguments[index] == "--db" {
            if db_path.is_some() {
                return Err("--db may be specified only once".into());
            }
            if index + 1 >= arguments.len() {
                return Err("--db requires a path".into());
            }
            if matches!(
                arguments[index + 1].to_string_lossy().as_ref(),
                "dedup"
                    | "list"
                    | "resume"
                    | "status"
                    | "verify"
                    | "commit"
                    | "finish"
                    | "cleanup"
                    | "gui"
                    | "install"
            ) {
                return Err(
                    "--db is missing its path; use 'btrfs-dup list' for the default database, or 'btrfs-dup --db <path> list'".into(),
                );
            }
            db_path = Some(PathBuf::from(arguments.remove(index + 1)));
            arguments.remove(index);
        } else {
            index += 1;
        }
    }
    Ok(db_path.unwrap_or_else(|| PathBuf::from("btrfs-dup.sqlite")))
}

fn install() -> Result<()> {
    let shell_env = std::env::var_os("SHELL").ok_or(
        "cannot detect the current shell: SHELL is unset; run install from your login shell",
    )?;
    let shell_name = Path::new(&shell_env)
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or("cannot determine shell name from SHELL")?;
    let (completion_path, completion, hint) = match shell_name {
        "fish" => (
            PathBuf::from("/usr/local/share/fish/vendor_completions.d/btrfs-dup.fish"),
            FISH_COMPLETION,
            "Start a new Fish session or run: source /usr/local/share/fish/vendor_completions.d/btrfs-dup.fish",
        ),
        "bash" => (
            PathBuf::from("/usr/local/share/bash-completion/completions/btrfs-dup"),
            BASH_COMPLETION,
            "Start a new Bash session with bash-completion enabled.",
        ),
        "zsh" => (
            PathBuf::from("/usr/local/share/zsh/site-functions/_btrfs-dup"),
            ZSH_COMPLETION,
            "Start a new Zsh session with /usr/local/share/zsh/site-functions in fpath.",
        ),
        _ => {
            return Err(
                format!("unsupported shell {shell_name:?}; supported: bash, fish, zsh").into(),
            );
        }
    };

    let source = std::env::current_exe()?;
    let binary = PathBuf::from("/usr/local/bin/btrfs-dup");
    let binary_parent = binary.parent().ok_or("binary install path has no parent")?;
    let completion_parent = completion_path
        .parent()
        .ok_or("completion install path has no parent")?;
    fs::create_dir_all(binary_parent)?;
    fs::create_dir_all(completion_parent)?;

    let already_installed = match (fs::canonicalize(&source), fs::canonicalize(&binary)) {
        (Ok(source), Ok(binary)) => source == binary,
        _ => false,
    };
    if !already_installed {
        let temp_binary = binary_parent.join(format!(".btrfs-dup.tmp-{}", std::process::id()));
        fs::copy(&source, &temp_binary)?;
        fs::set_permissions(&temp_binary, fs::Permissions::from_mode(0o755))?;
        fs::rename(&temp_binary, &binary)?;
    }
    let temp_completion =
        completion_parent.join(format!(".btrfs-dup-completion.tmp-{}", std::process::id()));
    fs::write(&temp_completion, completion)?;
    fs::rename(&temp_completion, &completion_path)?;

    println!("Installed binary: {}", binary.display());
    println!(
        "Installed {shell_name} completion: {}",
        completion_path.display()
    );
    println!("{hint}");
    Ok(())
}

const FISH_COMPLETION: &str = r#"
complete -c btrfs-dup -f -n '__fish_use_subcommand' -a 'dedup list status verify commit resume gui install'
complete -c btrfs-dup -n '__fish_seen_subcommand_from dedup' -l dry-run -d 'Index without deduplicating file contents'
complete -c btrfs-dup -n '__fish_seen_subcommand_from dedup' -l resume -d 'Resume a scan by ID'
complete -c btrfs-dup -n '__fish_seen_subcommand_from resume status verify commit' -a '(btrfs-dup list 2>/dev/null | cut -f1)'
"#;

const BASH_COMPLETION: &str = r#"
_btrfs_dup_completion() {
    local cur prev
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"
    if (( COMP_CWORD == 1 )); then
        COMPREPLY=( $(compgen -W "dedup list status verify commit resume gui install" -- "$cur") )
        return
    fi
    case "$prev" in
        dedup)
            COMPREPLY=( $(compgen -W "--dry-run --resume" -- "$cur") )
            if [[ "$cur" != -* ]]; then
                COMPREPLY+=( $(compgen -f -- "$cur") )
            fi
            ;;
        resume|status|verify|commit|--resume)
            COMPREPLY=( $(btrfs-dup list 2>/dev/null | cut -f1 | while read -r id; do [[ "$id" == "$cur"* ]] && printf '%s\n' "$id"; done) )
            ;;
    esac
}
complete -F _btrfs_dup_completion btrfs-dup
"#;

const ZSH_COMPLETION: &str = r#"
#compdef btrfs-dup

_btrfs_dup() {
    local -a commands
    commands=(
        'dedup:Start a guided scan or resume one'
        'list:List scans'
        'status:Show scan status'
        'verify:Verify recorded duplicate contents'
        'commit:Commit a saved scan to its source filesystem'
        'resume:Resume a scan'
        'gui:Browse saved database records'
        'install:Install binary and shell completion'
    )
    if (( CURRENT == 2 )); then
        _describe 'command' commands
        return
    fi
    case "$words[2]" in
        dedup)
            _arguments \
                '--dry-run:Index without deduplicating file contents' \
                '--resume[Resume a scan]:scan ID:' \
                '*:mounted path or device:_files'
            ;;
        resume|status|verify|commit)
            _arguments '*:scan ID:($(btrfs-dup list 2>/dev/null | cut -f1))'
            ;;
    esac
}
_btrfs_dup "$@"
"#;

fn prompt(message: &str) -> Result<String> {
    print!("{message}");
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let input = input.trim();
    if input.is_empty() {
        return Err("no path entered".into());
    }
    Ok(input.to_owned())
}

fn prompt_yes_no(message: &str, default: bool) -> Result<bool> {
    print!("{message}");
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    match input.trim().to_ascii_lowercase().as_str() {
        "" => Ok(default),
        "y" | "yes" => Ok(true),
        "n" | "no" => Ok(false),
        _ => Err("please answer y or n".into()),
    }
}

fn resolve_btrfs_mount(input: &Path) -> Result<PathBuf> {
    let mounts = read_mounts()?;
    let input = fs::canonicalize(input)?;
    if input.starts_with("/dev/") || input.metadata()?.file_type().is_block_device() {
        let device = fs::metadata(&input)?;
        let canonical_device = fs::canonicalize(&input).ok();
        let mut candidates = mounts
            .iter()
            .filter(|m| m.fs_type == "btrfs")
            .filter(|m| {
                let source_matches = canonical_device.as_ref().is_some_and(|device| {
                    fs::canonicalize(&m.source).ok().as_ref() == Some(device)
                });
                let device_matches = fs::metadata(&m.target)
                    .map(|meta| meta.dev() == device.rdev())
                    .unwrap_or(false);
                source_matches || device_matches
            })
            .map(|m| m.target.clone())
            .collect::<Vec<_>>();
        candidates.sort();
        candidates.dedup();
        return choose_mount(candidates, &input);
    }

    let mount = deepest_mount_for_path(&input, &mounts)
        .ok_or_else(|| format!("{} is not on a mounted filesystem", input.display()))?;
    if mount.fs_type != "btrfs" {
        return Err(format!(
            "{} is on {} filesystem mounted at {}, not Btrfs",
            input.display(),
            mount.fs_type,
            mount.target.display()
        )
        .into());
    }
    Ok(mount.target.clone())
}

struct Mount {
    source: PathBuf,
    target: PathBuf,
    fs_type: String,
}

fn deepest_mount_for_path<'a>(input: &Path, mounts: &'a [Mount]) -> Option<&'a Mount> {
    mounts
        .iter()
        .filter(|mount| input.starts_with(&mount.target))
        .max_by_key(|mount| mount.target.components().count())
}

fn read_mounts() -> Result<Vec<Mount>> {
    let contents = fs::read_to_string("/proc/self/mounts")?;
    Ok(contents
        .lines()
        .filter_map(|line| {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            (fields.len() >= 3).then(|| Mount {
                source: PathBuf::from(unescape_mount_field(fields[0])),
                target: PathBuf::from(unescape_mount_field(fields[1])),
                fs_type: fields[2].to_owned(),
            })
        })
        .collect())
}

fn unescape_mount_field(field: &str) -> std::ffi::OsString {
    let bytes = field.as_bytes();
    let mut result = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 3 < bytes.len() {
            if let Ok(value) = std::str::from_utf8(&bytes[i + 1..i + 4]) {
                if let Ok(byte) = u8::from_str_radix(value, 8) {
                    result.push(byte);
                    i += 4;
                    continue;
                }
            }
        }
        result.push(bytes[i]);
        i += 1;
    }
    std::ffi::OsString::from_vec(result)
}

fn choose_mount(mut candidates: Vec<PathBuf>, device: &Path) -> Result<PathBuf> {
    if candidates.is_empty() {
        return Err(format!("no mounted Btrfs filesystem found for {}", device.display()).into());
    }
    if candidates.len() == 1 {
        return Ok(candidates.remove(0));
    }
    println!("That device is associated with multiple mounted Btrfs paths:");
    for (index, path) in candidates.iter().enumerate() {
        println!("  {}) {}", index + 1, path.display());
    }
    let choice = prompt("Choose the mounted path number: ")?.parse::<usize>()?;
    candidates
        .get(choice.checked_sub(1).ok_or("invalid choice")?)
        .cloned()
        .ok_or_else(|| "invalid choice".into())
}

async fn create_scan(store: &Store, source: &Path, dry_run: bool) -> Result<Scan> {
    let source = fs::canonicalize(source)?;
    let source_fd = File::open(&source)?;
    let snapshot_parent = source.join(SNAPSHOT_DIR);
    fs::create_dir_all(&snapshot_parent)?;
    let base = source_label(&source);
    let id = format!(
        "{}-{}",
        base,
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let snap_name = CString::new(format!(".btrfs-dup-{id}"))?;
    let parent_fd = File::open(&snapshot_parent)?;
    subvolume::snapshot_create(parent_fd.as_fd(), source_fd.as_fd(), &snap_name, true, &[])?;
    let snapshot = snapshot_parent.join(snap_name.to_string_lossy().as_ref());
    let scan = Scan {
        scan_id: Some(id.clone()),
        source: source.to_string_lossy().into_owned(),
        snapshot: snapshot.to_string_lossy().into_owned(),
        state: "running".into(),
        dry_run,
        files: 0,
        blocks: 0,
    };
    write_scan(store, &scan)?;
    Ok(scan)
}

async fn run_scan(store: &Store, mut scan: Scan) -> Result<()> {
    let snapshot = PathBuf::from(&scan.snapshot);
    let source = PathBuf::from(&scan.source);
    let source_fd = File::open(&source)?;
    let sectorsize = filesystem_info(source_fd.as_fd())?.sectorsize as u64;
    if sectorsize == 0 {
        return Err("Btrfs reported a zero-byte sector size".into());
    }
    println!("Counting files and planned blocks in the snapshot...");
    let mut inventory = Inventory::default();
    let source_mount_id =
        mount_id(&source)?.ok_or("selected source disappeared before inventory")?;
    inventory_directory(
        &snapshot,
        &snapshot,
        &source,
        source_mount_id,
        sectorsize,
        &mut inventory,
    )?;
    if io::stderr().is_terminal() && inventory.last_draw.is_some() {
        eprintln!();
    }
    eprintln!(
        "Snapshot inventory: {} files, {} blocks, {} bytes.",
        inventory.files, inventory.blocks, inventory.bytes
    );
    println!("Loading the saved block index...");
    let mut buckets: HashMap<(usize, u64), Vec<(PathBuf, u64, Option<Vec<u8>>)>> = HashMap::new();
    // Rebuild the in-memory candidate map from persisted block rows on resume;
    // this also makes repeated indexing safe through deterministic record IDs.
    let prior_records = load_records(store, "block", Some(scan.key()))?;
    let prior = prior_records
        .iter()
        .map(decode_record::<Block>)
        .collect::<Result<Vec<_>>>()?;
    let mut batch = WriteBatch::new();
    scan.blocks = prior.len() as u64;
    let prior_file_records = load_records(store, "file", Some(scan.key()))?;
    let prior_files = prior_file_records
        .iter()
        .map(decode_record::<ScannedFile>)
        .collect::<Result<Vec<_>>>()?;
    let mut completed_paths = HashSet::new();
    let mut completed_bytes = 0;
    let mut completed_blocks = 0;
    for file in &prior_files {
        let path_bytes = file.path.clone();
        let relative = PathBuf::from(std::ffi::OsString::from_vec(path_bytes.clone()));
        let size = if file.bytes > 0 {
            file.bytes
        } else {
            fs::metadata(snapshot.join(&relative))
                .map(|metadata| metadata.len())
                .unwrap_or(0)
        };
        let block_count = if file.blocks > 0 {
            file.blocks
        } else {
            blocks_for_size(size, sectorsize)
        };
        completed_paths.insert(path_bytes);
        completed_bytes += size;
        completed_blocks += block_count;
    }
    scan.files = completed_paths.len() as u64;
    for b in prior {
        let xxh3 = b.xxh3;
        let relative = PathBuf::from(std::ffi::OsString::from_vec(b.path.clone()));
        let absolute = snapshot.join(&relative);
        if b.blake3.is_none() {
            let block_key = block_id(scan.key(), &b.path, b.offset);
            batch.collision_candidates.insert(block_key, b.clone());
        }
        buckets
            .entry((b.len, xxh3))
            .or_default()
            .push((absolute, b.offset, b.blake3.clone()));
    }
    let prior_duplicates = load_records(store, "duplicate", Some(scan.key()))?
        .iter()
        .map(decode_record::<Duplicate>)
        .collect::<Result<Vec<_>>>()
        .map_err(|error| error.to_string())?;
    let duplicate_count = prior_duplicates.len() as u64;
    let duplicate_bytes = prior_duplicates.iter().map(|duplicate| duplicate.len).sum();
    println!(
        "Scanning {} files in {} blocks (XXH3 first; BLAKE3 only for collisions).",
        inventory.files, inventory.blocks
    );
    let mut progress = ScanProgress::new(
        &inventory,
        scan.files,
        completed_blocks,
        completed_bytes,
        duplicate_count,
        duplicate_bytes,
        ProgressMode::Scan,
    );
    println!(
        "Resuming: skipping {} completed files ({}).",
        progress.files,
        format_bytes(progress.bytes)
    );
    progress.draw(true)?;
    let pending_files = inventory
        .paths
        .into_iter()
        .filter(|(_, relative)| !completed_paths.contains(&path_bytes(relative)))
        .collect();
    scan_files_parallel(
        store,
        &source,
        &mut scan,
        &mut buckets,
        &mut progress,
        &mut completed_paths,
        &mut batch,
        pending_files,
        sectorsize,
    )
    .await?;
    batch.flush(store, &scan).await?;
    progress.draw(true)?;
    scan.state = "complete".into();
    write_scan(store, &scan)?;
    Ok(())
}

fn inventory_directory(
    root: &Path,
    dir: &Path,
    source_root: &Path,
    source_mount_id: u64,
    sectorsize: u64,
    inventory: &mut Inventory,
) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if dir == root && path.file_name() == Some(std::ffi::OsStr::new(SNAPSHOT_DIR)) {
            continue;
        }
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            continue;
        }
        let relative = path.strip_prefix(root)?;
        let source_path = source_root.join(relative);
        let Some(path_mount_id) = mount_id(&source_path)? else {
            continue;
        };
        if path_mount_id != source_mount_id {
            continue;
        }
        if metadata.is_dir() {
            inventory_directory(
                root,
                &path,
                source_root,
                source_mount_id,
                sectorsize,
                inventory,
            )?;
        } else if metadata.is_file() {
            let size = metadata.len();
            let block_count = blocks_for_size(size, sectorsize);
            if block_count == 0 {
                continue;
            }
            inventory.files += 1;
            inventory.bytes += size;
            inventory.blocks += block_count;
            inventory.paths.push((path.clone(), relative.to_path_buf()));
            if io::stderr().is_terminal()
                && inventory
                    .last_draw
                    .is_none_or(|last| last.elapsed() >= Duration::from_millis(150))
            {
                inventory.last_draw = Some(Instant::now());
                eprint!(
                    "\rCounting snapshot: {} files, {} blocks, {} bytes",
                    inventory.files, inventory.blocks, inventory.bytes
                );
                io::stderr().flush()?;
            }
        }
    }
    Ok(())
}

fn mount_id(path: &Path) -> Result<Option<u64>> {
    let path = CString::new(path.as_os_str().as_bytes())?;
    let mut stat = std::mem::MaybeUninit::<libc::statx>::zeroed();
    let result = unsafe {
        libc::statx(
            libc::AT_FDCWD,
            path.as_ptr(),
            libc::AT_STATX_SYNC_AS_STAT | libc::AT_SYMLINK_NOFOLLOW,
            libc::STATX_MNT_ID,
            stat.as_mut_ptr(),
        )
    };
    if result != 0 {
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::NotFound {
            return Ok(None);
        }
        return Err(error.into());
    }
    let stat = unsafe { stat.assume_init() };
    if stat.stx_mask & libc::STATX_MNT_ID == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "kernel did not return a mount ID for statx",
        )
        .into());
    }
    Ok(Some(stat.stx_mnt_id))
}

fn blocks_for_size(size: u64, sectorsize: u64) -> u64 {
    if sectorsize == 0 {
        return 0;
    }
    let aligned_size = size - size % sectorsize;
    match block_decision_function(size) {
        BlockDecision::Exclude => 0,
        BlockDecision::WholeFile if aligned_size > 0 => 1,
        BlockDecision::WholeFile => 0,
        BlockDecision::FixedChunks(chunk_size) => aligned_size.div_ceil(chunk_size as u64),
    }
}

async fn scan_files_parallel(
    store: &Store,
    source: &Path,
    scan: &mut Scan,
    buckets: &mut HashMap<(usize, u64), Vec<(PathBuf, u64, Option<Vec<u8>>)>>,
    progress: &mut ScanProgress,
    completed_paths: &mut HashSet<Vec<u8>>,
    batch: &mut WriteBatch,
    pending_files: Vec<(PathBuf, PathBuf)>,
    sectorsize: u64,
) -> Result<()> {
    let available = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(2);
    let worker_count = available.saturating_sub(1).max(1).min(8);
    println!(
        "Scan hashing: up to {worker_count} file batches run at once; each batch reads files and calculates XXH3 hashes. Batches may finish out of order."
    );

    let mut jobs: VecDeque<_> = pending_files.into();
    let bytes_read = Arc::new(AtomicU64::new(progress.bytes));
    let mut workers = tokio::task::JoinSet::new();
    let mut ticker = tokio::time::interval(Duration::from_millis(200));
    let mut next_batch_id = 1usize;

    while !jobs.is_empty() || !workers.is_empty() {
        while workers.len() < worker_count {
            if jobs.is_empty() {
                break;
            }
            let worker_jobs = jobs
                .drain(..jobs.len().min(HASH_WORKER_BATCH_FILES))
                .collect::<Vec<_>>();
            let bytes_read = Arc::clone(&bytes_read);
            let worker_store = store.clone();
            let scan_id = scan.key().to_owned();
            let batch_id = next_batch_id;
            next_batch_id += 1;
            println!(
                "Scan batch {batch_id}: starting XXH3 hashing for {} files in one background worker (up to {worker_count} batches run concurrently).",
                worker_jobs.len()
            );
            workers.spawn(async move {
                let hashed_files = tokio::task::spawn_blocking(move || {
                    worker_jobs
                        .into_iter()
                        .map(|(path, relative)| {
                            hash_file(path, relative, Arc::clone(&bytes_read), sectorsize)
                        })
                        .collect::<std::io::Result<Vec<_>>>()
                })
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
                println!(
                    "Scan batch {batch_id}: XXH3 hashing finished for {} files; next, save their block hashes to SQLite.",
                    hashed_files.len()
                );
                persist_hashed_blocks(&worker_store, &scan_id, &hashed_files).await?;
                println!(
                    "Scan batch {batch_id}: block hashes saved; next, check possible duplicates and update the index in order."
                );
                Ok::<_, String>((batch_id, hashed_files))
            });
        }
        tokio::select! {
            joined = workers.join_next(), if !workers.is_empty() => {
                let (batch_id, hashed_files) = joined
                    .ok_or("hash worker stopped unexpectedly")?
                    .map_err(|error| error.to_string())?
                    .map_err(std::io::Error::other)?;
                progress.bytes = bytes_read.load(Ordering::Relaxed);
                let files_count = hashed_files.len();
                let blocks_before = progress.blocks;
                let duplicates_before = progress.duplicates;
                let strong_hashes =
                    hash_candidate_ranges_parallel(batch_id, &hashed_files, buckets).await?;
                for hashed in hashed_files {
                    let writable = source.join(&hashed.relative);
                    let relative_bytes = path_bytes(&hashed.relative);
                    let file_key = file_id(scan.key(), &relative_bytes);
                    let file_blocks_before = progress.blocks;
                    index_hashed_file(
                        scan,
                        &hashed,
                        &writable,
                        buckets,
                        &strong_hashes,
                        progress,
                        batch,
                    )?;
                    progress.files += 1;
                    scan.files = progress.files;
                    scan.blocks = progress.blocks;
                    batch.files.insert(file_key, ScannedFile {
                        scan: scan.key().to_owned(),
                        path: relative_bytes.clone(),
                        bytes: hashed.size,
                        blocks: progress.blocks - file_blocks_before,
                    });
                    completed_paths.insert(relative_bytes);
                    progress.draw(false)?;
                    if batch.last_flush.elapsed() >= Duration::from_secs(30) {
                        batch.flush(store, scan).await?;
                    }
                }
                println!(
                    "Scan batch {batch_id}: updated the index for {files_count} files and {} blocks; found {} duplicate ranges. Index updates and dedupe ioctls run sequentially.",
                    progress.blocks - blocks_before,
                    progress.duplicates - duplicates_before
                );
            }
            _ = ticker.tick() => {
                progress.bytes = bytes_read.load(Ordering::Relaxed);
                progress.draw(false)?;
            }
        }
    }
    progress.bytes = bytes_read.load(Ordering::Relaxed);
    Ok(())
}

async fn persist_hashed_blocks(
    store: &Store,
    scan_id: &str,
    files: &[HashedFile],
) -> std::result::Result<(), String> {
    let records = files
        .iter()
        .flat_map(|file| {
            let path = path_bytes(&file.relative);
            file.blocks
                .iter()
                .filter(|block| block.len > 0)
                .map(move |block| {
                    let id = block_id(scan_id, &path, block.offset);
                    stored_record(
                        &id,
                        "block",
                        scan_id,
                        block.len as u64,
                        &block.xxh3.to_be_bytes(),
                        &Block {
                            scan: scan_id.to_owned(),
                            xxh3: block.xxh3,
                            len: block.len,
                            path: path.clone(),
                            offset: block.offset,
                            blake3: None,
                        },
                    )
                })
        })
        .collect::<Result<Vec<_>>>()
        .map_err(|error| error.to_string())?;
    write_records_off_thread(store, records)
        .await
        .map_err(|error| error.to_string())
}

fn hash_file(
    path: PathBuf,
    relative: PathBuf,
    bytes_read: Arc<AtomicU64>,
    sectorsize: u64,
) -> std::io::Result<HashedFile> {
    if sectorsize == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Btrfs sector size cannot be zero",
        ));
    }
    let sectorsize = usize::try_from(sectorsize).map_err(std::io::Error::other)?;
    let mut file = File::open(&path)?;
    let size = file.metadata()?.len();
    let chunk_size = match block_decision_function(size) {
        BlockDecision::Exclude => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("file is below the 2 MiB scan minimum: {}", path.display()),
            ));
        }
        BlockDecision::WholeFile => usize::try_from(size).unwrap_or(usize::MAX).max(1),
        BlockDecision::FixedChunks(size) => size,
    };
    let mut buffer = vec![0; chunk_size];
    let mut offset = 0u64;
    let mut blocks = Vec::new();
    loop {
        let mut len = 0;
        while len < buffer.len() {
            let read = file.read(&mut buffer[len..])?;
            if read == 0 {
                break;
            }
            len += read;
            bytes_read.fetch_add(read as u64, Ordering::Relaxed);
        }
        if len == 0 {
            break;
        }
        let aligned_len = len - len % sectorsize;
        if aligned_len > 0 {
            let mut hasher = XxHash3_64::default();
            hasher.write(&buffer[..aligned_len]);
            blocks.push(HashedBlock {
                offset,
                len: aligned_len,
                xxh3: hasher.finish(),
            });
        }
        offset += len as u64;
    }
    Ok(HashedFile {
        path,
        relative,
        size,
        blocks,
    })
}

async fn hash_candidate_ranges_parallel(
    batch_id: usize,
    files: &[HashedFile],
    buckets: &HashMap<(usize, u64), Vec<(PathBuf, u64, Option<Vec<u8>>)>>,
) -> Result<HashMap<(PathBuf, u64), Vec<u8>>> {
    let mut current_paths: HashMap<(usize, u64), HashSet<PathBuf>> = HashMap::new();
    for file in files {
        for block in &file.blocks {
            current_paths
                .entry((block.len, block.xxh3))
                .or_default()
                .insert(file.path.clone());
        }
    }

    let mut ranges = HashMap::new();
    for file in files {
        for block in &file.blocks {
            let key = (block.len, block.xxh3);
            let members = buckets.get(&key);
            let has_other_candidate = members
                .is_some_and(|members| members.iter().any(|member| member.0 != file.path))
                || current_paths[&key].iter().any(|path| path != &file.path);
            if !has_other_candidate {
                continue;
            }

            ranges.insert((file.path.clone(), block.offset), block.len);
            if let Some(members) = members {
                for member in members.iter().filter(|member| member.2.is_none()) {
                    ranges.insert((member.0.clone(), member.1), block.len);
                }
            }
        }
    }

    if ranges.is_empty() {
        println!(
            "Scan batch {batch_id}: no possible duplicate ranges; skipping BLAKE3 verification and continuing to ordered indexing."
        );
        return Ok(HashMap::new());
    }

    let ranges = ranges.into_iter().collect::<Vec<_>>();
    let available = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(2);
    let worker_count = available.saturating_sub(1).max(1).min(8).min(ranges.len());
    let chunk_size = ranges.len().div_ceil(worker_count);
    println!(
        "Scan batch {batch_id}: confirming {} possible matches with BLAKE3 on {worker_count} workers; then update the duplicate index in order.",
        ranges.len()
    );
    let mut workers = tokio::task::JoinSet::new();
    for chunk in ranges.chunks(chunk_size) {
        let chunk = chunk.to_vec();
        workers.spawn_blocking(move || {
            chunk
                .into_iter()
                .map(|((path, offset), len)| {
                    hash_range(&path, offset, len)
                        .map(|digest| ((path, offset), digest))
                        .map_err(|error| error.to_string())
                })
                .collect::<std::result::Result<Vec<_>, _>>()
        });
    }

    let mut hashes = HashMap::with_capacity(ranges.len());
    while let Some(result) = workers.join_next().await {
        let chunk_hashes = result
            .map_err(std::io::Error::other)?
            .map_err(std::io::Error::other)?;
        for (range, digest) in chunk_hashes {
            hashes.insert(range, digest);
        }
    }
    println!(
        "Scan batch {batch_id}: confirmed {} candidate ranges; now update the duplicate index and, unless dry-run, issue dedupe ioctls sequentially.",
        hashes.len()
    );
    Ok(hashes)
}

fn index_hashed_file(
    scan: &mut Scan,
    file: &HashedFile,
    writable: &Path,
    buckets: &mut HashMap<(usize, u64), Vec<(PathBuf, u64, Option<Vec<u8>>)>>,
    strong_hashes: &HashMap<(PathBuf, u64), Vec<u8>>,
    progress: &mut ScanProgress,
    batch: &mut WriteBatch,
) -> Result<()> {
    for hashed_block in &file.blocks {
        let offset = hashed_block.offset;
        let len = hashed_block.len;
        if len == 0 {
            continue;
        }
        let xxh3 = hashed_block.xxh3;
        let key = (len, xxh3);
        let members = buckets.entry(key).or_default();
        let same_block = members
            .iter()
            .find(|m| m.0 == file.path && m.1 == offset)
            .and_then(|m| m.2.clone());
        let has_other_candidate = members.iter().any(|m| m.0 != file.path);
        let strong = if !has_other_candidate {
            same_block
        } else {
            let digest = strong_hashes
                .get(&(file.path.clone(), offset))
                .cloned()
                .ok_or("candidate range was not strong-hashed")?;
            if members.iter().any(|m| m.2.is_none()) {
                for member in members.iter_mut().filter(|m| m.2.is_none()) {
                    let old_digest = strong_hashes
                        .get(&(member.0.clone(), member.1))
                        .cloned()
                        .ok_or("existing candidate range was not strong-hashed")?;
                    member.2 = Some(old_digest.clone());
                    let old_relative = member.0.strip_prefix(&scan.snapshot)?;
                    let old_id =
                        block_id(scan.key(), path_bytes(old_relative).as_slice(), member.1);
                    if let Some(old_record) = batch.blocks.get_mut(&old_id) {
                        old_record.blake3 = Some(old_digest);
                        batch.collision_candidates.remove(&old_id);
                    } else {
                        let old_record = batch
                            .collision_candidates
                            .remove(&old_id)
                            .ok_or("collision block missing from loaded index")?;
                        batch.blocks.insert(
                            old_id,
                            Block {
                                blake3: Some(old_digest),
                                ..old_record
                            },
                        );
                    }
                }
            }
            Some(digest)
        };
        let raw_path = path_bytes(&file.relative);
        let record_id = block_id(scan.key(), &raw_path, offset);
        let block = Block {
            scan: scan.key().to_owned(),
            xxh3,
            len,
            path: raw_path,
            offset,
            blake3: strong.clone(),
        };
        if strong.is_some() {
            batch.blocks.insert(record_id, block);
        } else {
            batch.collision_candidates.insert(record_id, block);
        }
        let member = (file.path.clone(), offset, strong.clone());
        if let Some(existing) = members
            .iter_mut()
            .find(|m| m.0 == file.path && m.1 == offset)
        {
            existing.2 = strong.clone();
        } else {
            members.push(member);
        }

        if let Some(digest) = strong {
            for (candidate_snapshot, candidate_offset, candidate_digest) in members.iter() {
                if candidate_snapshot == &file.path {
                    continue;
                }
                if candidate_digest.as_deref() != Some(digest.as_slice()) {
                    progress.log(format!(
                        "XXH3 collision: {} @ {} and {} @ {} share XXH3 but have different BLAKE3 hashes ({} bytes)",
                        file.relative.display(),
                        offset,
                        candidate_snapshot
                            .strip_prefix(&scan.snapshot)
                            .unwrap_or(candidate_snapshot)
                            .display(),
                        candidate_offset,
                        len
                    ))?;
                }
            }
            let duplicate_source = members
                .iter()
                .find(|(candidate_path, _, candidate_digest)| {
                    candidate_path != &file.path
                        && candidate_digest.as_deref() == Some(digest.as_slice())
                });
            if let Some((candidate_snapshot, candidate_offset, _)) = duplicate_source {
                progress.duplicates += 1;
                progress.duplicate_bytes += len as u64;
                let relative = candidate_snapshot.strip_prefix(&scan.snapshot)?;
                let candidate_live = PathBuf::from(&scan.source).join(relative);
                let (deduped_bytes, commited) = if scan.dry_run {
                    (0, false)
                } else {
                    match dedupe_live_ranges(
                        writable,
                        offset,
                        &candidate_live,
                        *candidate_offset,
                        len,
                    )? {
                        DedupeOutcome::Deduped(bytes) => (bytes, true),
                        DedupeOutcome::AlreadyShared => (0, true),
                        DedupeOutcome::DataDiffers | DedupeOutcome::Skipped => (0, false),
                    }
                };
                let duplicate_path = path_bytes(&file.relative);
                let duplicate_id = duplicate_id(scan.key(), &duplicate_path, offset);
                batch.duplicates.insert(
                    duplicate_id,
                    Duplicate {
                        scan: scan.key().to_owned(),
                        duplicate_path,
                        duplicate_offset: offset,
                        source_path: path_bytes(relative),
                        source_offset: *candidate_offset,
                        len: len as u64,
                        deduped_bytes,
                        dry_run: scan.dry_run,
                        commited,
                    },
                );
            }
        }
        progress.blocks += 1;
        scan.blocks = progress.blocks;
    }
    progress.draw(false)?;
    Ok(())
}

fn hash_range(path: &Path, offset: u64, len: usize) -> Result<Vec<u8>> {
    let mut file = File::open(path)?;
    file.seek_to(offset)?;
    let mut bytes = vec![0; len];
    file.read_exact(&mut bytes)?;
    Ok(blake3::hash(&bytes).as_bytes().to_vec())
}

fn block_id(scan_id: &str, path: &[u8], offset: u64) -> String {
    format!("block:{}", location_id(scan_id, path, offset))
}

fn file_id(scan_id: &str, path: &[u8]) -> String {
    format!("file:{}", location_id(scan_id, path, 0))
}

fn duplicate_id(scan_id: &str, path: &[u8], offset: u64) -> String {
    format!("duplicate:{}", location_id(scan_id, path, offset))
}

fn scan_record_id(scan_id: &str) -> String {
    format!("scan:{scan_id}")
}

fn location_id(scan_id: &str, path: &[u8], offset: u64) -> String {
    let mut identity = Vec::with_capacity(scan_id.len() + path.len() + 16);
    identity.extend_from_slice(&(scan_id.len() as u64).to_be_bytes());
    identity.extend_from_slice(scan_id.as_bytes());
    identity.extend_from_slice(&(path.len() as u64).to_be_bytes());
    identity.extend_from_slice(path);
    identity.extend_from_slice(&offset.to_be_bytes());
    blake3::hash(&identity).to_hex().to_string()
}

fn source_label(source: &Path) -> String {
    source
        .file_name()
        .unwrap_or_else(|| std::ffi::OsStr::new("root"))
        .to_string_lossy()
        .into_owned()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DedupeOutcome {
    Deduped(u64),
    AlreadyShared,
    DataDiffers,
    Skipped,
}

fn dedupe_chunk_lengths(len: u64, sectorsize: u64) -> Result<Vec<u64>> {
    if sectorsize == 0 {
        return Err("Btrfs reported a zero-byte sector size".into());
    }
    let max_chunk = MAX_DEDUPE_IOCTL_LEN - MAX_DEDUPE_IOCTL_LEN % sectorsize;
    if max_chunk == 0 {
        return Err(format!(
            "Btrfs sector size {sectorsize} exceeds the maximum dedupe ioctl range"
        )
        .into());
    }
    let aligned_len = len - len % sectorsize;
    let mut chunks = Vec::new();
    let mut remaining = aligned_len;
    while remaining > 0 {
        let chunk = remaining.min(max_chunk);
        chunks.push(chunk);
        remaining -= chunk;
    }
    Ok(chunks)
}

fn dedupe_live_ranges(
    source_path: &Path,
    source_offset: u64,
    dest_path: &Path,
    dest_offset: u64,
    len: usize,
) -> Result<DedupeOutcome> {
    if len == 0 {
        return Ok(DedupeOutcome::Skipped);
    }
    let source = File::open(source_path)?;
    let dest = File::options().read(true).write(true).open(dest_path)?;
    let source_metadata = source.metadata()?;
    let dest_metadata = dest.metadata()?;
    if !source_metadata.is_file() || !dest_metadata.is_file() {
        return Err("dedupe source and target must both be regular files".into());
    }
    if source_metadata.dev() != dest_metadata.dev() {
        return Err("dedupe source and target are on different filesystems".into());
    }
    if source_metadata.ino() == dest_metadata.ino() {
        return if source_offset == dest_offset {
            Ok(DedupeOutcome::AlreadyShared)
        } else {
            Ok(DedupeOutcome::Skipped)
        };
    }
    let sectorsize = filesystem_info(source.as_fd())?.sectorsize as u64;
    if sectorsize == 0 {
        return Err("Btrfs reported a zero-byte sector size".into());
    }
    if source_offset % sectorsize != 0 || dest_offset % sectorsize != 0 {
        return Ok(DedupeOutcome::Skipped);
    }
    let chunks = dedupe_chunk_lengths(u64::try_from(len)?, sectorsize)?;
    if chunks.is_empty() {
        return Ok(DedupeOutcome::Skipped);
    }
    let aligned_len = chunks.iter().sum::<u64>();
    let source_end = source_offset
        .checked_add(aligned_len)
        .ok_or("dedupe source range overflows")?;
    let dest_end = dest_offset
        .checked_add(aligned_len)
        .ok_or("dedupe target range overflows")?;
    if source_end > source_metadata.len() || dest_end > dest_metadata.len() {
        return Err(format!(
            "dedupe range exceeds live file size (source ends at {source_end} of {}, target ends at {dest_end} of {})",
            source_metadata.len(),
            dest_metadata.len()
        )
        .into());
    }
    // SAFETY: `dest` remains open until after the ioctl returns.
    let dest_fd = unsafe { BorrowedFd::borrow_raw(dest.as_raw_fd()) };
    let mut total_deduped = 0u64;
    let mut offset = 0u64;
    for chunk_len in chunks {
        let targets = [DedupeTarget {
            fd: dest_fd,
            logical_offset: dest_offset + offset,
        }];
        let results =
            file_extent_same(source.as_fd(), source_offset + offset, chunk_len, &targets)?;
        match results[0] {
            DedupeResult::Deduped(bytes) => {
                total_deduped = total_deduped.saturating_add(bytes);
            }
            DedupeResult::DataDiffers => return Ok(DedupeOutcome::DataDiffers),
            DedupeResult::Error(code) => {
                return Err(format!(
                    "Btrfs dedupe target error: {code} at source offset {} and target offset {} for {} bytes",
                    source_offset + offset,
                    dest_offset + offset,
                    chunk_len
                )
                .into());
            }
        }
        offset += chunk_len;
    }
    Ok(DedupeOutcome::Deduped(total_deduped))
}

fn verify_duplicates(scan: &Scan, duplicates: &[Duplicate]) -> Result<()> {
    let root = Path::new(&scan.source);
    let total_bytes = duplicates
        .iter()
        .fold(0u64, |total, duplicate| total.saturating_add(duplicate.len));
    let inventory = Inventory {
        files: duplicates.len() as u64,
        blocks: duplicates.len() as u64,
        bytes: total_bytes,
        ..Inventory::default()
    };
    let mut progress = ScanProgress::new(&inventory, 0, 0, 0, 0, 0, ProgressMode::Verify);
    progress.draw(true)?;
    let mut verified = 0u64;
    let mut mismatches = 0u64;
    let mut errors = 0u64;
    for duplicate in duplicates {
        let duplicate_path = PathBuf::from(std::ffi::OsString::from_vec(
            duplicate.duplicate_path.clone(),
        ));
        let source_path =
            PathBuf::from(std::ffi::OsString::from_vec(duplicate.source_path.clone()));
        if duplicate.scan != scan.key()
            || duplicate.len == 0
            || !safe_relative_path(&duplicate_path)
            || !safe_relative_path(&source_path)
        {
            errors += 1;
            progress.duplicates += 1;
            progress.log(format!(
                "ANOMALY invalid stored duplicate relation: {} @ {} vs {} @ {} ({} bytes)",
                duplicate_path.display(),
                duplicate.duplicate_offset,
                source_path.display(),
                duplicate.source_offset,
                duplicate.len
            ))?;
            update_pair_progress(&mut progress)?;
            continue;
        }
        let len = match usize::try_from(duplicate.len) {
            Ok(len) => len,
            Err(error) => {
                errors += 1;
                progress.duplicates += 1;
                progress.log(format!(
                    "ANOMALY invalid duplicate length {}: {error}",
                    duplicate.len
                ))?;
                update_pair_progress(&mut progress)?;
                continue;
            }
        };
        let duplicate_absolute = root.join(&duplicate_path);
        let source_absolute = root.join(&source_path);
        let result = (|| -> Result<bool> {
            let mut duplicate_file = File::open(&duplicate_absolute)?;
            let mut source_file = File::open(&source_absolute)?;
            compare_file_ranges(
                &mut duplicate_file,
                duplicate.duplicate_offset,
                &mut source_file,
                duplicate.source_offset,
                len,
                &mut |bytes| {
                    progress.bytes = progress.bytes.saturating_add(bytes);
                    progress.draw(false)
                },
            )
        })();
        match result {
            Ok(true) => {
                verified += 1;
            }
            Ok(false) => {
                mismatches += 1;
                progress.duplicates += 1;
                progress.log(format!(
                    "ANOMALY contents differ: {} @ {} != {} @ {} ({} bytes)",
                    duplicate_path.display(),
                    duplicate.duplicate_offset,
                    source_path.display(),
                    duplicate.source_offset,
                    duplicate.len
                ))?;
            }
            Err(error) => {
                errors += 1;
                progress.duplicates += 1;
                progress.log(format!(
                    "ANOMALY could not verify {} @ {} vs {} @ {} ({} bytes): {error}",
                    duplicate_path.display(),
                    duplicate.duplicate_offset,
                    source_path.display(),
                    duplicate.source_offset,
                    duplicate.len
                ))?;
            }
        }
        update_pair_progress(&mut progress)?;
    }
    progress.draw(true)?;
    let compared_bytes = progress.bytes;
    println!(
        "Verification for scan {}: verified={}, mismatches={}, errors={}, compared={}",
        scan.key(),
        verified,
        mismatches,
        errors,
        format_bytes(compared_bytes)
    );
    if mismatches > 0 || errors > 0 {
        return Err(
            format!("verification failed: {mismatches} mismatches and {errors} errors").into(),
        );
    }
    Ok(())
}

async fn commit_duplicates(store: &Store, scan: &Scan, duplicates: &[Duplicate]) -> Result<()> {
    let root = PathBuf::from(&scan.source);
    let mounted_root = resolve_btrfs_mount(&root)?;
    let canonical_root = fs::canonicalize(&root)?;
    if mounted_root != canonical_root {
        return Err(format!(
            "saved scan source {} is not the root of its mounted Btrfs filesystem {}",
            canonical_root.display(),
            mounted_root.display()
        )
        .into());
    }

    let pending_duplicates = duplicates
        .iter()
        .filter(|duplicate| !duplicate.commited)
        .cloned()
        .collect::<Vec<_>>();
    let total_bytes = pending_duplicates
        .iter()
        .fold(0u64, |total, duplicate| total.saturating_add(duplicate.len));
    let inventory = Inventory {
        files: pending_duplicates.len() as u64,
        blocks: pending_duplicates.len() as u64,
        bytes: total_bytes,
        ..Inventory::default()
    };
    let mut progress = ScanProgress::new(&inventory, 0, 0, 0, 0, 0, ProgressMode::Commit);
    progress.draw(true)?;
    let mut commited_pairs = 0u64;
    let mut commited_bytes = 0u64;
    let mut mismatches = 0u64;
    let mut errors = 0u64;
    let mut pending_records = Vec::with_capacity(512);
    let mut pending_duplicates: VecDeque<_> = pending_duplicates.into();
    let available = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(2);
    let worker_count = available.saturating_sub(1).max(1).min(8);
    println!(
        "Committing {} pairs with up to {worker_count} concurrent Btrfs workers; results may complete out of order.",
        pending_duplicates.len()
    );
    let mut workers = tokio::task::JoinSet::new();

    while !pending_duplicates.is_empty() || !workers.is_empty() {
        while workers.len() < worker_count {
            let Some(duplicate) = pending_duplicates.pop_front() else {
                break;
            };
            let duplicate_path = PathBuf::from(std::ffi::OsString::from_vec(
                duplicate.duplicate_path.clone(),
            ));
            let source_path =
                PathBuf::from(std::ffi::OsString::from_vec(duplicate.source_path.clone()));
            if duplicate.scan != scan.key()
                || duplicate.len == 0
                || !safe_relative_path(&duplicate_path)
                || !safe_relative_path(&source_path)
            {
                errors += 1;
                progress.duplicates += 1;
                progress.log(format!(
                    "ANOMALY invalid stored duplicate relation: {} @ {} vs {} @ {} ({} bytes)",
                    duplicate_path.display(),
                    duplicate.duplicate_offset,
                    source_path.display(),
                    duplicate.source_offset,
                    duplicate.len
                ))?;
                update_pair_progress(&mut progress)?;
                continue;
            }
            let len = match usize::try_from(duplicate.len) {
                Ok(len) => len,
                Err(error) => {
                    errors += 1;
                    progress.duplicates += 1;
                    progress.log(format!(
                        "ANOMALY invalid duplicate length {}: {error}",
                        duplicate.len
                    ))?;
                    update_pair_progress(&mut progress)?;
                    continue;
                }
            };
            let duplicate_absolute = root.join(&duplicate_path);
            let source_absolute = root.join(&source_path);
            workers.spawn_blocking(move || {
                let outcome = dedupe_live_ranges(
                    &duplicate_absolute,
                    duplicate.duplicate_offset,
                    &source_absolute,
                    duplicate.source_offset,
                    len,
                )
                .map_err(|error| error.to_string());
                (duplicate, outcome)
            });
        }

        let (mut duplicate, outcome) = workers
            .join_next()
            .await
            .ok_or("commit worker stopped unexpectedly")?
            .map_err(std::io::Error::other)?;
        let duplicate_path = PathBuf::from(std::ffi::OsString::from_vec(
            duplicate.duplicate_path.clone(),
        ));
        let source_path =
            PathBuf::from(std::ffi::OsString::from_vec(duplicate.source_path.clone()));
        match outcome {
            Ok(DedupeOutcome::Deduped(bytes)) => {
                duplicate.deduped_bytes = bytes;
                duplicate.commited = true;
                commited_pairs += 1;
                commited_bytes = commited_bytes.saturating_add(bytes);
                progress.successful_pairs += 1;
                progress.duplicate_bytes = progress.duplicate_bytes.saturating_add(bytes);
                pending_records.push(stored_duplicate_record(
                    &duplicate_id(
                        scan.key(),
                        &duplicate.duplicate_path,
                        duplicate.duplicate_offset,
                    ),
                    &duplicate,
                )?);
            }
            Ok(DedupeOutcome::AlreadyShared) => {
                duplicate.deduped_bytes = 0;
                duplicate.commited = true;
                commited_pairs += 1;
                progress.successful_pairs += 1;
                pending_records.push(stored_duplicate_record(
                    &duplicate_id(
                        scan.key(),
                        &duplicate.duplicate_path,
                        duplicate.duplicate_offset,
                    ),
                    &duplicate,
                )?);
            }
            Ok(DedupeOutcome::DataDiffers) => {
                mismatches += 1;
                progress.duplicates += 1;
                duplicate.deduped_bytes = 0;
                progress.log(format!(
                    "ANOMALY contents differ; not deduped: {} @ {} vs {} @ {} ({} bytes)",
                    duplicate_path.display(),
                    duplicate.duplicate_offset,
                    source_path.display(),
                    duplicate.source_offset,
                    duplicate.len
                ))?;
                pending_records.push(stored_duplicate_record(
                    &duplicate_id(
                        scan.key(),
                        &duplicate.duplicate_path,
                        duplicate.duplicate_offset,
                    ),
                    &duplicate,
                )?);
            }
            Ok(DedupeOutcome::Skipped) => {
                progress.duplicates += 1;
                duplicate.deduped_bytes = 0;
                progress.log(format!(
                    "Skipped range unsupported for dedupe: {} @ {} vs {} @ {} ({} bytes; no full aligned range or source and target are the same file)",
                    duplicate_path.display(),
                    duplicate.duplicate_offset,
                    source_path.display(),
                    duplicate.source_offset,
                    duplicate.len
                ))?;
                pending_records.push(stored_duplicate_record(
                    &duplicate_id(
                        scan.key(),
                        &duplicate.duplicate_path,
                        duplicate.duplicate_offset,
                    ),
                    &duplicate,
                )?);
            }
            Err(error) => {
                errors += 1;
                progress.duplicates += 1;
                progress.log(format!(
                    "ANOMALY could not commit {} @ {} vs {} @ {} ({} bytes): {error}",
                    duplicate_path.display(),
                    duplicate.duplicate_offset,
                    source_path.display(),
                    duplicate.source_offset,
                    duplicate.len
                ))?;
            }
        }
        progress.bytes = progress.bytes.saturating_add(duplicate.len);
        update_pair_progress(&mut progress)?;
        if pending_records.len() >= 512 {
            write_records_off_thread(store, std::mem::take(&mut pending_records)).await?;
        }
    }
    if !pending_records.is_empty() {
        write_records_off_thread(store, pending_records).await?;
    }
    progress.draw(true)?;
    println!(
        "Commit for scan {}: deduped_pairs={}, deduped={}, mismatches={}, errors={}",
        scan.key(),
        commited_pairs,
        format_bytes(commited_bytes),
        mismatches,
        errors
    );
    if mismatches > 0 || errors > 0 {
        return Err(
            format!("commit completed with {mismatches} mismatches and {errors} errors").into(),
        );
    }
    Ok(())
}

fn update_pair_progress(progress: &mut ScanProgress) -> Result<()> {
    progress.files += 1;
    progress.blocks += 1;
    progress.draw(false)
}

fn safe_relative_path(path: &Path) -> bool {
    !path.as_os_str().is_empty()
        && path.components().all(|component| {
            matches!(
                component,
                std::path::Component::Normal(_) | std::path::Component::CurDir
            )
        })
}

fn compare_file_ranges(
    duplicate: &mut File,
    duplicate_offset: u64,
    source: &mut File,
    source_offset: u64,
    len: usize,
    on_compared: &mut impl FnMut(u64) -> Result<()>,
) -> Result<bool> {
    const COMPARE_BUFFER_SIZE: usize = 1024 * 1024;
    duplicate.seek_to(duplicate_offset)?;
    source.seek_to(source_offset)?;
    let mut duplicate_buffer = vec![0; COMPARE_BUFFER_SIZE.min(len)];
    let mut source_buffer = vec![0; COMPARE_BUFFER_SIZE.min(len)];
    let mut remaining = len;
    while remaining > 0 {
        let chunk_len = remaining.min(duplicate_buffer.len());
        duplicate.read_exact(&mut duplicate_buffer[..chunk_len])?;
        source.read_exact(&mut source_buffer[..chunk_len])?;
        on_compared(chunk_len as u64)?;
        if duplicate_buffer[..chunk_len] != source_buffer[..chunk_len] {
            return Ok(false);
        }
        remaining -= chunk_len;
    }
    Ok(true)
}

fn path_bytes(path: &Path) -> Vec<u8> {
    path.as_os_str().as_bytes().to_vec()
}

trait SeekTo {
    fn seek_to(&mut self, offset: u64) -> std::io::Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_source_has_a_snapshot_label() {
        assert_eq!(source_label(Path::new("/")), "root");
        assert_eq!(source_label(Path::new("/mnt/data")), "data");
    }

    #[test]
    fn path_on_nested_non_btrfs_mount_is_not_resolved_to_parent_btrfs() {
        let mounts = [
            Mount {
                source: PathBuf::from("/dev/btrfs"),
                target: PathBuf::from("/"),
                fs_type: "btrfs".into(),
            },
            Mount {
                source: PathBuf::from("/dev/ext4"),
                target: PathBuf::from("/e"),
                fs_type: "ext4".into(),
            },
        ];

        let deepest = deepest_mount_for_path(Path::new("/e"), &mounts)
            .expect("nested mount should be selected");

        assert_eq!(deepest.target, Path::new("/e"));
        assert_ne!(deepest.fs_type, "btrfs");
    }

    #[test]
    fn statx_returns_mount_id_for_root() {
        assert_ne!(
            mount_id(Path::new("/"))
                .expect("stat root mount ID")
                .expect("root exists"),
            0
        );
    }

    #[test]
    fn statx_skips_snapshot_paths_missing_from_live_source() {
        let missing = std::env::temp_dir().join(format!(
            "btrfs-dup-missing-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ));
        assert_eq!(mount_id(&missing).expect("stat missing path"), None);
    }

    #[test]
    fn database_path_option_is_removed_from_command_arguments() {
        let mut arguments = vec!["--db".into(), "/var/lib/btrfs-dup".into(), "list".into()];

        let path = take_db_path(&mut arguments).expect("parse database path");

        assert_eq!(path, PathBuf::from("/var/lib/btrfs-dup"));
        assert_eq!(arguments, vec!["list"]);
    }

    #[test]
    fn database_path_option_before_gui_leaves_gui_as_the_command() {
        let mut arguments = vec![
            "--db".into(),
            "/e/space/btrfs-dup.sqlite".into(),
            "gui".into(),
        ];

        let path = take_db_path(&mut arguments).expect("parse database path");

        assert_eq!(path, PathBuf::from("/e/space/btrfs-dup.sqlite"));
        assert_eq!(arguments, vec!["gui"]);
    }

    #[test]
    fn database_option_does_not_consume_command_as_its_path() {
        let mut arguments = vec!["--db".into(), "list".into()];

        assert!(take_db_path(&mut arguments).is_err());
    }

    #[test]
    fn database_option_recognizes_finish_as_a_command() {
        let mut arguments = vec!["--db".into(), "finish".into(), "scan-1".into()];

        assert!(take_db_path(&mut arguments).is_err());
    }

    #[test]
    fn database_option_recognizes_cleanup_as_a_command() {
        let mut arguments = vec!["--db".into(), "cleanup".into(), "--orphans".into()];

        assert!(take_db_path(&mut arguments).is_err());
    }

    #[test]
    fn scan_does_not_create_duplicate_pairs_between_ranges_in_one_file() {
        let file_path = PathBuf::from("/snapshot/repeated-data.bin");
        let file = HashedFile {
            path: file_path.clone(),
            relative: PathBuf::from("repeated-data.bin"),
            size: 8192,
            blocks: vec![HashedBlock {
                offset: 4096,
                len: 4096,
                xxh3: 7,
            }],
        };
        let mut buckets = HashMap::from([((4096, 7), vec![(file_path, 0, None)])]);
        let mut scan = Scan {
            scan_id: Some("scan-1".into()),
            source: "/source".into(),
            snapshot: "/snapshot".into(),
            state: "running".into(),
            dry_run: true,
            files: 0,
            blocks: 0,
        };
        let mut progress =
            ScanProgress::new(&Inventory::default(), 0, 0, 0, 0, 0, ProgressMode::Scan);
        let mut batch = WriteBatch::new();

        index_hashed_file(
            &mut scan,
            &file,
            Path::new("/source/repeated-data.bin"),
            &mut buckets,
            &HashMap::new(),
            &mut progress,
            &mut batch,
        )
        .expect("index a same-file range");

        assert!(batch.duplicates.is_empty());
        assert_eq!(progress.duplicates, 0);
    }

    #[test]
    fn block_record_preserves_xxh3_and_length() {
        let length = 4096u64;
        let hash = 0x0102_0304_0506_0708u64.to_be_bytes();
        let record = stored_record("id", "block", "scan", length, &hash, &"payload")
            .expect("serialize block row");

        assert_eq!(record.len, length);
        assert_eq!(record.xxh3, u64::from_be_bytes(hash));
    }

    #[test]
    fn unified_table_record_ids_are_namespaced_by_kind() {
        let block = block_id("scan", b"file", 0);
        let file = file_id("scan", b"file");
        let duplicate = duplicate_id("scan", b"file", 0);

        assert_ne!(block, file);
        assert_ne!(block, duplicate);
        assert_ne!(file, duplicate);
    }

    #[test]
    fn sqlite_creates_scan_and_block_composite_indexes() {
        let path = std::env::temp_dir().join(format!(
            "btrfs-dup-index-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ));
        let store = open_store(&path).expect("open temporary SQLite database");
        let connection = store.connection.lock().expect("lock SQLite connection");
        for (index, expected) in [
            ("records_kind_scan_id", vec!["kind", "scan", "id"]),
            ("records_block_match", vec!["kind", "scan", "len", "xxh3"]),
            (
                "records_pending_duplicates",
                vec!["kind", "scan", "commited", "id"],
            ),
        ] {
            let mut statement = connection
                .prepare(&format!("PRAGMA index_info({index})"))
                .expect("prepare index info query");
            let columns = statement
                .query_map([], |row| row.get::<_, String>(2))
                .expect("query index columns")
                .collect::<std::result::Result<Vec<_>, _>>()
                .expect("read index columns");
            assert_eq!(columns, expected);
        }
        drop(connection);
        drop(store);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn pending_duplicate_query_excludes_commited_rows() {
        let path = std::env::temp_dir().join(format!(
            "btrfs-dup-pending-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ));
        let store = open_store(&path).expect("open temporary SQLite database");
        let mut duplicate = Duplicate {
            scan: "scan-1".into(),
            duplicate_path: b"duplicate".to_vec(),
            duplicate_offset: 0,
            source_path: b"source".to_vec(),
            source_offset: 0,
            len: 4096,
            deduped_bytes: 0,
            dry_run: false,
            commited: false,
        };
        let pending_row =
            stored_duplicate_record("dup-pending", &duplicate).expect("encode pending row");
        duplicate.duplicate_path = b"already-deduped".to_vec();
        duplicate.commited = true;
        duplicate.deduped_bytes = 3072;
        let commited_row =
            stored_duplicate_record("dup-commited", &duplicate).expect("encode commited row");
        write_records(&store, vec![pending_row, commited_row]).expect("write duplicates");

        let duplicates =
            load_pending_duplicates(&store, "scan-1").expect("load pending duplicates");
        assert_eq!(duplicates.len(), 1);
        assert!(!duplicates[0].commited);
        assert_eq!(duplicates[0].duplicate_path, b"duplicate");
        let stats = load_duplicate_stats(&store, "scan-1").expect("load duplicate stats");
        assert_eq!(
            stats,
            ScanDuplicateStats {
                commited: DuplicateStats {
                    pairs: 1,
                    range_bytes: 4096,
                    deduped_bytes: 3072,
                },
                uncommited: DuplicateStats {
                    pairs: 1,
                    range_bytes: 4096,
                    deduped_bytes: 0,
                },
            }
        );

        drop(store);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn deleting_scan_records_keeps_other_scans_intact() {
        let path = std::env::temp_dir().join(format!(
            "btrfs-dup-finish-delete-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ));
        let store = open_store(&path).expect("open temporary SQLite database");
        let records = vec![
            stored_record("scan:one", "scan", "one", 0, &[], &"scan one").expect("encode scan one"),
            stored_record("block:one", "block", "one", 4096, &[], &"block one")
                .expect("encode block one"),
            stored_record("scan:two", "scan", "two", 0, &[], &"scan two").expect("encode scan two"),
        ];
        write_records(&store, records).expect("write test records");

        assert_eq!(
            delete_scan_records(&store, "one").expect("delete scan one"),
            2
        );
        assert!(
            load_records(&store, "scan", Some("one"))
                .expect("query deleted scan")
                .is_empty()
        );
        assert!(
            load_records(&store, "block", Some("one"))
                .expect("query deleted blocks")
                .is_empty()
        );
        assert_eq!(
            load_records(&store, "scan", Some("two"))
                .expect("query scan two")
                .len(),
            1
        );

        drop(store);
        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn write_batch_flushes_stable_record_ids() {
        let path = std::env::temp_dir().join(format!(
            "btrfs-dup-batch-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ));
        let store = open_store(&path).expect("open temporary SQLite database");
        let scan = Scan {
            scan_id: Some("scan-1".into()),
            source: "/source".into(),
            snapshot: "/snapshot".into(),
            state: "running".into(),
            dry_run: true,
            files: 1,
            blocks: 1,
        };
        let mut batch = WriteBatch::new();
        batch.blocks.insert(
            "block-1".into(),
            Block {
                scan: "scan-1".into(),
                xxh3: u64::MAX,
                len: 4096,
                path: b"file".to_vec(),
                offset: 0,
                blake3: None,
            },
        );
        batch.files.insert(
            "file-1".into(),
            ScannedFile {
                scan: "scan-1".into(),
                path: b"file".to_vec(),
                bytes: 4096,
                blocks: 1,
            },
        );
        batch.flush(&store, &scan).await.expect("flush batch");
        let blocks = load_records(&store, "block", Some("scan-1")).expect("load persisted blocks");
        let first_block = blocks
            .iter()
            .find(|row| row.id == "block-1")
            .expect("first block row exists");
        assert_eq!(
            decode_record::<Block>(first_block)
                .expect("decode first block")
                .xxh3,
            u64::MAX
        );
        let hashed = [HashedFile {
            path: PathBuf::from("/snapshot/worker-file"),
            relative: PathBuf::from("worker-file"),
            size: 4096,
            blocks: vec![HashedBlock {
                offset: 0,
                len: 4096,
                xxh3: 2,
            }],
        }];
        persist_hashed_blocks(&store, "scan-1", &hashed)
            .await
            .expect("persist worker block batch");
        let worker_block_id = block_id("scan-1", b"worker-file", 0);
        let worker_block = load_records(&store, "block", Some("scan-1"))
            .expect("load worker block")
            .into_iter()
            .find(|row| row.id == worker_block_id)
            .expect("worker row exists");
        assert_eq!(
            decode_record::<Block>(&worker_block)
                .expect("decode block")
                .len,
            4096
        );
        assert_eq!(
            decode_record::<Block>(&worker_block)
                .expect("decode block")
                .xxh3,
            2
        );
        drop(store);
        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn candidate_ranges_are_strong_hashed_in_parallel_batches() {
        let suffix = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        );
        let current_path = std::env::temp_dir().join(format!("btrfs-dup-current-{suffix}"));
        let existing_path = std::env::temp_dir().join(format!("btrfs-dup-existing-{suffix}"));
        let current_contents = vec![0x3a; 4096];
        let existing_contents = vec![0xa5; 4096];
        fs::write(&current_path, &current_contents).expect("write current candidate");
        fs::write(&existing_path, &existing_contents).expect("write existing candidate");

        let file = HashedFile {
            path: current_path.clone(),
            relative: PathBuf::from("current"),
            size: current_contents.len() as u64,
            blocks: vec![HashedBlock {
                offset: 0,
                len: current_contents.len(),
                xxh3: 7,
            }],
        };
        let buckets = HashMap::from([(
            (current_contents.len(), 7),
            vec![(existing_path.clone(), 0, None)],
        )]);

        let hashes = hash_candidate_ranges_parallel(1, &[file], &buckets)
            .await
            .expect("hash candidate ranges");

        assert_eq!(hashes.len(), 2);
        assert_eq!(
            hashes.get(&(current_path.clone(), 0)),
            Some(&blake3::hash(&current_contents).as_bytes().to_vec())
        );
        assert_eq!(
            hashes.get(&(existing_path.clone(), 0)),
            Some(&blake3::hash(&existing_contents).as_bytes().to_vec())
        );
        fs::remove_file(current_path).expect("remove current candidate");
        fs::remove_file(existing_path).expect("remove existing candidate");
    }

    #[test]
    fn progress_formatters_use_readable_units() {
        assert_eq!(format_bytes(1_500_000), "1.50 MB");
        assert_eq!(format_bytes(2_500_000_000), "2.50 GB");
        assert_eq!(format_byte_rate(12_000_000.0), "12.00 MB/s");
        assert_eq!(format_byte_rate(2_000_000_000.0), "2.00 GB/s");
    }

    #[test]
    fn block_decision_function_uses_requested_tiers() {
        assert_eq!(blocks_for_size(0, 4096), 0);
        assert_eq!(block_decision_function(0), BlockDecision::Exclude);
        assert_eq!(block_decision_function(2 * MIB - 1), BlockDecision::Exclude);
        assert_eq!(block_decision_function(2 * MIB), BlockDecision::WholeFile);
        assert_eq!(
            block_decision_function(BLOCK_CHUNK_SIZE as u64 - 1),
            BlockDecision::WholeFile
        );
        assert_eq!(
            block_decision_function(BLOCK_CHUNK_SIZE as u64),
            BlockDecision::FixedChunks(BLOCK_CHUNK_SIZE)
        );
        assert_eq!(
            block_decision_function(2 * GIB - 1),
            BlockDecision::FixedChunks(BLOCK_CHUNK_SIZE)
        );
        assert_eq!(
            block_decision_function(2 * GIB),
            BlockDecision::FixedChunks(LARGE_BLOCK_CHUNK_SIZE)
        );
        assert_eq!(blocks_for_size(2 * MIB + 1, 4096), 1);
        assert_eq!(blocks_for_size(2 * MIB + 1, 0), 0);
    }

    #[test]
    fn hash_file_trims_the_final_block_to_a_full_sector() {
        let path = std::env::temp_dir().join(format!(
            "btrfs-dup-aligned-hash-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ));
        File::create(&path)
            .expect("create sparse test file")
            .set_len(2 * MIB + 1)
            .expect("size sparse test file");
        let hashed = hash_file(
            path.clone(),
            PathBuf::from("aligned-test"),
            Arc::new(AtomicU64::new(0)),
            4096,
        )
        .expect("hash sector-aligned prefix");
        assert_eq!(hashed.blocks.len(), 1);
        assert_eq!(hashed.blocks[0].offset, 0);
        assert_eq!(hashed.blocks[0].len as u64, 2 * MIB);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn dedupe_ioctl_ranges_are_sector_aligned_and_capped() {
        let chunks = dedupe_chunk_lengths(100 * MIB, 4096).expect("plan dedupe chunks");
        assert_eq!(chunks.iter().sum::<u64>(), 100 * MIB);
        assert_eq!(chunks.len(), 7);
        assert!(chunks.iter().all(|chunk| *chunk <= MAX_DEDUPE_IOCTL_LEN));
        assert!(chunks.iter().all(|chunk| chunk % 4096 == 0));

        let unaligned = dedupe_chunk_lengths(8_772_223, 4096).expect("align final range");
        assert_eq!(unaligned.iter().sum::<u64>(), 8_772_223 - 8_772_223 % 4096);
        assert!(unaligned.iter().all(|chunk| chunk % 4096 == 0));
        assert!(dedupe_chunk_lengths(4096, 0).is_err());
    }
}
impl SeekTo for File {
    fn seek_to(&mut self, offset: u64) -> std::io::Result<()> {
        use std::io::Seek;
        self.seek(std::io::SeekFrom::Start(offset)).map(|_| ())
    }
}

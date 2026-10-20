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

use btrfs_uapi::dedupe::{DedupeResult, DedupeTarget, file_extent_same};
use btrfs_uapi::subvolume;
use serde::{Deserialize, Serialize};
use surrealdb::{
    Surreal,
    engine::local::Db,
    types::{Bytes, RecordId, RecordIdKey, SurrealValue, Value},
};
use twox_hash::XxHash3_64;

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;
const TEN_MIB: usize = 10 * 1024 * 1024;
const HUNDRED_MIB: usize = 100 * 1024 * 1024;
const HASH_WORKER_BATCH_FILES: usize = 256;
const SNAPSHOT_DIR: &str = ".btrfs-dup-snapshots";
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockDecision {
    WholeFile,
    FixedChunks(usize),
}

fn block_decision_function(file_size: u64) -> BlockDecision {
    if file_size < 10 * MIB {
        BlockDecision::WholeFile
    } else if file_size < 2 * GIB {
        BlockDecision::FixedChunks(TEN_MIB)
    } else {
        BlockDecision::FixedChunks(HUNDRED_MIB)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, SurrealValue)]
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

#[derive(Debug, Deserialize, SurrealValue)]
struct ScanListRow {
    id: RecordId,
    scan_id: Option<String>,
    source: String,
    state: String,
    dry_run: bool,
    files: u64,
    blocks: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct Block {
    scan: String,
    xxh3: Bytes,
    len: usize,
    path: Bytes,
    offset: u64,
    blake3: Option<Bytes>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct ScannedFile {
    scan: String,
    path: Bytes,
    #[serde(default)]
    bytes: u64,
    #[serde(default)]
    blocks: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct Duplicate {
    scan: String,
    duplicate_path: Bytes,
    duplicate_offset: u64,
    source_path: Bytes,
    source_offset: u64,
    len: u64,
    deduped_bytes: u64,
    dry_run: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
struct BatchRecord {
    id: String,
    content: Value,
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

    async fn flush(&mut self, db: &Surreal<Db>, scan: &Scan) -> Result<()> {
        if self.blocks.is_empty() && self.files.is_empty() && self.duplicates.is_empty() {
            self.last_flush = Instant::now();
            return Ok(());
        }
        let blocks = std::mem::take(&mut self.blocks);
        let files = std::mem::take(&mut self.files);
        let duplicates = std::mem::take(&mut self.duplicates);
        let block_records = blocks
            .iter()
            .map(|(id, block)| BatchRecord {
                id: id.clone(),
                content: block.clone().into_value(),
            })
            .collect::<Vec<_>>()
            .into_value();
        let file_records = files
            .iter()
            .map(|(id, file)| BatchRecord {
                id: id.clone(),
                content: file.clone().into_value(),
            })
            .collect::<Vec<_>>()
            .into_value();
        let duplicate_records = duplicates
            .iter()
            .map(|(id, duplicate)| BatchRecord {
                id: id.clone(),
                content: duplicate.clone().into_value(),
            })
            .collect::<Vec<_>>()
            .into_value();
        let db = db.clone();
        let scan_id = scan.key().to_owned();
        let scan = scan.clone().into_value();
        let write = tokio::task::spawn_blocking(move || {
            tokio::runtime::Handle::current().block_on(async move {
                db.query(
                "FOR $item IN $blocks { UPSERT type::record('block', $item.id) CONTENT $item.content; };
                 FOR $item IN $files { UPSERT type::record('scanned_file', $item.id) CONTENT $item.content; };
                 FOR $item IN $duplicates { UPSERT type::record('duplicate', $item.id) CONTENT $item.content; };
                 UPSERT type::record('scan', $scan_id) CONTENT $scan;",
            )
            .bind(("blocks", block_records))
            .bind(("files", file_records))
            .bind(("duplicates", duplicate_records))
            .bind(("scan_id", scan_id))
            .bind(("scan", scan))
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
            })
        })
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
        if let Err(error) = write {
            self.blocks.extend(blocks);
            self.files.extend(files);
            self.duplicates.extend(duplicates);
            return Err(error.into());
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
}

impl ScanProgress {
    fn new(
        inventory: &Inventory,
        completed_files: u64,
        completed_blocks: u64,
        completed_bytes: u64,
        duplicates: u64,
        duplicate_bytes: u64,
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
        }
    }

    fn draw(&mut self, force: bool) -> Result<()> {
        let now = Instant::now();
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
        let fraction = if self.total_bytes == 0 {
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
        let lines = [
            format!("Scan [{bar}] {percent:3}%"),
            format!(
                "Bytes read: {} / {} | {} remaining | read rate {} | elapsed {elapsed}",
                format_bytes(self.bytes),
                format_bytes(self.total_bytes),
                format_bytes(remaining_bytes),
                rate,
            ),
            format!(
                "Files: {}/{} | Blocks: {}/{} | Found dups: {} ({})",
                self.files,
                self.total_files,
                self.blocks,
                self.total_blocks,
                self.duplicates,
                format_bytes(self.duplicate_bytes)
            ),
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
        } else if force || self.last_draw.elapsed() >= Duration::from_secs(5) {
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

#[tokio::main]
async fn main() -> Result<()> {
    let mut arguments = std::env::args_os().skip(1).collect::<Vec<_>>();
    let db_path = take_db_path(&mut arguments)?;
    let command = arguments.first().cloned().unwrap_or_else(|| "dedup".into());
    if command == "install" {
        if arguments.len() > 1 {
            return Err("usage: btrfs-dup install".into());
        }
        return install();
    }
    let mut args = arguments.into_iter().skip(1);
    let db: Surreal<Db> = Surreal::new::<surrealdb::engine::local::SurrealKv>(db_path).await?;
    db.use_ns("btrfs_dup").use_db("index").await?;
    db.query("DEFINE TABLE IF NOT EXISTS scan SCHEMALESS")
        .await?;
    db.query("DEFINE TABLE IF NOT EXISTS block SCHEMALESS")
        .await?;
    db.query("DEFINE TABLE IF NOT EXISTS scanned_file SCHEMALESS")
        .await?;
    db.query("DEFINE TABLE IF NOT EXISTS duplicate SCHEMALESS")
        .await?;
    db.query("DEFINE INDEX IF NOT EXISTS block_scan ON TABLE block FIELDS scan")
        .await?;
    db.query("DEFINE INDEX IF NOT EXISTS block_hash ON TABLE block FIELDS len, xxh3")
        .await?;
    db.query("DEFINE INDEX IF NOT EXISTS scanned_file_scan ON TABLE scanned_file FIELDS scan")
        .await?;
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
                let mut scan: Scan = db
                    .select(("scan", id.as_str()))
                    .await?
                    .ok_or("scan not found")?;
                scan.scan_id = Some(id.clone());
                scan.state = "running".into();
                let _: Option<Scan> = db
                    .update(("scan", id.as_str()))
                    .content(scan.clone())
                    .await?;
                return run_scan(&db, scan).await;
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
            let scan = create_scan(&db, &source, dry_run).await?;
            run_scan(&db, scan).await
        }
        "resume" => {
            let id = args
                .next()
                .ok_or("usage: btrfs_dup resume <scan-id>")?
                .to_string_lossy()
                .into_owned();
            let mut scan: Scan = db
                .select(("scan", id.as_str()))
                .await?
                .ok_or("scan not found")?;
            scan.scan_id = Some(id.clone());
            scan.state = "running".into();
            let _: Option<Scan> = db
                .update(("scan", id.as_str()))
                .content(scan.clone())
                .await?;
            run_scan(&db, scan).await
        }
        "list" => {
            let scans: Vec<ScanListRow> = db.select("scan").await?;
            let duplicates: Vec<Duplicate> = db.select("duplicate").await?;
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
                let id = s.scan_id.unwrap_or_else(|| match s.id.key {
                    RecordIdKey::String(id) => id,
                    key => format!("{key:?}"),
                });
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
            let s: Option<Scan> = db.select(("scan", id.as_str())).await?;
            let mut s = s.ok_or("scan not found")?;
            s.scan_id = Some(id);
            println!("{s:#?}");
            Ok(())
        }
        _ => usage(),
    }
}

fn usage<T>() -> Result<T> {
    Err("usage: btrfs-dup [--db <path>] [dedup [<mount-path|/dev/device>] [--dry-run] | dedup --resume <scan-id> | list | status <scan-id> | resume <scan-id> | install]".into())
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
            db_path = Some(PathBuf::from(arguments.remove(index + 1)));
            arguments.remove(index);
        } else {
            index += 1;
        }
    }
    Ok(db_path.unwrap_or_else(|| PathBuf::from("btrfs-dup.db")))
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
complete -c btrfs-dup -f -n '__fish_use_subcommand' -a 'dedup list status resume install'
complete -c btrfs-dup -n '__fish_seen_subcommand_from dedup' -l dry-run -d 'Index without deduplicating file contents'
complete -c btrfs-dup -n '__fish_seen_subcommand_from dedup' -l resume -d 'Resume a scan by ID'
complete -c btrfs-dup -n '__fish_seen_subcommand_from resume status' -a '(btrfs-dup list 2>/dev/null | cut -f1)'
"#;

const BASH_COMPLETION: &str = r#"
_btrfs_dup_completion() {
    local cur prev
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"
    if (( COMP_CWORD == 1 )); then
        COMPREPLY=( $(compgen -W "dedup list status resume install" -- "$cur") )
        return
    fi
    case "$prev" in
        dedup)
            COMPREPLY=( $(compgen -W "--dry-run --resume" -- "$cur") )
            if [[ "$cur" != -* ]]; then
                COMPREPLY+=( $(compgen -f -- "$cur") )
            fi
            ;;
        resume|status|--resume)
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
        'resume:Resume a scan'
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
        resume|status)
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

async fn create_scan(db: &Surreal<Db>, source: &Path, dry_run: bool) -> Result<Scan> {
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
    let _: Option<Scan> = db
        .create(("scan", id.as_str()))
        .content(scan.clone())
        .await?;
    Ok(scan)
}

async fn run_scan(db: &Surreal<Db>, mut scan: Scan) -> Result<()> {
    let snapshot = PathBuf::from(&scan.snapshot);
    let source = PathBuf::from(&scan.source);
    println!("Counting files and planned blocks in the snapshot...");
    let mut inventory = Inventory::default();
    inventory_directory(&snapshot, &snapshot, &mut inventory)?;
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
    let prior: Vec<Block> = db
        .query("SELECT * FROM block WHERE scan = $id")
        .bind(("id", scan.key().to_owned()))
        .await?
        .take(0)?;
    let mut batch = WriteBatch::new();
    scan.blocks = prior.len() as u64;
    let prior_files: Vec<ScannedFile> = db
        .query("SELECT * FROM scanned_file WHERE scan = $id")
        .bind(("id", scan.key().to_owned()))
        .await?
        .take(0)?;
    let mut completed_paths = HashSet::new();
    let mut completed_bytes = 0;
    let mut completed_blocks = 0;
    for file in &prior_files {
        let path_bytes = file.path.to_vec();
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
            blocks_for_size(size)
        };
        completed_paths.insert(path_bytes);
        completed_bytes += size;
        completed_blocks += block_count;
    }
    scan.files = completed_paths.len() as u64;
    for b in prior {
        let xxh3 = u64::from_be_bytes(
            b.xxh3
                .as_ref()
                .try_into()
                .map_err(|_| "stored XXH3 hash must contain exactly 8 bytes")?,
        );
        let relative = PathBuf::from(std::ffi::OsString::from_vec(b.path.to_vec()));
        let absolute = snapshot.join(&relative);
        if b.blake3.is_none() {
            let block_key = block_id(scan.key(), b.path.as_ref(), b.offset);
            batch.collision_candidates.insert(block_key, b.clone());
        }
        buckets.entry((b.len, xxh3)).or_default().push((
            absolute,
            b.offset,
            b.blake3.map(|digest| digest.to_vec()),
        ));
    }
    let prior_duplicates: Vec<Duplicate> = db
        .query("SELECT * FROM duplicate WHERE scan = $id")
        .bind(("id", scan.key().to_owned()))
        .await?
        .take(0)?;
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
        db,
        &source,
        &mut scan,
        &mut buckets,
        &mut progress,
        &mut completed_paths,
        &mut batch,
        pending_files,
    )
    .await?;
    batch.flush(db, &scan).await?;
    progress.draw(true)?;
    scan.state = "complete".into();
    let _: Option<Scan> = db
        .update(("scan", scan.key()))
        .content(scan.clone())
        .await?;
    Ok(())
}

fn inventory_directory(root: &Path, dir: &Path, inventory: &mut Inventory) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if dir == root && path.file_name() == Some(std::ffi::OsStr::new(SNAPSHOT_DIR)) {
            continue;
        }
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            continue;
        }
        if metadata.is_dir() {
            inventory_directory(root, &path, inventory)?;
        } else if metadata.is_file() {
            inventory.files += 1;
            inventory.bytes += metadata.len();
            inventory.blocks += blocks_for_size(metadata.len());
            inventory
                .paths
                .push((path.clone(), path.strip_prefix(root)?.to_path_buf()));
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

fn blocks_for_size(size: u64) -> u64 {
    if size == 0 {
        return 0;
    }
    match block_decision_function(size) {
        BlockDecision::WholeFile => 1,
        BlockDecision::FixedChunks(chunk_size) => size.div_ceil(chunk_size as u64),
    }
}

async fn scan_files_parallel(
    db: &Surreal<Db>,
    source: &Path,
    scan: &mut Scan,
    buckets: &mut HashMap<(usize, u64), Vec<(PathBuf, u64, Option<Vec<u8>>)>>,
    progress: &mut ScanProgress,
    completed_paths: &mut HashSet<Vec<u8>>,
    batch: &mut WriteBatch,
    pending_files: Vec<(PathBuf, PathBuf)>,
) -> Result<()> {
    let available = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(2);
    let worker_count = available.saturating_sub(1).max(1).min(8);
    println!("Hashing files with {worker_count} workers (reserving one core where possible).");

    let mut jobs: VecDeque<_> = pending_files.into();
    let bytes_read = Arc::new(AtomicU64::new(progress.bytes));
    let mut workers = tokio::task::JoinSet::new();
    let mut ticker = tokio::time::interval(Duration::from_millis(200));

    while !jobs.is_empty() || !workers.is_empty() {
        while workers.len() < worker_count {
            if jobs.is_empty() {
                break;
            }
            let worker_jobs = jobs
                .drain(..jobs.len().min(HASH_WORKER_BATCH_FILES))
                .collect::<Vec<_>>();
            let bytes_read = Arc::clone(&bytes_read);
            let worker_db = db.clone();
            let scan_id = scan.key().to_owned();
            workers.spawn(async move {
                let hashed_files = tokio::task::spawn_blocking(move || {
                    worker_jobs
                        .into_iter()
                        .map(|(path, relative)| hash_file(path, relative, Arc::clone(&bytes_read)))
                        .collect::<std::io::Result<Vec<_>>>()
                })
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
                persist_hashed_blocks(&worker_db, &scan_id, &hashed_files).await?;
                Ok::<_, String>(hashed_files)
            });
        }
        tokio::select! {
            joined = workers.join_next(), if !workers.is_empty() => {
                let hashed_files = joined
                    .ok_or("hash worker stopped unexpectedly")?
                    .map_err(|error| error.to_string())?
                    .map_err(std::io::Error::other)?;
                progress.bytes = bytes_read.load(Ordering::Relaxed);
                for hashed in hashed_files {
                    let writable = source.join(&hashed.relative);
                    let relative_bytes = path_bytes(&hashed.relative);
                    let file_key = block_id(scan.key(), &relative_bytes, 0);
                    let blocks_before = progress.blocks;
                    index_hashed_file(scan, &hashed, &writable, buckets, progress, batch)?;
                    progress.files += 1;
                    scan.files = progress.files;
                    scan.blocks = progress.blocks;
                    batch.files.insert(file_key, ScannedFile {
                        scan: scan.key().to_owned(),
                        path: Bytes::from(relative_bytes.clone()),
                        bytes: hashed.size,
                        blocks: progress.blocks - blocks_before,
                    });
                    completed_paths.insert(relative_bytes);
                    progress.draw(false)?;
                    if batch.last_flush.elapsed() >= Duration::from_secs(30) {
                        batch.flush(db, scan).await?;
                    }
                }
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
    db: &Surreal<Db>,
    scan_id: &str,
    files: &[HashedFile],
) -> std::result::Result<(), String> {
    let records = files
        .iter()
        .flat_map(|file| {
            let path = path_bytes(&file.relative);
            file.blocks.iter().map(move |block| {
                let id = block_id(scan_id, &path, block.offset);
                BatchRecord {
                    id,
                    content: Block {
                        scan: scan_id.to_owned(),
                        xxh3: Bytes::from(block.xxh3.to_be_bytes().to_vec()),
                        len: block.len,
                        path: Bytes::from(path.clone()),
                        offset: block.offset,
                        blake3: None,
                    }
                    .into_value(),
                }
            })
        })
        .collect::<Vec<_>>()
        .into_value();
    let db = db.clone();
    tokio::task::spawn_blocking(move || {
        tokio::runtime::Handle::current().block_on(async move {
            db.query(
                "FOR $item IN $blocks { UPSERT type::record('block', $item.id) CONTENT $item.content; };",
            )
            .bind(("blocks", records))
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
        })
    })
    .await
    .map_err(|error| error.to_string())?
}

fn hash_file(
    path: PathBuf,
    relative: PathBuf,
    bytes_read: Arc<AtomicU64>,
) -> std::io::Result<HashedFile> {
    let mut file = File::open(&path)?;
    let size = file.metadata()?.len();
    let chunk_size = match block_decision_function(size) {
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
        let mut hasher = XxHash3_64::default();
        hasher.write(&buffer[..len]);
        blocks.push(HashedBlock {
            offset,
            len,
            xxh3: hasher.finish(),
        });
        offset += len as u64;
    }
    Ok(HashedFile {
        path,
        relative,
        size,
        blocks,
    })
}

fn index_hashed_file(
    scan: &mut Scan,
    file: &HashedFile,
    writable: &Path,
    buckets: &mut HashMap<(usize, u64), Vec<(PathBuf, u64, Option<Vec<u8>>)>>,
    progress: &mut ScanProgress,
    batch: &mut WriteBatch,
) -> Result<()> {
    for hashed_block in &file.blocks {
        let offset = hashed_block.offset;
        let len = hashed_block.len;
        let xxh3 = hashed_block.xxh3;
        let key = (len, xxh3);
        let members = buckets.entry(key).or_default();
        let same_block = members
            .iter()
            .find(|m| m.0 == file.path && m.1 == offset)
            .and_then(|m| m.2.clone());
        let has_other_candidate = members.iter().any(|m| m.0 != file.path || m.1 != offset);
        let strong = if !has_other_candidate {
            same_block
        } else {
            let digest = hash_range(&file.path, offset, len)?;
            if members.iter().any(|m| m.2.is_none()) {
                for member in members.iter_mut().filter(|m| m.2.is_none()) {
                    let old_digest = hash_range(&member.0, member.1, len)?;
                    member.2 = Some(old_digest.clone());
                    let old_relative = member.0.strip_prefix(&scan.snapshot)?;
                    let old_id =
                        block_id(scan.key(), path_bytes(old_relative).as_slice(), member.1);
                    if let Some(old_record) = batch.blocks.get_mut(&old_id) {
                        old_record.blake3 = Some(Bytes::from(old_digest));
                        batch.collision_candidates.remove(&old_id);
                    } else {
                        let old_record = batch
                            .collision_candidates
                            .remove(&old_id)
                            .ok_or("collision block missing from loaded index")?;
                        batch.blocks.insert(
                            old_id,
                            Block {
                                blake3: Some(Bytes::from(old_digest)),
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
            xxh3: Bytes::from(xxh3.to_be_bytes().to_vec()),
            len,
            path: Bytes::from(raw_path),
            offset,
            blake3: strong.clone().map(Bytes::from),
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
                if candidate_snapshot == &file.path && *candidate_offset == offset {
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
            let duplicate_source =
                members
                    .iter()
                    .find(|(candidate_path, candidate_offset, candidate_digest)| {
                        !(candidate_path == &file.path && *candidate_offset == offset)
                            && candidate_digest.as_deref() == Some(digest.as_slice())
                    });
            if let Some((candidate_snapshot, candidate_offset, _)) = duplicate_source {
                progress.duplicates += 1;
                progress.duplicate_bytes += len as u64;
                let relative = candidate_snapshot.strip_prefix(&scan.snapshot)?;
                let candidate_live = PathBuf::from(&scan.source).join(relative);
                let deduped_bytes = if scan.dry_run {
                    0
                } else {
                    dedupe_live_ranges(writable, offset, &candidate_live, *candidate_offset, len)?
                };
                let duplicate_path = path_bytes(&file.relative);
                let duplicate_id = block_id(scan.key(), &duplicate_path, offset);
                batch.duplicates.insert(
                    duplicate_id,
                    Duplicate {
                        scan: scan.key().to_owned(),
                        duplicate_path: Bytes::from(duplicate_path),
                        duplicate_offset: offset,
                        source_path: Bytes::from(path_bytes(relative)),
                        source_offset: *candidate_offset,
                        len: len as u64,
                        deduped_bytes,
                        dry_run: scan.dry_run,
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

fn dedupe_live_ranges(
    source_path: &Path,
    source_offset: u64,
    dest_path: &Path,
    dest_offset: u64,
    len: usize,
) -> Result<u64> {
    if len % 4096 != 0 || source_path == dest_path && source_offset == dest_offset {
        return Ok(0);
    }
    let source = File::open(source_path)?;
    let dest = File::options().read(true).write(true).open(dest_path)?;
    // SAFETY: `dest` remains open until after the ioctl returns.
    let dest_fd = unsafe { BorrowedFd::borrow_raw(dest.as_raw_fd()) };
    let targets = [DedupeTarget {
        fd: dest_fd,
        logical_offset: dest_offset,
    }];
    let results = file_extent_same(source.as_fd(), source_offset, len as u64, &targets)?;
    match results[0] {
        DedupeResult::Deduped(bytes) => Ok(bytes),
        DedupeResult::DataDiffers => Ok(0),
        DedupeResult::Error(code) => Err(format!("Btrfs dedupe target error: {code}").into()),
    }
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
    fn database_path_option_is_removed_from_command_arguments() {
        let mut arguments = vec!["--db".into(), "/var/lib/btrfs-dup".into(), "list".into()];

        let path = take_db_path(&mut arguments).expect("parse database path");

        assert_eq!(path, PathBuf::from("/var/lib/btrfs-dup"));
        assert_eq!(arguments, vec!["list"]);
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
        let db: Surreal<Db> = Surreal::new::<surrealdb::engine::local::SurrealKv>(path.clone())
            .await
            .expect("open temporary database");
        db.use_ns("test")
            .use_db("batch")
            .await
            .expect("select test database");
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
                xxh3: Bytes::from(1u64.to_be_bytes().to_vec()),
                len: 4096,
                path: Bytes::from(b"file".to_vec()),
                offset: 0,
                blake3: None,
            },
        );
        batch.files.insert(
            "file-1".into(),
            ScannedFile {
                scan: "scan-1".into(),
                path: Bytes::from(b"file".to_vec()),
                bytes: 4096,
                blocks: 1,
            },
        );
        batch.flush(&db, &scan).await.expect("flush batch");
        let block: Option<Block> = db
            .select(("block", "block-1"))
            .await
            .expect("select flushed block");
        assert!(block.is_some());
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
        persist_hashed_blocks(&db, "scan-1", &hashed)
            .await
            .expect("persist worker block batch");
        let worker_block_id = block_id("scan-1", b"worker-file", 0);
        let worker_block: Option<Block> = db
            .select(("block", worker_block_id.as_str()))
            .await
            .expect("select worker block");
        assert_eq!(worker_block.expect("worker row exists").len, 4096);
        drop(db);
        let _ = fs::remove_dir_all(path);
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
        assert_eq!(block_decision_function(0), BlockDecision::WholeFile);
        assert_eq!(
            block_decision_function(10 * MIB - 1),
            BlockDecision::WholeFile
        );
        assert_eq!(
            block_decision_function(10 * MIB),
            BlockDecision::FixedChunks(TEN_MIB)
        );
        assert_eq!(
            block_decision_function(2 * GIB - 1),
            BlockDecision::FixedChunks(TEN_MIB)
        );
        assert_eq!(
            block_decision_function(2 * GIB),
            BlockDecision::FixedChunks(HUNDRED_MIB)
        );
    }
}
impl SeekTo for File {
    fn seek_to(&mut self, offset: u64) -> std::io::Result<()> {
        use std::io::Seek;
        self.seek(std::io::SeekFrom::Start(offset)).map(|_| ())
    }
}

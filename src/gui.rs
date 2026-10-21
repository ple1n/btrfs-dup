use std::{path::PathBuf, sync::OnceLock};

use dioxus::prelude::*;

use crate::{Block, Duplicate, Scan, ScannedFile, format_bytes};

const PAGE_SIZE: usize = 50;
static DATABASE_PATH: OnceLock<PathBuf> = OnceLock::new();

struct BrowserRow {
    id: String,
    kind: String,
    scan: String,
    summary: String,
    duplicate: Option<Duplicate>,
    commited: bool,
}

struct BrowserPage {
    rows: Vec<BrowserRow>,
    has_next: bool,
}

pub fn launch(database_path: PathBuf) -> crate::Result<()> {
    DATABASE_PATH
        .set(database_path)
        .map_err(|_| "GUI database path was already initialized")?;
    let config = dioxus::desktop::Config::new().with_window(
        dioxus::desktop::WindowBuilder::new()
            .with_title("btrfs-dup | Database Browser")
            .with_inner_size(dioxus::desktop::LogicalSize::new(1280.0, 820.0)),
    );
    dioxus::LaunchBuilder::desktop()
        .with_cfg(config)
        .launch(App);
    Ok(())
}

#[component]
fn App() -> Element {
    let mut selected_scan = use_signal(|| None::<String>);
    let mut kind = use_signal(|| "scan".to_owned());
    let mut page = use_signal(|| 0usize);
    let database_path = DATABASE_PATH
        .get()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "unknown database".to_owned());

    let browser = use_resource(move || {
        let selected_scan = selected_scan.read().clone();
        let kind = kind.read().clone();
        let page = page();
        let database_path = DATABASE_PATH.get().cloned().unwrap_or_default();
        async move { load_page_off_ui(database_path, selected_scan, kind, page).await }
    });

    let current_scan = selected_scan.read().clone();
    let current_kind = kind.read().clone();
    let current_page = page();
    let page_content = match &*browser.read_unchecked() {
        Some(Ok(data)) => {
            let start = current_page * PAGE_SIZE + 1;
            let end = current_page * PAGE_SIZE + data.rows.len();
            let range_label = if data.rows.is_empty() {
                "No records".to_owned()
            } else {
                format!("Showing {start}-{end}")
            };
            let page_label = if data.has_next {
                format!("Page {} (more available)", current_page + 1)
            } else {
                format!("Page {} (last page)", current_page + 1)
            };
            rsx! {
            div { style: "display:flex; justify-content:space-between; align-items:center; margin:16px 0 10px; color:#94a3b8; font-size:13px;",
                span { "{range_label}" }
                span { "{page_label}" }
            }
            if current_kind == "duplicate" {
            div { style: "overflow:auto; border:1px solid #263244; border-radius:10px; background:#111a27;",
                table { style: "width:100%; border-collapse:collapse; text-align:left; font-size:13px;",
                    thead {
                        tr { style: "color:#94a3b8; background:#172233;",
                            th { style: "padding:12px 14px; width:50%;", "Source" }
                            th { style: "padding:12px 14px; width:50%;", "Target" }
                        }
                    }
                    tbody {
                        for row in data.rows.iter() {
                            if let Some(duplicate) = &row.duplicate {
                                tr { key: "{row.id}", style: "border-top:1px solid #263244; color:#dbe5f2;",
                                    td { style: "padding:12px 14px; min-width:300px; font-family:monospace; overflow-wrap:anywhere;",
                                        div { "{String::from_utf8_lossy(&duplicate.source_path)}" }
                                        div { style: "margin-top:5px; color:#94a3b8; font-size:12px; white-space:nowrap;",
                                            "({format_bytes(duplicate.source_offset)}, {format_bytes(duplicate.source_offset.saturating_add(duplicate.len))})"
                                        }
                                    }
                                    td { style: "padding:12px 14px; min-width:300px; font-family:monospace; overflow-wrap:anywhere;",
                                        div { "{String::from_utf8_lossy(&duplicate.duplicate_path)}" }
                                        div { style: "margin-top:5px; color:#94a3b8; font-size:12px; white-space:nowrap;",
                                            "({format_bytes(duplicate.duplicate_offset)}, {format_bytes(duplicate.duplicate_offset.saturating_add(duplicate.len))}) · {format_bytes(duplicate.len)}"
                                        }
                                        div { style: "margin-top:4px; color:#94a3b8; font-size:12px;",
                                            "Deduped {format_bytes(duplicate.deduped_bytes)} · "
                                            if row.commited {
                                                "Commited"
                                            } else {
                                                "Not commited"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            } else {
            div { style: "overflow:auto; border:1px solid #263244; border-radius:10px; background:#111a27;",
                table { style: "width:100%; border-collapse:collapse; text-align:left; font-size:13px;",
                    thead {
                        tr { style: "color:#94a3b8; background:#172233;",
                            th { style: "padding:12px 14px;", "Kind" }
                            th { style: "padding:12px 14px;", "Record ID" }
                            th { style: "padding:12px 14px;", "Details" }
                            if current_scan.is_none() {
                                th { style: "padding:12px 14px;", "Open" }
                            }
                        }
                    }
                    tbody {
                        for row in data.rows.iter() {
                            tr { key: "{row.id}", style: "border-top:1px solid #263244; color:#dbe5f2;",
                                td { style: "padding:12px 14px; white-space:nowrap;",
                                    span { style: "color:#7dd3fc; font-weight:600;", "{row.kind}" }
                                }
                                td { style: "padding:12px 14px; font-family:monospace; color:#a5b4c8;", "{row.id}" }
                                td { style: "padding:12px 14px; min-width:300px;", "{row.summary}" }
                                if current_scan.is_none() && row.kind == "scan" {
                                    td { style: "padding:12px 14px;",
                                        button {
                                            style: "padding:6px 12px; border:1px solid #334155; border-radius:6px; color:#dbeafe; background:#1e293b; cursor:pointer;",
                                            onclick: {
                                                let scan_id = row.scan.clone();
                                                move |_| {
                                                    selected_scan.set(Some(scan_id.clone()));
                                                    kind.set("all".to_owned());
                                                    page.set(0);
                                                }
                                            },
                                            "Browse"
                                        }
                                    }
                                } else if current_scan.is_none() {
                                    td { style: "padding:12px 14px; color:#64748b;", "—" }
                                }
                            }
                        }
                    }
                }
            }
            }
            div { style: "display:flex; justify-content:flex-end; align-items:center; gap:10px; margin-top:14px;",
                button {
                    disabled: current_page == 0,
                    style: "padding:8px 14px; border:1px solid #334155; border-radius:7px; color:#e2e8f0; background:#172233; cursor:pointer;",
                    onclick: move |_| page.set(current_page.saturating_sub(1)),
                    "Previous"
                }
                button {
                    disabled: !data.has_next,
                    style: "padding:8px 14px; border:1px solid #334155; border-radius:7px; color:#e2e8f0; background:#172233; cursor:pointer;",
                    onclick: move |_| page.set(current_page + 1),
                    "Next"
                }
            }
            }
        }
        Some(Err(error)) => rsx! {
            div { style: "margin-top:20px; padding:16px; border:1px solid #7f1d1d; border-radius:10px; color:#fecaca; background:#2a151b;",
                "Could not browse database: {error}"
            }
        },
        None => rsx! {
            div { style: "padding:30px; color:#94a3b8; text-align:center;", "Loading database page..." }
        },
    };

    rsx! {
        div { style: "display:flex; min-height:100vh; color:#e2e8f0; background:#0b1220; font-family:Inter,ui-sans-serif,system-ui,sans-serif;",
            style { "html, body {{ margin: 0; padding: 0; }}" }
            aside { style: "display:flex; flex-direction:column; width:230px; flex-shrink:0; padding:22px 16px; border-right:1px solid #263244; background:#0f1724;",
                div { style: "margin:0 8px 28px; font-size:19px; font-weight:750; letter-spacing:.2px; color:#f8fafc;",
                    "btrfs-dup"
                }
                div { style: "margin:0 8px 9px; color:#64748b; font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:1px;",
                    "Workspace"
                }
                button { style: "margin-bottom:20px; padding:10px 11px; border:1px solid #334155; border-radius:8px; color:#bae6fd; background:#132538; text-align:left; cursor:pointer;",
                    "▦  Database browser"
                }
                div { style: "margin:0 8px 9px; color:#64748b; font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:1px;",
                    "Commands"
                }
                for (label, detail) in [
                    ("Scan / dedup", "TODO"),
                    ("Verify scan", "TODO"),
                    ("Commit scan", "TODO"),
                    ("Resume scan", "TODO"),
                    ("Scan status", "TODO"),
                ] {
                    button { disabled: true, style: "display:flex; justify-content:space-between; margin:2px 0; padding:10px 9px; border:0; border-radius:7px; color:#64748b; background:transparent; text-align:left;",
                        span { "{label}" }
                        span { style: "font-size:10px; color:#475569;", "{detail}" }
                    }
                }
                div { style: "margin-top:auto; padding:10px 8px; color:#64748b; font-size:11px; overflow-wrap:anywhere;",
                    "Database"
                    div { style: "margin-top:5px; color:#94a3b8; font-family:monospace;", "{database_path}" }
                }
            }
            main { style: "flex:1; min-width:0; box-sizing:border-box; padding:24px 32px;",
                div { style: "display:flex; justify-content:space-between; align-items:flex-start; gap:20px; margin-bottom:28px;",
                    div {
                        div { style: "margin-bottom:7px; color:#7dd3fc; font-size:12px; font-weight:700; text-transform:uppercase; letter-spacing:1.1px;",
                            "Local database"
                        }
                        h1 { style: "margin:0; color:#f8fafc; font-size:27px; letter-spacing:-.5px;",
                            if current_scan.is_some() { "Scan records" } else { "Database browser" }
                        }
                        p { style: "margin:8px 0 0; color:#94a3b8; font-size:14px;",
                            if let Some(scan_id) = current_scan.as_ref() {
                                "Browsing scan {scan_id}"
                            } else {
                                "Browse saved scans and indexed records."
                            }
                        }
                    }
                    if current_scan.is_some() {
                        button {
                            style: "padding:9px 13px; border:1px solid #334155; border-radius:7px; color:#dbeafe; background:#172233; cursor:pointer;",
                            onclick: move |_| {
                                selected_scan.set(None);
                                kind.set("scan".to_owned());
                                page.set(0);
                            },
                            "← All scans"
                        }
                    }
                }
                if current_scan.is_some() {
                    div { style: "display:flex; flex-wrap:wrap; gap:8px; margin-bottom:18px;",
                        for (label, filter) in [
                            ("All records", "all"),
                            ("Blocks", "block"),
                            ("Files", "file"),
                            ("Duplicates", "duplicate"),
                        ] {
                            button {
                                style: if current_kind == filter {
                                    "padding:8px 12px; border:1px solid #0284c7; border-radius:7px; color:#e0f2fe; background:#075985; cursor:pointer;"
                                } else {
                                    "padding:8px 12px; border:1px solid #334155; border-radius:7px; color:#cbd5e1; background:#111a27; cursor:pointer;"
                                },
                                onclick: move |_| {
                                    kind.set(filter.to_owned());
                                    page.set(0);
                                },
                                "{label}"
                            }
                        }
                    }
                }
                {page_content}
            }
        }
    }
}

fn load_page(
    database_path: PathBuf,
    scan: Option<String>,
    kind: String,
    page: usize,
) -> Result<BrowserPage, String> {
    let offset = page.saturating_mul(PAGE_SIZE);
    let records = crate::load_record_page(
        &database_path,
        scan.as_deref(),
        &kind,
        PAGE_SIZE + 1,
        offset,
    )
    .map_err(|error| error.to_string())?;
    let mut rows = decode_rows(&records).map_err(|error| error.to_string())?;
    let has_next = rows.len() > PAGE_SIZE;
    rows.truncate(PAGE_SIZE);
    Ok(BrowserPage { rows, has_next })
}

async fn load_page_off_ui(
    database_path: PathBuf,
    scan: Option<String>,
    kind: String,
    page: usize,
) -> Result<BrowserPage, String> {
    let (result_tx, result_rx) = futures::channel::oneshot::channel();
    std::thread::Builder::new()
        .name("btrfs-dup-db-browser".to_owned())
        .spawn(move || {
            let result = load_page(database_path, scan, kind, page);
            let _ = result_tx.send(result);
        })
        .map_err(|error| format!("could not start database worker thread: {error}"))?;
    result_rx
        .await
        .map_err(|_| "database worker stopped before returning the page".to_owned())?
}

fn decode_rows(
    records: &[crate::StoredRecord],
) -> Result<Vec<BrowserRow>, Box<dyn std::error::Error>> {
    let mut rows = Vec::with_capacity(records.len());
    for record in records {
        let kind = record.kind.clone();
        let payload = record.payload.as_str();
        let mut duplicate_row = None;
        let summary = match kind.as_str() {
            "scan" => {
                let scan: Scan = serde_json::from_str(payload)?;
                format!(
                    "{} · {} · {} files · {} blocks · dry-run={}",
                    scan.state, scan.source, scan.files, scan.blocks, scan.dry_run
                )
            }
            "block" => {
                let block: Block = serde_json::from_str(payload)?;
                format!(
                    "{} @ {} · {} · XXH3 {:016x} · BLAKE3 {}",
                    String::from_utf8_lossy(&block.path),
                    block.offset,
                    format_bytes(block.len as u64),
                    block.xxh3,
                    if block.blake3.is_some() {
                        "present"
                    } else {
                        "pending"
                    }
                )
            }
            "file" => {
                let file: ScannedFile = serde_json::from_str(payload)?;
                format!(
                    "{} · {} · {} blocks",
                    String::from_utf8_lossy(&file.path),
                    format_bytes(file.bytes),
                    file.blocks
                )
            }
            "duplicate" => {
                let mut duplicate: Duplicate = serde_json::from_str(payload)?;
                duplicate.commited = record.commited;
                let summary = format!(
                    "{} @ {} ↔ {} @ {} · {} · deduped {}",
                    String::from_utf8_lossy(&duplicate.duplicate_path),
                    duplicate.duplicate_offset,
                    String::from_utf8_lossy(&duplicate.source_path),
                    duplicate.source_offset,
                    format_bytes(duplicate.len),
                    format_bytes(duplicate.deduped_bytes)
                );
                duplicate_row = Some(duplicate);
                summary
            }
            other => format!(
                "unknown kind {other} · len={} · XXH3 {:016x}",
                record.len, record.xxh3
            ),
        };
        rows.push(BrowserRow {
            id: record.id.clone(),
            kind,
            scan: record.scan.clone(),
            summary,
            duplicate: duplicate_row,
            commited: record.commited,
        });
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn scan_browser_pages_rows_without_loading_the_whole_table() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "btrfs-dup-gui-pagination-{}-{unique}",
            std::process::id()
        ));
        let store = crate::open_store(&path).expect("open temporary SQLite database");
        let records = (0..=PAGE_SIZE)
            .map(|index| {
                let id = format!("scan-{index:03}");
                let scan = Scan {
                    scan_id: Some(id.clone()),
                    source: format!("/source/{index}"),
                    snapshot: format!("/snapshot/{index}"),
                    state: "complete".to_owned(),
                    dry_run: true,
                    files: 1,
                    blocks: 1,
                };
                crate::stored_record(&crate::scan_record_id(&id), "scan", &id, 0, &[], &scan)
            })
            .collect::<crate::Result<Vec<_>>>()
            .expect("encode scans");
        crate::write_records(&store, records).expect("insert scans");
        drop(store);

        let first = load_page_off_ui(path.clone(), None, "scan".to_owned(), 0)
            .await
            .expect("load first page");
        let second = load_page_off_ui(path.clone(), None, "scan".to_owned(), 1)
            .await
            .expect("load second page");
        assert_eq!(first.rows.len(), PAGE_SIZE);
        assert!(first.has_next);
        assert_eq!(second.rows.len(), 1);
        assert!(!second.has_next);

        let _ = std::fs::remove_file(path);
    }
}

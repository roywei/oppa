//! Forgetting a text (M-10, scenario 10). `preview` lists the places that
//! hold it without changing anything; `forget`, given that preview's digest,
//! removes it from the places it covers, checks none still holds it, and
//! reports where it is still found.
//!
//! Covered: memory notes and their branch history (`vault::forget`); message
//! bodies and conversation titles, each replaced whole by a dated tombstone,
//! so an event keeps its place and author; and the native session files of
//! the sessions in OPPA's sessions table, found by exact id and deleted with
//! their row, so the next turn starts fresh. That table also keeps every
//! other session a turn or task used, which no turn resumes, and a forget
//! reads the record of a run serve did not see end before a turn keeps its
//! sessions there.
//! Other database columns, pages the database has not reused (checked after a
//! forget), run records, task boards, logs, private configuration, reflogs and
//! objects no branch reaches are only reported.

use crate::{
    config::Paths,
    vault::{Vault, VaultCopies},
};
use anyhow::{ensure, Context, Result};
use rusqlite::{params, types::FromSql, Connection, Params};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Debug, Serialize)]
pub struct Report {
    /// Confirms that the places a forget removes are those the owner saw;
    /// keyed with the instance's secret, so no guess of the text matches it.
    pub digest: String,
    /// Places a forget removes the text from, or removed it from.
    pub covered: Vec<String>,
    /// Places that hold the text and that a forget leaves as they are.
    pub uncovered: Vec<String>,
    /// Why a forget cannot run now; empty when it can.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub blocked: Vec<String>,
}

/// What holds a text, and by exact id what in the database a forget changes.
struct Scan {
    report: Report,
    events: Vec<i64>,
    titles: Vec<String>,
    sessions: Vec<(String, String, String)>,
}

/// Where the text is, without changing anything. A turn running in any
/// conversation but `own` blocks a forget: it could write the text back.
pub fn preview(paths: &Paths, text: &str, own: &[String]) -> Result<Report> {
    let vault = Vault::open(&paths.vault())?.find_text(text)?;
    let running = turns_running(paths, own)?.1;
    Ok(scan(paths, text, &vault, running)?.report)
}

/// Remove the text from every covered place in the preview whose digest is
/// `digest`, then check none holds it. `start` runs once that preview is
/// confirmed, before the first change.
pub fn forget(
    paths: &Paths,
    text: &str,
    digest: &str,
    own: &[String],
    start: impl FnOnce() -> Result<()>,
) -> Result<Report> {
    // No other turn starts, or saves a session, until the forget ends.
    let (_forgetting, running) = turns_running(paths, own)?;
    let tombstone = tombstone();
    let mut approved = None;
    let vault = Vault::open(&paths.vault())?.forget_text(text, &tombstone, |vault| {
        let scan = scan(paths, text, vault, running)?;
        ensure!(
            scan.report.digest == digest,
            "the places changed since the preview; preview again"
        );
        ensure!(scan.report.blocked.is_empty(), "blocked; preview again");
        start()?;
        approved = Some(scan);
        Ok(())
    })?;
    let before = approved.context("the vault was not checked")?;
    ensure!(
        vault.notes.is_empty() && vault.history.is_empty(),
        "the vault still holds the text"
    );
    let database = open(paths)?;
    database.pragma_update(None, "secure_delete", "ON")?;
    // A native session goes before its row, so a failure leaves the row to
    // find it again; a row whose files are gone only starts a fresh session.
    for (_, _, handle) in &before.sessions {
        remove_native_session(handle)?;
    }
    // Only what the preview named: anything newer is reported below.
    let transaction = database.unchecked_transaction()?;
    for seq in &before.events {
        let event = "UPDATE events SET body=?2, payload_digest=?4
            WHERE seq=?1 AND instr(body,?3)>0";
        let forgotten = crate::store::FORGOTTEN;
        transaction.execute(event, params![seq, tombstone, text, forgotten])?;
    }
    for id in &before.titles {
        let title = "UPDATE conversations SET title=?2 WHERE id=?1 AND instr(title,?3)>0";
        transaction.execute(title, params![id, tombstone, text])?;
    }
    for (conversation, agent, handle) in &before.sessions {
        let session = "DELETE FROM sessions WHERE conversation=?1 AND agent=?2 AND handle=?3";
        transaction.execute(session, params![conversation, agent, handle])?;
    }
    transaction.commit()?;
    // The search index and the write-ahead log keep old pages until merged
    // and checkpointed; secure_delete zeroes what they free.
    database.execute("INSERT INTO events_fts(events_fts) VALUES('optimize')", [])?;
    database.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))?;
    let vault = Vault::open(&paths.vault())?.find_text(text)?;
    let after = scan(paths, text, &vault, vec![])?.report;
    let seen = &before.report.covered;
    if let Some(left) = after.covered.iter().find(|place| seen.contains(place)) {
        anyhow::bail!("still holding the text: {left}");
    }
    let mut uncovered = after.uncovered;
    let newer = after.covered.into_iter();
    uncovered.extend(newer.map(|place| format!("{place}, new since the preview")));
    // The database's own files, which may keep a text in pages not yet reused.
    let pages = [paths.database(), paths.runtime.join("state.db-wal")];
    let pages = files_holding(&pages, &paths.runtime, text)?;
    uncovered.extend(pages.iter().map(|place| hide(place, text)));
    Ok(Report {
        uncovered,
        ..before.report
    })
}

fn scan(paths: &Paths, text: &str, vault: &VaultCopies, running: Vec<String>) -> Result<Scan> {
    let fits = text.chars().count() >= 3 && text.len() <= 1024;
    ensure!(
        fits && !text.contains(char::is_control) && !tombstone().contains(text),
        "the text to forget is one line of 3 characters to 1 KiB, not part of a tombstone"
    );
    let notes = vault.notes.iter().map(|path| format!("note {path}"));
    let mut covered: Vec<_> = notes.chain(vault.history.iter().cloned()).collect();
    let mut uncovered = vault.elsewhere.clone();
    let database = open(paths)?;
    let mut blocked = running;
    let sending = "SELECT format('message %d is still being sent',e.seq) FROM events e
        JOIN outbox o ON o.event_seq=e.seq WHERE instr(e.body,?1)>0 AND o.state IN ('queued','sending')";
    blocked.extend(column(&database, sending, [text])?);
    let matching = "FROM events WHERE instr(body,?1)>0 ORDER BY seq";
    let events = column(&database, &format!("SELECT seq {matching}"), [text])?;
    let messages =
        format!("SELECT format('message %d in %s by %s',seq,conversation,author) {matching}");
    covered.extend(column::<String>(&database, &messages, [text])?);
    let titles = "SELECT id FROM conversations WHERE instr(title,?1)>0 ORDER BY id";
    let titles: Vec<String> = column(&database, titles, [text])?;
    covered.extend(titles.iter().map(|id| format!("title of {id}")));
    let sessions = sessions_holding(paths, &database, text)?;
    for (conversation, agent, _) in &sessions {
        covered.push(format!("native session of {conversation} with {agent}"));
    }
    // Every column but those a forget changes.
    let columns =
        "SELECT m.name||'.'||p.name AS c FROM sqlite_master m JOIN pragma_table_info(m.name) p
        WHERE m.type='table' AND m.name NOT LIKE 'sqlite%' AND m.name NOT LIKE 'events_fts%'
        AND c NOT IN ('events.body','conversations.title')";
    for column in column::<String>(&database, columns, [])? {
        let (table, name) = column.split_once('.').context("malformed column")?;
        let query =
            format!("SELECT count(*) FROM \"{table}\" WHERE instr(CAST(\"{name}\" AS TEXT),?1)>0");
        let count: i64 = database.query_row(&query, [text], |row| row.get(0))?;
        if count > 0 {
            uncovered.push(format!("database {column}, {count} rows"));
        }
    }
    let mut records = Vec::new();
    for area in ["runs", "tasks", "logs"] {
        records.extend(files(&paths.runtime.join(area))?);
    }
    uncovered.extend(files_holding(&records, &paths.runtime, text)?);
    let mut private = files(&paths.private)?;
    // A started knowledge wiki's pages are listed like any private file; its
    // Git internals are not, and a linked wiki is a symlink, not walked.
    private.retain(|file| {
        !file.starts_with(paths.vault())
            && !file.starts_with(paths.private.join(".git"))
            && !file.starts_with(paths.knowledge().join(".git"))
    });
    let private = files_holding(&private, &paths.private, text)?;
    uncovered.extend(private.into_iter().map(|place| format!("private {place}")));
    // A name that holds the text never shows it.
    for place in covered.iter_mut().chain(&mut uncovered).chain(&mut blocked) {
        *place = hide(place, text);
    }
    // A session's place names no handle, so the digest binds the handles.
    let places = serde_json::to_vec(&(text, &covered, &blocked, &sessions))?;
    let digest = Sha256::digest([secret(paths)?, places].concat());
    Ok(Scan {
        report: Report {
            digest: format!("{digest:x}"),
            covered,
            uncovered,
            blocked,
        },
        events,
        titles,
        sessions,
    })
}

/// The forget lock, held whole, and a place for each conversation but `own`
/// whose turn is running. Every turn takes the lock, shared, before its lease
/// (`runtime::conversation_lease`), so while it is held no turn starts: one
/// that tries waits. Each lease is taken and let go in turn, one file at a
/// time. A turn that took its lease before its conversation was recorded
/// left the lease's file.
fn turns_running(paths: &Paths, own: &[String]) -> Result<(File, Vec<String>)> {
    use crate::runtime::{forget_lock, lease_path, try_lease};
    let forgetting = forget_lock(paths)?;
    forgetting.lock()?;
    let lease = |id: &String| (lease_path(paths, id), id.clone());
    let recorded: Vec<String> = column(&open(paths)?, "SELECT id FROM conversations", [])?;
    let mut all: std::collections::BTreeMap<_, _> = recorded.iter().map(lease).collect();
    for file in files(&paths.runtime.join("locks"))? {
        let name = file.file_name().unwrap_or_default().to_string_lossy();
        if name.len() == 69 && name.ends_with(".lock") {
            all.entry(file)
                .or_insert_with(|| "a conversation not yet recorded".into());
        }
    }
    for id in own {
        all.remove(&lease_path(paths, id));
    }
    let mut running = Vec::new();
    for (path, id) in all {
        if try_lease(&path)?.is_none() {
            running.push(format!("a turn is running in {id}"));
        }
    }
    Ok((forgetting, running))
}

/// The instance's random secret for preview digests, made on first use and
/// kept in the private folder, outside its Git (`*.key`).
fn secret(paths: &Paths) -> Result<Vec<u8>> {
    use std::os::unix::fs::OpenOptionsExt;
    let path = paths.private.join("forget.key");
    if !path.exists() {
        let mut secret = [0u8; 32];
        File::open("/dev/urandom")?.read_exact(&mut secret)?;
        // A copy left by a stopped process stays out of Git too.
        let pid = std::process::id();
        let temporary = path.with_file_name(format!("forget-{pid}.key"));
        let mut options = fs::OpenOptions::new();
        let file = options.write(true).create_new(true).mode(0o600);
        file.open(&temporary)?.write_all(&secret)?;
        fs::rename(&temporary, &path)?;
    }
    let secret = fs::read(&path)?;
    ensure!(secret.len() == 32, "{} is damaged", path.display());
    Ok(secret)
}

/// `place` with the text, as written or escaped, left out.
fn hide(place: &str, text: &str) -> String {
    let [text, escaped] = forms(text);
    place
        .replace(&text, "[the text]")
        .replace(&escaped, "[the text]")
}

/// The text as written, and escaped in a JSON string.
fn forms(text: &str) -> [String; 2] {
    let quoted = serde_json::to_string(text).unwrap_or_default();
    [text.into(), quoted[1..quoted.len() - 1].into()]
}

/// Delete the native files of the session with exact id `handle`, whatever
/// they hold: a run that forgot something keeps the text in its own session.
pub fn remove_native_session(handle: &str) -> Result<()> {
    for file in native_files(handle)? {
        // A session's own folder goes whole, with every file in it.
        let target = file
            .ancestors()
            .find(|folder| folder.ends_with(handle))
            .unwrap_or(&file);
        let removed = match target.is_dir() {
            true => fs::remove_dir_all(target),
            false => fs::remove_file(target),
        };
        if let Err(error) = removed {
            ensure!(error.kind() == std::io::ErrorKind::NotFound, error);
        }
    }
    Ok(())
}

fn tombstone() -> String {
    format!("[forgotten {}]", chrono::Local::now().format("%Y-%m-%d"))
}

fn open(paths: &Paths) -> Result<Connection> {
    // The state store owns the schema; opening it first migrates an old one.
    crate::store::Store::open(&paths.database())?;
    let database = Connection::open(paths.database())?;
    database.busy_timeout(Duration::from_secs(5))?;
    Ok(database)
}

fn column<T: FromSql>(
    database: &Connection,
    query: &str,
    parameters: impl Params,
) -> Result<Vec<T>> {
    let mut statement = database.prepare(query)?;
    let rows = statement.query_map(parameters, |row| row.get(0))?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// Sessions whose native files hold the text: those in the table, and those
/// the record of a conversation's last run names that are not kept there yet.
/// Sorted, so a preview and its forget list them alike whether or not a turn
/// kept one in between.
fn sessions_holding(
    paths: &Paths,
    database: &Connection,
    text: &str,
) -> Result<Vec<(String, String, String)>> {
    let query = "SELECT conversation,agent,handle FROM sessions";
    let mut statement = database.prepare(query)?;
    let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
    let mut sessions = rows.collect::<rusqlite::Result<Vec<(String, String, String)>>>()?;
    for conversation in column::<String>(database, "SELECT id FROM conversations", [])? {
        let unsettled = crate::runtime::unsettled_sessions(paths, &conversation)?.into_iter();
        sessions.extend(unsettled.map(|(agent, handle)| (conversation.clone(), agent, handle)));
    }
    sessions.sort();
    sessions.dedup();
    let all = session_files()?;
    let mut found = Vec::new();
    for session in sessions {
        let files = all.get(&session.2).map_or(&[][..], Vec::as_slice);
        if !files_holding(files, Path::new(""), text)?.is_empty() {
            found.push(session);
        }
    }
    Ok(found)
}

/// The files a native harness keeps for the session with exact id `handle`.
fn native_files(handle: &str) -> Result<Vec<PathBuf>> {
    Ok(session_files()?.remove(handle).unwrap_or_default())
}

/// The files in the folders where native harnesses keep sessions, by the id
/// of the session each belongs to: Claude's `projects/<folder>/<id>.jsonl`
/// and what is in `<id>/` beside it, and Codex's
/// `sessions/<date>/rollout-<time>-<id>.jsonl`. A scan walks the folders
/// once and looks each session up, however many sessions are kept.
fn session_files() -> Result<HashMap<String, Vec<PathBuf>>> {
    let home = PathBuf::from(std::env::var_os("HOME").unwrap_or_default());
    let mut found: HashMap<String, Vec<PathBuf>> = HashMap::new();
    for (variable, folder, sessions) in [
        ("CLAUDE_CONFIG_DIR", ".claude", "projects"),
        ("CODEX_HOME", ".codex", "sessions"),
    ] {
        let root = std::env::var_os(variable).map_or(home.join(folder), PathBuf::from);
        let root = root.join(sessions);
        for file in files(&root)? {
            let relative = file.strip_prefix(&root).unwrap_or(&file);
            let names = relative
                .components()
                .map(|c| c.as_os_str().to_string_lossy());
            let mut ids: Vec<String> = names.map(String::from).filter(|n| session_id(n)).collect();
            let name = relative.file_name().unwrap_or_default().to_string_lossy();
            let stem = name.strip_suffix(".jsonl").unwrap_or_default();
            let cut = stem.len().saturating_sub(36);
            if let (Some(before), Some(id)) = (stem.get(..cut), stem.get(cut..)) {
                if session_id(id) && (before.is_empty() || before.ends_with('-')) {
                    ids.push(id.into());
                }
            }
            ids.sort();
            ids.dedup();
            for id in ids {
                found.entry(id).or_default().push(file.clone());
            }
        }
    }
    Ok(found)
}

/// Whether `text` has the shape of the UUIDs adapters report as session ids;
/// nothing else names a session's files.
fn session_id(text: &str) -> bool {
    text.len() == 36 && text.chars().all(|c| c.is_ascii_hexdigit() || c == '-')
}

/// The files that hold the text, each named relative to `root`.
fn files_holding(files: &[PathBuf], root: &Path, text: &str) -> Result<Vec<String>> {
    let forms = forms(text);
    let mut found = Vec::new();
    for file in files {
        let bytes = match fs::read(file) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            bytes => bytes?,
        };
        // As written, or escaped in a JSON string.
        let holds = |form: &String| position(&bytes, form.as_bytes()).is_some();
        if forms.iter().any(holds) {
            let place = file.strip_prefix(root).unwrap_or(file);
            found.push(format!("file {}", place.display()));
        }
    }
    Ok(found)
}

/// Where `needle` first is in `haystack`; an empty one is nowhere.
pub(crate) fn position(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    let mut windows = haystack.windows(needle.len().max(1));
    windows.position(|window| window == needle)
}

/// `bytes` with every `needle` replaced by `with`.
pub(crate) fn replaced(bytes: &[u8], needle: &[u8], with: &[u8]) -> Vec<u8> {
    let (mut out, mut rest) = (Vec::with_capacity(bytes.len()), bytes);
    while let Some(at) = position(rest, needle) {
        out.extend_from_slice(&rest[..at]);
        out.extend_from_slice(with);
        rest = &rest[at + needle.len()..];
    }
    out.extend_from_slice(rest);
    out
}

/// Regular files below `root`, sorted; symbolic links are not followed.
pub(crate) fn files(root: &Path) -> Result<Vec<PathBuf>> {
    let (mut found, mut pending) = (Vec::new(), vec![root.to_path_buf()]);
    while let Some(folder) = pending.pop() {
        let entries = match fs::read_dir(&folder) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            entries => entries.with_context(|| format!("read {}", folder.display()))?,
        };
        for entry in entries {
            let entry = entry?;
            match entry.file_type()? {
                kind if kind.is_dir() => pending.push(entry.path()),
                kind if kind.is_file() => found.push(entry.path()),
                _ => {}
            }
        }
    }
    found.sort();
    Ok(found)
}

//! `oppa memory import`: copy a folder of Markdown notes into the vault as one
//! managed commit, refusing the whole import when any target is taken.
use super::{valid_relative_path, validate_provenance, Edit, Provenance, Vault};
use crate::MAX_NOTE;
use anyhow::{ensure, Context, Result};
use std::{
    fs,
    io::{self, Read},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
};

/// An import this large holds the vault lock for seconds, mostly for the two
/// fsyncs each note gets.
const LARGE_IMPORT: usize = 500;

/// Plan the import of every note under `dir`, then make the plan one managed
/// commit unless this is a dry run or a target is taken or invalid.
pub fn run(
    vault: &Vault,
    dir: &Path,
    into: &str,
    provenance: &Provenance,
    dry_run: bool,
) -> Result<()> {
    // What the commit would refuse is refused first, so a dry run shows it.
    validate_provenance(provenance)?;
    let into = into.trim_end_matches('/');
    ensure!(
        valid_relative_path(into),
        "--into must be a vault folder such as imported/<source>"
    );
    let (mut notes, mut skipped) = (Vec::new(), Vec::new());
    collect(dir, Path::new(""), &mut notes, &mut skipped)?;
    if notes.len() > LARGE_IMPORT {
        eprintln!(
            "warning: the vault stays locked while {} notes are written; stop oppa serve and oppa work first so memory tools do not wait",
            notes.len()
        );
    }
    vault.check_ready()?;
    let revision = vault.revision()?;
    let (paths, edits): (Vec<_>, Vec<_>) = notes
        .into_iter()
        .map(|(file, content)| {
            let path = format!("{into}/{file}");
            (path.clone(), Edit::Write { path, content })
        })
        .unzip();
    let conflicts: Vec<_> = paths
        .iter()
        .zip(vault.new_note_conflicts(&revision, &paths)?)
        .filter_map(|(path, reason)| Some(serde_json::json!({"path":path,"reason":reason?})))
        .collect();
    // The commit is checked against the revision the plan was made on, so a
    // concurrent change refuses it instead of overwriting or adopting a note.
    let receipt = if conflicts.is_empty() && !edits.is_empty() && !dry_run {
        Some(vault.commit(&revision, &edits, provenance)?)
    } else {
        None
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "dry_run": dry_run,
            (if dry_run { "would_import" } else { "imported" }):
                if conflicts.is_empty() { edits.len() } else { 0 },
            "skipped": skipped.len(),
            "notes": edits.iter().map(Edit::path).collect::<Vec<_>>(),
            "skipped_files": skipped,
            "conflicts": conflicts,
            "receipt": receipt,
        }))?
    );
    ensure!(
        conflicts.is_empty(),
        "import refused: {} target(s) exist, differ only in case from another path, or fail the vault's path checks; nothing was written",
        conflicts.len()
    );
    ensure!(!edits.is_empty(), "no notes to import; nothing was written");
    Ok(())
}

/// Walk an import folder without following symlinks. Regular UTF-8 `.md` files
/// within the broker's note limit become notes; every other entry is listed.
fn collect(
    root: &Path,
    relative: &Path,
    notes: &mut Vec<(String, String)>,
    skipped: &mut Vec<serde_json::Value>,
) -> Result<()> {
    let folder = root.join(relative);
    let mut entries = fs::read_dir(&folder)
        .with_context(|| format!("cannot read {}", folder.display()))?
        .collect::<io::Result<Vec<_>>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let (name, kind) = (entry.file_name(), entry.file_type()?);
        let file = relative.join(&name);
        let reason = if name.to_string_lossy().starts_with('.') {
            "hidden".into()
        } else if kind.is_symlink() {
            "symlink".into()
        } else if name.to_str().is_none() {
            "name is not UTF-8".into()
        } else if name.to_string_lossy().contains([':', '\\', '\n', '\r']) {
            // Vault paths refuse these. A '/' typed in a Finder name is stored
            // as ':', so such a note is left out rather than the whole import.
            "name has a colon, backslash or line break".into()
        } else if kind.is_dir() {
            collect(root, &file, notes, skipped)?;
            continue;
        } else if !kind.is_file() {
            "not a regular file".into()
        } else if file.extension().and_then(|ext| ext.to_str()) != Some("md") {
            "not a .md file".into()
        } else {
            match read_note(&entry.path()).with_context(|| format!("cannot read {file:?}"))? {
                Ok(content) => {
                    notes.push((file.to_string_lossy().into_owned(), content));
                    continue;
                }
                Err(reason) => reason,
            }
        };
        skipped.push(serde_json::json!({"file":file.to_string_lossy(),"reason":reason}));
    }
    Ok(())
}

/// Read a note without following a symlink, judging the file that was opened
/// rather than the one listed, and reading at most one byte past the limit.
fn read_note(path: &Path) -> Result<std::result::Result<String, String>> {
    let file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let metadata = file.metadata()?;
    let mut content = Vec::new();
    Ok(if !metadata.is_file() {
        Err("not a regular file".into())
    } else if metadata.nlink() > 1 {
        Err("hard link".into())
    } else if file.take(MAX_NOTE as u64 + 1).read_to_end(&mut content)? > MAX_NOTE {
        Err(format!("larger than {} KiB", MAX_NOTE / 1024))
    } else {
        String::from_utf8(content).map_err(|_| "not UTF-8".into())
    })
}

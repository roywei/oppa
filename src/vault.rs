//! Owner-local Markdown files with Git revisions and recoverable managed edits.
//!
//! This library does not authenticate a caller. Expose it only behind the owner
//! boundary. Deletion and revert change the current tree, not Git history or
//! other logs, and are not secure erasure.

use anyhow::{anyhow, bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

mod forget;
pub use forget::VaultCopies;
mod git;
pub mod import;
mod names;

use git::{groups, Temporary};
use names::{clash, remember, Probe};

static TEMP_ID: AtomicU64 = AtomicU64::new(0);
const JOURNAL: &str = "oppa-vault-transaction.json";

#[derive(Debug, Clone)]
pub struct Vault {
    root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Edit {
    Write { path: String, content: String },
    Delete { path: String },
}

impl Edit {
    pub fn path(&self) -> &str {
        match self {
            Self::Write { path, .. } | Self::Delete { path } => path,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Provenance {
    pub author: String,
    pub source: String,
    pub run_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitReceipt {
    pub revision: String,
    pub parent_revision: String,
    pub provenance: Provenance,
    pub paths: Vec<String>,
    pub reverted_commit: Option<String>,
}

/// Typed outcome evidence for a failed managed commit. Rejected means this
/// invocation made no owner-visible edit or completed a verified rollback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitFailure {
    Rejected,
    NeedsRecovery,
}
impl std::fmt::Display for CommitFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Rejected => "vault write rejected or rolled back",
            Self::NeedsRecovery => "vault write requires recovery",
        })
    }
}
impl std::error::Error for CommitFailure {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchHit {
    pub path: String,
    pub score: usize,
    pub excerpt: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Change {
    path: String,
    before: Option<String>,
    after: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Transaction {
    receipt: CommitReceipt,
    changes: Vec<Change>,
}

impl Vault {
    /// Initialize an empty, private local directory. Never adopts existing files.
    pub fn init(root: &Path) -> Result<Self> {
        if root.exists() {
            ensure!(
                !fs::symlink_metadata(root)?.file_type().is_symlink(),
                "vault root is a symlink"
            );
            ensure!(
                root.is_dir() && fs::read_dir(root)?.next().is_none(),
                "vault initialization requires an empty directory"
            );
        } else {
            fs::create_dir_all(root)?;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(root, fs::Permissions::from_mode(0o700))?;
        }
        let vault = Self {
            root: root.canonicalize()?,
        };
        // Git templates can contain a remote, hooks, or arbitrary configuration.
        // A private vault must begin without any inherited repository template.
        vault.git(&["init", "--quiet", "--initial-branch=main", "--template="])?;
        vault.git(&[
            "-c",
            "user.name=OPPA Vault",
            "-c",
            "user.email=vault@localhost",
            "-c",
            "commit.gpgSign=false",
            "commit",
            "--quiet",
            "--allow-empty",
            "-m",
            "Initialize private vault",
        ])?;
        Ok(vault)
    }

    pub fn open(root: &Path) -> Result<Self> {
        ensure!(
            !fs::symlink_metadata(root)?.file_type().is_symlink(),
            "vault root is a symlink"
        );
        let vault = Self {
            root: root.canonicalize()?,
        };
        let metadata = fs::symlink_metadata(vault.root.join(".git"))?;
        ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "vault needs its own Git directory"
        );
        let top = vault.git(&["rev-parse", "--show-toplevel"])?;
        ensure!(
            Path::new(top.trim()).canonicalize()? == vault.root,
            "vault must be a Git repository root"
        );
        vault.revision()?;
        Ok(vault)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn revision(&self) -> Result<String> {
        Ok(self
            .git(&["rev-parse", "--verify", "HEAD"])?
            .trim()
            .to_owned())
    }

    /// Inspect repository validity and interrupted work without changing files,
    /// acquiring a committer lock, or performing recovery. Owner edits are valid.
    /// This is a readiness snapshot; read/write operations still take the lock.
    pub fn check_ready(&self) -> Result<()> {
        let verified = Self::open(&self.root)?;
        verified.require_no_transaction()
    }

    pub fn read(&self, path: &str) -> Result<String> {
        let _lock = self.lock()?;
        self.require_no_transaction()?;
        self.current(path)?
            .ok_or_else(|| anyhow!("note does not exist: {path}"))
    }

    /// A note as it is on disk now, or None when absent, without the committer
    /// lock or the interrupted-transaction check; each file is read whole.
    pub(crate) fn snapshot(&self, path: &str) -> Result<Option<String>> {
        self.current(path)
    }

    /// Searches the actual Markdown files, including owner edits not yet committed.
    /// Terms are OR-ranked. English words and short CJK sequences are supported.
    pub fn search(&self, query: &str) -> Result<Vec<SearchHit>> {
        self.search_scoped(query, &["*".to_owned()])
    }

    /// Restrict candidate paths before reading note contents or ranking results.
    pub fn search_scoped(&self, query: &str, scopes: &[String]) -> Result<Vec<SearchHit>> {
        let _lock = self.lock()?;
        self.require_no_transaction()?;
        ensure!(
            scopes.iter().all(|scope| valid_scope(scope)),
            "invalid memory scope"
        );
        let tokens = search_tokens(query);
        if tokens.is_empty() {
            return Ok(Vec::new());
        }
        let mut paths = Vec::new();
        self.walk(&self.root, &mut paths)?;
        let mut hits = Vec::new();
        for path in paths {
            if !scope_allows(&path, scopes) {
                continue;
            }
            let text = self.current(&path)?.unwrap_or_default();
            let haystack = text.to_lowercase();
            // A note is found by its path as well: a question about the owner
            // must reach people/owner/ even when no note text says "owner".
            let named = path.to_lowercase();
            let score = tokens
                .iter()
                .map(|token| {
                    haystack.matches(token.as_str()).count() + named.matches(token.as_str()).count()
                })
                .sum();
            if score > 0 {
                let first = tokens
                    .iter()
                    .filter_map(|token| haystack.find(token))
                    .min()
                    .unwrap_or(0);
                // Lowercasing can expand Unicode characters, so use a character
                // position rather than slicing the original string by that byte.
                let position = haystack[..first].chars().count();
                let excerpt = text
                    .chars()
                    .skip(position.saturating_sub(60))
                    .take(300)
                    .collect();
                hits.push(SearchHit {
                    path,
                    score,
                    excerpt,
                });
            }
        }
        hits.sort_by(|a, b| b.score.cmp(&a.score).then(a.path.cmp(&b.path)));
        Ok(hits)
    }

    /// Why a new note cannot be written at each of `paths`, or None where it
    /// can: the path fails the edit checks, a note is already there in
    /// `revision` or on disk, it differs only in case from an earlier path
    /// here or an existing file or folder, or this volume stores it as the
    /// same file as an earlier path here.
    pub fn new_note_conflicts(
        &self,
        revision: &str,
        paths: &[String],
    ) -> Result<Vec<Option<String>>> {
        let _lock = self.lock()?;
        self.require_no_transaction()?;
        validate_revision(revision)?;
        let tree = self.git(&["ls-tree", "-r", "-t", "-z", "--name-only", revision])?;
        let mut names: Vec<_> = tree.split_terminator('\0').map(str::to_owned).collect();
        self.walk(&self.root, &mut names)?;
        let mut spellings = BTreeMap::new();
        for name in &names {
            remember(&mut spellings, name);
        }
        let invalid: Vec<_> = paths
            .iter()
            .map(|path| self.checked_path(path).err())
            .collect();
        let valid: Vec<_> = paths
            .iter()
            .zip(&invalid)
            .filter(|(_, error)| error.is_none())
            .map(|(path, _)| path.as_str())
            .collect();
        let listing = ["ls-tree", "-z", revision, "--"];
        let found = self.lookup(&listing, &valid, |path| {
            Ok(self.blob(revision, path)?.map(drop))
        })?;
        let committed: BTreeSet<_> = valid
            .iter()
            .zip(found)
            .filter(|(_, found)| found.is_some())
            .map(|(path, _)| *path)
            .collect();
        let (mut conflicts, mut probe) = (Vec::new(), Probe::new(self)?);
        for (path, invalid) in paths.iter().zip(invalid) {
            let conflict = if let Some(error) = invalid {
                Some(format!("{error:#}"))
            } else if let Some(other) = clash(&spellings, path) {
                Some(format!("differs only in case from {other}"))
            } else if committed.contains(path.as_str()) || self.current(path)?.is_some() {
                Some("already exists in the vault".to_owned())
            } else {
                probe.lay(path)?
            };
            // Only an accepted path adds spellings; a refused one must not
            // make its folders look established for the paths after it.
            if conflict.is_none() {
                remember(&mut spellings, path);
            }
            conflicts.push(conflict);
        }
        Ok(conflicts)
    }

    pub fn commit(
        &self,
        expected_revision: &str,
        edits: &[Edit],
        provenance: &Provenance,
    ) -> Result<CommitReceipt> {
        let _lock = self.lock().context(CommitFailure::Rejected)?;
        self.commit_locked(expected_revision, edits, provenance, None)
    }

    /// Revert a managed commit by making a new commit. All affected current
    /// contents must still equal that commit's result; later edits are conflicts.
    pub fn revert(
        &self,
        expected_revision: &str,
        target: &str,
        provenance: &Provenance,
    ) -> Result<CommitReceipt> {
        let _lock = self.lock()?;
        self.require_no_transaction()?;
        validate_revision(expected_revision)?;
        ensure!(
            self.revision()? == expected_revision,
            "stale vault revision; read the current revision before retrying"
        );
        validate_revision(target)?;
        let receipt = self
            .receipt(target)?
            .ok_or_else(|| anyhow!("only managed vault commits can be reverted"))?;
        let ancestor = crate::process::output(self.command().args([
            "merge-base",
            "--is-ancestor",
            target,
            expected_revision,
        ]))?;
        ensure!(
            ancestor.status.success(),
            "revert target is not an ancestor of the expected revision"
        );
        let mut edits = Vec::new();
        for path in receipt.paths {
            let target_content = self.blob(target, &path)?;
            ensure!(
                self.blob(expected_revision, &path)? == target_content,
                "later committed edit conflicts with revert: {path}"
            );
            ensure!(
                self.current(&path)? == target_content,
                "owner edit conflicts with revert: {path}"
            );
            edits.push(match self.blob(&receipt.parent_revision, &path)? {
                Some(content) => Edit::Write { path, content },
                None => Edit::Delete { path },
            });
        }
        self.commit_locked(
            expected_revision,
            &edits,
            provenance,
            Some(target.to_owned()),
        )
    }

    /// Returns managed receipts among the most recent `limit` Git commits.
    /// Owner-created Git commits remain in Git history but have no managed receipt.
    pub fn history(&self, limit: usize) -> Result<Vec<CommitReceipt>> {
        let _lock = self.lock()?;
        self.require_no_transaction()?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let log = self.git(&["log", "--format=%H", &format!("--max-count={limit}")])?;
        log.lines()
            .filter_map(|revision| self.receipt(revision).transpose())
            .collect()
    }

    /// Resolve a previously interrupted transaction. Before the HEAD update it
    /// restores old contents; after that update it finishes the new state. It
    /// refuses unexpected owner edits instead of overwriting them.
    pub fn recover(&self) -> Result<Option<CommitReceipt>> {
        let _lock = self.lock()?;
        self.recover_locked()
    }

    fn commit_locked(
        &self,
        expected: &str,
        edits: &[Edit],
        provenance: &Provenance,
        reverted_commit: Option<String>,
    ) -> Result<CommitReceipt> {
        let mut admitted = false;
        let result = (|| -> Result<CommitReceipt> {
            self.require_no_transaction()?;
            validate_revision(expected)?;
            validate_provenance(provenance)?;
            ensure!(
                self.revision()? == expected,
                "stale vault revision; read the current revision before retrying"
            );
            // The owner may have unrelated working-file edits, but staged changes
            // belong to their own commit and must never be silently adopted.
            ensure!(
                crate::process::status(self.command().args([
                    "diff",
                    "--cached",
                    "--quiet",
                    "--exit-code"
                ]))?
                .success(),
                "owner has staged changes; commit or unstage them first"
            );
            ensure!(!edits.is_empty(), "an edit is required");
            let (mut unique, mut probe) = (BTreeSet::new(), Probe::new(self)?);
            let paths: Vec<_> = edits.iter().map(Edit::path).collect();
            for path in &paths {
                self.checked_path(path)?;
                // Two edits of one file are refused before any write. Case is
                // refused on every volume; the probe asks this volume about
                // the rest, such as ß and ss or composed and decomposed é.
                let folded = path.to_lowercase();
                ensure!(
                    unique.insert(folded),
                    "duplicate edited path, ignoring case: {path}"
                );
                if let Some(clash) = probe.lay(path)? {
                    bail!("edited path {path} {clash}");
                }
            }
            drop(probe);
            let listing = ["ls-tree", "-z", expected, "--"];
            let befores = self.lookup(&listing, &paths, |path| self.blob(expected, path))?;
            let mut changes = Vec::new();
            for (edit, before) in edits.iter().zip(befores) {
                let path = edit.path();
                ensure!(
                    self.current(path)? == before,
                    "owner dirty file conflicts with managed edit: {path}"
                );
                let after = match edit {
                    Edit::Write { content, .. } => Some(content.clone()),
                    Edit::Delete { .. } => None,
                };
                ensure!(before != after, "edit makes no change: {path}");
                changes.push(Change {
                    path: path.to_owned(),
                    before,
                    after,
                });
            }
            let index = Temporary::new(&self.root.join(".git"), "index");
            self.git_index(&index.path, &["read-tree", expected], None)?;
            self.stage(&index.path, &changes)?;
            let tree = self.git_index(&index.path, &["write-tree"], None)?;
            let paths: Vec<_> = changes.iter().map(|change| change.path.clone()).collect();
            let message = commit_message(provenance, &paths, reverted_commit.as_deref())?;
            let revision = self
                .git_input(
                    &[
                        "-c",
                        "user.name=OPPA Vault",
                        "-c",
                        "user.email=vault@localhost",
                        "-c",
                        "commit.gpgSign=false",
                        "commit-tree",
                        tree.trim(),
                        "-p",
                        expected,
                    ],
                    message.as_bytes(),
                )?
                .trim()
                .to_owned();
            let receipt = CommitReceipt {
                revision,
                parent_revision: expected.to_owned(),
                provenance: provenance.clone(),
                paths,
                reverted_commit,
            };
            let transaction = Transaction {
                receipt: receipt.clone(),
                changes,
            };
            // The journal is durable before changing any owner-visible file.
            self.write_journal(&transaction)?;
            admitted = true;
            let result = (|| -> Result<()> {
                self.verify_working(&transaction.changes, false)?;
                for change in &transaction.changes {
                    self.replace(
                        &change.path,
                        change.before.as_deref(),
                        change.after.as_deref(),
                    )?;
                }
                self.git(&["update-ref", "HEAD", &receipt.revision, expected])?;
                self.finish_index(&transaction)?;
                self.remove_journal()?;
                Ok(())
            })();
            if let Err(error) = result {
                match self.recover_locked() {
                    Ok(Some(recovered)) => return Ok(recovered),
                    Ok(None) => {
                        admitted = false;
                        return Err(error.context("vault edit rolled back"));
                    }
                    Err(recovery) => {
                        bail!("vault transaction needs recovery: {error:#}; recovery: {recovery:#}")
                    }
                }
            }
            Ok(receipt)
        })();
        result.map_err(|error| {
            error.context(if admitted {
                CommitFailure::NeedsRecovery
            } else {
                CommitFailure::Rejected
            })
        })
    }

    fn recover_locked(&self) -> Result<Option<CommitReceipt>> {
        let path = self.root.join(".git").join(JOURNAL);
        if !path.exists() {
            return Ok(None);
        }
        let transaction: Transaction = serde_json::from_slice(&fs::read(&path)?)?;
        validate_revision(&transaction.receipt.revision)?;
        validate_revision(&transaction.receipt.parent_revision)?;
        for change in &transaction.changes {
            self.checked_path(&change.path)?;
        }
        let revision = self.revision()?;
        let committed = revision == transaction.receipt.revision;
        ensure!(committed || revision == transaction.receipt.parent_revision, "HEAD changed outside the interrupted transaction; recovery requires owner reconciliation");
        // Verify every file before modifying any, so an owner edit blocks the
        // complete recovery rather than causing a partial overwrite.
        self.verify_working(&transaction.changes, true)?;
        for change in &transaction.changes {
            let (from, to) = if committed {
                (&change.before, &change.after)
            } else {
                (&change.after, &change.before)
            };
            if self.current(&change.path)? != *to {
                self.replace(&change.path, from.as_deref(), to.as_deref())?;
            }
        }
        if committed {
            self.finish_index(&transaction)?;
        }
        self.remove_journal()?;
        Ok(committed.then_some(transaction.receipt))
    }

    fn finish_index(&self, transaction: &Transaction) -> Result<()> {
        let changes = &transaction.changes;
        let paths: Vec<_> = changes.iter().map(|change| change.path.as_str()).collect();
        let listing = ["ls-files", "--stage", "-z", "--"];
        let staged = self.lookup(&listing, &paths, |path| self.index_blob(path))?;
        for (change, staged) in changes.iter().zip(staged) {
            ensure!(
                staged == change.before || staged == change.after,
                "owner staged edit conflicts with recovery: {}",
                change.path
            );
        }
        let reset = ["reset", "--quiet", &transaction.receipt.revision, "--"];
        for group in groups(&paths) {
            self.git(&[&reset[..], &paths[group]].concat())?;
        }
        Ok(())
    }

    fn verify_working(&self, changes: &[Change], allow_after: bool) -> Result<()> {
        for change in changes {
            let actual = self.current(&change.path)?;
            ensure!(
                actual == change.before || (allow_after && actual == change.after),
                "owner edit conflicts with transaction: {}",
                change.path
            );
        }
        Ok(())
    }

    fn checked_path(&self, path: &str) -> Result<PathBuf> {
        ensure!(
            !path.is_empty() && path.len() <= 1024 && !path.contains(['\\', ':', '\0', '\n', '\r']),
            "invalid note path"
        );
        let relative = Path::new(path);
        ensure!(!relative.is_absolute(), "note path must be relative");
        ensure!(
            path.split('/')
                .all(|part| !part.is_empty() && !part.starts_with('.')),
            "hidden paths and traversal are forbidden"
        );
        ensure!(
            relative.extension().and_then(|ext| ext.to_str()) == Some("md"),
            "vault notes must have .md extension"
        );
        let mut current = self.root.clone();
        let components: Vec<_> = relative.components().collect();
        for (index, component) in components.iter().enumerate() {
            let Component::Normal(part) = component else {
                bail!("invalid path component");
            };
            current.push(part);
            match fs::symlink_metadata(&current) {
                Ok(meta) => {
                    ensure!(
                        !meta.file_type().is_symlink(),
                        "symlink note paths are forbidden"
                    );
                    ensure!(
                        if index + 1 == components.len() {
                            meta.is_file()
                        } else {
                            meta.is_dir()
                        },
                        "unexpected file type in note path"
                    );
                    #[cfg(unix)]
                    if meta.is_file() {
                        use std::os::unix::fs::MetadataExt;
                        ensure!(meta.nlink() == 1, "hard-linked note files are forbidden");
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(current)
    }

    fn current(&self, path: &str) -> Result<Option<String>> {
        let full = self.checked_path(path)?;
        match fs::read_to_string(full) {
            Ok(content) => Ok(Some(content)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn replace(&self, path: &str, expected: Option<&str>, content: Option<&str>) -> Result<()> {
        ensure!(
            self.current(path)?.as_deref() == expected,
            "file changed during transaction: {path}"
        );
        let full = self.checked_path(path)?;
        match content {
            Some(content) => {
                fs::create_dir_all(full.parent().context("note has no parent")?)?;
                self.checked_path(path)?;
                let temporary = full.with_file_name(format!(
                    ".oppa-note-{}-{}",
                    std::process::id(),
                    TEMP_ID.fetch_add(1, Ordering::Relaxed)
                ));
                let result = (|| -> Result<()> {
                    let mut file = OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&temporary)?;
                    file.write_all(content.as_bytes())?;
                    file.sync_all()?;
                    ensure!(
                        self.current(path)?.as_deref() == expected,
                        "file changed during transaction: {path}"
                    );
                    fs::rename(&temporary, &full)?;
                    sync_directory(full.parent().unwrap())?;
                    Ok(())
                })();
                let _ = fs::remove_file(temporary);
                result?;
            }
            None => {
                fs::remove_file(&full)?;
                sync_directory(full.parent().unwrap())?;
            }
        }
        Ok(())
    }

    fn walk(&self, directory: &Path, paths: &mut Vec<String>) -> Result<()> {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            if entry.file_name().to_string_lossy().starts_with('.') || kind.is_symlink() {
                continue;
            }
            if kind.is_dir() {
                self.walk(&entry.path(), paths)?;
            } else if kind.is_file()
                && entry.path().extension().and_then(|ext| ext.to_str()) == Some("md")
            {
                let path = entry.path();
                paths.push(
                    path.strip_prefix(&self.root)?
                        .to_str()
                        .context("non-UTF-8 note path")?
                        .to_owned(),
                );
            }
        }
        Ok(())
    }

    fn blob(&self, revision: &str, path: &str) -> Result<Option<String>> {
        validate_revision(revision)?;
        self.checked_path(path)?;
        let listing = self.git(&["ls-tree", "-z", revision, "--", path])?;
        if listing.is_empty() {
            return Ok(None);
        }
        let entry = listing.trim_end_matches('\0');
        ensure!(!entry.contains('\0'), "ambiguous tree entry for note");
        let (metadata, _) = entry.split_once('\t').context("malformed Git tree entry")?;
        let fields: Vec<_> = metadata.split_whitespace().collect();
        ensure!(
            fields.len() == 3 && fields[1] == "blob",
            "note is not a Git blob"
        );
        Ok(Some(self.git(&["cat-file", "blob", fields[2]])?))
    }

    fn index_blob(&self, path: &str) -> Result<Option<String>> {
        let listing = self.git(&["ls-files", "--stage", "-z", "--", path])?;
        if listing.is_empty() {
            return Ok(None);
        }
        let entry = listing.trim_end_matches('\0');
        ensure!(
            !entry.contains('\0'),
            "owner has an unresolved index conflict"
        );
        let (metadata, _) = entry
            .split_once('\t')
            .context("malformed Git index entry")?;
        let fields: Vec<_> = metadata.split_whitespace().collect();
        ensure!(
            fields.len() == 3 && fields[2] == "0",
            "owner has an unresolved index conflict"
        );
        Ok(Some(self.git(&["cat-file", "blob", fields[1]])?))
    }

    fn receipt(&self, revision: &str) -> Result<Option<CommitReceipt>> {
        validate_revision(revision)?;
        let body = self.git(&["show", "--no-patch", "--format=%P%n%B", revision])?;
        let mut lines = body.lines();
        let parent = lines.next().unwrap_or_default();
        // Owner-authored commits are valid history, but only our complete final
        // trailer block is a receipt. Never parse the subject as host metadata.
        if validate_revision(parent).is_err() {
            return Ok(None);
        }
        let message = lines.collect::<Vec<_>>().join("\n");
        let Some((_, trailers)) = message.trim_end().rsplit_once("\n\n") else {
            return Ok(None);
        };
        let parse = || -> Result<CommitReceipt> {
            let mut fields = std::collections::BTreeMap::new();
            for line in trailers.lines() {
                let (key, value) = line.split_once(": ").context("invalid trailer")?;
                ensure!(
                    key.starts_with("Oppa-") && fields.insert(key, value).is_none(),
                    "invalid or duplicate trailer"
                );
            }
            let required = |name| fields.get(name).copied().context("missing managed trailer");
            let provenance = Provenance {
                author: required("Oppa-Author")?.to_owned(),
                source: required("Oppa-Source")?.to_owned(),
                run_id: required("Oppa-Run")?.to_owned(),
                reason: required("Oppa-Reason")?.to_owned(),
            };
            validate_provenance(&provenance)?;
            let paths: Vec<String> = serde_json::from_str(required("Oppa-Paths")?)?;
            ensure!(
                !paths.is_empty() && paths.iter().all(|path| valid_relative_path(path)),
                "invalid managed receipt paths"
            );
            let reverted_commit = fields.get("Oppa-Reverts").map(|value| value.to_string());
            if let Some(revision) = &reverted_commit {
                validate_revision(revision)?;
            }
            Ok(CommitReceipt {
                revision: revision.to_owned(),
                parent_revision: parent.to_owned(),
                provenance,
                paths,
                reverted_commit,
            })
        };
        // Malformed or foreign commit metadata is not a managed receipt. Git
        // failures above still propagate; missing proof never permits replay.
        Ok(parse().ok())
    }

    fn lock(&self) -> Result<File> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.root.join(".git/oppa-vault.lock"))?;
        file.lock()?;
        Ok(file)
    }

    fn require_no_transaction(&self) -> Result<()> {
        ensure!(
            !self.root.join(".git").join(JOURNAL).exists(),
            "vault has an interrupted transaction; recover before continuing"
        );
        Ok(())
    }

    fn write_journal(&self, transaction: &Transaction) -> Result<()> {
        let path = self.root.join(".git").join(JOURNAL);
        let temporary = self.root.join(".git").join(format!("{JOURNAL}.tmp"));
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temporary)?;
        file.write_all(&serde_json::to_vec(transaction)?)?;
        file.sync_all()?;
        fs::rename(temporary, path)?;
        sync_directory(&self.root.join(".git"))
    }

    fn remove_journal(&self) -> Result<()> {
        fs::remove_file(self.root.join(".git").join(JOURNAL))?;
        sync_directory(&self.root.join(".git"))
    }
}

pub(crate) fn valid_scope(scope: &str) -> bool {
    scope == "*" || valid_relative_path(scope)
}

pub fn valid_relative_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 1024
        && !path.contains(['\\', ':', '\0', '\n', '\r'])
        && path
            .split('/')
            .all(|part| !part.is_empty() && !part.starts_with('.'))
        && Path::new(path)
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

pub(crate) fn scope_allows(path: &str, scopes: &[String]) -> bool {
    valid_relative_path(path)
        && scopes.iter().any(|scope| {
            scope == "*"
                || path == scope
                || path
                    .strip_prefix(scope)
                    .is_some_and(|suffix| suffix.starts_with('/'))
        })
}

fn validate_revision(revision: &str) -> Result<()> {
    ensure!(
        [40, 64].contains(&revision.len()) && revision.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "expected a full Git revision"
    );
    Ok(())
}

pub fn validate_provenance(provenance: &Provenance) -> Result<()> {
    for value in [
        &provenance.author,
        &provenance.source,
        &provenance.run_id,
        &provenance.reason,
    ] {
        ensure!(
            !value.trim().is_empty() && !value.contains(['\n', '\r', '\0']),
            "provenance fields must be nonempty single lines"
        );
    }
    Ok(())
}

fn commit_message(
    provenance: &Provenance,
    paths: &[String],
    reverted: Option<&str>,
) -> Result<String> {
    let mut message = format!(
        "{}\n\nOppa-Author: {}\nOppa-Source: {}\nOppa-Run: {}\nOppa-Reason: {}\nOppa-Paths: {}\n",
        provenance.reason,
        provenance.author,
        provenance.source,
        provenance.run_id,
        provenance.reason,
        serde_json::to_string(paths)?
    );
    if let Some(revision) = reverted {
        message.push_str(&format!("Oppa-Reverts: {revision}\n"));
    }
    Ok(message)
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all().context("sync directory")
}

fn is_cjk(character: char) -> bool {
    matches!(character as u32, 0x3400..=0x4dbf | 0x4e00..=0x9fff | 0xf900..=0xfaff | 0x20000..=0x2ffff)
}

fn search_tokens(query: &str) -> BTreeSet<String> {
    let mut tokens = BTreeSet::new();
    let mut current = String::new();
    let mut cjk = false;
    let flush = |current: &mut String, cjk: bool, tokens: &mut BTreeSet<String>| {
        if !current.is_empty() {
            let characters: Vec<_> = current.chars().collect();
            if cjk && characters.len() > 2 {
                for pair in characters.windows(2) {
                    tokens.insert(pair.iter().collect());
                }
            } else {
                tokens.insert(current.clone());
            }
            current.clear();
        }
    };
    for character in query.to_lowercase().chars() {
        if !character.is_alphanumeric() {
            flush(&mut current, cjk, &mut tokens);
            continue;
        }
        let next_cjk = is_cjk(character);
        if !current.is_empty() && next_cjk != cjk {
            flush(&mut current, cjk, &mut tokens);
        }
        cjk = next_cjk;
        current.push(character);
    }
    flush(&mut current, cjk, &mut tokens);
    tokens
}

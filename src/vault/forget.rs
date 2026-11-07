//! Forgetting a text in the vault (M-10). Each note on disk, and each file and
//! commit message in branch history, has the text replaced by a tombstone:
//! Git exports the commits that hold it and every commit after them, every
//! data block is edited, and Git imports them again, moving each branch that
//! holds it to its rewritten commits. Every other commit keeps its id and its
//! signature; a rewritten commit loses its signature, so each signed commit a
//! rewrite changes is listed with the history. File names, commit authors,
//! reflogs, stashes, tags, files on disk that are not notes and objects no
//! branch reaches stay as they are; `find_text` reports them.

use super::Vault;
use crate::forget::{position, replaced};
use anyhow::{bail, ensure, Context, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, Read, Write};
use std::process::Stdio;

/// Where the vault holds a text.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct VaultCopies {
    /// Notes on disk.
    pub notes: Vec<String>,
    /// Files in branch history, by every path that held one, branch commits
    /// whose message holds it, and signed commits a rewrite changes.
    pub history: Vec<String>,
    /// What a forget leaves as it is.
    pub elsewhere: Vec<String>,
    /// What Git exports to rewrite history: each branch that holds the text,
    /// then, excluded, each commit kept beneath the commits it rewrites.
    rewrite: Vec<String>,
}

impl Vault {
    pub fn find_text(&self, text: &str) -> Result<VaultCopies> {
        let _lock = self.lock()?;
        self.copies(text)
    }

    /// Replace `text` with `tombstone` in notes and branch history, once
    /// `approve` accepts what holds it, and return what still holds it. The
    /// vault stays locked from that check to the end, so only what `approve`
    /// saw changes.
    pub fn forget_text(
        &self,
        text: &str,
        tombstone: &str,
        approve: impl FnOnce(&VaultCopies) -> Result<()>,
    ) -> Result<VaultCopies> {
        let _lock = self.lock()?;
        self.require_no_transaction()?;
        ensure!(
            self.git(&["symbolic-ref", "-q", "HEAD"]).is_ok(),
            "the vault is not on a branch; check one out first"
        );
        // The index is set to the rewritten HEAD, so nothing may be staged.
        ensure!(
            self.git(&["diff", "--cached", "--quiet"]).is_ok(),
            "the vault has staged changes; commit or unstage them first"
        );
        let (needle, with) = (text.as_bytes(), tombstone.as_bytes());
        let found = self.copies(text)?;
        // Git exports signed commits from 2.50 on; an older one is refused
        // here, before anything changes, rather than partway.
        if !found.rewrite.is_empty() {
            let version = self.git(&["version"])?;
            let number = version.split_whitespace().nth(2).unwrap_or_default();
            let number: Vec<u32> = number.split('.').map_while(|n| n.parse().ok()).collect();
            ensure!(number >= vec![2, 50], "rewriting needs Git 2.50 or newer");
        }
        approve(&found)?;
        if !found.rewrite.is_empty() {
            // Done ends the stream, so an export cut short moves no branch.
            let options = "fast-export --reference-excluded-parents --reencode=no \
                --signed-commits=strip --signed-tags=strip --use-done-feature";
            let mut export = self.command();
            export
                .args(options.split_whitespace())
                .args(&found.rewrite)
                .arg("--")
                .stdout(Stdio::piped());
            let mut export = crate::process::spawning(|| export.spawn())?;
            let mut import = self.command();
            import
                .args(["fast-import", "--force", "--quiet"])
                .stdin(Stdio::piped())
                .stdout(Stdio::null());
            let mut import = crate::process::spawning(|| import.spawn())?;
            let mut stream = std::io::BufReader::new(export.stdout.take().context("no export")?);
            let mut edited = std::io::BufWriter::new(import.stdin.take().context("no import")?);
            // `data <size>` is followed by exactly that many bytes of a file or message.
            let mut line = Vec::new();
            while stream.read_until(b'\n', &mut line)? > 0 {
                match line.strip_prefix(b"data ") {
                    Some(size) => {
                        let mut data = vec![0; std::str::from_utf8(size)?.trim().parse()?];
                        stream.read_exact(&mut data)?;
                        let data = replaced(&data, needle, with);
                        edited.write_all(format!("data {}\n", data.len()).as_bytes())?;
                        edited.write_all(&data)?;
                    }
                    None => edited.write_all(&line)?,
                }
                line.clear();
            }
            drop(edited.into_inner()?);
            ensure!(
                export.wait()?.success() && import.wait()?.success(),
                "Git could not rewrite the vault's history"
            );
            self.git(&["reset", "--quiet"])?;
        }
        for path in &found.notes {
            let before = self
                .current(path)?
                .context("note vanished while forgetting")?;
            let after = String::from_utf8(replaced(before.as_bytes(), needle, with))?;
            self.replace(path, Some(&before), Some(&after))?;
        }
        self.copies(text)
    }

    fn copies(&self, text: &str) -> Result<VaultCopies> {
        let needle = text.as_bytes();
        let holds = |bytes: &[u8]| position(bytes, needle).is_some();
        let mut found = VaultCopies::default();
        let mut notes = Vec::new();
        self.walk(&self.root, &mut notes)?;
        notes.sort();
        for path in &notes {
            if self
                .current(path)?
                .is_some_and(|note| holds(note.as_bytes()))
            {
                found.notes.push(path.clone());
            }
        }
        // Every other file in the folder, tracked or not, outside Git's own.
        let (git_dir, files) = (self.root.join(".git"), crate::forget::files(&self.root)?);
        for full in files.iter().filter(|file| !file.starts_with(&git_dir)) {
            let path = full.strip_prefix(&self.root)?.to_string_lossy();
            if !notes.iter().any(|note| *note == path) && holds(&std::fs::read(full)?) {
                found.elsewhere.push(format!("file {path}, not a note"));
            }
        }
        // Every object that holds the text, reachable or not, read one at a
        // time: in a file or a commit message, which a rewrite edits, or in
        // a name or an author, which it keeps. And every signed commit.
        let (mut data, mut names, mut signed) = (BTreeSet::new(), BTreeSet::new(), BTreeSet::new());
        let mut git = self.command();
        git.args(["cat-file", "--batch-all-objects", "--batch"])
            .stdout(Stdio::piped());
        let mut git = crate::process::spawning(|| git.spawn())?;
        let mut objects = std::io::BufReader::new(git.stdout.take().context("no Git output")?);
        let (mut heading, mut body) = (String::new(), Vec::new());
        while objects.read_line(&mut heading)? > 0 {
            let fields: Vec<&str> = heading.split_whitespace().collect();
            let [id, kind, size] = fields[..] else {
                bail!("malformed object heading");
            };
            body.resize(size.parse()?, 0);
            objects.read_exact(&mut body)?;
            objects.read_exact(&mut [0])?;
            let (head, message) = match (kind, position(&body, b"\n\n")) {
                ("blob", _) => (&body[..0], &body[..]),
                ("commit", Some(at)) => (&body[..at], &body[at..]),
                _ => (&body[..], &body[..0]),
            };
            if holds(message) {
                data.insert(id.to_owned());
            }
            if holds(head) {
                names.insert(id.to_owned());
            }
            if kind == "commit" && position(head, b"\ngpgsig").is_some() {
                signed.insert(id.to_owned());
            }
            heading.clear();
        }
        ensure!(git.wait()?.success(), "Git could not read objects");
        // Branch history, parents first: each commit with its tree and
        // parents, then every file and folder it changes from its first
        // parent, by path, so each path an object had is seen. A commit that
        // holds the text, or has a parent that does, is rewritten.
        let listing = "log --branches --topo-order --reverse --root -r -t --raw -z --no-abbrev \
            --no-renames --no-show-signature --diff-merges=first-parent --format=%x01%H%x20%T%x20%P";
        let log = self.git(&listing.split_whitespace().collect::<Vec<_>>())?;
        let (mut history, mut kept, mut reached) =
            (BTreeSet::new(), BTreeSet::new(), BTreeSet::new());
        let (mut rewritten, mut parents, mut commit) = (BTreeSet::new(), BTreeMap::new(), "");
        let mut fields = log.split('\0');
        while let Some(field) = fields.next() {
            if let Some(heading) = field.strip_prefix('\u{1}') {
                let ids: Vec<&str> = heading.split_whitespace().collect();
                let [id, tree, ref above @ ..] = ids[..] else {
                    bail!("malformed history");
                };
                let place = format!("commit {}", &id[..12]);
                if data.contains(id) {
                    history.insert(format!("message of {place}"));
                }
                if names.contains(id) {
                    kept.insert(format!("author or committer of {place}"));
                }
                if names.contains(tree) {
                    kept.insert("file names in history of /".to_owned());
                }
                if data.contains(id) || above.iter().any(|parent| rewritten.contains(parent)) {
                    rewritten.insert(id);
                }
                parents.insert(id, above.to_vec());
                reached.extend([id, tree]);
                commit = id;
            } else if let Some(change) = field.trim_start_matches('\n').strip_prefix(':') {
                let path = fields.next().context("malformed history")?;
                let id = change.split(' ').nth(3).context("malformed history")?;
                if data.contains(id) {
                    history.insert(format!("history of {path}"));
                    rewritten.insert(commit);
                }
                if names.contains(id) {
                    kept.insert(format!("file names in history of /{path}"));
                }
                reached.insert(id);
            }
        }
        for id in rewritten.iter().filter(|id| signed.contains(**id)) {
            history.insert(format!("signature of commit {}", &id[..12]));
        }
        // Each branch that holds the text, and the commits beneath it that stay.
        let format = "--format=%(objectname) %(refname)";
        let tips = self.git(&["for-each-ref", format, "refs/heads"])?;
        for (tip, branch) in tips.lines().filter_map(|line| line.split_once(' ')) {
            if rewritten.contains(tip) {
                found.rewrite.push(branch.to_owned());
            }
        }
        let beneath = rewritten.iter().flat_map(|id| &parents[id]);
        let beneath: BTreeSet<_> = beneath.filter(|id| !rewritten.contains(*id)).collect();
        let excluded = beneath.iter().map(|id| format!("^{id}"));
        found.rewrite.extend(excluded);
        found.history = history.into_iter().collect();
        found.elsewhere.extend(kept);
        let unreached = data
            .union(&names)
            .filter(|id| !reached.contains(id.as_str()));
        let count = unreached.count();
        if count > 0 {
            let place = "Git objects no branch reaches (reflogs, stashes, tags or unreferenced)";
            found.elsewhere.push(format!("{count} {place}"));
        }
        let logs = git_dir.join("logs");
        for log in crate::forget::files(&logs)? {
            if holds(&std::fs::read(&log)?) {
                let name = log.strip_prefix(&logs)?.display();
                found.elsewhere.push(format!("reflog {name}"));
            }
        }
        for place in found.history.iter_mut().chain(&mut found.elsewhere) {
            *place = format!("vault {place}");
        }
        Ok(found)
    }
}

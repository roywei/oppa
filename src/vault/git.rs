//! How the vault runs Git: in its own repository only, without hooks, a file
//! monitor or quoted paths, and with every pathspec taken literally.

use super::{Change, Vault, TEMP_ID};
use anyhow::{anyhow, ensure, Context, Result};
use std::fs;
use std::io::Write;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::Ordering;

/// The most path bytes one Git command is given. A command line is bounded
/// (1 MiB on macOS and usually 2 MiB on Linux, environment included), so many
/// paths go in several calls. They stay arguments rather than a pathspec file:
/// on macOS Git precomposes Unicode in arguments only, and every path has to
/// reach the tree and the index spelled as the other vault calls spell it.
const ARGUMENT_BYTES: usize = 128 * 1024;

impl Vault {
    pub(super) fn command(&self) -> Command {
        let mut command = Command::new("git");
        command.current_dir(&self.root).args([
            "--literal-pathspecs",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.quotePath=false",
        ]);
        // The caller's shell cannot redirect this vault into a different repo.
        for key in [
            "GIT_DIR",
            "GIT_WORK_TREE",
            "GIT_INDEX_FILE",
            "GIT_OBJECT_DIRECTORY",
            "GIT_ALTERNATE_OBJECT_DIRECTORIES",
            "GIT_COMMON_DIR",
        ] {
            command.env_remove(key);
        }
        command.env("GIT_TERMINAL_PROMPT", "0");
        command
    }

    pub(super) fn git(&self, args: &[&str]) -> Result<String> {
        output_text(crate::process::output(self.command().args(args))?)
    }

    pub(super) fn git_input(&self, args: &[&str], input: &[u8]) -> Result<String> {
        run_input(self.command().args(args), input)
    }

    pub(super) fn git_index(
        &self,
        index: &Path,
        args: &[&str],
        input: Option<&[u8]>,
    ) -> Result<String> {
        let mut command = self.command();
        command.env("GIT_INDEX_FILE", index).args(args);
        match input {
            Some(input) => run_input(&mut command, input),
            None => output_text(crate::process::output(&mut command)?),
        }
    }

    /// Answer `one` for every path. A group of paths costs one `listing` call
    /// when Git lists nothing for it, as for the new notes of an import;
    /// otherwise each of its paths is asked alone, so no answer depends on
    /// matching the spelling Git prints against the spelling it was given.
    pub(super) fn lookup<T>(
        &self,
        listing: &[&str],
        paths: &[&str],
        one: impl Fn(&str) -> Result<Option<T>>,
    ) -> Result<Vec<Option<T>>> {
        let mut found = Vec::with_capacity(paths.len());
        for group in groups(paths) {
            if self
                .git(&[listing, &paths[group.clone()]].concat())?
                .is_empty()
            {
                found.extend(group.map(|_| None));
            } else {
                for path in &paths[group] {
                    found.push(one(path)?);
                }
            }
        }
        Ok(found)
    }

    /// Record `changes` in a temporary index. New contents are written by one
    /// Git process and the index is updated a bounded group at a time.
    pub(super) fn stage(&self, index: &Path, changes: &[Change]) -> Result<()> {
        let (written, removed): (Vec<_>, Vec<_>) =
            changes.iter().partition(|change| change.after.is_some());
        let paths: Vec<_> = removed.iter().map(|change| change.path.as_str()).collect();
        for group in groups(&paths) {
            let remove = ["update-index", "--force-remove", "--"];
            self.git_index(index, &[&remove[..], &paths[group]].concat(), None)?;
        }
        let contents: Vec<_> = written
            .iter()
            .filter_map(|change| change.after.as_deref())
            .collect();
        let ids = self.store_blobs(&contents)?;
        let paths: Vec<_> = written.iter().map(|change| change.path.as_str()).collect();
        for group in groups(&paths) {
            let mut args = vec!["update-index", "--add"];
            for position in group {
                args.extend(["--cacheinfo", "100644", &ids[position], paths[position]]);
            }
            self.git_index(index, &args, None)?;
        }
        Ok(())
    }

    /// Write `contents` as Git blobs with one process, through files in a
    /// temporary folder, and return their ids in the same order.
    fn store_blobs(&self, contents: &[&str]) -> Result<Vec<String>> {
        if contents.is_empty() {
            return Ok(Vec::new());
        }
        let folder = Temporary::new(&self.root.join(".git"), "blobs");
        fs::create_dir(&folder.path)?;
        let mut list = String::new();
        for (number, content) in contents.iter().enumerate() {
            fs::write(folder.path.join(number.to_string()), content)?;
            list += &format!(".git/{}/{number}\n", folder.name);
        }
        let stored = ["hash-object", "-w", "--no-filters", "--stdin-paths"];
        let ids = self.git_input(&stored, list.as_bytes())?;
        let ids: Vec<_> = ids.lines().map(str::to_owned).collect();
        ensure!(ids.len() == contents.len(), "Git did not store every note");
        Ok(ids)
    }
}

/// Split `paths` into ranges that each fit one command line.
pub(super) fn groups(paths: &[&str]) -> Vec<Range<usize>> {
    let (mut groups, mut start, mut bytes) = (Vec::new(), 0, 0);
    for (position, path) in paths.iter().enumerate() {
        // Room for the options and object id that go with a path.
        let size = path.len() + 128;
        if position > start && bytes + size > ARGUMENT_BYTES {
            groups.push(start..position);
            (start, bytes) = (position, 0);
        }
        bytes += size;
    }
    if start < paths.len() {
        groups.push(start..paths.len());
    }
    groups
}

/// A file or folder in the Git directory, removed with its lock when dropped.
pub(super) struct Temporary {
    pub(super) path: PathBuf,
    name: String,
}
impl Temporary {
    /// Callers hold the vault lock and the name carries this process's id,
    /// so anything already there was left by a stopped process with that id.
    pub(super) fn new(git: &Path, kind: &str) -> Self {
        let name = format!(
            "oppa-{kind}-{}-{}",
            std::process::id(),
            TEMP_ID.fetch_add(1, Ordering::Relaxed)
        );
        let temporary = Self {
            path: git.join(&name),
            name,
        };
        temporary.clear();
        temporary
    }

    fn clear(&self) {
        let _ = fs::remove_file(&self.path);
        let _ = fs::remove_dir_all(&self.path);
        let _ = fs::remove_file(self.path.with_extension("lock"));
    }
}
impl Drop for Temporary {
    fn drop(&mut self) {
        self.clear();
    }
}

fn output_text(output: Output) -> Result<String> {
    ensure!(
        output.status.success(),
        "Git command failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    String::from_utf8(output.stdout).context("Git returned non-UTF-8 content")
}

fn run_input(command: &mut Command, input: &[u8]) -> Result<String> {
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = crate::process::spawning(|| command.spawn())?;
    let mut stdin = child.stdin.take().context("missing Git stdin")?;
    // Git may answer before it has read everything. Writing from another
    // thread keeps a full output pipe from stalling both processes.
    let (written, output) = std::thread::scope(|scope| {
        let writer = scope.spawn(move || stdin.write_all(input));
        let output = child.wait_with_output();
        (writer.join(), output)
    });
    let text = output_text(output?)?;
    written.map_err(|_| anyhow!("writing to Git failed"))??;
    Ok(text)
}

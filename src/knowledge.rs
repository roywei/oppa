//! The owner's knowledge wiki: public, reusable
//! knowledge (sources, ideas, theses) in a Git repository of its own, apart
//! from the vault's personal memory. An owner who already keeps one links it;
//! otherwise the instance starts one from a small layout the model follows.
use crate::config::Paths;
use anyhow::{ensure, Context, Result};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

const LAYOUT: [(&str, &str); 3] = [
    (
        "AGENTS.md",
        include_str!("../resources/knowledge/AGENTS.md"),
    ),
    (
        "wiki/index.md",
        include_str!("../resources/knowledge/index.md"),
    ),
    ("wiki/log.md", include_str!("../resources/knowledge/log.md")),
];
const FOLDERS: [&str; 5] = [
    "raw",
    "wiki/sources",
    "wiki/entities",
    "wiki/concepts",
    "wiki/syntheses",
];

/// The folder `path` names, if it can be the knowledge wiki: a Git repository
/// with an AGENTS.md for the model to follow, apart from the private
/// directory, which holds personal memory. A leading `~/` is the home folder,
/// as typed at a terminal. Checked before an instance is created, so a bad
/// path leaves nothing behind.
pub fn resolve(paths: &Paths, path: &Path) -> Result<PathBuf> {
    let path = match (path.strip_prefix("~"), std::env::var_os("HOME")) {
        (Ok(rest), Some(home)) => PathBuf::from(home).join(rest),
        _ => path.to_path_buf(),
    };
    let target = path
        .canonicalize()
        .with_context(|| format!("{} is not a readable folder", path.display()))?;
    ensure!(target.is_dir(), "{} is not a folder", target.display());
    ensure!(
        target.join(".git").exists() && target.join("AGENTS.md").is_file(),
        "{} must be a Git repository with an AGENTS.md giving its layout and rules for adding \
         to it; without one, leave the path out to start a new wiki",
        target.display()
    );
    // The private directory may not exist yet: compare with its parent's real path.
    let private = match paths.private.canonicalize() {
        Ok(private) => private,
        Err(_) => paths
            .private
            .parent()
            .and_then(|parent| parent.canonicalize().ok())
            .zip(paths.private.file_name())
            .map(|(parent, name)| parent.join(name))
            .unwrap_or_else(|| paths.private.clone()),
    };
    ensure!(
        !target.starts_with(&private) && !private.starts_with(&target),
        "the knowledge wiki must be apart from the private directory, which holds personal memory"
    );
    Ok(target)
}

/// Links `existing` as the knowledge wiki, or starts a new one when it is
/// None. Either way the wiki is `paths.knowledge()`, which must not exist yet.
pub fn prepare(paths: &Paths, existing: Option<&Path>) -> Result<()> {
    let wiki = paths.knowledge();
    ensure!(
        fs::symlink_metadata(&wiki).is_err(),
        "{} already exists; move it away first to link or start another",
        wiki.display()
    );
    if let Some(existing) = existing {
        std::os::unix::fs::symlink(resolve(paths, existing)?, &wiki)?;
        return Ok(());
    }
    start(&wiki).inspect_err(|_| {
        // A half-made wiki would refuse every retry.
        let _ = fs::remove_dir_all(&wiki);
    })
}

fn start(wiki: &Path) -> Result<()> {
    for folder in FOLDERS {
        fs::create_dir_all(wiki.join(folder))?;
        fs::write(wiki.join(folder).join(".gitkeep"), "")?;
    }
    for (path, text) in LAYOUT {
        fs::write(wiki.join(path), text)?;
    }
    for args in [
        &["init", "--quiet", "--initial-branch=main", "--template="][..],
        &["add", "--all"],
        &[
            "-c",
            "user.name=OPPA",
            "-c",
            "user.email=oppa@localhost",
            "-c",
            "commit.gpgSign=false",
            "commit",
            "--quiet",
            "-m",
            "Start the knowledge wiki",
        ],
    ] {
        // Like the vault: no inherited redirect, hook or template.
        let mut git = Command::new("git");
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("GIT_") {
                git.env_remove(key);
            }
        }
        let status = crate::process::status(
            git.args([
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "core.fsmonitor=false",
                "-C",
            ])
            .arg(wiki)
            .args(args),
        )?;
        ensure!(
            status.success(),
            "git {} failed in the knowledge wiki",
            args[0]
        );
    }
    Ok(())
}

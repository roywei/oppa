//! Which spellings of a path name one note. Git keeps every spelling apart;
//! the volume under the vault may fold some of them into one file.

use super::git::Temporary;
use super::Vault;
use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::CString;
use std::fs::{self, File};
use std::io::{self, ErrorKind};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::MetadataExt;

/// Spellings of files and folders in use, by lowercase key.
pub(super) type Spellings = BTreeMap<String, BTreeSet<String>>;

/// Record every folder and the file that make up `path`.
pub(super) fn remember(spellings: &mut Spellings, path: &str) {
    for prefix in prefixes(path) {
        let spelled = spellings.entry(prefix.to_lowercase()).or_default();
        spelled.insert(prefix.to_owned());
    }
}

/// An existing spelling of a folder or file of `path` that differs only in case.
pub(super) fn clash(spellings: &Spellings, path: &str) -> Option<String> {
    prefixes(path).find_map(|prefix| {
        let spelled = spellings.get(&prefix.to_lowercase())?;
        if spelled.contains(prefix) {
            None
        } else {
            spelled.first().cloned()
        }
    })
}

fn prefixes(path: &str) -> impl Iterator<Item = &str> {
    let folders = path.match_indices('/').map(move |(end, _)| &path[..end]);
    folders.chain([path])
}

/// Paths laid out as empty files in a scratch folder on the vault's volume,
/// so the volume itself says which spellings it stores as one file. A
/// case-insensitive APFS volume, the macOS default, folds case and Unicode
/// forms: Tea.md and tea.md, Straße.md and Strasse.md, or a composed and a
/// decomposed é. Two edits of one file could neither finish nor roll back.
pub(super) struct Probe {
    // Paths are created relative to this open folder, so the scratch prefix
    // never counts toward the system's path length limit.
    folder: File,
    files: BTreeMap<u64, String>,
    _scratch: Temporary,
}

impl Probe {
    pub(super) fn new(vault: &Vault) -> Result<Self> {
        let scratch = Temporary::new(&vault.root.join(".git"), "names");
        fs::create_dir(&scratch.path)?;
        Ok(Self {
            folder: File::open(&scratch.path)?,
            files: BTreeMap::new(),
            _scratch: scratch,
        })
    }

    /// Lay `path` out, or say which earlier path the volume already holds
    /// under it.
    pub(super) fn lay(&mut self, path: &str) -> Result<Option<String>> {
        match self.create(path) {
            Ok(inode) => {
                self.files.insert(inode, path.to_owned());
                Ok(None)
            }
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::AlreadyExists | ErrorKind::NotADirectory
                ) =>
            {
                Ok(Some(
                    match self.inode(path).and_then(|inode| self.files.get(&inode)) {
                        Some(other) => format!("is the same file as {other} on this volume"),
                        None => {
                            "collides with a file or folder of another path on this volume".into()
                        }
                    },
                ))
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Create `path` and its folders as needed; return the new file's inode.
    fn create(&self, path: &str) -> io::Result<u64> {
        let folder = self.folder.as_raw_fd();
        for (end, _) in path.match_indices('/') {
            let name = CString::new(&path[..end]).map_err(io::Error::other)?;
            // An existing name is fine here: if it is a file, creating the
            // note below it fails with NotADirectory.
            if unsafe { libc::mkdirat(folder, name.as_ptr(), 0o700) } != 0 {
                let error = io::Error::last_os_error();
                if error.kind() != ErrorKind::AlreadyExists {
                    return Err(error);
                }
            }
        }
        let name = CString::new(path).map_err(io::Error::other)?;
        let flags = libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW;
        let flags = flags | libc::O_CLOEXEC;
        let created = unsafe { libc::openat(folder, name.as_ptr(), flags, 0o600 as libc::c_uint) };
        if created < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: openat returned a new descriptor that nothing else owns.
        let file = unsafe { File::from_raw_fd(created) };
        Ok(file.metadata()?.ino())
    }

    fn inode(&self, path: &str) -> Option<u64> {
        let name = CString::new(path).ok()?;
        // SAFETY: stat is plain data, and fstatat fills it or fails.
        let mut status: libc::stat = unsafe { std::mem::zeroed() };
        let folder = self.folder.as_raw_fd();
        let found = unsafe { libc::fstatat(folder, name.as_ptr(), &mut status, 0) };
        (found == 0).then_some(status.st_ino as u64)
    }
}

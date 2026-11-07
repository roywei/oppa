//! Who the assistant is. Its soul is the vault note `assistant/soul.md`, which
//! the model writes in words: name, look, languages and personality. Its face
//! is one picture the owner chose, kept in the private folder beside the vault,
//! which takes only Markdown notes. Setting a face keeps the ones before it, so
//! going back is setting an earlier one again.

use crate::config::{private_directory, Paths};
use anyhow::{bail, ensure, Context, Result};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

/// The note every chat reads first.
pub const NOTE: &str = "assistant/soul.md";

/// The personalities setup offers and the dashboard starts from: an id, the
/// name the owner sees, and the note with NAME, OWNER, LOOK and LANGUAGES to
/// fill in.
pub const PRESETS: [(&str, &str, &str); 4] = [
    (
        "chief-of-staff",
        "Chief of staff",
        include_str!("../resources/souls/chief-of-staff.md"),
    ),
    (
        "warm-friend",
        "Warm friend",
        include_str!("../resources/souls/warm-friend.md"),
    ),
    (
        "calm-minimal",
        "Calm and minimal",
        include_str!("../resources/souls/calm-minimal.md"),
    ),
    (
        "playful",
        "Playful",
        include_str!("../resources/souls/playful.md"),
    ),
];

/// A face is square, this many pixels on a side: enough for a contact photo
/// and an app icon.
const SIDE: u32 = 1024;
/// A larger file is not a picture anyone meant as a face.
const SOURCE_BYTES: u64 = 20 * 1024 * 1024;
/// Earlier faces a listing names, newest first.
const EARLIER_SHOWN: usize = 8;

/// Make the picture at `source` the face: its centered square, scaled to SIDE
/// pixels as a PNG by macOS `sips`, which also refuses what is not a picture.
/// The face it replaces moves to `avatars/`, named by when it was replaced.
pub fn set_avatar(paths: &Paths, source: &Path) -> Result<PathBuf> {
    let metadata =
        fs::metadata(source).with_context(|| format!("cannot read {}", source.display()))?;
    ensure!(metadata.is_file(), "{} is not a file", source.display());
    ensure!(metadata.len() <= SOURCE_BYTES, "a face is at most 20 MiB");
    let (width, height) = dimensions(source)?;
    let side = width.min(height).to_string();
    let staging = paths.private.join(format!(
        ".avatar-{}-{}.png",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let made = sips(
        &["-s", "format", "png", "--cropToHeightWidth", &side, &side],
        source,
        &staging,
    )
    .and_then(|()| {
        let side = SIDE.to_string();
        sips(&["-z", &side, &side], &staging, &staging)
    })
    .and_then(|()| match dimensions(&staging)? {
        (SIDE, SIDE) => Ok(()),
        (width, height) => bail!("the face came out {width}x{height}, not {SIDE}x{SIDE}"),
    });
    if let Err(error) = made {
        let _ = fs::remove_file(&staging);
        return Err(error);
    }
    let face = paths.avatar();
    if face.exists() {
        let earlier = paths.avatars();
        private_directory(&earlier)?;
        let stamp = chrono::Utc::now().format("%Y%m%dT%H%M%S%.3fZ");
        fs::rename(&face, earlier.join(format!("{stamp}.png")))?;
    }
    fs::rename(&staging, &face)?;
    Ok(face)
}

/// The current face, when there is one.
pub fn avatar(paths: &Paths) -> Option<PathBuf> {
    let face = paths.avatar();
    face.is_file().then_some(face)
}

/// Earlier faces, newest first.
pub fn earlier(paths: &Paths) -> Vec<PathBuf> {
    let mut faces: Vec<PathBuf> = fs::read_dir(paths.avatars())
        .map(|entries| {
            entries
                .filter_map(|entry| entry.ok().map(|entry| entry.path()))
                .filter(|path| path.extension().is_some_and(|ext| ext == "png"))
                .collect()
        })
        .unwrap_or_default();
    faces.sort();
    faces.reverse();
    faces.truncate(EARLIER_SHOWN);
    faces
}

/// What `oppa soul` shows and the dashboard reads.
pub fn show(paths: &Paths) -> serde_json::Value {
    serde_json::json!({
        "note": NOTE,
        "avatar": avatar(paths),
        "earlier": earlier(paths),
        "presets": PRESETS.map(|(id, title, text)| serde_json::json!({"id":id,"title":title,"text":text})),
    })
}

fn dimensions(picture: &Path) -> Result<(u32, u32)> {
    let output = crate::process::output(
        Command::new("sips")
            .args(["-g", "pixelWidth", "-g", "pixelHeight"])
            .arg(picture),
    )
    .context("setting a face needs macOS sips")?;
    let text = String::from_utf8_lossy(&output.stdout);
    let field = |name: &str| {
        text.lines()
            .find_map(|line| line.trim().strip_prefix(name)?.trim().parse::<u32>().ok())
    };
    match (field("pixelWidth:"), field("pixelHeight:")) {
        (Some(width), Some(height)) if output.status.success() && width > 0 && height > 0 => {
            Ok((width, height))
        }
        _ => bail!("{} is not a picture", picture.display()),
    }
}

fn sips(options: &[&str], source: &Path, out: &Path) -> Result<()> {
    let output = crate::process::output(
        Command::new("sips")
            .args(options)
            .arg(source)
            .arg("--out")
            .arg(out),
    )
    .context("setting a face needs macOS sips")?;
    ensure!(
        output.status.success() && out.is_file(),
        "sips could not convert {}: {}",
        source.display(),
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(())
}

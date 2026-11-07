//! The launcher is meant to keep macOS privacy grants across releases, on a
//! premise the owner has yet to confirm (README, Permissions for the service).
//! That needs it to stay the running program: it starts the `oppa` beside its
//! real path as a child, passes signals and the exit status through, and kills
//! a child that outlives the stop grace period. Synthetic sibling scripts in
//! temporary directories outside any checkout.
use std::{
    fs,
    os::unix::{
        fs::PermissionsExt,
        process::{CommandExt, ExitStatusExt},
    },
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant},
};
use tempfile::TempDir;

fn directory() -> TempDir {
    let base = if cfg!(target_os = "macos") {
        PathBuf::from("/private/tmp")
    } else {
        std::env::temp_dir()
    };
    tempfile::Builder::new()
        .prefix("oppa-launch-")
        .tempdir_in(base)
        .unwrap()
}

/// A copy of the built launcher, as the owner installs it once.
fn launcher_in(directory: &Path) -> PathBuf {
    let launcher = directory.join("oppa-launch");
    fs::copy(env!("CARGO_BIN_EXE_oppa-launch"), &launcher).unwrap();
    launcher
}

fn sibling_in(directory: &Path, script: &str) {
    let path = directory.join("oppa");
    fs::write(&path, format!("#!/bin/sh\n{script}\n")).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
}

/// Polls for up to ten seconds.
fn eventually(mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done() {
        if Instant::now() > deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    true
}

/// The launcher started as launchd starts a job, leading its own process
/// group, so a failed test can stop everything it started with one signal,
/// and with the stop signals at their defaults. A runner started in the
/// background by a shell ignores SIGINT, which every child inherits, and an
/// ignored signal never reaches the launcher (2026-10-05, a review's gate run).
fn job(launcher: &Path) -> Command {
    let mut command = Command::new(launcher);
    command.process_group(0);
    unsafe {
        command.pre_exec(|| {
            for signal in [libc::SIGTERM, libc::SIGINT, libc::SIGHUP] {
                libc::signal(signal, libc::SIG_DFL);
            }
            Ok(())
        });
    }
    command
}

/// Waits for the launcher to exit; if it does not, kills its whole process
/// group, the sibling included, and fails.
fn exit_status(launcher: &mut Child, after: &str) -> ExitStatus {
    let mut status = None;
    if !eventually(|| {
        status = launcher.try_wait().unwrap();
        status.is_some()
    }) {
        unsafe { libc::killpg(launcher.id() as libc::pid_t, libc::SIGKILL) };
        launcher.wait().unwrap();
        panic!("the launcher did not exit after {after}");
    }
    status.unwrap()
}

#[test]
fn sibling_runs_as_a_child_with_the_launchers_arguments_environment_and_status() {
    // Exec would give the process `oppa`'s identity; a child keeps the launcher's.
    let root = directory();
    let launcher = launcher_in(root.path());
    sibling_in(
        root.path(),
        r#"printf '%s\n' "$PPID" "$@" "$OPPA_LAUNCH_PROBE" > "$(dirname "$0")/seen"; exit 7"#,
    );
    let mut child = Command::new(&launcher)
        .args(["serve", "--help", "two words"])
        .env("OPPA_LAUNCH_PROBE", "synthetic value")
        .spawn()
        .unwrap();
    assert_eq!(child.wait().unwrap().code(), Some(7));
    assert_eq!(
        fs::read_to_string(root.path().join("seen")).unwrap(),
        format!(
            "{}\nserve\n--help\ntwo words\nsynthetic value\n",
            child.id()
        )
    );
}

#[test]
fn stop_signals_to_the_launcher_reach_the_child_and_report_its_signal() {
    // launchd stops a job with SIGTERM to the program it started; someone
    // running the launcher by hand may send SIGINT or SIGHUP instead.
    for signal in [libc::SIGTERM, libc::SIGINT, libc::SIGHUP] {
        let root = directory();
        let launcher = launcher_in(root.path());
        sibling_in(
            root.path(),
            r#": > "$(dirname "$0")/started"; exec sleep 30"#,
        );
        let mut launcher = job(&launcher).arg("work").spawn().unwrap();
        let started = root.path().join("started");
        assert!(eventually(|| started.exists()), "the sibling did not start");
        assert_eq!(
            unsafe { libc::kill(launcher.id() as libc::pid_t, signal) },
            0
        );
        let status = exit_status(&mut launcher, &format!("signal {signal}"));
        assert_eq!(status.signal(), None, "signal {signal} killed the launcher");
        assert_eq!(status.code(), Some(128 + signal), "after signal {signal}");
    }
}

#[test]
fn a_child_still_running_when_the_grace_period_ends_is_killed() {
    // launchd sends SIGKILL only to the program it started. A draining
    // `oppa serve` left behind keeps the serve lock, so every restart fails.
    let root = directory();
    let launcher = launcher_in(root.path());
    sibling_in(
        root.path(),
        r#"trap '' TERM; : > "$(dirname "$0")/started"; exec sleep 60"#,
    );
    let mut launcher = job(&launcher)
        .arg("serve")
        .env("OPPA_LAUNCH_GRACE_SECONDS", "1")
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let started = root.path().join("started");
    assert!(eventually(|| started.exists()), "the sibling did not start");
    let sent = Instant::now();
    assert_eq!(
        unsafe { libc::kill(launcher.id() as libc::pid_t, libc::SIGTERM) },
        0
    );
    let status = exit_status(&mut launcher, "the grace period");
    assert!(sent.elapsed() >= Duration::from_secs(1), "killed too early");
    assert_eq!(status.signal(), None, "the launcher itself died: {status}");
    assert_eq!(status.code(), Some(128 + libc::SIGKILL));
}

#[test]
fn only_the_oppa_beside_the_launchers_real_file_can_run() {
    // No argument, PATH entry or symlink to the launcher chooses another path.
    let real = directory();
    let launcher = launcher_in(real.path());
    let decoy = directory();
    sibling_in(decoy.path(), r#"touch "$(dirname "$0")/ran""#);
    let link = decoy.path().join("oppa-launch");
    std::os::unix::fs::symlink(&launcher, &link).unwrap();
    let output = Command::new(&link)
        .arg(decoy.path().join("oppa"))
        .env("PATH", decoy.path())
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert_eq!(stderr.lines().count(), 1, "{stderr}");
    let expected = real.path().canonicalize().unwrap().join("oppa");
    assert!(stderr.contains(&expected.display().to_string()), "{stderr}");
    assert!(!decoy.path().join("ran").exists());
}

#[cfg(target_os = "macos")]
#[test]
fn launcher_embeds_the_info_plist_privacy_prompts_read() {
    let root = directory();
    let extracted = root.path().join("Info.plist");
    let status = Command::new("segedit")
        .arg(env!("CARGO_BIN_EXE_oppa-launch"))
        .args(["-extract", "__TEXT", "__info_plist"])
        .arg(&extracted)
        .status()
        .unwrap();
    assert!(status.success(), "no __TEXT,__info_plist section");
    let identifier = Command::new("plutil")
        .args(["-extract", "CFBundleIdentifier", "raw", "-o", "-"])
        .arg(&extracted)
        .output()
        .unwrap();
    assert_eq!(
        String::from_utf8(identifier.stdout).unwrap().trim(),
        "ai.oppa.launch"
    );
}

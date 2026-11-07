//! Every process OPPA starts goes through here.
//!
//! On macOS a spawn makes its pipes and only then marks them close-on-exec
//! (there is no `pipe2`), so a process another thread starts in between keeps
//! a copy of them. Whoever waits for such a pipe's end of file, as a connector
//! call waits for its answer and a connector waits for its request, then waits
//! until that stranger exits. One lock around each spawn closes the gap.

use std::io;
use std::process::{Command, ExitStatus, Output, Stdio};
use std::sync::Mutex;

static SPAWN: Mutex<()> = Mutex::new(());

/// Runs `start`, which spawns one process (std or tokio), while no other
/// thread of this process spawns one.
pub fn spawning<T>(start: impl FnOnce() -> T) -> T {
    let _only = SPAWN
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    start()
}

/// `Command::output`, spawned under the lock and awaited outside it. Unlike
/// std's, it sets all three handles itself (stdin empty, the others
/// captured), over any the caller set.
pub fn output(command: &mut Command) -> io::Result<Output> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    spawning(|| command.spawn())?.wait_with_output()
}

/// `Command::status`, spawned under the lock and awaited outside it.
pub fn status(command: &mut Command) -> io::Result<ExitStatus> {
    spawning(|| command.spawn())?.wait()
}

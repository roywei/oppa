//! The service's permission holder, on a premise not yet confirmed on a real
//! Mac: macOS attributes a launchd job's privacy access to the program launchd
//! starts, and that program's children use it too. Every release replaces the
//! ad hoc signed `oppa`, which loses grants given to it, so launchd starts this
//! binary instead: installed once, never rebuilt by releases, it runs the
//! `oppa` beside it as a child. Exec would give the process `oppa`'s code
//! identity, which holds no grants after a release.
use std::io::Write;
use std::os::unix::process::CommandExt;
use std::process::{exit, Command};
use std::time::Duration;

fn main() {
    // Blocked before the child exists, so an early stop request waits to be
    // forwarded instead of killing the launcher.
    let (forwarded, original) = unsafe {
        let mut set: libc::sigset_t = std::mem::zeroed();
        let mut original: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut set);
        for signal in [libc::SIGTERM, libc::SIGINT, libc::SIGHUP] {
            libc::sigaddset(&mut set, signal);
        }
        libc::pthread_sigmask(libc::SIG_BLOCK, &set, &mut original);
        (set, original)
    };
    // The file named `oppa` beside this file's real path, whatever that file
    // is or links to: no argument, environment variable, PATH entry or
    // symlink to the launcher chooses another path.
    let program = match std::env::current_exe().and_then(|path| path.canonicalize()) {
        Ok(path) => path.with_file_name("oppa"),
        Err(error) => fail(format_args!("cannot resolve its own path: {error}")),
    };
    // When a job outlives its ExitTimeOut, launchd sends SIGKILL to the
    // launcher alone, and a draining `oppa` would run on without it, holding
    // its locks. So the launcher kills `oppa` first: 15 seconds after the
    // first forwarded signal by default, inside launchd's usual 20-second
    // ExitTimeOut even when the agent's plist does not raise it.
    let grace = match std::env::var_os("OPPA_LAUNCH_GRACE_SECONDS") {
        None => Duration::from_secs(15),
        Some(value) => match value.to_str().and_then(|seconds| seconds.parse().ok()) {
            Some(seconds) => Duration::from_secs(seconds),
            None => fail(format_args!(
                "OPPA_LAUNCH_GRACE_SECONDS is not whole seconds: {value:?}"
            )),
        },
    };
    let mut command = Command::new(&program);
    command.args(std::env::args_os().skip(1));
    // std passes the spawning thread's signal mask on; the child gets the
    // mask this launcher started with, not the forwarding one.
    unsafe {
        command.pre_exec(move || {
            match libc::sigprocmask(libc::SIG_SETMASK, &original, std::ptr::null_mut()) {
                0 => Ok(()),
                _ => Err(std::io::Error::last_os_error()),
            }
        });
    }
    let pid = match command.spawn() {
        Ok(child) => child.id() as libc::pid_t,
        Err(error) => fail(format_args!("cannot start {}: {error}", program.display())),
    };
    let (stopping, first_stop) = std::sync::mpsc::channel();
    std::thread::spawn(move || loop {
        let mut signal = 0;
        if unsafe { libc::sigwait(&forwarded, &mut signal) } == 0 {
            unsafe { libc::kill(pid, signal) };
            let _ = stopping.send(signal);
        }
    });
    let name = program.display().to_string();
    std::thread::spawn(move || {
        if let Ok(signal) = first_stop.recv() {
            std::thread::sleep(grace);
            let seconds = grace.as_secs();
            let _ = writeln!(
                std::io::stderr(),
                "oppa-launch: {name} still running {seconds}s after signal {signal}; killing it"
            );
            unsafe { libc::kill(pid, libc::SIGKILL) };
        }
    });
    // Never reaped here: while the launcher lives, the exited child keeps its
    // pid, so a late forwarded signal or SIGKILL cannot reach a process that
    // reused it. launchd reaps it after the launcher exits.
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    let flags = libc::WEXITED | libc::WNOWAIT;
    while unsafe { libc::waitid(libc::P_PID, pid as libc::id_t, &mut info, flags) } != 0 {
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::Interrupted {
            fail(format_args!("lost {}: {error}", program.display()));
        }
    }
    let status = unsafe { info.si_status() };
    exit(match info.si_code {
        libc::CLD_EXITED => status,
        _ => 128 + status,
    })
}

fn fail(message: std::fmt::Arguments) -> ! {
    eprintln!("oppa-launch: {message}");
    exit(127)
}

//! Versioned process boundary for trusted-adult harnesses.
//!
//! The adapter inherits the operator's environment and native login stores.
//! This is not child/outsider isolation. Stderr is bounded and discarded; only
//! normalized events can enter the runtime. Adapters are trusted host programs.

use crate::config::private_directory;
use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::{fs, future::Future, path::Path, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
    process::Command,
};

pub const PROTOCOL_VERSION: u32 = 1;

const MAX_REQUEST: usize = 2 * 1024 * 1024;
const MAX_LINE: usize = 256 * 1024;
const MAX_STDOUT: usize = 4 * 1024 * 1024;
const MAX_STDERR: usize = 64 * 1024;

/// Adapters built into this binary. `init` and every process that runs turns
/// install them into the instance, so a newer core never meets adapters written
/// by an older build. Custom adapters belong outside the managed directory.
/// Harness adapters shape what a model sees, so they are part of the session
/// policy; a channel adapter is transport and is not.
pub const HARNESS_ADAPTERS: [(&str, &str); 3] = [
    ("codex.py", include_str!("../resources/adapters/codex.py")),
    ("claude.py", include_str!("../resources/adapters/claude.py")),
    ("common.py", include_str!("../resources/adapters/common.py")),
];
pub const CHANNEL_ADAPTERS: [(&str, &str); 6] = [
    (
        "imessage.py",
        include_str!("../resources/adapters/imessage.py"),
    ),
    ("wechat.py", include_str!("../resources/adapters/wechat.py")),
    ("slack.py", include_str!("../resources/adapters/slack.py")),
    (
        "slack_format.py",
        include_str!("../resources/adapters/slack_format.py"),
    ),
    ("web.py", include_str!("../resources/adapters/web.py")),
    ("web.html", include_str!("../resources/adapters/web.html")),
];
/// Script connectors are called through the broker, which lists their tools
/// at each run, so like channel adapters they are not session policy. A slice,
/// so adding a connector adds only its own entry.
pub const CONNECTOR_ADAPTERS: &[(&str, &str)] = &[
    ("apple.py", include_str!("../resources/connectors/apple.py")),
    (
        "onepassword.py",
        include_str!("../resources/connectors/onepassword.py"),
    ),
    (
        "apple.swift",
        include_str!("../resources/connectors/apple.swift"),
    ),
    // Not a connector: the memory sync's command, installed beside them.
    (
        "memory_changes.py",
        include_str!("../resources/adapters/memory_changes.py"),
    ),
];

fn bundled_adapters() -> impl Iterator<Item = &'static (&'static str, &'static str)> {
    HARNESS_ADAPTERS
        .iter()
        .chain(CHANNEL_ADAPTERS.iter())
        .chain(CONNECTOR_ADAPTERS.iter())
}

/// Install the bundled adapters into `<runtime>/adapters`, replacing any copy
/// that differs. Each file is renamed into place, so a starting harness reads
/// the old adapter or the new one, never a partial file.
pub fn install_bundled_adapters(runtime: &Path) -> Result<()> {
    install(runtime, bundled_adapters())
}

/// Install only the connector adapters, for commands that run no turns and
/// must not replace the harness adapters a running service uses.
pub fn install_connector_adapters(runtime: &Path) -> Result<()> {
    install(runtime, CONNECTOR_ADAPTERS.iter())
}

fn install<'a>(
    runtime: &Path,
    adapters: impl Iterator<Item = &'a (&'static str, &'static str)>,
) -> Result<()> {
    let directory = runtime.join("adapters");
    private_directory(&directory)?;
    for (name, content) in adapters {
        let path = directory.join(name);
        if fs::read(&path).is_ok_and(|current| current == content.as_bytes()) {
            continue;
        }
        let temporary = directory.join(format!(".{name}.{}.tmp", std::process::id()));
        let written = fs::write(&temporary, content).and_then(|()| fs::rename(&temporary, &path));
        if written.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        written?;
    }
    Ok(())
}

/// Bundled adapters whose installed copy is missing or differs from this build.
pub fn stale_bundled_adapters(runtime: &Path) -> Vec<&'static str> {
    bundled_adapters()
        .filter(|(name, content)| {
            fs::read(runtime.join("adapters").join(name))
                .map_or(true, |current| current != content.as_bytes())
        })
        .map(|(name, _)| *name)
        .collect()
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifest {
    pub id: String,
    pub family: String,
    pub command: Vec<String>,
    /// A conversation turn's limit.
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,
    /// A background task's limit: delegated work such as writing a program
    /// runs far longer than a reply.
    #[serde(default = "default_task_timeout")]
    pub task_timeout_seconds: u64,
    /// The model to run; the harness's own default when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// low, medium, high, xhigh or max; medium when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
}

/// Set in the environment of every harness run and what it starts.
pub const RUN_MARKER: &str = "OPPA_RUN";

/// The longest any harness run may last: twelve hours.
pub const MAX_TIMEOUT_SECONDS: u64 = 12 * 3600;

fn default_timeout() -> u64 {
    120
}

fn default_task_timeout() -> u64 {
    3600
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessRequest {
    pub protocol_version: u32,
    pub conversation: String,
    pub prompt: String,
    pub session: Option<String>,
    /// Standing instructions for the harness's system or developer channel.
    /// Adapters must keep them out of the transcript so turns do not repeat them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp: Option<McpServer>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_directory: Option<String>,
    /// The model this harness runs, when the owner chose one; the harness's
    /// own default otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// How hard the model thinks: low, medium (the default), high, xhigh or max.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    /// Whether an owner run has the harness's own browser (Claude in Chrome,
    /// Codex browser use).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub browser: bool,
    /// What the harness may touch beyond the broker. Read runs get only the
    /// broker; the others are the harness as the owner set it up and route
    /// every native permission prompt to the broker's `permission.decide` tool.
    #[serde(default, skip_serializing_if = "Access::is_read")]
    pub access: Access,
    /// A folder of this turn's attached files, copied by the host, that the
    /// harness may read at any access; the host removes it after the run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readable_dir: Option<String>,
    /// A file the host keeps, never sent: each native session handle the
    /// adapter reports is added to it as it arrives, as a JSON line
    /// `{"agent":<harness id>,"session":<handle>}`.
    #[serde(skip)]
    pub session_log: Option<std::path::PathBuf>,
}

/// The owner's choice of harness reach. `connectors` adds every account, MCP server and plugin the owner
/// configured in the harness, its web search and browser; `computer` (also
/// called `full`) adds every tool and file.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Access {
    #[default]
    Read,
    Connectors,
    #[serde(alias = "full")]
    #[value(alias = "full")]
    Computer,
}

impl Access {
    pub fn is_read(&self) -> bool {
        *self == Self::Read
    }
}

/// Host-created stdio launch descriptor. Grants stay in a private file named in
/// args; the request carries no credential values or model-authored authority.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct McpServer {
    pub command: String,
    pub args: Vec<String>,
    /// Connector read tools this broker offers, so the adapter can allow them
    /// and count their calls as reads.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub read_tools: Vec<String>,
    /// Connector write tools a full-access run's broker offers and journals;
    /// the adapter allows them and counts their calls as effects.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub write_tools: Vec<String>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum Activity {
    #[default]
    None,
    Read,
    Effect,
    Unmediated,
}

impl Activity {
    pub fn may_have_effects(self) -> bool {
        matches!(self, Self::Effect | Self::Unmediated)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FailureCategory {
    Quota,
    Auth,
    Unavailable,
    SessionInvalid,
    Timeout,
    Protocol,
    Failed,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Failure {
    pub category: FailureCategory,
    pub diagnostic: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum HarnessEvent {
    Session {
        handle: String,
    },
    Activity {
        kind: Activity,
    },
    /// A Read of a file by name: opened, or refused by the harness's permissions.
    FileRead {
        name: String,
        opened: bool,
    },
    Answer {
        text: String,
    },
    Failure {
        category: FailureCategory,
        diagnostic: String,
    },
    Completed,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct RunOutcome {
    pub session: Option<String>,
    pub answer: Option<String>,
    pub failure: Option<Failure>,
    pub activity: Activity,
    /// The names of files a harness read, and of those its permissions refused;
    /// no paths.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files_read: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files_refused: Vec<String>,
    /// Whether the run's outcome was completely observed. This does not turn
    /// absent telemetry into evidence that an action occurred.
    pub telemetry_complete: bool,
}

/// These reasons come from the host, never from adapter output.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    Requested,
    AuthorityChanged,
    MonitorUnavailable,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CleanupStatus {
    NotStarted,
    Confirmed,
    Unconfirmed { diagnostic: String },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ControlledRun {
    pub outcome: RunOutcome,
    pub stop: Option<StopReason>,
    pub cleanup: CleanupStatus,
}

impl RunOutcome {
    fn fail(&mut self, category: FailureCategory, diagnostic: &str, incomplete: bool) {
        // A late fault must not erase already observed output from the private
        // diagnostic record. A failure still prevents runtime delivery.
        self.failure = Some(Failure {
            category,
            diagnostic: diagnostic.into(),
        });
        self.telemetry_complete = !incomplete;
    }
}

/// No shell is involved. On Unix every run owns a process group, so timeout,
/// malformed output and cancellation also terminate native CLI descendants.
/// Detached processes are outside this boundary; adapters must not detach.
pub async fn run(manifest: &AgentManifest, request: &HarnessRequest) -> Result<RunOutcome> {
    let result = run_controlled(manifest, request, std::future::pending()).await?;
    if let CleanupStatus::Unconfirmed { diagnostic } = result.cleanup {
        bail!("{diagnostic}");
    }
    Ok(result.outcome)
}

/// Await this call to observe cancellation and owned-group cleanup. Dropping
/// the future only attempts emergency cleanup and provides no stop evidence.
/// After launch, even unconfirmed cleanup returns the partial observed outcome.
pub async fn run_controlled<C: Future<Output = StopReason>>(
    manifest: &AgentManifest,
    request: &HarnessRequest,
    stop: C,
) -> Result<ControlledRun> {
    ensure!(
        request.protocol_version == PROTOCOL_VERSION,
        "unsupported harness protocol"
    );
    ensure!(!request.conversation.is_empty(), "conversation is required");
    if let Some(directory) = &request.working_directory {
        let path = std::path::Path::new(directory);
        ensure!(
            path.is_absolute() && path.is_dir(),
            "working directory must be an existing absolute directory"
        );
    }
    if let Some(mcp) = &request.mcp {
        ensure!(
            !mcp.command.trim().is_empty() && !mcp.command.contains('\0'),
            "invalid MCP executable"
        );
        ensure!(
            mcp.args.len() <= 128 && mcp.args.iter().all(|arg| !arg.contains('\0')),
            "invalid MCP arguments"
        );
    }
    ensure!(
        !manifest.id.is_empty() && !manifest.family.is_empty(),
        "agent identity is required"
    );
    ensure!(
        !manifest.command.is_empty() && !manifest.command[0].is_empty(),
        "adapter command is required"
    );
    ensure!(
        (1..=MAX_TIMEOUT_SECONDS).contains(&manifest.timeout_seconds),
        "adapter timeout must be at most twelve hours"
    );
    ensure!(
        cfg!(unix),
        "harness process containment currently requires Unix"
    );
    let mut input = serde_json::to_vec(request)?;
    ensure!(
        input.len() < MAX_REQUEST,
        "harness request exceeds size limit"
    );
    input.push(b'\n');

    tokio::pin!(stop);
    let early_stop = tokio::select! {
        biased;
        reason = &mut stop => Some(reason),
        () = std::future::ready(()) => None,
    };
    if let Some(reason) = early_stop {
        let mut outcome = RunOutcome::default();
        outcome.fail(FailureCategory::Failed, "adapter stopped by host", true);
        return Ok(ControlledRun {
            outcome,
            stop: Some(reason),
            cleanup: CleanupStatus::NotStarted,
        });
    }

    let mut command = Command::new(&manifest.command[0]);
    if let Some(directory) = &request.working_directory {
        command.current_dir(directory);
    }
    // Marks everything a run starts, its shell included, so commands that are
    // the owner's alone (`oppa settings access`) refuse it.
    command.env(RUN_MARKER, "1");
    command
        .args(&manifest.command[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // OwnedProcess controls all signals before reaping. Tokio must not
        // independently signal its cached PID after a lost ownership check.
        .kill_on_drop(false);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.as_std_mut().process_group(0);
    }
    let mut outcome = RunOutcome::default();
    let child = match crate::process::spawning(|| command.spawn()) {
        Ok(child) => child,
        Err(_) => {
            outcome.fail(
                FailureCategory::Unavailable,
                "adapter could not be started",
                false,
            );
            return Ok(ControlledRun {
                outcome,
                stop: None,
                cleanup: CleanupStatus::NotStarted,
            });
        }
    };
    let mut process = OwnedProcess::new(child);
    enum Observed {
        Finished(Result<()>),
        TimedOut,
        Stopped(StopReason),
    }
    let observed = {
        let execution = async {
            let mut stdin = process
                .child
                .stdin
                .take()
                .context("missing adapter stdin")?;
            let stdout = process
                .child
                .stdout
                .take()
                .context("missing adapter stdout")?;
            let stderr = process
                .child
                .stderr
                .take()
                .context("missing adapter stderr")?;
            let write = async {
                stdin
                    .write_all(&input)
                    .await
                    .context("adapter input failed")?;
                stdin
                    .shutdown()
                    .await
                    .context("adapter input shutdown failed")?;
                drop(stdin);
                Ok::<_, anyhow::Error>(())
            };
            let wait = async {
                process.wait_until_exit().await?;
                // Keep the leader unreaped while closing inherited pipes.
                process.kill_group().context("adapter group cleanup failed")
            };
            let (_, (), (), ()) = tokio::try_join!(
                write,
                read_events(
                    stdout,
                    &mut outcome,
                    request
                        .session_log
                        .as_deref()
                        .map(|log| (log, manifest.id.as_str())),
                ),
                drain_stderr(stderr),
                wait
            )?;
            Ok::<_, anyhow::Error>(())
        };
        tokio::select! {
            biased;
            reason = &mut stop => Observed::Stopped(reason),
            result = tokio::time::timeout(Duration::from_secs(manifest.timeout_seconds), execution) => {
                match result {
                    Ok(result) => Observed::Finished(result),
                    Err(_) => Observed::TimedOut,
                }
            }
        }
    };
    let stop = match observed {
        Observed::Stopped(reason) => Some(reason),
        _ => None,
    };
    let status = process.finish().await;
    let cleanup = if status.is_ok() {
        CleanupStatus::Confirmed
    } else {
        CleanupStatus::Unconfirmed {
            diagnostic: "adapter termination could not be confirmed".into(),
        }
    };
    match observed {
        Observed::Finished(Ok(())) if status.as_ref().is_ok_and(|status| status.success()) => {}
        Observed::Finished(_) => outcome.fail(
            FailureCategory::Protocol,
            "adapter stream or exit was invalid",
            true,
        ),
        Observed::TimedOut => outcome.fail(
            FailureCategory::Timeout,
            "adapter exceeded its deadline",
            true,
        ),
        Observed::Stopped(_) => {
            outcome.fail(FailureCategory::Failed, "adapter stopped by host", true)
        }
    }
    if status.is_err() {
        outcome.fail(
            FailureCategory::Failed,
            "adapter termination could not be confirmed",
            true,
        );
    }
    Ok(ControlledRun {
        outcome,
        stop,
        cleanup,
    })
}

async fn read_events(
    reader: impl AsyncRead + Unpin,
    outcome: &mut RunOutcome,
    session_log: Option<(&std::path::Path, &str)>,
) -> Result<()> {
    let mut reader = BufReader::new(reader);
    let mut total = 0;
    let mut completed = false;
    loop {
        let mut line = Vec::new();
        let count = (&mut reader)
            .take((MAX_LINE + 1) as u64)
            .read_until(b'\n', &mut line)
            .await?;
        if count == 0 {
            break;
        }
        total += count;
        ensure!(
            count <= MAX_LINE && total <= MAX_STDOUT,
            "adapter output limit exceeded"
        );
        ensure!(line.ends_with(b"\n"), "incomplete adapter event");
        ensure!(!completed, "event after completion");
        match serde_json::from_slice::<HarnessEvent>(&line)? {
            HarnessEvent::Session { handle } => {
                ensure!(
                    outcome.session.is_none() && !handle.is_empty() && handle.len() <= 4096,
                    "invalid session event"
                );
                if let Some((log, agent)) = session_log {
                    let mut log = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(log)?;
                    let line = serde_json::json!({"agent":agent,"session":handle});
                    std::io::Write::write_all(&mut log, format!("{line}\n").as_bytes())?;
                }
                outcome.session = Some(handle);
            }
            HarnessEvent::Activity { kind } => outcome.activity = outcome.activity.max(kind),
            HarnessEvent::FileRead { name, opened } => {
                ensure!(
                    !name.is_empty() && name.len() <= 255 && !name.contains(['/', '\0']),
                    "invalid file read event"
                );
                // Each name once, and at most 64, however many reads a run makes.
                let names = if opened {
                    &mut outcome.files_read
                } else {
                    &mut outcome.files_refused
                };
                if names.len() < 64 && !names.contains(&name) {
                    names.push(name);
                }
            }
            HarnessEvent::Answer { text } => {
                ensure!(
                    outcome.answer.is_none() && outcome.failure.is_none(),
                    "conflicting terminal events"
                );
                outcome.answer = Some(text);
            }
            HarnessEvent::Failure {
                category,
                diagnostic,
            } => {
                ensure!(
                    outcome.failure.is_none()
                        && outcome.answer.is_none()
                        && diagnostic.len() <= 4096,
                    "invalid failure event"
                );
                outcome.failure = Some(Failure {
                    category,
                    diagnostic,
                });
            }
            HarnessEvent::Completed => completed = true,
        }
    }
    ensure!(completed, "missing adapter completion");
    ensure!(
        outcome.answer.is_some() || outcome.failure.is_some(),
        "missing explicit answer or failure"
    );
    outcome.telemetry_complete = !outcome.failure.as_ref().is_some_and(|failure| {
        matches!(
            failure.category,
            FailureCategory::Protocol | FailureCategory::Timeout | FailureCategory::Failed
        )
    });
    Ok(())
}

async fn drain_stderr(mut reader: impl AsyncRead + Unpin) -> Result<()> {
    let mut buffer = [0u8; 4096];
    let mut total = 0;
    loop {
        let count = reader.read(&mut buffer).await?;
        if count == 0 {
            return Ok(());
        }
        total += count;
        if total > MAX_STDERR {
            bail!("adapter stderr limit exceeded");
        }
    }
}

/// Child ownership and process-group cleanup share one lifetime. No reaping wait
/// is polled until the group identifier is disarmed. Cancellation drops this
/// guard before Tokio can drop/reap the contained child. Harness adapters and
/// schedule commands run under it; spawn the child in its own process group
/// with `kill_on_drop(false)`.
pub(crate) struct OwnedProcess {
    pub(crate) child: tokio::process::Child,
    group: Option<u32>,
}

impl OwnedProcess {
    pub(crate) fn new(child: tokio::process::Child) -> Self {
        let group = child.id();
        Self { child, group }
    }

    pub(crate) async fn wait_until_exit(&self) -> Result<()> {
        let pid = self.group.context("process group already disarmed")?;
        loop {
            if child_has_exited(pid).context("observe process exit without reaping")? {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    pub(crate) fn kill_group(&mut self) -> std::io::Result<()> {
        let Some(pid) = self.group else {
            return Ok(());
        };
        // Never signal a number after ownership was lost, including an external
        // waiter violating the normal exclusive-Child ownership contract.
        match child_has_exited(pid) {
            Ok(_) => {}
            Err(error) if error.raw_os_error() == Some(libc::ECHILD) => {
                self.group = None;
                return Err(error);
            }
            Err(error) => return Err(error),
        }
        #[cfg(unix)]
        if unsafe { libc::kill(-(pid as libc::pid_t), libc::SIGKILL) } != 0 {
            let error = std::io::Error::last_os_error();
            #[cfg(target_os = "macos")]
            if error.raw_os_error() == Some(libc::EPERM) && group_finishes_exit_transition(pid) {
                return Ok(());
            }
            if error.raw_os_error() != Some(libc::ESRCH) {
                return Err(error);
            }
        }
        Ok(())
    }

    pub(crate) async fn finish(&mut self) -> Result<std::process::ExitStatus> {
        self.kill_group().context("process group cleanup failed")?;
        let group = self.group.context("process ownership is unconfirmed")?;
        // SIGKILL acceptance is not yet termination evidence. Keep the leader
        // unreaped while observing every remaining member as absent or dead.
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if child_has_exited(group)? && group_has_no_live_members(group) {
                    return Ok::<_, std::io::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .context("process group termination timed out")??;
        self.group = None;
        // This is the sole normal reaping boundary. group is already None even
        // if this future is cancelled, so Drop cannot signal a reused PGID.
        self.child.wait().await.context("process reap failed")
    }
}

#[cfg(target_os = "macos")]
fn group_finishes_exit_transition(leader: u32) -> bool {
    // Darwin can stop accepting signals before waitid reports the leader as
    // exited. An immediate EPERM classification races that transition. Keep
    // the child unreaped, wait briefly for its terminal state, then still check
    // every remaining group member. A live permission denial remains an error.
    // This synchronous bound also applies when cancellation runs our Drop.
    let deadline = std::time::Instant::now() + Duration::from_millis(100);
    loop {
        match child_has_exited(leader) {
            Ok(true) if group_has_no_live_members(leader) => return true,
            Ok(_) => {}
            Err(_) => return false,
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[cfg(target_os = "macos")]
fn group_has_no_live_members(leader: u32) -> bool {
    // Darwin killpg1 filters zombies, then reports EPERM when no signalable
    // member remains. Verify that narrow terminal case; do not hide an actual
    // permission failure for a live descendant. The unreaped leader reserves
    // this group's identity throughout the inspection.
    // https://github.com/apple-oss-distributions/xnu/blob/main/bsd/kern/kern_sig.c
    const PROC_PGRP_ONLY: u32 = 2;
    let mut members = [0i32; 256];
    let capacity = std::mem::size_of_val(&members) as i32;
    let bytes = unsafe {
        libc::proc_listpids(
            PROC_PGRP_ONLY,
            leader,
            members.as_mut_ptr().cast(),
            capacity,
        )
    };
    if bytes <= 0 || bytes >= capacity || bytes as usize % std::mem::size_of::<i32>() != 0 {
        return false;
    }
    let members = &members[..bytes as usize / std::mem::size_of::<i32>()];
    if !members.contains(&(leader as i32)) {
        return false;
    }
    for &pid in members {
        if pid == leader as i32 {
            continue;
        }
        let mut information: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
        let expected = std::mem::size_of_val(&information) as i32;
        let read = unsafe {
            libc::proc_pidinfo(
                pid,
                libc::PROC_PIDTBSDINFO,
                0,
                (&mut information as *mut libc::proc_bsdinfo).cast(),
                expected,
            )
        };
        if read == expected {
            if information.pbi_pgid == leader && information.pbi_status != libc::SZOMB {
                return false;
            }
        } else if read != 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH) {
            return false;
        }
    }
    true
}

#[cfg(target_os = "linux")]
fn group_has_no_live_members(leader: u32) -> bool {
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return false;
    };
    let mut found_leader = false;
    for entry in entries {
        let Ok(entry) = entry else { return false };
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        let stat = match std::fs::read_to_string(entry.path().join("stat")) {
            Ok(stat) => stat,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return false,
        };
        // comm can contain whitespace and parentheses; fields following its
        // final ')' are state, parent PID, then process-group ID.
        let Some((_, fields)) = stat.rsplit_once(')') else {
            return false;
        };
        let mut fields = fields.split_whitespace();
        let state = fields.next();
        let _parent = fields.next();
        let group = fields.next().and_then(|field| field.parse::<u32>().ok());
        if group == Some(leader) {
            found_leader |= pid == leader;
            if !matches!(state, Some("Z" | "X")) {
                return false;
            }
        }
    }
    found_leader
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn group_has_no_live_members(_leader: u32) -> bool {
    false
}

impl Drop for OwnedProcess {
    fn drop(&mut self) {
        let _ = self.kill_group();
        self.group = None;
    }
}

#[cfg(unix)]
fn child_has_exited(pid: u32) -> std::io::Result<bool> {
    loop {
        let mut information: libc::siginfo_t = unsafe { std::mem::zeroed() };
        let result = unsafe {
            libc::waitid(
                libc::P_PID,
                pid as libc::id_t,
                &mut information,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        if result == 0 {
            return Ok(unsafe { information.si_pid() } == pid as libc::pid_t);
        }
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

#[cfg(not(unix))]
fn child_has_exited(_pid: u32) -> std::io::Result<bool> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "owned process groups require Unix",
    ))
}

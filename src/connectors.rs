//! Connector adapter v1: the host starts a script
//! connector's command without a shell, writes one JSON request on stdin and
//! reads one JSON object from stdout. The adapter owns the service and its
//! credentials; the host offers only operations whose tier it can enforce.

use crate::config::{ConnectorKind, ConnectorManifest};
use anyhow::{bail, ensure, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    io::{Read, Write},
    os::unix::process::CommandExt,
    process::{Command, Stdio},
    sync::mpsc,
    time::{Duration, Instant},
};

const MAX_RESPONSE: u64 = 1024 * 1024;
/// Harness adapters accept at most 256 connector tools in all; one connector
/// may offer this many, and the broker stops adding connectors at the total.
pub const MAX_OPERATIONS: usize = 64;
/// Only these reach an adapter; a connector gets its own credentials from its
/// own store, never from the host's environment.
const ENVIRONMENT: [&str; 7] = [
    "PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "USER", "LOGNAME",
];
/// Enough for the operating system to ask up to three access questions.
pub const PROMPT_TIMEOUT_SECONDS: u64 = 400;

/// One operation a connector offers. `read` operations reach every run; the
/// broker offers `reversible` and `outward` ones only to full-access runs and
/// journals each call.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Operation {
    pub name: String,
    pub tier: String,
    pub description: String,
    pub input_schema: Value,
}

#[derive(Deserialize)]
struct Description {
    protocol_version: u32,
    operations: Vec<Operation>,
}

/// The operations a script connector offers, validated.
pub fn describe(connector: &ConnectorManifest) -> Result<Vec<Operation>> {
    let value = exchange(connector, json!({"protocol_version":1,"method":"describe"}))?;
    let description: Description =
        serde_json::from_value(value).context("connector description is malformed")?;
    ensure!(
        description.protocol_version == 1,
        "unsupported connector protocol"
    );
    let mut names = std::collections::HashSet::new();
    ensure!(
        description.operations.len() <= MAX_OPERATIONS,
        "connector offers too many operations"
    );
    let mut operations = Vec::new();
    for operation in description.operations {
        let schema = &operation.input_schema;
        ensure!(
            !operation.name.is_empty()
                && operation.name.len() <= 64
                && operation
                    .name
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
                && names.insert(operation.name.clone())
                && operation.description.len() <= 1024
                && schema.get("type") == Some(&json!("object"))
                && schema.get("properties").is_none_or(Value::is_object)
                && schema
                    .get("properties")
                    .is_none_or(|p| p.get("context").is_none())
                && schema
                    .get("required")
                    .is_none_or(|r| r.as_array().is_some_and(|r| r.iter().all(Value::is_string)))
                && ["read", "reversible", "outward"].contains(&operation.tier.as_str()),
            "connector operation is malformed"
        );
        operations.push(operation);
    }
    Ok(operations)
}

/// Call an operation. The result is the adapter's own object: `status`
/// `observed` with a `result`, or `error` with a plain `error` and optional `fix`.
pub fn call(
    connector: &ConnectorManifest,
    operation: &str,
    arguments: Value,
    may_prompt: bool,
) -> Result<Value> {
    let value = exchange(
        connector,
        json!({"protocol_version":1,"method":"call","operation":operation,"arguments":arguments,"may_prompt":may_prompt}),
    )?;
    match value.get("status").and_then(Value::as_str) {
        Some("observed") if value.get("result").is_some() => Ok(value),
        Some("error") if value.get("error").is_some_and(Value::is_string) => Ok(value),
        _ => bail!("connector response is malformed"),
    }
}

/// Readiness without side effects; with `may_prompt`, the adapter may ask the
/// operating system for access, which shows a prompt to whoever is at the Mac.
pub fn health(connector: &ConnectorManifest, may_prompt: bool) -> Result<Value> {
    let mut connector = connector.clone();
    if may_prompt {
        connector.timeout_seconds = connector.timeout_seconds.max(PROMPT_TIMEOUT_SECONDS);
    }
    let value = exchange(
        &connector,
        json!({"protocol_version":1,"method":"health","may_prompt":may_prompt}),
    )?;
    ensure!(
        value.get("ready").is_some_and(Value::is_boolean),
        "connector health is malformed"
    );
    Ok(value)
}

fn exchange(connector: &ConnectorManifest, request: Value) -> Result<Value> {
    ensure!(
        connector.kind == ConnectorKind::Script,
        "only script connectors run as processes"
    );
    let (program, arguments) = connector
        .command
        .split_first()
        .context("connector has no command")?;
    let mut command = Command::new(program);
    command
        .args(arguments)
        .env_clear()
        .envs(
            ENVIRONMENT
                .iter()
                .filter_map(|k| std::env::var_os(k).map(|v| (k, v))),
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .process_group(0);
    let mut child = crate::process::spawning(|| command.spawn())
        .with_context(|| format!("connector {} could not start", connector.id))?;
    let leader = child.id() as i32;
    let deadline = Instant::now() + Duration::from_secs(connector.timeout_seconds);
    let mut stdin = child.stdin.take().context("connector stdin")?;
    let body = serde_json::to_vec(&request)?;
    // Detached: a child that never reads stdin must not hold the call open.
    std::thread::spawn(move || {
        let _ = stdin.write_all(&body);
    });
    let stdout = child.stdout.take().context("connector stdout")?;
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let mut buffer = Vec::new();
        let read = stdout
            .take(MAX_RESPONSE + 1)
            .read_to_end(&mut buffer)
            .map(|_| buffer);
        let _ = sender.send(read);
    });
    // Wait for the leader without reaping it, so its group id stays ours while
    // anything it left behind, such as a child holding stdout, is stopped.
    let exited = loop {
        if leader_exited(leader) {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    // An answer is complete once the leader exits; a straggler still holding
    // stdout gets a moment, then the group is stopped and the answer read.
    let output = if exited {
        let grace =
            Duration::from_millis(200).min(deadline.saturating_duration_since(Instant::now()));
        receiver.recv_timeout(grace).ok().or_else(|| {
            unsafe {
                libc::killpg(leader, libc::SIGKILL);
            }
            receiver.recv_timeout(Duration::from_secs(1)).ok()
        })
    } else {
        None
    };
    unsafe {
        libc::killpg(leader, libc::SIGKILL);
    }
    let status = child.wait()?;
    let output =
        output.with_context(|| format!("connector {} did not answer in time", connector.id))??;
    ensure!(
        output.len() as u64 <= MAX_RESPONSE,
        "connector response is too large"
    );
    ensure!(status.success(), "connector {} failed", connector.id);
    let value: Value = serde_json::from_slice(&output).context("connector response is not JSON")?;
    ensure!(value.is_object(), "connector response is not an object");
    Ok(value)
}

/// Whether the leader has exited, without reaping it.
fn leader_exited(leader: i32) -> bool {
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    let flags = libc::WEXITED | libc::WNOHANG | libc::WNOWAIT;
    let result = unsafe { libc::waitid(libc::P_PID, leader as libc::id_t, &mut info, flags) };
    result == 0 && unsafe { info.si_pid() } == leader
}

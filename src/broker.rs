//! Owner-local tools. The host creates grants; tool arguments cannot grant
//! authority, choose credentials, or supply their own write provenance.
//!
//! These checks constrain this broker, not the operator's native processes.
//! Restricted workers need a separate process isolation boundary.

// Tool families hold their arguments, schemas and handlers. A new family is
// one more module here, a line in `definitions`, its names in `call`, and its
// file in `config::POLICY_SOURCES`; a family that records effects must also
// report them in `observations`.
mod connectors;
mod history;
mod memory;
mod messages;
mod people;
mod permission;
mod schedules;
mod soul;
mod tasks;
mod wakes;

use crate::{
    config::Paths,
    harness::{Access, Activity},
    store::OWNER_INPUT_KINDS,
    tasks::TaskOrigin,
    vault::{valid_scope, CommitReceipt, Provenance, Vault},
};
use anyhow::{ensure, Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

const MAX_ARGS: usize = 512 * 1024;
const MAX_RESULT: usize = 1024 * 1024;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// What a run may do with tasks and schedules; each level includes the one before.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskAccess {
    #[default]
    None,
    Read,
    Manage,
}

/// One channel account whose conversations a run may discover and read. The
/// host derives it from the originating conversation, never from model input.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationScope {
    pub channel: String,
    pub account: String,
    /// The owner's own account: read here, never sent through.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub owners_account: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunGrant {
    pub context: String,
    pub requester: String,
    pub run_id: String,
    pub input_seq: i64,
    /// Always names the originating conversation; other conversations are
    /// reached through `conversation_scopes`, so this list stays small.
    pub conversations: Vec<String>,
    #[serde(default)]
    pub conversation_scopes: Vec<ConversationScope>,
    pub read_scopes: Vec<String>,
    pub write_scopes: Vec<String>,
    pub expires_at: i64,
    #[serde(default)]
    pub task_access: TaskAccess,
    #[serde(default)]
    pub eligible_agents: Vec<String>,
    /// The harness reach the host chose for this run; it decides what
    /// `permission.decide` allows.
    #[serde(default)]
    pub access: Access,
    /// The requester's role, read from the registry when the host launched
    /// this run; the turn's session, prompt and harness request follow it.
    pub role: crate::config::Role,
    /// Whether the run may set wakes for the assistant's own later looks
    /// (`broker/wakes.rs`): the owner's conversation turns and the heartbeat's.
    #[serde(default)]
    pub wakes: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerLaunch {
    pub grant: RunGrant,
    pub private_dir: PathBuf,
    pub database: PathBuf,
    pub vault: PathBuf,
    pub journal: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolResult {
    pub value: Value,
    pub is_error: bool,
}

pub struct Broker {
    launch: BrokerLaunch,
    run_directory: PathBuf,
    binding: String,
    connectors: std::sync::OnceLock<connectors::Described>,
}

#[derive(Debug)]
struct Fault {
    code: &'static str,
    message: &'static str,
}
type ToolCall = std::result::Result<Value, Fault>;

fn invalid() -> Fault {
    Fault {
        code: "invalid_arguments",
        message: "Arguments do not match this tool's schema or limits.",
    }
}
fn denied() -> Fault {
    Fault {
        code: "access_denied",
        message: "This request is not allowed by the current run context.",
    }
}
fn unavailable() -> Fault {
    Fault {
        code: "storage_unavailable",
        message: "The requested storage is unavailable or needs owner reconciliation.",
    }
}

// Run journal records. Their tool families write them; they live here because
// `observations` reads them to summarize a run.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum WriteState {
    Pending,
    Committed { receipt: CommitReceipt },
    Rejected { code: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WriteRecord {
    request_digest: String,
    expected_revision: String,
    provenance: Provenance,
    paths: Vec<String>,
    state: WriteState,
}

/// Durable admission evidence, without briefs, results or provider observations.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct TaskWriteRecord {
    request_digest: String,
    tool: String,
    task_id: String,
    origin: TaskOrigin,
    spec_digest: String,
    receipt: Option<Value>,
}

/// An effect a schedule or message tool made for the owner: journaled before it
/// runs and given its receipt after, so a lost answer is reported, not repeated.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct EffectRecord {
    tool: String,
    tier: String,
    request_digest: String,
    receipt: Option<Value>,
}

#[derive(Default, Serialize, Deserialize)]
struct ReadActivity {
    read_calls: u64,
}

impl Broker {
    /// An expired grant can be opened to inspect receipts; every tool invocation
    /// separately checks its expiration and current canonical input binding.
    pub fn open(mut launch: BrokerLaunch) -> Result<Self> {
        let grant = &launch.grant;
        for value in [&grant.context, &grant.requester, &grant.run_id] {
            ensure!(
                !value.trim().is_empty()
                    && value.len() <= 4096
                    && !value.contains(['\0', '\n', '\r']),
                "invalid broker grant identity"
            );
        }
        ensure!(
            grant.input_seq > 0
                && !grant.conversations.is_empty()
                && grant.conversations.len() <= 256,
            "invalid broker conversation grant"
        );
        ensure!(
            grant
                .conversations
                .iter()
                .all(|conversation| !conversation.is_empty() && conversation.len() <= 4096),
            "invalid broker conversation grant"
        );
        ensure!(
            grant.conversation_scopes.len() <= 16
                && grant.conversation_scopes.iter().all(|scope| {
                    [&scope.channel, &scope.account].iter().all(|value| {
                        !value.trim().is_empty()
                            && value.len() <= 256
                            && !value.contains(['\0', '\n', '\r'])
                    })
                }),
            "invalid broker conversation scope"
        );
        ensure!(
            grant.read_scopes.len() <= 256
                && grant.write_scopes.len() <= 256
                && grant
                    .read_scopes
                    .iter()
                    .chain(&grant.write_scopes)
                    .all(|scope| valid_scope(scope)),
            "invalid broker memory scope"
        );
        ensure!(
            grant.eligible_agents.len() <= 256
                && grant.eligible_agents.iter().all(|id| !id.trim().is_empty()
                    && id.len() <= 128
                    && !id.contains(['\0', '\n', '\r'])),
            "invalid task agent grant"
        );
        ensure!(
            grant.task_access == TaskAccess::Manage || grant.eligible_agents.is_empty(),
            "read-only grant cannot delegate to agents"
        );
        launch.private_dir = existing_directory(&launch.private_dir)?;
        launch.database = existing_file(&launch.database)?;
        launch.vault = Vault::open(&launch.vault)?.root().to_owned();
        fs::create_dir_all(&launch.journal)?;
        launch.journal = existing_directory(&launch.journal)?;
        private_directory(&launch.journal)?;
        ensure!(
            !launch.journal.starts_with(&launch.vault)
                && !launch.database.starts_with(&launch.vault),
            "runtime journals must stay outside the memory vault"
        );
        let run_directory = launch.journal.join(digest(&launch.grant.run_id)?);
        fs::create_dir_all(&run_directory)?;
        private_directory(&run_directory)?;
        let binding = digest(&launch)?;
        let broker = Self {
            launch,
            run_directory,
            binding,
            connectors: std::sync::OnceLock::new(),
        };
        broker.validate_paths()?;
        broker.validate_input()?;
        let _lock = broker.lock()?;
        let binding_path = broker.run_directory.join("binding.json");
        if binding_path.exists() {
            let stored: String = read_json(&binding_path)?;
            ensure!(
                stored == broker.binding,
                "run journal belongs to a different grant"
            );
        } else {
            atomic_json(&binding_path, &broker.binding)?;
        }
        Ok(broker)
    }

    pub fn definitions(&self) -> Vec<Value> {
        let context = json!({"type":"string","description":"The opaque broker_context supplied in the current host prompt."});
        let mut tools = history::definitions(&context);
        tools.extend(memory::definitions(&self.launch.grant, &context));
        tools.extend(tasks::definitions(&self.launch.grant, &context));
        tools.extend(schedules::definitions(&self.launch.grant, &context));
        tools.extend(wakes::definitions(&self.launch.grant, &context));
        tools.extend(messages::definitions(&self.launch.grant, &context));
        tools.extend(soul::definitions(&self.launch.grant, &context));
        tools.extend(people::definitions(self.people_reach(), &context));
        tools.extend(self.connector_definitions(&context));
        tools.extend(permission::definitions(self.launch.grant.access));
        tools
    }

    pub fn call(&self, name: &str, args: Value) -> ToolResult {
        let result = (|| -> ToolCall {
            if args
                .as_object()
                .is_none_or(|fields| fields.values().any(Value::is_null))
            {
                return Err(invalid());
            }
            if serde_json::to_vec(&args).map_err(|_| invalid())?.len() > MAX_ARGS {
                return Err(invalid());
            }
            let value = match name {
                "history.search" => self.history_search(parse(args)?),
                "history.read" => self.history_read(parse(args)?),
                "history.conversations" => self.history_conversations(parse(args)?),
                "history.turn" => self.history_turn(parse(args)?),
                "memory.search" => self.memory_search(parse(args)?),
                "memory.read" => self.memory_read(parse(args)?),
                "memory.commit" => self.memory_commit(parse(args)?),
                "memory.forget" => self.memory_forget(parse(args)?),
                "memory.import" => self.memory_import(parse(args)?),
                "tasks.delegate" => self.task_delegate(parse(args)?),
                "tasks.list" => self.task_list(parse(args)?),
                "tasks.read" => self.task_read(parse(args)?),
                "tasks.stop" => self.task_stop(parse(args)?),
                "schedules.create" => self.schedule_create(parse(args)?),
                "schedules.list" => self.schedule_list(parse(args)?),
                "schedules.cancel" => self.schedule_cancel(parse(args)?),
                "wakes.set" => self.wake_set(parse(args)?),
                "wakes.list" => self.wake_list(parse(args)?),
                "wakes.cancel" => self.wake_cancel(parse(args)?),
                "messages.send" => self.message_send(parse(args)?),
                "messages.react" => self.message_react(parse(args)?),
                "messages.contact_card" => self.contact_card(parse(args)?),
                "soul.avatar" => self.soul_avatar(parse(args)?),
                "messages.ask_owner" => self.ask_owner(parse(args)?),
                "people.list" => self.people_list(parse(args)?),
                "people.propose" => self.people_propose(parse(args)?),
                "people.answer" => self.people_answer(parse(args)?),
                "people.group" => self.people_group(parse(args)?),
                "permission.decide" if !self.launch.grant.access.is_read() => {
                    self.permission_decide(args)
                }
                _ => match self.connector_call(name, args) {
                    Some(value) => value,
                    None => Err(Fault {
                        code: "unknown_tool",
                        message: "This broker does not provide that tool.",
                    }),
                },
            }?;
            if serde_json::to_vec(&value).map_err(|_| unavailable())?.len() > MAX_RESULT {
                return Err(Fault {
                    code: "result_too_large",
                    message: "Narrow the query or use a smaller history page or event slice.",
                });
            }
            Ok(value)
        })();
        match result {
            Ok(value) => ToolResult {
                value,
                is_error: false,
            },
            Err(error) => ToolResult {
                value: json!({"error":{"code":error.code,"message":error.message}}),
                is_error: true,
            },
        }
    }

    pub fn activity(&self) -> Result<Activity> {
        let observations = self.observations()?;
        Ok(serde_json::from_value(observations["activity"].clone())?)
    }

    /// Source-backed continuation context, including admitted attempts whose
    /// outcome is still pending. No raw note contents or tool arguments are logged.
    pub fn observations(&self) -> Result<Value> {
        self.validate_paths()?;
        self.validate_binding()?;
        let _lock = self.lock()?;
        let read_calls = self.read_activity()?.read_calls;
        let mut writes = Vec::new();
        let mut activity = if read_calls > 0 {
            Activity::Read
        } else {
            Activity::None
        };
        for entry in fs::read_dir(&self.run_directory)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_str().context("invalid broker journal filename")?;
            if name.starts_with("task-") && name.ends_with(".json") {
                let mut record: TaskWriteRecord = read_json(&entry.path())?;
                self.reconcile_task_write(&mut record)?;
                activity = Activity::Effect;
                writes.push(
                    json!({"request_digest":record.request_digest,"tool":record.tool,
                    "status":if record.receipt.is_some() {"committed"} else {"pending"},
                    "receipt":record.receipt,"task_id":record.task_id,"origin":record.origin}),
                );
                continue;
            }
            if name.starts_with("effect-") && name.ends_with(".json") {
                let record: EffectRecord = read_json(&entry.path())?;
                activity = Activity::Effect;
                writes.push(
                    json!({"request_digest":record.request_digest,"tool":record.tool,"tier":record.tier,
                    "status":if record.receipt.is_some() {"committed"} else {"pending"},"receipt":record.receipt}),
                );
                continue;
            }
            if name.starts_with("connector-") && name.ends_with(".json") {
                let record: connectors::ConnectorWrite = read_json(&entry.path())?;
                activity = Activity::Effect;
                writes.push(
                    json!({"request_digest":record.request_digest,"tool":record.tool,
                    "tier":record.tier,"status":match &record.outcome {
                        None => "pending",
                        Some(outcome) if outcome["observation"]["status"] == "error" => "failed",
                        Some(_) => "observed",
                    }}),
                );
                continue;
            }
            if !name.starts_with("write-") || !name.ends_with(".json") {
                continue;
            }
            let mut record: WriteRecord = read_json(&entry.path())?;
            if matches!(record.state, WriteState::Pending) {
                self.reconcile(&mut record)?;
            }
            let mut summary = json!({"request_digest":record.request_digest});
            match &record.state {
                WriteState::Committed { receipt } => {
                    activity = Activity::Effect;
                    summary["status"] = json!("committed");
                    summary["receipt"] = json!(receipt);
                }
                WriteState::Pending => {
                    activity = Activity::Effect;
                    summary["status"] = json!("pending");
                    summary["outcome"] = json!("unverified; do not repeat the edit");
                    summary["attempted_paths"] = json!(record.paths);
                    summary["expected_revision"] = json!(record.expected_revision);
                    summary["provenance"] = json!(record.provenance);
                }
                WriteState::Rejected { code } => {
                    summary["status"] = json!("rejected");
                    summary["code"] = json!(code);
                }
            }
            writes.push(summary);
        }
        writes.sort_by(|a, b| {
            a["request_digest"]
                .as_str()
                .cmp(&b["request_digest"].as_str())
        });
        let decisions = self.decisions()?;
        if !decisions.allowed.is_empty() {
            activity = Activity::Unmediated;
        }
        let mut observations = json!({"source":self.source(),"activity":activity,"read_calls":read_calls,"writes":writes});
        if !(decisions.allowed.is_empty() && decisions.denied.is_empty()) {
            observations["native_permissions"] = json!(decisions);
        }
        Ok(observations)
    }

    fn paths(&self) -> Result<Paths> {
        let runtime = self
            .launch
            .database
            .parent()
            .context("database needs a parent")?;
        let paths = Paths::new(runtime.into(), Some(self.launch.private_dir.clone()))?;
        ensure!(
            existing_directory(&self.launch.private_dir)? == self.launch.private_dir,
            "private configuration directory changed"
        );
        Ok(paths)
    }

    /// A run that forgot something calls nothing more: its context still
    /// holds what was removed (`memory.forget`).
    fn authorize(&self, context: &str) -> std::result::Result<(), Fault> {
        if context != self.launch.grant.context
            || Utc::now().timestamp() >= self.launch.grant.expires_at
            || self.forgot()
        {
            return Err(denied());
        }
        self.validate_paths().map_err(|_| unavailable())?;
        self.validate_binding().map_err(|_| denied())?;
        self.validate_input().map_err(|_| denied())
    }

    fn validate_input(&self) -> Result<()> {
        validate_input(
            &self.launch.database,
            &self.launch.grant.requester,
            self.launch.grant.input_seq,
            &self.launch.grant.conversations,
        )
    }

    fn validate_paths(&self) -> Result<()> {
        let paths = self.paths()?;
        admits(&paths.load()?, &self.launch.grant)?;
        ensure!(
            paths.database().canonicalize()? == self.launch.database
                && paths.vault().canonicalize()? == self.launch.vault,
            "broker storage binding changed"
        );
        ensure!(
            existing_file(&self.launch.database)? == self.launch.database,
            "database path changed"
        );
        ensure!(
            Vault::open(&self.launch.vault)?.root() == self.launch.vault,
            "vault path changed"
        );
        ensure!(
            existing_directory(&self.launch.journal)? == self.launch.journal
                && existing_directory(&self.run_directory)? == self.run_directory,
            "journal path changed"
        );
        Ok(())
    }

    fn validate_binding(&self) -> Result<()> {
        let stored: String = read_json(&self.run_directory.join("binding.json"))?;
        ensure!(stored == self.binding, "run journal binding changed");
        Ok(())
    }

    fn source(&self) -> Value {
        json!({"requester":self.launch.grant.requester,"run_id":self.launch.grant.run_id,"input_seq":self.launch.grant.input_seq})
    }

    fn note_read(&self) -> std::result::Result<(), Fault> {
        let _lock = self.lock().map_err(|_| unavailable())?;
        let mut activity = self.read_activity().map_err(|_| unavailable())?;
        activity.read_calls = activity.read_calls.saturating_add(1);
        atomic_json(&self.run_directory.join("reads.json"), &activity).map_err(|_| unavailable())
    }

    fn read_activity(&self) -> Result<ReadActivity> {
        let path = self.run_directory.join("reads.json");
        if path.exists() {
            read_json(&path)
        } else {
            Ok(ReadActivity::default())
        }
    }

    /// Run one owner effect at most once per identical request in this run: a
    /// committed one returns its receipt with `replayed`, and one whose outcome
    /// was lost is refused, since it may have happened. `effect` is one SQLite
    /// transaction, so an error from it means nothing changed and the request
    /// may be made again.
    fn journaled_effect(
        &self,
        tool: &str,
        tier: &str,
        args: &impl Serialize,
        effect: impl FnOnce() -> std::result::Result<Value, Fault>,
    ) -> ToolCall {
        let request_digest = digest(&(tool, args)).map_err(|_| unavailable())?;
        let path = self
            .run_directory
            .join(format!("effect-{request_digest}.json"));
        {
            let _lock = self.lock().map_err(|_| unavailable())?;
            if path.exists() {
                let record: EffectRecord = read_json(&path).map_err(|_| unavailable())?;
                return match record.receipt {
                    Some(receipt) => Ok(json!({"source":self.source(),"receipt":receipt,"replayed":true})),
                    None => Err(Fault {
                        code: "write_unverified",
                        message: "An identical request in this run has no recorded outcome; it may have happened. Check before trying again.",
                    }),
                };
            }
            let record = EffectRecord {
                tool: tool.into(),
                tier: tier.into(),
                request_digest: request_digest.clone(),
                receipt: None,
            };
            atomic_json(&path, &record).map_err(|_| unavailable())?;
        }
        let receipt = match effect() {
            Ok(receipt) => receipt,
            Err(fault) => {
                let _lock = self.lock().map_err(|_| unavailable())?;
                fs::remove_file(&path).map_err(|_| unavailable())?;
                return Err(fault);
            }
        };
        let record = EffectRecord {
            tool: tool.into(),
            tier: tier.into(),
            request_digest,
            receipt: Some(receipt.clone()),
        };
        let _lock = self.lock().map_err(|_| unavailable())?;
        atomic_json(&path, &record).map_err(|_| unavailable())?;
        Ok(json!({"source":self.source(),"receipt":receipt,"replayed":false}))
    }

    fn task_record_path(&self, digest: &str) -> PathBuf {
        self.run_directory.join(format!("task-{digest}.json"))
    }

    fn save_task_write(&self, record: &TaskWriteRecord) -> Result<()> {
        atomic_json(&self.task_record_path(&record.request_digest), record)
    }

    fn record_path(&self, digest: &str) -> PathBuf {
        self.run_directory.join(format!("write-{digest}.json"))
    }
    fn save_record(&self, record: &WriteRecord) -> Result<()> {
        atomic_json(&self.record_path(&record.request_digest), record)
    }

    fn lock(&self) -> Result<File> {
        let path = self.run_directory.join("broker.lock");
        if path.exists() {
            existing_file(&path)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        file.lock()?;
        Ok(file)
    }
}

fn tool(
    name: &str,
    description: &str,
    properties: Value,
    required: &[&str],
    read_only: bool,
) -> Value {
    json!({"name":name,"description":description,"inputSchema":{"type":"object","properties":properties,"required":required,"additionalProperties":false},"annotations":{"readOnlyHint":read_only,"destructiveHint":!read_only,"idempotentHint":true,"openWorldHint":false}})
}

fn parse<T: serde::de::DeserializeOwned>(args: Value) -> std::result::Result<T, Fault> {
    serde_json::from_value(args).map_err(|_| invalid())
}

/// Whether the registry still gives a grant's requester a turn, so removing
/// someone ends their runs mid-way.
pub(crate) fn admits(config: &crate::config::Config, grant: &RunGrant) -> Result<()> {
    let role = config
        .turn_role(&grant.requester)
        .context("grant requester is not the owner, a registered adult or a child")?;
    ensure!(
        !grant.wakes || role == crate::config::Role::Owner,
        "only the owner's runs set wakes"
    );
    Ok(())
}

pub(crate) fn validate_input(
    database: &Path,
    requester: &str,
    input_seq: i64,
    conversations: &[String],
) -> Result<()> {
    // Launch rejection must not initialize storage, change pragmas or checkpoint
    // another connection's WAL. This query only checks existing host facts.
    let connection = rusqlite::Connection::open_with_flags(
        database,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    connection.busy_timeout(std::time::Duration::from_secs(5))?;
    // The author was resolved by the host from console identity or adapter
    // sender evidence mapped in configuration; it is the authentication.
    let (conversation, direction, kind, author): (String, String, String, String) = connection
        .query_row(
            "SELECT e.conversation,e.direction,e.kind,e.author
            FROM events e JOIN conversations c ON e.conversation=c.id WHERE e.seq=?1",
            [input_seq],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
    // A heartbeat is the owner's too, but it binds only a read-only grant
    // (launch.rs) and never originates a task.
    ensure!(
        direction == "in"
            && (OWNER_INPUT_KINDS.contains(&kind.as_str()) || kind == "heartbeat")
            && author == requester
            && conversations.contains(&conversation),
        "broker grant must bind the original owner input"
    );
    Ok(())
}

fn digest(value: &impl Serialize) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(value)?)))
}

fn existing_file(path: &Path) -> Result<PathBuf> {
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "expected a regular private file"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        ensure!(metadata.nlink() == 1, "private file cannot be hard linked");
    }
    Ok(path.canonicalize()?)
}

fn existing_directory(path: &Path) -> Result<PathBuf> {
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "expected a private directory"
    );
    Ok(path.canonicalize()?)
}

fn private_directory(path: &Path) -> Result<()> {
    existing_directory(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    existing_file(path)?;
    ensure!(
        fs::metadata(path)?.len() <= 1024 * 1024,
        "journal record too large"
    );
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    if path.exists() {
        existing_file(path)?;
    }
    let temporary = path.with_extension(format!(
        "{}-{}.tmp",
        std::process::id(),
        TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        serde_json::to_writer(&mut file, value)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        File::open(path.parent().context("journal path needs parent")?)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}

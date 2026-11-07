use crate::harness::{Access, AgentManifest};
use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::{Path, PathBuf},
};

/// Sources that shape what a model sees or may call. A change to any of them
/// invalidates cached native sessions; tests/policy.rs checks that every broker
/// tool family is listed.
pub const POLICY_SOURCES: [(&str, &str); 17] = [
    ("harness.rs", include_str!("harness.rs")),
    ("runtime.rs", include_str!("runtime.rs")),
    ("prompt.rs", include_str!("prompt.rs")),
    ("broker.rs", include_str!("broker.rs")),
    ("broker/history.rs", include_str!("broker/history.rs")),
    ("broker/memory.rs", include_str!("broker/memory.rs")),
    ("broker/tasks.rs", include_str!("broker/tasks.rs")),
    ("broker/connectors.rs", include_str!("broker/connectors.rs")),
    ("broker/permission.rs", include_str!("broker/permission.rs")),
    ("broker/schedules.rs", include_str!("broker/schedules.rs")),
    ("broker/messages.rs", include_str!("broker/messages.rs")),
    ("broker/people.rs", include_str!("broker/people.rs")),
    ("broker/soul.rs", include_str!("broker/soul.rs")),
    ("broker/wakes.rs", include_str!("broker/wakes.rs")),
    ("connectors.rs", include_str!("connectors.rs")),
    ("launch.rs", include_str!("launch.rs")),
    ("mcp.rs", include_str!("mcp.rs")),
];

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub version: u32,
    pub owner: String,
    /// Reach of the owner's conversation turns and of the background tasks
    /// they start. Result presentations always run with `read`.
    #[serde(default = "default_access")]
    pub access: Access,
    /// This assistant's own source checkout. At `computer` access the model is
    /// told where it is, so it can propose changes to itself on a branch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    pub agents: Vec<AgentManifest>,
    /// Skipped when empty: `channels = []` would make an appended
    /// `[[channels]]` table a TOML error.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub channels: Vec<ChannelManifest>,
    /// The owner's registry: who each channel handle is, and their role.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub people: Vec<Person>,
    /// Groups the owner shares with another person's assistant.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<SharedGroup>,
    #[serde(default)]
    pub sessions: SessionPolicy,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connectors: Vec<ConnectorManifest>,
    /// Off when absent. Serve reads it at start, so a change needs a restart.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heartbeat: Option<crate::heartbeat::Heartbeat>,
    /// The daily memory sync from the owner's other agents; off when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_sync: Option<crate::memory_sync::MemorySync>,
    /// Whether the owner's runs have the harness's own browser (Claude in
    /// Chrome, Codex browser use). On unless the owner turns it off.
    #[serde(default = "default_browser")]
    pub browser: bool,
}

fn default_browser() -> bool {
    true
}

/// An adapter process the broker calls (connector adapter
/// v1); its writes are offered only at full access. Accounts the owner signed
/// in to the harness itself need no entry: an owner run has them all.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ConnectorManifest {
    pub id: String,
    pub kind: ConnectorKind,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command: Vec<String>,
    #[serde(default = "default_connector_timeout")]
    pub timeout_seconds: u64,
    /// Offered to family turns alone, never a read-only run (a heartbeat or a
    /// result presentation), such as the owner's password vault.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub owner_only: bool,
}

fn default_access() -> Access {
    Access::Connectors
}

fn default_connector_timeout() -> u64 {
    30
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorKind {
    Script,
}

/// When a conversation's native harness session is retired. The next turn
/// starts fresh from the canonical log's recent window; nothing is deleted.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, default)]
pub struct SessionPolicy {
    /// Retire a session unused for this many minutes; 0 disables.
    pub idle_minutes: u32,
    /// Retire sessions started before the latest local HH:MM; empty disables.
    pub daily_rotation: String,
    /// Seconds a turn waits after a message, so a person's quick
    /// follow-ups (a typo fix, an added detail) are answered with it in one
    /// reply; 0 starts at once. Init writes 3.
    #[serde(skip_serializing_if = "is_zero")]
    pub gather_seconds: u64,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}
impl SessionPolicy {
    pub fn rotation_time(&self) -> Option<chrono::NaiveTime> {
        chrono::NaiveTime::parse_from_str(&self.daily_rotation, "%H:%M").ok()
    }

    /// Whether a session last used at `used_at` and started at `started_at`
    /// must be retired at `now`. Missing times retire it.
    pub fn retires(
        &self,
        started_at: Option<&str>,
        used_at: Option<&str>,
        now: chrono::DateTime<chrono::Local>,
    ) -> bool {
        let parse =
            |value: Option<&str>| value.and_then(|v| chrono::DateTime::parse_from_rfc3339(v).ok());
        let (Some(started), Some(used)) = (parse(started_at), parse(used_at)) else {
            return true;
        };
        if self.idle_minutes > 0
            && now.signed_duration_since(used) > chrono::Duration::minutes(self.idle_minutes.into())
        {
            return true;
        }
        let Some(time) = self.rotation_time() else {
            return false;
        };
        // The latest local rotation instant at or before now.
        let today = now.date_naive().and_time(time);
        let boundary = match today.and_local_timezone(chrono::Local).earliest() {
            Some(instant) if instant <= now => Some(instant),
            _ => (today - chrono::Duration::days(1))
                .and_local_timezone(chrono::Local)
                .earliest(),
        };
        boundary.is_some_and(|boundary| started < boundary)
    }
}

impl Default for SessionPolicy {
    fn default() -> Self {
        Self {
            idle_minutes: 360,
            daily_rotation: "04:00".into(),
            gather_seconds: 0,
        }
    }
}

/// A person the owner has registered. Each handle is `<channel id>:<native
/// sender handle>`, so a person is recognized on every account of that channel
/// and on no other channel. The owner, adults and children get turns, each
/// under their own role; everyone else is identified and logged.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Person {
    pub id: String,
    pub role: Role,
    #[serde(default)]
    pub handles: Vec<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Owner,
    Adult,
    Child,
    Friend,
    Guest,
    /// Another person's assistant: its message never starts a turn.
    Agent,
}

/// The owner's settings for one group. Where another person's assistant is
/// present, the owners choose which assistant takes questions addressed to
/// neither, and turns for anyone but the owner wait so the model sees what the
/// other assistant said. Apart from that, the owner chooses whether to hear
/// about messages from people who get no turn there.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SharedGroup {
    /// `<channel>/<account>/<native id>`, as `oppa conversations` lists it.
    pub conversation: String,
    /// Set only where another person's assistant shares the group.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub takes_unaddressed: Option<bool>,
    #[serde(default = "default_group_wait")]
    pub wait_seconds: u64,
    /// Whether the owner hears when someone who gets no turn writes here;
    /// when absent, where the channel lists up to six members.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_people: Option<bool>,
}

fn default_group_wait() -> u64 {
    DEFAULT_GROUP_WAIT
}

/// Seconds a turn for anyone but the owner waits in a shared group, unless
/// the owner chose another wait.
pub const DEFAULT_GROUP_WAIT: u64 = 20;

/// The largest group, in members other than the assistant, whose newcomers
/// the owner hears about without asking for it.
pub const SMALL_GROUP: usize = 6;

/// One messaging surface served by an adapter process. Senders are mapped to
/// people through the `[[people]]` handles.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ChannelManifest {
    pub id: String,
    pub account: String,
    pub command: Vec<String>,
    #[serde(default = "default_channel_timeout")]
    pub timeout_seconds: u64,
    /// Whose account the adapter is signed in to. Its own messages from
    /// elsewhere (`is_me`) are logged as that party's outbound messages.
    #[serde(default)]
    pub account_holder: AccountHolder,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AccountHolder {
    /// A dedicated assistant account: its other messages are the assistant's.
    #[default]
    Assistant,
    /// The owner's own account, attached as a connection: everything on it is
    /// logged for the owner's runs to read, no message there starts a turn,
    /// and nothing is sent through it, since that would speak as the owner.
    Owner,
}
fn default_channel_timeout() -> u64 {
    30
}

#[derive(Clone, Debug)]
pub struct Paths {
    pub runtime: PathBuf,
    pub private: PathBuf,
}

impl Paths {
    pub fn new(runtime: PathBuf, private: Option<PathBuf>) -> Result<Self> {
        let runtime = absolute(runtime)?;
        let private = match private {
            Some(path) => absolute(path)?,
            None => runtime.with_file_name(format!(
                "{}-private",
                runtime
                    .file_name()
                    .context("runtime directory must have a name")?
                    .to_string_lossy()
            )),
        };
        if runtime.starts_with(&private) || private.starts_with(&runtime) {
            bail!("runtime and private Git directories must be separate siblings");
        }
        reject_source_checkout(&runtime)?;
        if let Some(parent) = private.parent() {
            reject_source_checkout(parent)?;
        }
        Ok(Self { runtime, private })
    }
    pub fn config_file(&self) -> PathBuf {
        self.private.join("config.toml")
    }
    pub fn vault(&self) -> PathBuf {
        self.private.join("vault")
    }
    /// Skills the assistant writes for itself: a folder each, with SKILL.md
    /// and its scripts.
    pub fn skills(&self) -> PathBuf {
        self.private.join("skills")
    }
    /// The owner's knowledge wiki: public, reusable knowledge kept apart from
    /// the vault's personal memory, in its own Git repository here or a link
    /// to one the owner already keeps.
    pub fn knowledge(&self) -> PathBuf {
        self.private.join("knowledge")
    }
    /// The assistant's face, beside the vault, which takes only Markdown notes.
    pub fn avatar(&self) -> PathBuf {
        self.private.join("avatar.png")
    }
    /// Faces it had before, named by when each was replaced.
    pub fn avatars(&self) -> PathBuf {
        self.private.join("avatars")
    }
    pub fn database(&self) -> PathBuf {
        self.runtime.join("state.db")
    }
    pub fn load(&self) -> Result<Config> {
        let text = fs::read_to_string(self.config_file())
            .context("instance is not initialized; run oppa init")?;
        let table = toml::from_str::<toml::Table>(&text).unwrap_or_default();
        if table.contains_key("schedules") {
            bail!("config.toml still has [[schedules]]; schedules now live in the instance: add each with `oppa schedules add` and remove the entries");
        }
        if table
            .get("channels")
            .and_then(|channels| channels.as_array())
            .is_some_and(|channels| channels.iter().any(|c| c.get("owner_handles").is_some()))
        {
            bail!("config.toml still has owner_handles; move them to a [[people]] entry with your owner id and role = \"owner\", each written as \"<channel id>:<handle>\", and remove the owner_handles lines");
        }
        if table
            .get("connectors")
            .and_then(|connectors| connectors.as_array())
            .is_some_and(|connectors| {
                connectors
                    .iter()
                    .any(|c| c.get("kind").and_then(|k| k.as_str()) == Some("vendor"))
            })
        {
            bail!("config.toml still has connectors with kind = \"vendor\"; an owner run now has every account signed in to the harness itself, so remove those entries");
        }
        let config: Config = toml::from_str(&text)?;
        config.validate()?;
        Ok(config)
    }
    pub fn prepare(&self) -> Result<()> {
        private_directory(&self.runtime)?;
        private_directory(&self.private)?;
        for name in ["runs", "locks", "adapters"] {
            private_directory(&self.runtime.join(name))?;
        }
        Ok(())
    }
}

/// A handle as people are matched by it: an email address in any case, since
/// a channel may report it in another case than the owner typed it; every
/// other handle exactly.
pub fn handle_key(handle: &str) -> String {
    if handle.contains('@') {
        handle.to_ascii_lowercase()
    } else {
        handle.to_owned()
    }
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        if self.version != 1 {
            bail!("unsupported configuration version");
        }
        if self.owner.is_empty()
            || !self
                .owner
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            bail!("owner identifier must contain letters, digits, '-' or '_'");
        }
        if self.agents.is_empty() {
            bail!("at least one harness is required");
        }
        if self.sessions.gather_seconds > 60 {
            bail!("sessions.gather_seconds must be 0..=60");
        }
        if !self.sessions.daily_rotation.is_empty() && self.sessions.rotation_time().is_none() {
            bail!("sessions.daily_rotation must be a local HH:MM time or empty");
        }
        let mut ids = std::collections::HashSet::new();
        for agent in &self.agents {
            if agent.id.is_empty()
                || agent.family.is_empty()
                || !ids.insert(&agent.id)
                || agent.command.first().is_none_or(|s| s.is_empty())
                || !(1..=3600).contains(&agent.timeout_seconds)
                || !(1..=crate::harness::MAX_TIMEOUT_SECONDS).contains(&agent.task_timeout_seconds)
            {
                bail!("invalid or duplicate agent manifest");
            }
            if agent.model.as_deref().is_some_and(|model| {
                model.is_empty()
                    || model.len() > 128
                    || !model
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"._:[]/-".contains(&b))
            }) {
                bail!(
                    "agent {}: model must be a model name, such as claude-opus-5-5",
                    agent.id
                );
            }
            if agent
                .effort
                .as_deref()
                .is_some_and(|effort| !["low", "medium", "high", "xhigh", "max"].contains(&effort))
            {
                bail!(
                    "agent {}: effort must be low, medium, high, xhigh or max",
                    agent.id
                );
            }
        }
        // An owner turn's scopes are every channel account plus the console,
        // and a grant holds at most 16.
        if self.channels.len() > 15 {
            bail!("at most 15 channels are supported");
        }
        let mut channel_ids = std::collections::HashSet::new();
        for channel in &self.channels {
            if channel.id.is_empty()
                || channel.id == "console"
                || !channel
                    .id
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
                || !channel_ids.insert(&channel.id)
                || channel.account.is_empty()
                || channel.account.contains('/')
                || channel.command.first().is_none_or(|s| s.is_empty())
                || !(1..=600).contains(&channel.timeout_seconds)
            {
                bail!("invalid or duplicate channel manifest");
            }
        }
        let mut person_ids = std::collections::HashSet::new();
        let mut handles = std::collections::HashSet::new();
        for person in &self.people {
            // An id is an event author, so it may not look like the assistant's
            // or an unidentified sender's (`channel:handle`).
            if person.id.is_empty()
                || person.id == crate::store::ASSISTANT
                || !person
                    .id
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
                || !person_ids.insert(&person.id)
                || (person.role == Role::Owner) != (person.id == self.owner)
            {
                bail!("invalid or duplicate person {:?}: ids use letters, digits, '-' or '_', and only the owner id has role \"owner\"", person.id);
            }
            for handle in &person.handles {
                let known = handle.split_once(':').is_some_and(|(channel, native)| {
                    self.channels.iter().any(|c| c.id == channel)
                        && !native.is_empty()
                        && native == native.trim()
                });
                ensure!(
                    known,
                    "handle {handle:?} of {} must be \"<channel id>:<handle>\" for a configured channel",
                    person.id
                );
                ensure!(
                    handles.insert(handle_key(handle)),
                    "handle {handle:?} is listed twice"
                );
            }
        }
        let mut connector_ids = std::collections::HashSet::new();
        for connector in &self.connectors {
            if connector.id.is_empty()
                || connector.id.len() > 64
                || !connector
                    .id
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
                || !connector_ids.insert(&connector.id)
                // The broker's own tool families; a connector there would collide.
                || ["history", "memory", "tasks", "permission", "schedules", "messages", "people"]
                    .contains(&connector.id.as_str())
                || !connector.command.first().is_some_and(|s| !s.is_empty())
                || !(1..=300).contains(&connector.timeout_seconds)
            {
                bail!("invalid or duplicate connector manifest");
            }
        }
        if self.connectors.len() > 16 {
            bail!("at most 16 connectors are supported");
        }
        let mut groups = std::collections::HashSet::new();
        for group in &self.groups {
            ensure!(
                is_channel_conversation(&group.conversation, &self.channels)
                    && groups.insert(&group.conversation),
                "group {:?} must be <channel id>/<account>/<native id> of a channel on the assistant's own account, listed once",
                group.conversation
            );
            ensure!(
                (1..=120).contains(&group.wait_seconds),
                "group wait_seconds must be 1..=120"
            );
        }
        if let Some(sync) = &self.memory_sync {
            sync.validate(&self.channels)?;
        }
        if let Some(heartbeat) = &self.heartbeat {
            heartbeat.validate(&self.channels)?;
        }
        if self
            .source
            .as_ref()
            .is_some_and(|source| !Path::new(source).is_absolute() || source.contains('\0'))
        {
            bail!("source must be the absolute path of this assistant's source checkout");
        }
        Ok(())
    }

    /// The registered person a channel sender's handle names.
    pub fn person(&self, channel: &str, handle: &str) -> Option<&Person> {
        let handle = handle_key(&format!("{channel}:{handle}"));
        self.people.iter().find(|person| {
            person
                .handles
                .iter()
                .any(|known| handle_key(known) == handle)
        })
    }

    /// The registered person an event author is, by id.
    pub fn person_by_id(&self, id: &str) -> Option<&Person> {
        self.people.iter().find(|person| person.id == id)
    }

    /// Who a logged author is now: a registered person's id as it is, and an
    /// unregistered sender's `<channel>:<handle>` as the person that handle
    /// names since, if anyone. History keeps what was logged; this only reads it.
    pub fn current_author<'a>(&'a self, author: &'a str) -> &'a str {
        if self.person_by_id(author).is_some() {
            return author;
        }
        author
            .split_once(':')
            .filter(|(channel, _)| self.channels.iter().any(|c| c.id == *channel))
            .and_then(|(channel, handle)| self.person(channel, handle))
            .map_or(author, |person| person.id.as_str())
    }

    /// The role of an author whose message gets a turn: the owner, or a
    /// registered adult or child, family whose turns have the owner's access.
    /// Everyone else is logged only.
    pub fn turn_role(&self, author: &str) -> Option<Role> {
        if author == self.owner {
            return Some(Role::Owner);
        }
        self.person_by_id(author)
            .map(|person| person.role)
            .filter(|role| matches!(role, Role::Adult | Role::Child))
    }

    /// Whether a channel is the owner's own account rather than the
    /// assistant's: logged and read, never answered or sent through.
    pub fn owners_account(&self, channel: &str) -> bool {
        self.channels
            .iter()
            .any(|c| c.id == channel && c.account_holder == AccountHolder::Owner)
    }

    /// The owner's settings for a group, where it has any.
    pub fn group(&self, conversation: &str) -> Option<&SharedGroup> {
        self.groups
            .iter()
            .find(|group| group.conversation == conversation)
    }

    /// The owner's settings for a group shared with another assistant.
    pub fn shared_group(&self, conversation: &str) -> Option<&SharedGroup> {
        self.group(conversation)
            .filter(|group| group.takes_unaddressed.is_some())
    }

    /// How long a turn for `author` waits before it starts: everyone who gets
    /// turns waits `sessions.gather_seconds`, and in a shared group everyone
    /// but the owner waits at least the group's wait.
    pub fn turn_wait(&self, conversation: &str, author: &str) -> Option<std::time::Duration> {
        let answered = self.turn_role(author).is_some();
        let gather = self.sessions.gather_seconds;
        let group = self
            .shared_group(conversation)
            .filter(|_| author != self.owner && answered)
            .map_or(0, |group| group.wait_seconds);
        let wait = if answered { group.max(gather) } else { 0 };
        (wait > 0).then(|| std::time::Duration::from_secs(wait))
    }

    /// Whether the owner hears about a message from `author`, who gets no
    /// turn of their own, in a conversation of `kind` with `members` other
    /// members as its channel listed them: in direct chats and groups listed
    /// with up to six, unless the owner chose otherwise for that group.
    /// Another assistant's messages never count.
    pub fn tells_owner_about(
        &self,
        conversation: &str,
        kind: &str,
        members: Option<usize>,
        author: &str,
    ) -> bool {
        let author = self.current_author(author);
        if author == self.owner
            || author == crate::store::ASSISTANT
            || self.turn_role(author).is_some()
            || self
                .person_by_id(author)
                .is_some_and(|person| person.role == Role::Agent)
        {
            return false;
        }
        // A group whose channel does not list its members could be any size.
        match self.group(conversation).and_then(|group| group.new_people) {
            Some(chosen) => chosen,
            None => kind == "dm" || members.is_some_and(|members| members <= SMALL_GROUP),
        }
    }
    pub fn policy_hash(&self, manual: &str) -> Result<String> {
        let mut digest = Sha256::new();
        digest.update(toml::to_string(self)?);
        digest.update(manual);
        // Cached native sessions must not carry an older prompt, tool or launch
        // policy into a changed build. Only sources that shape what a model sees
        // or may call are covered; storage and transport changes keep sessions.
        let sources = POLICY_SOURCES.map(|(_, content)| content);
        let adapters = crate::harness::HARNESS_ADAPTERS.map(|(_, content)| content);
        for policy in sources.into_iter().chain(adapters) {
            digest.update(policy.as_bytes().len().to_le_bytes());
            digest.update(policy);
        }
        Ok(format!("{:x}", digest.finalize()))
    }
}

/// One writer of the private configuration at a time: `oppa settings` and a
/// change made in conversation (`people.rs`) each hold it while they read,
/// edit and replace the file.
pub fn lock(paths: &Paths) -> Result<fs::File> {
    let locks = paths.runtime.join("locks");
    fs::create_dir_all(&locks)?;
    let lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(locks.join("config.lock"))?;
    lock.lock()?;
    Ok(lock)
}

/// Rewrite the top-level `access` line of the private configuration, keeping
/// everything else as the owner wrote it, and return the level it replaced.
/// The new file is checked whole before it replaces the old one.
pub fn set_access(paths: &Paths, level: Access) -> Result<Access> {
    // Not a boundary against a run with the owner's shell, which could edit the
    // file itself; it stops the plain path of a model asked to raise its reach.
    ensure!(
        std::env::var_os(crate::harness::RUN_MARKER).is_none(),
        "access is the owner's to change; a model run cannot change it"
    );
    let _lock = lock(paths)?;
    let previous = paths.load()?.access;
    let path = paths.config_file();
    let text = fs::read_to_string(&path)?;
    let name = serde_json::to_value(level)?;
    let line = format!(
        "access = {}",
        toml::Value::from(name.as_str().context("access level")?)
    );
    // Top-level keys come before the first table header.
    let header = text
        .lines()
        .position(|l| l.trim_start().starts_with('['))
        .unwrap_or(usize::MAX);
    let mut lines: Vec<String> = text.lines().map(str::to_owned).collect();
    match lines.iter().take(header).position(|l| {
        l.trim_start()
            .strip_prefix("access")
            .is_some_and(|rest| rest.trim_start().starts_with('='))
    }) {
        Some(index) => lines[index] = line,
        None => lines.insert(header.min(lines.len()), line),
    }
    let mut updated = lines.join("\n");
    updated.push('\n');
    let config: Config =
        toml::from_str(&updated).context("the updated configuration is invalid")?;
    config.validate()?;
    ensure!(config.access == level, "the access line could not be set");
    replace(&path, &updated)?;
    Ok(previous)
}

/// Rewrite the private configuration through `change`, which sees it as it is
/// under the lock and edits it as the owner wrote it. The result is checked
/// whole, and by `check`, before it replaces the old file.
pub fn rewrite<T>(
    paths: &Paths,
    change: impl FnOnce(&Config, &mut toml_edit::DocumentMut) -> Result<T>,
    check: impl FnOnce(&Config, &T) -> Result<()>,
) -> Result<(Config, T)> {
    let _lock = lock(paths)?;
    let path = paths.config_file();
    let text = fs::read_to_string(&path)?;
    let current: Config = toml::from_str(&text).context("config.toml is not a configuration")?;
    let mut document: toml_edit::DocumentMut = text.parse().context("config.toml is not TOML")?;
    let made = change(&current, &mut document)?;
    let updated = document.to_string();
    let config: Config =
        toml::from_str(&updated).context("the updated configuration is invalid")?;
    config.validate()?;
    check(&config, &made)?;
    replace(&path, &updated)?;
    Ok((config, made))
}

/// Put `text` in place of the private configuration at once, keeping the
/// file's permissions as the owner set them.
pub fn replace(path: &Path, text: &str) -> Result<()> {
    let temporary = path.with_extension(format!("toml.{}.tmp", std::process::id()));
    fs::write(&temporary, text)?;
    fs::set_permissions(&temporary, fs::metadata(path)?.permissions())?;
    fs::rename(&temporary, path)?;
    Ok(())
}

/// Whether `conversation` is one the assistant may speak in: a chat on a
/// configured channel of its own account, never the owner's.
pub fn is_channel_conversation(conversation: &str, channels: &[ChannelManifest]) -> bool {
    crate::schedule::channel_conversation(conversation).is_some_and(
        |(channel, account, native_id)| {
            !native_id.trim().is_empty()
                && !native_id.contains('\0')
                && channels.iter().any(|manifest| {
                    manifest.id == channel
                        && manifest.account == account
                        && manifest.account_holder == AccountHolder::Assistant
                })
        },
    )
}

pub fn private_directory(path: &Path) -> Result<()> {
    // Managed storage must not be redirected into another tree by a child link.
    // Paths::new resolves intentionally chosen root aliases before this point.
    reject_directory_links(path)?;
    fs::create_dir_all(path)?;
    reject_directory_links(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn absolute(path: PathBuf) -> Result<PathBuf> {
    let path = if path.is_absolute() {
        path
    } else {
        std::env::current_dir()?.join(path)
    };
    // Resolve existing ancestors to prevent symlink aliases bypassing separation.
    let mut ancestor = path.as_path();
    let mut missing = Vec::new();
    while !ancestor.exists() {
        missing.push(
            ancestor
                .file_name()
                .context("invalid directory")?
                .to_owned(),
        );
        ancestor = ancestor.parent().context("invalid directory")?;
    }
    let mut result = ancestor.canonicalize()?;
    for part in missing.into_iter().rev() {
        if part == "." || part == ".." {
            bail!("directory traversal is not allowed");
        }
        result.push(part);
    }
    Ok(result)
}

fn reject_source_checkout(path: &Path) -> Result<()> {
    for parent in path.ancestors() {
        if parent.join(".git").exists() {
            bail!("runtime data cannot be placed inside a Git checkout");
        }
    }
    Ok(())
}

/// Reject links in managed child paths before any read, write, or chmod follows them.
pub fn reject_directory_links(path: &Path) -> Result<()> {
    for parent in path.ancestors() {
        match fs::symlink_metadata(parent) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                bail!("managed storage directory is a symlink")
            }
            Ok(metadata) if !metadata.is_dir() => bail!("managed storage path is not a directory"),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

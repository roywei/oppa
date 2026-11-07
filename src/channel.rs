//! Channel adapters: long-lived trusted processes that normalize one messaging
//! surface over newline-delimited JSON-RPC on stdio. Sender identity is adapter
//! evidence mapped to a person by host configuration; message text never
//! confers authority. The host owns the canonical log, delivery receipts and
//! per-conversation serialization. Adapters are trusted host programs, not a
//! sandbox for other people.
use crate::{
    config::{AccountHolder, ChannelManifest, Config, Paths},
    runtime::{ConversationIdentity, Deliver, Runtime},
    schedule::{Schedule, ScheduleStore},
    store::{Attachment, DeliveryObservation, OutboxEntry, Store, ASSISTANT},
};
use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::HashMap, fs::OpenOptions, future::Future, process::Stdio, sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::Command,
    sync::{mpsc, oneshot},
};

pub const PROTOCOL_VERSION: u32 = 1;
/// Adapter error code meaning the message was definitely not sent.
pub const NOT_SENT: i64 = 1001;
const MAX_LINE: usize = 1024 * 1024;
const MAX_BODY: usize = 256 * 1024;
const MAX_ID: usize = 4096;
const MAX_STDERR: usize = 64 * 1024;
/// How long after a newcomer's message the owner is told, so a burst of
/// messages is one look.
const NEWCOMER_SETTLE: Duration = Duration::from_secs(30);
/// Looks about newcomers that run a turn in any hour, in one owner's chat,
/// so busy chats cannot run up turns; and at most one of them about the same
/// chat, so one sender who keeps writing is looked at once an hour.
const NEWCOMER_LOOKS_PER_HOUR: usize = 12;
const LOOK_WINDOW: Duration = Duration::from_secs(3600);
const LOOK_LIMIT_REACHED: &str =
    "a newcomer here was not told to the owner: the hourly limit was reached";
/// Tries of a look that cannot start, for example while another turn holds
/// the owner's chat, one settle apart.
const NEWCOMER_TRIES: u8 = 3;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NativeConversation {
    pub native_id: String,
    /// An adapter that does not say is never taken to mean a direct
    /// conversation: the model must not write for one reader where others read.
    #[serde(default = "group")]
    pub kind: String,
    #[serde(default)]
    pub title: Option<String>,
    /// A group's other members as sender handles, when the channel lists them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub members: Option<Vec<String>>,
}
fn group() -> String {
    "group".into()
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Sender {
    pub handle: String,
    #[serde(default)]
    pub display_name: Option<String>,
    /// Sent from this account itself, for example from the owner's phone.
    #[serde(default)]
    pub is_me: bool,
}

/// One native message as the adapter observed it. Everything here is evidence
/// about the channel, never an instruction to the host.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InboundMessage {
    pub conversation: NativeConversation,
    pub native_id: String,
    pub sender: Sender,
    #[serde(default)]
    pub body: String,
    #[serde(default)]
    pub attachments: Vec<Attachment>,
    #[serde(default)]
    pub reply_to: Option<String>,
    #[serde(default)]
    pub at: Option<String>,
}
impl InboundMessage {
    fn validate(&self) -> Result<()> {
        for value in [
            &self.conversation.native_id,
            &self.native_id,
            &self.sender.handle,
        ] {
            ensure!(
                !value.trim().is_empty() && value.len() <= MAX_ID && !value.contains('\0'),
                "invalid channel identifier"
            );
        }
        ensure!(
            matches!(self.conversation.kind.as_str(), "dm" | "group")
                && self
                    .conversation
                    .title
                    .as_ref()
                    .is_none_or(|title| title.len() <= MAX_ID && !title.contains('\0')),
            "invalid channel conversation shape"
        );
        ensure!(
            self.conversation.members.as_ref().is_none_or(|members| {
                members.len() <= 64
                    && members.iter().all(|member| {
                        !member.trim().is_empty()
                            && member.len() <= MAX_ID
                            && !member.contains('\0')
                    })
            }),
            "invalid channel conversation members"
        );
        ensure!(
            self.body.len() <= MAX_BODY && !self.body.contains('\0'),
            "channel message body exceeds bounds"
        );
        ensure!(
            self.attachments.len() <= 64
                && self.attachments.iter().all(|attachment| {
                    !attachment.path.trim().is_empty() && attachment.path.len() <= MAX_ID
                }),
            "invalid channel attachments"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct Capabilities {
    pub attachments: bool,
    pub receipts: bool,
    pub threads: bool,
    pub reactions: bool,
    pub typing: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct RpcError {
    pub code: i64,
    #[serde(default)]
    pub message: String,
}

#[derive(Debug)]
enum CallFailure {
    Adapter(RpcError),
    Transport(String),
    Timeout,
}

struct Outgoing {
    method: &'static str,
    params: Value,
    reply: Option<oneshot::Sender<std::result::Result<Value, CallFailure>>>,
}

/// Something the adapter process reported to the server loop.
#[derive(Debug)]
pub enum AdapterEvent {
    Message(InboundMessage),
    /// The adapter's stdout ended or its process died; the server stops.
    Exited {
        diagnostic: String,
    },
}

/// Cloneable handle to one adapter process. Requests are serialized through
/// the pump task that owns the child's pipes.
#[derive(Clone)]
pub struct ChannelClient {
    pub manifest: ChannelManifest,
    pub capabilities: Capabilities,
    requests: mpsc::Sender<Outgoing>,
}

impl ChannelClient {
    /// Spawn the adapter and complete `initialize`. Inbound messages and the
    /// adapter's exit are reported on `events`, tagged with the channel id.
    pub async fn start(
        manifest: ChannelManifest,
        events: mpsc::Sender<(String, AdapterEvent)>,
    ) -> Result<Self> {
        ensure!(
            manifest
                .command
                .iter()
                .all(|arg| !arg.is_empty() && !arg.contains('\0')),
            "invalid channel adapter command"
        );
        let mut command = Command::new(&manifest.command[0]);
        // An adapter that runs `oppa` itself, as the web page does, runs this
        // build, so a configuration need not name one that an upgrade replaces.
        if let Ok(executable) = std::env::current_exe() {
            command.env("OPPA_EXECUTABLE", executable);
        }
        command
            .args(&manifest.command[1..])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = crate::process::spawning(|| command.spawn())
            .with_context(|| format!("channel adapter {} could not start", manifest.id))?;
        let stdin = child.stdin.take().context("missing adapter stdin")?;
        let stdout = child.stdout.take().context("missing adapter stdout")?;
        let stderr = child.stderr.take().context("missing adapter stderr")?;
        let (requests, request_receiver) = mpsc::channel(64);
        let pump = Pump {
            channel: manifest.id.clone(),
            child,
            stdin,
            pending: HashMap::new(),
            next_id: 1,
            events,
        };
        tokio::spawn(pump.run(request_receiver, stdout));
        tokio::spawn(async move {
            let mut buffer = Vec::new();
            let _ = stderr
                .take(MAX_STDERR as u64)
                .read_to_end(&mut buffer)
                .await;
        });
        let mut client = Self {
            manifest,
            capabilities: Capabilities::default(),
            requests,
        };
        let answer = client
            .call(
                "initialize",
                json!({"protocol_version":PROTOCOL_VERSION,"account":client.manifest.account}),
                Duration::from_secs(client.manifest.timeout_seconds),
            )
            .await;
        let answer = match answer {
            Ok(answer) => answer,
            Err(failure) => bail!(
                "channel adapter {} failed to initialize: {failure:?}",
                client.manifest.id
            ),
        };
        ensure!(
            answer["protocol_version"].as_u64() == Some(PROTOCOL_VERSION as u64),
            "channel adapter {} speaks an unsupported protocol",
            client.manifest.id
        );
        client.capabilities = serde_json::from_value(answer["capabilities"].clone())
            .context("invalid channel capabilities")?;
        Ok(client)
    }

    async fn call(
        &self,
        method: &'static str,
        params: Value,
        timeout: Duration,
    ) -> std::result::Result<Value, CallFailure> {
        let (reply, receiver) = oneshot::channel();
        self.requests
            .send(Outgoing {
                method,
                params,
                reply: Some(reply),
            })
            .await
            .map_err(|_| CallFailure::Transport("adapter pump is gone".into()))?;
        match tokio::time::timeout(timeout, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(CallFailure::Transport(
                "adapter ended before replying".into(),
            )),
            Err(_) => Err(CallFailure::Timeout),
        }
    }

    async fn notify(&self, method: &'static str, params: Value) {
        let _ = self
            .requests
            .send(Outgoing {
                method,
                params,
                reply: None,
            })
            .await;
    }

    /// Delivery is only what the adapter proved: a native id means sent, the
    /// `NOT_SENT` error means not sent, and anything else stays unknown.
    pub async fn send_message(
        &self,
        native_conversation: &str,
        body: &str,
        reply_to: Option<&str>,
        attachments: &[Attachment],
    ) -> DeliveryObservation {
        let params = json!({"conversation":native_conversation,"body":body,"reply_to":reply_to,"attachments":attachments});
        self.deliver("send", params).await
    }

    async fn deliver(&self, method: &'static str, params: Value) -> DeliveryObservation {
        // Whatever queued it, nothing goes out as the owner.
        if self.manifest.account_holder == AccountHolder::Owner {
            return DeliveryObservation::ConfirmedNotSent {
                evidence: "This channel is the owner's own account; nothing is sent through it."
                    .into(),
            };
        }
        match self
            .call(
                method,
                params,
                Duration::from_secs(self.manifest.timeout_seconds),
            )
            .await
        {
            Ok(result) => match result["native_id"].as_str() {
                Some(native_id) if !native_id.trim().is_empty() && native_id.len() <= MAX_ID => {
                    DeliveryObservation::Sent {
                        native_id: native_id.into(),
                    }
                }
                _ => DeliveryObservation::Unknown {
                    evidence: "Adapter reported success without a valid native message id; verification required.".into(),
                },
            },
            Err(CallFailure::Adapter(error)) if error.code == NOT_SENT => {
                DeliveryObservation::ConfirmedNotSent {
                    evidence: bounded(&format!("Adapter refused to send: {}", error.message)),
                }
            }
            Err(CallFailure::Adapter(error)) => DeliveryObservation::Unknown {
                evidence: bounded(&format!(
                    "Adapter returned error {} without a not-sent confirmation: {}",
                    error.code, error.message
                )),
            },
            Err(CallFailure::Timeout) => DeliveryObservation::Unknown {
                evidence: "Adapter did not answer the send within its timeout; the message may have been delivered.".into(),
            },
            Err(CallFailure::Transport(detail)) => DeliveryObservation::Unknown {
                evidence: bounded(&format!("Adapter transport failed during send: {detail}")),
            },
        }
    }

    /// A tapback on the native message `target`. A channel that never said it
    /// takes reactions is not asked, so nothing reaches it.
    pub async fn react(
        &self,
        native_conversation: &str,
        target: &str,
        reaction: &str,
    ) -> DeliveryObservation {
        if !self.capabilities.reactions {
            return DeliveryObservation::ConfirmedNotSent {
                evidence: "This channel does not take reactions.".into(),
            };
        }
        self.deliver(
            "react",
            json!({"conversation":native_conversation,"message":target,"reaction":reaction}),
        )
        .await
    }

    /// The typing bubble, on a channel that says it shows one. Nothing is
    /// claimed from it, so its outcome is ignored.
    pub async fn typing(&self, native_conversation: &str, on: bool) {
        if !self.capabilities.typing || self.manifest.account_holder == AccountHolder::Owner {
            return;
        }
        let _ = self
            .call(
                "typing",
                json!({"conversation":native_conversation,"on":on}),
                Duration::from_secs(5),
            )
            .await;
    }

    pub async fn health(&self) -> Result<Value> {
        self.call("health", json!({}), Duration::from_secs(10))
            .await
            .map_err(|failure| anyhow::anyhow!("{failure:?}"))
    }

    pub async fn shutdown(&self) {
        self.notify("shutdown", json!({})).await;
    }
}

struct Pump {
    channel: String,
    child: tokio::process::Child,
    stdin: tokio::process::ChildStdin,
    pending: HashMap<u64, oneshot::Sender<std::result::Result<Value, CallFailure>>>,
    next_id: u64,
    events: mpsc::Sender<(String, AdapterEvent)>,
}

impl Pump {
    async fn run(
        mut self,
        mut requests: mpsc::Receiver<Outgoing>,
        stdout: tokio::process::ChildStdout,
    ) {
        let mut reader = BufReader::new(stdout);
        // Partial reads survive a cancelled branch: `line` is cleared only after
        // a complete frame was handled.
        let mut line = Vec::new();
        let diagnostic = loop {
            let read = async {
                (&mut reader)
                    .take((MAX_LINE + 1) as u64)
                    .read_until(b'\n', &mut line)
                    .await
            };
            tokio::select! {
                request = requests.recv() => {
                    let Some(request) = request else { break "server released the adapter".to_owned() };
                    let mut frame = json!({"jsonrpc":"2.0","method":request.method,"params":request.params});
                    if let Some(reply) = request.reply {
                        let id = self.next_id;
                        self.next_id += 1;
                        frame["id"] = json!(id);
                        self.pending.insert(id, reply);
                    }
                    let mut bytes = serde_json::to_vec(&frame).unwrap_or_default();
                    bytes.push(b'\n');
                    if self.stdin.write_all(&bytes).await.is_err() || self.stdin.flush().await.is_err() {
                        break "adapter stdin closed".to_owned();
                    }
                }
                read = read => {
                    match read {
                        Ok(0) => break "adapter stdout ended".to_owned(),
                        Ok(_) if line.len() > MAX_LINE || !line.ends_with(b"\n") => break "adapter emitted an oversized or incomplete frame".to_owned(),
                        Ok(_) => {}
                        Err(error) => break format!("adapter stdout failed: {error}"),
                    }
                    let message = serde_json::from_slice::<Value>(&line);
                    line.clear();
                    let Ok(message) = message else { continue };
                    self.handle(message).await;
                }
            }
        };
        for (_, reply) in self.pending.drain() {
            let _ = reply.send(Err(CallFailure::Transport(diagnostic.clone())));
        }
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
        let _ = self
            .events
            .send((self.channel.clone(), AdapterEvent::Exited { diagnostic }))
            .await;
    }

    async fn handle(&mut self, message: Value) {
        if let Some(id) = message.get("id").and_then(Value::as_u64) {
            let Some(reply) = self.pending.remove(&id) else {
                return;
            };
            let outcome = if let Some(error) = message.get("error") {
                Err(CallFailure::Adapter(
                    serde_json::from_value(error.clone()).unwrap_or(RpcError {
                        code: -32603,
                        message: "malformed adapter error".into(),
                    }),
                ))
            } else {
                Ok(message.get("result").cloned().unwrap_or(Value::Null))
            };
            let _ = reply.send(outcome);
            return;
        }
        if message.get("method").and_then(Value::as_str) == Some("event") {
            let Ok(inbound) = serde_json::from_value::<InboundMessage>(message["params"].clone())
            else {
                return;
            };
            if inbound.validate().is_err() {
                return;
            }
            let _ = self
                .events
                .send((self.channel.clone(), AdapterEvent::Message(inbound)))
                .await;
        }
    }
}

fn bounded(text: &str) -> String {
    text.chars().take(1024).collect()
}

/// Delivers one conversation's replies through its adapter.
pub struct ChannelDelivery {
    pub client: ChannelClient,
    pub native_conversation: String,
    pub paths: Paths,
}

impl Deliver for ChannelDelivery {
    fn deliver(
        &mut self,
        entry: &OutboxEntry,
    ) -> impl Future<Output = Result<DeliveryObservation>> {
        // The canonical reply_to names an event; the adapter needs the native
        // id of a channel message. A schedule or task result has none.
        let reply_to = entry
            .event
            .reply_to
            .as_deref()
            .and_then(|seq| seq.parse::<i64>().ok())
            .and_then(|seq| Store::open(&self.paths.database()).ok()?.event(seq).ok())
            .filter(|event| event.kind == "message")
            .and_then(|event| event.native_id);
        let client = self.client.clone();
        let conversation = self.native_conversation.clone();
        let reaction = entry.event.kind == "reaction";
        let body = entry.event.body.clone();
        let attachments = entry.event.attachments.clone();
        async move {
            if !reaction {
                return Ok(client
                    .send_message(&conversation, &body, reply_to.as_deref(), &attachments)
                    .await);
            }
            Ok(match reply_to {
                Some(target) => client.react(&conversation, &target, &body).await,
                None => DeliveryObservation::ConfirmedNotSent {
                    evidence: "The message to react to has no channel id.".into(),
                },
            })
        }
    }

    fn typing(&mut self, on: bool) -> impl Future<Output = ()> {
        let client = self.client.clone();
        let conversation = self.native_conversation.clone();
        async move { client.typing(&conversation, on).await }
    }
}

/// Conversation identity for a native conversation on one channel account.
pub fn identity(manifest: &ChannelManifest, native: &NativeConversation) -> ConversationIdentity {
    ConversationIdentity {
        id: format!("{}/{}/{}", manifest.id, manifest.account, native.native_id),
        channel: manifest.id.clone(),
        account: manifest.account.clone(),
        native_id: native.native_id.clone(),
        kind: native.kind.clone(),
        title: native.title.clone(),
        members: native.members.as_ref().map(|members| {
            members
                .iter()
                .map(|member| format!("{}:{member}", manifest.id))
                .collect()
        }),
    }
}

enum Job {
    Inbound {
        identity: ConversationIdentity,
        author: String,
        message: InboundMessage,
        observed_outbound: bool,
    },
    Schedule {
        schedule: Schedule,
        slot: chrono::DateTime<chrono::Local>,
    },
    /// A message another conversation's turn queued for this one.
    Deliver { outbox_id: i64 },
    /// Tell the owner, in this direct chat, about messages in `source` from
    /// someone who gets no turn there; a look that cannot start is tried
    /// again `again_after` later.
    Newcomer {
        source: String,
        again_after: Duration,
    },
    /// A wake the assistant set, run with the heartbeat serve runs.
    Wake {
        wake: Schedule,
        heartbeat: crate::heartbeat::Heartbeat,
        slot: chrono::DateTime<chrono::Local>,
    },
    /// A heartbeat slot, or the weekly digest's when `digest` is set.
    Heartbeat {
        heartbeat: crate::heartbeat::Heartbeat,
        slot: chrono::DateTime<chrono::Local>,
        digest: bool,
    },
}

/// New messages serve handed a conversation worker that it has not yet
/// taken, by author, each as its native id and text: a turn whose author has
/// one queued holds its answer. A redelivery, whose text is already queued or
/// is the logged message's latest, is not new; an edit, with new text, is.
type Queued = Arc<std::sync::Mutex<HashMap<String, Vec<(String, String)>>>>;

fn queue(
    queued: &Queued,
    store: Option<&Store>,
    conversation: &str,
    author: &str,
    message: &InboundMessage,
) {
    let key = (message.native_id.clone(), message.body.clone());
    let logged = store.is_some_and(|store| {
        store
            .latest_text(conversation, &message.native_id)
            .ok()
            .flatten()
            .is_some_and(|text| text == message.body)
    });
    let mut queued = queued
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let entries = queued.entry(author.to_owned()).or_default();
    if !logged && !entries.contains(&key) {
        entries.push(key);
    }
    if entries.is_empty() {
        queued.remove(author);
    }
}

fn take(queued: &Queued, author: &str, message: &InboundMessage) {
    let mut queued = queued
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(entries) = queued.get_mut(author) {
        entries
            .retain(|(native_id, body)| (native_id, body) != (&message.native_id, &message.body));
        if entries.is_empty() {
            queued.remove(author);
        }
    }
}

/// What a conversation worker does once its time comes.
enum Later {
    /// The turn of a logged message that waited for the other assistant or
    /// for its author's follow-ups.
    Turn { seq: i64, author: String },
    /// A look about newcomers in `source`, with the tries it has left.
    Look {
        source: String,
        again_after: Duration,
        tries: u8,
    },
}

struct ConversationWorker {
    /// Each job goes with the configuration serve read for it, so the
    /// worker decides a turn from the same registry that named its sender.
    jobs: std::sync::mpsc::Sender<(Arc<Config>, Job)>,
    /// Messages handed to the worker and not yet taken, by author.
    queued: Queued,
    thread: std::thread::JoinHandle<()>,
}

/// The long-lived loop behind `oppa serve`. One instance at a time: the serve
/// lease proves an earlier dispatcher is gone before interrupted sends are
/// marked unknown. Each conversation runs its turns on its own thread and
/// runtime, so short answers in one chat never wait behind another.
pub struct Server {
    paths: Paths,
    newcomer_settle: Duration,
    broker_executable: Option<std::path::PathBuf>,
}

impl Server {
    pub fn new(paths: Paths) -> Self {
        Self {
            paths,
            newcomer_settle: NEWCOMER_SETTLE,
            broker_executable: None,
        }
    }

    /// The `oppa` executable whose broker each turn runs, for a host that
    /// embeds serve in another program (`Runtime::with_broker_executable`).
    pub fn with_broker_executable(mut self, executable: std::path::PathBuf) -> Self {
        self.broker_executable = Some(executable);
        self
    }

    /// How long after a newcomer's message the owner is told.
    pub fn with_newcomer_settle(mut self, settle: Duration) -> Self {
        self.newcomer_settle = settle;
        self
    }

    pub async fn run(self, shutdown: impl Future<Output = ()>) -> Result<()> {
        let config = self.paths.load()?;
        ensure!(
            !config.channels.is_empty(),
            "no channel is configured; add a [[channels]] entry to the private config"
        );
        let _lease = serve_lease(&self.paths)?;
        crate::harness::install_bundled_adapters(&self.paths.runtime)?;
        Runtime::open(self.paths.clone())?;
        let recovered = Store::open(&self.paths.database())?.recover_interrupted_sends()?;
        if recovered > 0 {
            eprintln!("serve: {recovered} interrupted send(s) marked unknown for verification");
        }
        let (events, mut inbox) = mpsc::channel(256);
        let mut clients = HashMap::new();
        // A channel that cannot start (iMessage before Full Disk Access, say)
        // is tried again while the others serve; one added to config.toml
        // starts within a second. `oppa channels` shows which run.
        let (started, mut starts) = mpsc::channel(16);
        let mut serving = Serving::new(started);
        serving.launch(
            &self.paths,
            Serving::changed(&self.paths),
            &config,
            &clients,
            &events,
        );
        // The first starts are awaited, so serve refuses to run with none.
        while !serving.starting.is_empty() {
            let Some(start) = starts.recv().await else {
                break;
            };
            // Nothing is read before serve runs, so nothing is held yet.
            serving.settle(&self.paths, start, &mut clients);
        }
        if clients.is_empty() {
            let reasons = serving.failures();
            bail!("no channel adapter could start: {reasons}");
        }
        let mut workers: HashMap<String, ConversationWorker> = HashMap::new();
        // The registry is read for each message and each tick, so a person the
        // owner adds or removes counts from their next message; channels and
        // the heartbeat stay as serve started with them.
        let mut live = Arc::new(config.clone());
        // Chats where someone who gets no turn wrote, and when the owner is
        // told.
        let mut newcomers: HashMap<String, std::time::Instant> = HashMap::new();
        // Schedules are read on every tick, so one made in conversation or in
        // the dashboard needs no restart; the first tick fires what is due now.
        let mut schedules = tokio::time::interval(Duration::from_secs(30));
        schedules.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Messages a turn queued for another conversation go out within a second.
        let mut sends = tokio::time::interval(Duration::from_secs(1));
        sends.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Channels added since, or not yet able to start.
        let mut channels = tokio::time::interval(Duration::from_secs(1));
        channels.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut dispatched = HashMap::new();
        let mut report = Report::default();
        // Held messages of a channel that just started, read before the inbox.
        let mut replay: std::collections::VecDeque<(String, AdapterEvent)> = Default::default();
        tokio::pin!(shutdown);
        let outcome = 'serve: loop {
            tokio::select! {
                biased;
                () = &mut shutdown => break Ok(()),
                event = async { match replay.pop_front() { Some(event) => Some(event), None => inbox.recv().await } } => {
                    let Some((channel, event)) = event else { break Err(anyhow::anyhow!("every channel adapter has exited")) };
                    match event {
                        // A channel that never started (its initialize failed)
                        // reports its end too; serve was never relying on it.
                        AdapterEvent::Exited { .. } if !clients.contains_key(&channel) => continue,
                        AdapterEvent::Exited { diagnostic } => {
                            break Err(anyhow::anyhow!("channel adapter {channel} exited: {diagnostic}"));
                        }
                        AdapterEvent::Message(message) => {
                            match self.paths.load() {
                                Ok(fresh) => live = Arc::new(fresh),
                                Err(error) => report.error("config.toml", error),
                            }
                            let config = live.clone();
                            let Some(client) = clients.get(&channel) else {
                                if serving.starting.contains(&channel) {
                                    serving.held.entry(channel).or_default().push(message);
                                }
                                continue;
                            };
                            let manifest = &client.manifest;
                            let identity = identity(manifest, &message.conversation);
                            // The account's own messages from elsewhere belong to whoever holds it.
                            let author = if message.sender.is_me {
                                match manifest.account_holder {
                                    AccountHolder::Assistant => ASSISTANT.to_owned(),
                                    AccountHolder::Owner => config.owner.clone(),
                                }
                            } else {
                                config
                                    .person(&manifest.id, &message.sender.handle)
                                    .map_or_else(|| format!("{}:{}", manifest.id, message.sender.handle), |person| person.id.clone())
                            };
                            let observed_outbound = message.sender.is_me;
                            // Someone who gets no turn here: the owner hears
                            // about it in their direct chat once the burst
                            // settles. Never on the owner's own account.
                            if !observed_outbound
                                && manifest.account_holder == AccountHolder::Assistant
                                && config.tells_owner_about(
                                    &identity.id,
                                    &identity.kind,
                                    identity.members.as_ref().map(Vec::len),
                                    &author,
                                )
                            {
                                newcomers
                                    .entry(identity.id.clone())
                                    .or_insert_with(|| std::time::Instant::now() + self.newcomer_settle);
                            }
                            let worker = workers.entry(identity.id.clone()).or_insert_with(|| {
                                spawn_worker(&self.paths, self.broker_executable.clone(), client.clone(), &identity.id, &identity.native_id)
                            });
                            if !observed_outbound {
                                queue(&worker.queued, Store::open(&self.paths.database()).ok().as_ref(), &identity.id, &author, &message);
                            }
                            let job = Job::Inbound { identity: identity.clone(), author, message, observed_outbound };
                            if worker.jobs.send((config, job)).is_err() {
                                break Err(anyhow::anyhow!("conversation worker for {} stopped", identity.id));
                            }
                        }
                    }
                }
                _ = schedules.tick() => {
                    match self.paths.load() {
                        Ok(fresh) => live = Arc::new(fresh),
                        Err(error) => report.error("config.toml", error),
                    }
                    let now = chrono::Local::now();
                    // Like a schedule slot, a heartbeat slot is handed over on
                    // every tick while it is owed and runs once.
                    // Configured, or by default once anything is connected; its
                    // chat, when not configured, is the owner's latest direct one.
                    let running = Store::open(&self.paths.database())
                        .and_then(|store| crate::heartbeat::Heartbeat::serving(&config, &store, &self.paths))
                        .unwrap_or_else(|error| {
                            report.error("heartbeat", error);
                            None
                        });
                    let heartbeat = running.as_ref().and_then(|heartbeat| {
                        let (channel, _, native_id) = crate::schedule::channel_conversation(&heartbeat.conversation)?;
                        Some((heartbeat, clients.get(channel)?, native_id))
                    });
                    if let Some((heartbeat, client, native_id)) = heartbeat {
                        for (slot, digest) in [(heartbeat.due_slot(&now), false), (heartbeat.due_digest(&now), true)] {
                            let Some(slot) = slot else { continue };
                            let conversation = &heartbeat.conversation;
                            let worker = workers.entry(conversation.clone()).or_insert_with(|| {
                                spawn_worker(&self.paths, self.broker_executable.clone(), client.clone(), conversation, native_id)
                            });
                            // The settings that chose this worker and its delivery go
                            // with the job: a worker started after config.toml changed
                            // has read a newer heartbeat, which may name another chat.
                            if worker.jobs.send((live.clone(), Job::Heartbeat { heartbeat: heartbeat.clone(), slot, digest })).is_err() {
                                break 'serve Err(anyhow::anyhow!("conversation worker for {conversation} stopped"));
                            }
                        }
                    }
                    let stored = match ScheduleStore::open(&self.paths).and_then(|store| store.list(false)) {
                        Ok(stored) => stored,
                        Err(error) => {
                            report.error("schedules", error);
                            continue;
                        }
                    };
                    // The memory sync runs as a schedule of the host's in the
                    // owner's direct chat, once there is one: nightly after a
                    // first import, and every ten minutes while one runs.
                    // It needs a shell to open large files and mark its progress.
                    let sync = config.memory_sync.as_ref().filter(|_| config.access == crate::harness::Access::Computer).and_then(|sync| {
                        let store = Store::open(&self.paths.database()).ok()?;
                        let conversation = sync.chat(&store, &config.owner).ok()??;
                        Some(sync.due(&self.paths, store.last_seq().unwrap_or(0), &conversation, now))
                    }).into_iter().flatten();
                    // A console schedule runs only through `oppa schedules run`.
                    for schedule in stored.into_iter().map(|stored| stored.schedule).chain(sync).filter(|schedule| schedule.enabled) {
                        let (Some((channel, _, native_id)), Some(slot)) = (schedule.channel_destination(), schedule.due_slot(&now)) else { continue };
                        let Some(client) = clients.get(channel) else { continue };
                        // Reported as blocked by `oppa schedules`; never fired.
                        if client.manifest.account_holder == AccountHolder::Owner {
                            continue;
                        }
                        // A wake looks only while a heartbeat runs, in its waking hours.
                        let heartbeat = match &running {
                            _ if !schedule.wake => None,
                            Some(heartbeat) if heartbeat.awake(now.time()) => Some(heartbeat.clone()),
                            _ => continue,
                        };
                        // The worker fires each tick while the slot is due; a
                        // slot already fired is recognized there and skipped.
                        let worker = workers.entry(schedule.conversation.clone()).or_insert_with(|| {
                            spawn_worker(&self.paths, self.broker_executable.clone(), client.clone(), &schedule.conversation, native_id)
                        });
                        let conversation = schedule.conversation.clone();
                        let job = match heartbeat {
                            Some(heartbeat) => Job::Wake { wake: schedule, heartbeat, slot },
                            None => Job::Schedule { schedule, slot },
                        };
                        if worker.jobs.send((live.clone(), job)).is_err() {
                            break 'serve Err(anyhow::anyhow!("conversation worker for {conversation} stopped"));
                        }
                    }
                }
                _ = channels.tick() => {
                    if let Some(changed) = serving.due(&self.paths) {
                        match self.paths.load() {
                            Ok(fresh) => serving.launch(&self.paths, changed, &fresh, &clients, &events),
                            Err(error) => report.error("config.toml", error),
                        }
                    }
                }
                Some(start) = starts.recv() => replay.extend(serving.settle(&self.paths, start, &mut clients)),
                _ = sends.tick() => {
                    let settled: Vec<String> = newcomers
                        .iter()
                        .filter(|(_, at)| **at <= std::time::Instant::now())
                        .map(|(source, _)| source.clone())
                        .collect();
                    for source in settled {
                        newcomers.remove(&source);
                        let chat = match owner_chat(&self.paths, &source, &live.owner) {
                            Ok(Some(chat)) => chat,
                            Ok(None) => {
                                report.error(&source, "a newcomer here was not told to the owner: the owner has no direct chat with the assistant on this account");
                                continue;
                            }
                            Err(error) => {
                                report.error(&source, error);
                                continue;
                            }
                        };
                        let Some(client) = clients.get(&chat.channel) else { continue };
                        let worker = workers.entry(chat.id.clone()).or_insert_with(|| {
                            spawn_worker(&self.paths, self.broker_executable.clone(), client.clone(), &chat.id, &chat.native_id)
                        });
                        let job = Job::Newcomer { source, again_after: self.newcomer_settle };
                        if worker.jobs.send((live.clone(), job)).is_err() {
                            break 'serve Err(anyhow::anyhow!("conversation worker for {} stopped", chat.id));
                        }
                    }
                    let queued = Store::open(&self.paths.database()).and_then(|store| {
                        let mut ready = Vec::new();
                        let mut failed = None;
                        for (id, conversation, queued_at) in store.queued_sends()? {
                            // Too old to say now, whether or not its channel runs here.
                            let entry = if crate::runtime::send_is_stale(&queued_at) {
                                store.withdraw_send(id, crate::runtime::STALE_SEND).map(|_| None)
                            } else {
                                store.conversation(&conversation).map(|c| Some((id, c)))
                            };
                            // One unreadable row must not hold up the others.
                            match entry {
                                Ok(Some(ready_entry)) => ready.push(ready_entry),
                                Ok(None) => {}
                                Err(error) => failed = Some(error),
                            }
                        }
                        if let Some(error) = failed {
                            report.error("messages", error);
                        }
                        Ok(ready)
                    });
                    let queued = match queued {
                        Ok(queued) => queued,
                        Err(error) => {
                            report.error("messages", error);
                            continue;
                        }
                    };
                    // Each entry goes to its conversation's worker, again after
                    // 30 seconds if no attempt began, since a job can fail before
                    // its claim; the claim makes a second hand-off do nothing.
                    dispatched.retain(|id, _| queued.iter().any(|(queued, _)| queued == id));
                    for (outbox_id, conversation) in queued {
                        if dispatched.get(&outbox_id).is_some_and(|at: &std::time::Instant| at.elapsed() < Duration::from_secs(30)) {
                            continue;
                        }
                        let Some(client) = clients.get(&conversation.channel) else { continue };
                        if client.manifest.account != conversation.account {
                            continue;
                        }
                        let worker = workers.entry(conversation.id.clone()).or_insert_with(|| {
                            spawn_worker(&self.paths, self.broker_executable.clone(), client.clone(), &conversation.id, &conversation.native_id)
                        });
                        if worker.jobs.send((live.clone(), Job::Deliver { outbox_id })).is_err() {
                            break 'serve Err(anyhow::anyhow!("conversation worker for {} stopped", conversation.id));
                        }
                        dispatched.insert(outbox_id, std::time::Instant::now());
                    }
                }
            }
        };
        for (_, worker) in workers.drain() {
            drop(worker.jobs);
            let _ = worker.thread.join();
        }
        for client in clients.values() {
            client.shutdown().await;
        }
        drop(clients);
        outcome
    }
}

/// The owner's direct chat on the channel account of `source`, where a
/// newcomer there is told.
fn owner_chat(
    paths: &Paths,
    source: &str,
    owner: &str,
) -> Result<Option<crate::store::Conversation>> {
    let store = Store::open(&paths.database())?;
    let source = store.conversation(source)?;
    store
        .owner_chat(&source.channel, &source.account, owner)?
        .map(|chat| store.conversation(&chat))
        .transpose()
}

fn spawn_worker(
    paths: &Paths,
    broker_executable: Option<std::path::PathBuf>,
    client: ChannelClient,
    conversation: &str,
    native_conversation: &str,
) -> ConversationWorker {
    let (jobs, receiver) = std::sync::mpsc::channel::<(Arc<Config>, Job)>();
    let queued = Queued::default();
    let taken = queued.clone();
    let paths = paths.clone();
    let conversation = conversation.to_owned();
    let native_conversation = native_conversation.to_owned();
    let thread = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build();
        let mut report = Report::default();
        let Ok(runtime) = runtime else {
            report.error(&conversation, "worker runtime could not start");
            return;
        };
        runtime.block_on(async move {
            let opened = Runtime::open(paths.clone()).map(|host| match broker_executable {
                Some(executable) => host.with_broker_executable(executable),
                None => host,
            });
            let mut host = match opened {
                Ok(host) => host,
                Err(error) => {
                    report.error(&conversation, error);
                    return;
                }
            };
            let pending = taken.clone();
            host.followups = Some(Box::new(move |author, native_id, body| {
                pending
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .get(author)
                    .is_some_and(|entries| {
                        entries
                            .iter()
                            .any(|(queued, text)| (queued.as_str(), text.as_str()) != (native_id, body))
                    })
            }));
            let mut delivery = ChannelDelivery {
                client,
                native_conversation,
                paths: paths.clone(),
            };
            // Turns of messages that wait for the other assistant, and looks
            // about newcomers, each due at a time. The worker keeps taking jobs
            // meanwhile, so what the other assistant says during a wait is
            // logged before the turn reads the chat.
            let mut later: Vec<(std::time::Instant, Later)> = Vec::new();
            // Looks here that ran a turn in the last hour, and about which chat.
            let mut looked: Vec<(std::time::Instant, String)> = Vec::new();
            loop {
                let next = later.iter().map(|(at, _)| *at).min();
                let timeout = next.map_or(Duration::from_millis(500), |at| {
                    at.saturating_duration_since(std::time::Instant::now())
                        .min(Duration::from_millis(500))
                });
                let received = receiver.recv_timeout(timeout).map(|(config, job)| {
                    host.config = Config::clone(&config);
                    job
                });
                // Jobs queued behind a long turn are taken first, so a due
                // turn reads what was said while it was due.
                let idle = received.is_err();
                match received {
                    Ok(Job::Inbound {
                        identity,
                        author,
                        message,
                        observed_outbound,
                    }) => {
                        if !observed_outbound {
                            take(&taken, &author, &message);
                        }
                        let wait = host.config.turn_wait(&identity.id, &author);
                        let result = if observed_outbound {
                            host.observe_outbound(
                                &identity,
                                &author,
                                &message.native_id,
                                &message.body,
                                message.attachments,
                            )
                            .map(|_| ())
                        } else if let Some(wait) = wait {
                            host.receive_later(
                                &identity,
                                &author,
                                &message.native_id,
                                &message.body,
                                message.attachments,
                            )
                            .map(|input| {
                                later.push((
                                    std::time::Instant::now() + wait,
                                    Later::Turn {
                                        seq: input.seq,
                                        author: author.clone(),
                                    },
                                ))
                            })
                        } else {
                            host.receive(
                                &identity,
                                &author,
                                &message.native_id,
                                &message.body,
                                message.attachments,
                                &mut delivery,
                            )
                            .await
                            .map(|_| ())
                        };
                        if let Err(error) = result {
                            report.error(&identity.id, error);
                        }
                    }
                    Ok(Job::Schedule { schedule, slot }) => {
                        if let Err(error) =
                            host.fire_schedule(&schedule, &slot, &mut delivery).await
                        {
                            report
                                .error(&conversation, format!("schedule {}: {error}", schedule.id));
                        }
                    }
                    Ok(Job::Wake {
                        wake,
                        heartbeat,
                        slot,
                    }) => {
                        let now = chrono::Local::now;
                        if let Err(error) = host
                            .fire_wake(&wake, &heartbeat, &slot, now, &mut delivery)
                            .await
                        {
                            report.error(&conversation, format!("wake {}: {error}", wake.id));
                        }
                    }
                    Ok(Job::Heartbeat {
                        heartbeat,
                        slot,
                        digest,
                    }) => {
                        let now = chrono::Local::now;
                        let fired = if digest {
                            host.fire_digest(&heartbeat, &slot, now, &mut delivery)
                                .await
                        } else {
                            host.fire_heartbeat(&heartbeat, &slot, now, &mut delivery)
                                .await
                        };
                        if let Err(error) = fired {
                            report.error(&conversation, format!("heartbeat: {error}"));
                        }
                    }
                    Ok(Job::Deliver { outbox_id }) => {
                        if let Err(error) = host.deliver_queued(outbox_id, &mut delivery).await {
                            report.error(&conversation, format!("queued message: {error}"));
                        }
                    }
                    // A look already waiting for that chat will cover this message too.
                    Ok(Job::Newcomer {
                        source,
                        again_after,
                    }) => {
                        let queued = later.iter().any(|(_, item)| {
                            matches!(item, Later::Look { source: waiting, .. } if *waiting == source)
                        });
                        if !queued {
                            let look = Later::Look {
                                source,
                                again_after,
                                tries: NEWCOMER_TRIES,
                            };
                            later.push((std::time::Instant::now(), look));
                        }
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                    // Serve is stopping: turns still waiting run only if the
                    // channel delivers their messages again after a restart.
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                }
                // One due item per pass, the earliest: what arrives during it
                // is logged before the next one reads the chat.
                let now = std::time::Instant::now();
                let due = (0..later.len())
                    .filter(|index| idle && later[*index].0 <= now)
                    .min_by_key(|index| later[*index].0);
                match due.map(|index| later.remove(index).1) {
                    Some(Later::Turn { seq, author }) => {
                        // A later message from the same author is still
                        // waiting: its turn answers both.
                        let by = later.iter().find_map(|(_, waiting)| match waiting {
                            Later::Turn { seq: next, author: other }
                                if *other == author && *next > seq =>
                            {
                                Some(*next)
                            }
                            _ => None,
                        });
                        let answered = match by {
                            Some(by) => host.cover_waited(&conversation, seq, by),
                            None => host
                                .answer_waited(&conversation, seq, &mut delivery)
                                .await
                                .map(|_| ()),
                        };
                        if let Err(error) = answered {
                            report.error(&conversation, error);
                        }
                    }
                    Some(Later::Look {
                        source,
                        again_after,
                        tries,
                    }) => {
                        looked.retain(|(at, _)| at.elapsed() < LOOK_WINDOW);
                        let last = looked
                            .iter()
                            .find(|(_, chat)| *chat == source)
                            .map(|(at, _)| *at);
                        if let Some(at) = last {
                            // That chat was looked at within the hour: the next
                            // look covers everything since.
                            let look = Later::Look {
                                source,
                                again_after,
                                tries,
                            };
                            later.push((at + LOOK_WINDOW, look));
                        } else if looked.len() >= NEWCOMER_LOOKS_PER_HOUR {
                            report.error(&source, LOOK_LIMIT_REACHED);
                        } else {
                            match host
                                .fire_newcomer(&conversation, &source, &mut delivery)
                                .await
                            {
                                Ok(Some(_)) => looked.push((std::time::Instant::now(), source)),
                                Ok(None) => {}
                                Err(error) => {
                                    report.error(
                                        &conversation,
                                        format!("newcomer in {source}: {error}"),
                                    );
                                    if tries > 1 {
                                        let retry = Later::Look {
                                            source,
                                            again_after,
                                            tries: tries - 1,
                                        };
                                        later.push((now + again_after, retry));
                                    }
                                }
                            }
                        }
                    }
                    None => {}
                }
                // A held answer whose follow-up started no turn goes out once
                // nothing is queued or waiting here.
                let quiet = idle
                    && !later.iter().any(|(_, waiting)| matches!(waiting, Later::Turn { .. }))
                    && taken
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .is_empty();
                if quiet {
                    if let Err(error) = host.release_held(&conversation, &mut delivery).await {
                        report.error(&conversation, error);
                    }
                }
                if let Err(error) = host
                    .present_pending_with(&conversation, &mut delivery)
                    .await
                {
                    report.error(&conversation, error);
                }
            }
        });
    });
    ConversationWorker {
        jobs,
        queued,
        thread,
    }
}

/// Turn failures are reported as they happen so a long-running serve is never
/// silently deaf; a failure repeated by the presentation poll prints once.
#[derive(Default)]
struct Report {
    last: Option<String>,
}
impl Report {
    fn error(&mut self, conversation: &str, error: impl std::fmt::Display) {
        let line = format!("serve: {conversation}: {error}");
        if self.last.as_deref() != Some(line.as_str()) {
            eprintln!("{line}");
            self.last = Some(line);
        }
    }
}

/// Which configured channels serve runs, and when to look again for ones it
/// does not: when config.toml changes, and every 30 seconds while one could
/// not start. A start runs on its own task, so an adapter slow to initialize
/// never holds up messages on the others.
struct Serving {
    states: std::collections::BTreeMap<String, Value>,
    starting: std::collections::HashSet<String>,
    config_changed: Option<std::time::SystemTime>,
    retry_at: Option<std::time::Instant>,
    started: mpsc::Sender<(ChannelManifest, Result<ChannelClient>)>,
    /// Messages from a channel still starting: an adapter may hand over its
    /// backlog before it answers initialize, and has moved past it by then.
    held: HashMap<String, Vec<InboundMessage>>,
}

impl Serving {
    fn new(started: mpsc::Sender<(ChannelManifest, Result<ChannelClient>)>) -> Self {
        Self {
            states: Default::default(),
            starting: Default::default(),
            config_changed: None,
            retry_at: None,
            started,
            held: HashMap::new(),
        }
    }

    fn changed(paths: &Paths) -> Option<std::time::SystemTime> {
        std::fs::metadata(paths.config_file())
            .and_then(|m| m.modified())
            .ok()
    }

    /// The file's time when a look is due, read before the file is loaded so
    /// a change made meanwhile is looked at again.
    fn due(&self, paths: &Paths) -> Option<Option<std::time::SystemTime>> {
        let changed = Self::changed(paths);
        let retry = self
            .retry_at
            .is_some_and(|at| at <= std::time::Instant::now());
        (changed != self.config_changed || retry).then_some(changed)
    }

    fn launch(
        &mut self,
        paths: &Paths,
        changed: Option<std::time::SystemTime>,
        config: &Config,
        clients: &HashMap<String, ChannelClient>,
        events: &mpsc::Sender<(String, AdapterEvent)>,
    ) {
        self.config_changed = changed;
        self.retry_at = None;
        for manifest in &config.channels {
            if clients.contains_key(&manifest.id) || !self.starting.insert(manifest.id.clone()) {
                continue;
            }
            self.states
                .insert(manifest.id.clone(), json!({"state": "starting"}));
            let (manifest, events, started) =
                (manifest.clone(), events.clone(), self.started.clone());
            tokio::spawn(async move {
                let client = ChannelClient::start(manifest.clone(), events).await;
                let _ = started.send((manifest, client)).await;
            });
        }
        self.record(paths);
    }

    /// Record how a start ended. A channel that runs now returns the
    /// messages it sent while starting, oldest first; a failed one keeps
    /// them for its next start.
    fn settle(
        &mut self,
        paths: &Paths,
        (manifest, client): (ChannelManifest, Result<ChannelClient>),
        clients: &mut HashMap<String, ChannelClient>,
    ) -> Vec<(String, AdapterEvent)> {
        self.starting.remove(&manifest.id);
        let mut held = Vec::new();
        let state = match client {
            Ok(client) => {
                clients.insert(manifest.id.clone(), client);
                held = (self
                    .held
                    .remove(&manifest.id)
                    .unwrap_or_default()
                    .into_iter())
                .map(|message| (manifest.id.clone(), AdapterEvent::Message(message)))
                .collect();
                json!({"state": "running", "since": chrono::Utc::now().to_rfc3339()})
            }
            Err(error) => {
                eprintln!("serve: channel {}: {error:#}", manifest.id);
                self.retry_at = Some(std::time::Instant::now() + Duration::from_secs(30));
                json!({"state": "failed", "error": bounded(&format!("{error:#}")),
                    "at": chrono::Utc::now().to_rfc3339(), "retry_seconds": 30})
            }
        };
        self.states.insert(manifest.id, state);
        self.record(paths);
        held
    }

    /// A channel taken out of config.toml runs until serve restarts.
    fn record(&self, paths: &Paths) {
        if let Err(error) = crate::channels::record_serving(paths, &self.states) {
            eprintln!("serve: channels/serving.json: {error:#}");
        }
    }

    fn failures(&self) -> String {
        self.states
            .iter()
            .map(|(id, state)| format!("{id}: {}", state["error"].as_str().unwrap_or("?")))
            .collect::<Vec<_>>()
            .join("; ")
    }
}

/// Whether a serve process holds this instance's lease now.
pub fn serve_running(paths: &Paths) -> bool {
    let Ok(file) = OpenOptions::new()
        .read(true)
        .open(paths.runtime.join("locks").join("serve.lock"))
    else {
        return false;
    };
    matches!(file.try_lock(), Err(std::fs::TryLockError::WouldBlock))
}

fn serve_lease(paths: &Paths) -> Result<std::fs::File> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(paths.runtime.join("locks").join("serve.lock"))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => {
            bail!("another serve process owns this instance")
        }
        Err(error) => Err(error.into()),
    }
}

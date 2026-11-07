//! Conversation execution. The transcript supplies meaning; this module supplies records.
use crate::{
    broker::Broker,
    config::{private_directory, Config, Paths},
    harness::{self, Access, Activity, FailureCategory, HarnessRequest, McpServer, RunOutcome},
    launch::{BrokerRun, RunCapabilities},
    prompt::{context_window, profiles, readable_copies, turn_prompt},
    schedule::{Schedule, ScheduleStore},
    store::{
        Attachment, DeliveryObservation, DeliveryState, Event, InboundEvent, OutboxEntry, Store,
    },
    tasks::TaskStore,
};
use anyhow::{anyhow, bail, ensure, Context, Result};
use chrono::{DateTime, Local, Utc};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    future::Future,
    io::Write,
    path::{Path, PathBuf},
};

/// A transport for one conversation. It returns only what it observed: a native
/// id proves a send, a refusal proves none, and everything else stays unknown.
pub trait Deliver {
    fn deliver(&mut self, entry: &OutboxEntry)
        -> impl Future<Output = Result<DeliveryObservation>>;

    /// Show or clear "typing" while a reply is composed, where the channel can.
    /// Best effort: it proves nothing and never fails a turn.
    fn typing(&mut self, _on: bool) -> impl Future<Output = ()> {
        async {}
    }
}

/// How often the typing bubble is shown again while a turn runs, since
/// Messages lets one lapse.
const TYPING_REFRESH: std::time::Duration = std::time::Duration::from_secs(20);

/// The console and tests deliver text synchronously; a failed write is unknown
/// because bytes may already have reached the terminal.
pub struct ConsoleDelivery<F>(pub F);

impl<F> Deliver for ConsoleDelivery<F>
where
    F: FnMut(&str) -> Result<()>,
{
    fn deliver(
        &mut self,
        entry: &OutboxEntry,
    ) -> impl Future<Output = Result<DeliveryObservation>> {
        let observation = match (self.0)(&entry.event.body) {
            Ok(()) => DeliveryObservation::Sent {
                native_id: format!("console:{}", entry.id),
            },
            Err(_) => DeliveryObservation::Unknown {
                evidence: "console write did not complete; bytes may have reached the terminal"
                    .into(),
            },
        };
        std::future::ready(Ok(observation))
    }
}

/// Host-resolved identity of a conversation on one channel account.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConversationIdentity {
    pub id: String,
    pub channel: String,
    pub account: String,
    pub native_id: String,
    /// `dm` or `group`, as the channel reported it.
    pub kind: String,
    pub title: Option<String>,
    /// A group's other members as `<channel>:<handle>`, when the channel
    /// listed them with this message.
    pub members: Option<Vec<String>>,
}
impl ConversationIdentity {
    pub fn console(id: &str) -> Self {
        Self {
            id: id.into(),
            channel: "console".into(),
            account: "local".into(),
            native_id: id.into(),
            kind: "dm".into(),
            title: None,
            members: None,
        }
    }
}

pub const MANUAL: &str = include_str!("../resources/manual/MANUAL.md");
/// The one fixed notice: sent in a direct conversation when every attempt ended
/// without an answer and none may have acted. Resending is then safe, and no
/// automatic retry is promised because none happens.
pub const UNREACHABLE_NOTICE: &str = "I couldn't get an answer from my model just now, so I haven't replied to that. Please send it again in a little while.";
pub struct Runtime {
    pub paths: Paths,
    pub config: Config,
    pub store: Store,
    broker_executable: PathBuf,
    /// Whether a new message or edit from `author`, other than the one being
    /// answered (by native id and text), has arrived and waits to be logged:
    /// serve's worker
    /// sets it, so a reply its author has already added to is held and
    /// answered with the rest.
    pub followups: Option<Box<dyn Fn(&str, &str, &str) -> bool + Send + Sync>>,
}

#[derive(Debug, Serialize)]
pub struct TurnResult {
    pub input_seq: i64,
    pub reply_seq: Option<i64>,
    pub agent: String,
    pub silent: bool,
}

impl Runtime {
    pub fn open(paths: Paths) -> Result<Self> {
        let config = paths.load()?;
        if !paths.database().is_file() {
            bail!("runtime storage is missing; run oppa init");
        }
        let store = Store::open(&paths.database())?;
        private_directory(&paths.runtime.join("harness"))?;
        private_directory(&paths.runtime.join("harness-restricted"))?;
        private_directory(&paths.skills())?;
        Ok(Self {
            paths,
            config,
            store,
            broker_executable: std::env::current_exe()?,
            followups: None,
        })
    }

    /// Embedding/test hosts supply the actual CLI executable, not model input.
    pub fn with_broker_executable(mut self, executable: PathBuf) -> Self {
        self.broker_executable = executable;
        self
    }

    /// Local operator console: the process owner is the configured owner.
    pub async fn turn<F>(
        &self,
        conversation: &str,
        native_id: &str,
        text: &str,
        deliver: F,
    ) -> Result<TurnResult>
    where
        F: FnMut(&str) -> Result<()>,
    {
        self.turn_with(conversation, native_id, text, vec![], deliver)
            .await
    }

    /// A console turn with files the owner attached.
    pub async fn turn_with<F>(
        &self,
        conversation: &str,
        native_id: &str,
        text: &str,
        attachments: Vec<Attachment>,
        deliver: F,
    ) -> Result<TurnResult>
    where
        F: FnMut(&str) -> Result<()>,
    {
        let owner = self.config.owner.clone();
        self.receive(
            &ConversationIdentity::console(conversation),
            &owner,
            native_id,
            text,
            attachments,
            &mut ConsoleDelivery(deliver),
        )
        .await?
        .context("owner console input produced no turn")
    }

    /// Log a message the account sent from elsewhere; no model turn runs.
    pub fn observe_outbound(
        &self,
        identity: &ConversationIdentity,
        author: &str,
        native_id: &str,
        body: &str,
        attachments: Vec<Attachment>,
    ) -> Result<Event> {
        self.register(identity)?;
        self.store.append_observed_outbound(&InboundEvent {
            conversation: identity.id.clone(),
            native_id: native_id.into(),
            author: author.into(),
            body: body.into(),
            kind: "message".into(),
            attachments,
            ..Default::default()
        })
    }

    /// Record one inbound message and, when its host-resolved author is the
    /// owner, a registered adult or a child, run a model turn under that person's
    /// authority. Everyone else, registered or not, is logged and never
    /// answered until their role has its own slice; this fails closed. On the
    /// owner's own account nobody's message starts a turn.
    pub async fn receive<D: Deliver>(
        &self,
        identity: &ConversationIdentity,
        author: &str,
        native_id: &str,
        text: &str,
        attachments: Vec<Attachment>,
        deliver: &mut D,
    ) -> Result<Option<TurnResult>> {
        let _lease = conversation_lease(&self.paths, &identity.id)?
            .context("another turn owns this conversation")?;
        let (input, standing) = self.log_inbound(identity, author, native_id, text, attachments)?;
        if self.config.turn_role(author).is_none() || self.on_owners_account(&identity.id) {
            return Ok(None);
        }
        self.answer(&input, &standing, RunCapabilities::Conversation, deliver)
            .await
    }

    /// Record one inbound message whose turn waits (`Config::turn_wait`), so
    /// that what others say meanwhile is logged before the turn reads the
    /// conversation. Its turn is `answer_waited`; a serve stopped in between
    /// leaves the message logged without one until the channel delivers it
    /// again, which waits afresh and then runs that turn once.
    pub fn receive_later(
        &self,
        identity: &ConversationIdentity,
        author: &str,
        native_id: &str,
        text: &str,
        attachments: Vec<Attachment>,
    ) -> Result<Event> {
        let _lease = conversation_lease(&self.paths, &identity.id)?
            .context("another turn owns this conversation")?;
        Ok(self
            .log_inbound(identity, author, native_id, text, attachments)?
            .0)
    }

    /// The turn of a message `receive_later` recorded, under its author's
    /// authority, at most once.
    pub async fn answer_waited<D: Deliver>(
        &self,
        conversation: &str,
        input_seq: i64,
        deliver: &mut D,
    ) -> Result<Option<TurnResult>> {
        let _lease = conversation_lease(&self.paths, conversation)?
            .context("another turn owns this conversation")?;
        let standing = self.restanding(conversation)?;
        let input = self.store.event(input_seq)?;
        ensure!(
            input.conversation == conversation
                && input.direction == crate::store::Direction::In
                && matches!(input.kind.as_str(), "message" | "edit"),
            "event {input_seq} is not a message in {conversation}"
        );
        if self.config.turn_role(&input.author).is_none() || self.on_owners_account(conversation) {
            return Ok(None);
        }
        self.answer(&input, &standing, RunCapabilities::Conversation, deliver)
            .await
    }

    /// A waited message whose author wrote again before its turn began gets
    /// no turn of its own: the later message's turn reads both and answers
    /// them together.
    pub fn cover_waited(&self, conversation: &str, input_seq: i64, by: i64) -> Result<()> {
        let _lease = conversation_lease(&self.paths, conversation)?
            .context("another turn owns this conversation")?;
        let marker = self.turn_marker(input_seq);
        if !marker.try_exists()? {
            atomic_json(
                &marker,
                &serde_json::json!({"state":"covered","by_input_seq":by}),
            )?;
        }
        Ok(())
    }

    /// Send a held answer that no follow-up took up: the follow-up turned out
    /// to start no turn (a duplicate, or one that could not be logged), or
    /// serve restarted. Serve's worker calls this once the conversation has
    /// nothing queued or waiting.
    pub async fn release_held<D: Deliver>(
        &self,
        conversation: &str,
        deliver: &mut D,
    ) -> Result<Option<i64>> {
        let path = unsent_draft(&self.paths, conversation);
        let Ok(bytes) = fs::read(&path) else {
            return Ok(None);
        };
        let _lease = conversation_lease(&self.paths, conversation)?
            .context("another turn owns this conversation")?;
        let draft: serde_json::Value = serde_json::from_slice(&bytes)?;
        let fresh = draft["held_at"]
            .as_str()
            .and_then(|at| chrono::DateTime::parse_from_rfc3339(at).ok())
            .is_some_and(|at| Utc::now().signed_duration_since(at).num_minutes() < 60);
        let (Some(text), Some(seq), true) = (
            draft["text"].as_str(),
            draft["for_input_seq"].as_i64(),
            fresh,
        ) else {
            // Past the hour the person has likely moved on, and a newer reply
            // may already be out: an old answer now would come after it.
            fs::remove_file(&path)?;
            return Ok(None);
        };
        let entry = self.store.enqueue_reply(
            conversation,
            &format!("reply:held:{seq}"),
            text,
            vec![],
            Some(seq.to_string()),
        )?;
        // An error before here leaves the draft for the next quiet pass.
        fs::remove_file(&path)?;
        let observation = self.send(&entry, deliver).await?;
        self.store.finish_send(entry.id, observation)?;
        Ok(Some(entry.event.seq))
    }

    fn on_owners_account(&self, conversation: &str) -> bool {
        crate::schedule::channel_conversation(conversation)
            .is_some_and(|(channel, _, _)| self.config.owners_account(channel))
    }

    fn log_inbound(
        &self,
        identity: &ConversationIdentity,
        author: &str,
        native_id: &str,
        text: &str,
        attachments: Vec<Attachment>,
    ) -> Result<(Event, Standing)> {
        let standing = self.register(identity)?;
        let input = self.store.append_channel_message(&InboundEvent {
            conversation: identity.id.clone(),
            native_id: native_id.into(),
            author: author.into(),
            body: text.into(),
            kind: "message".into(),
            attachments,
            ..Default::default()
        })?;
        Ok((input, standing))
    }

    /// Whose turn a schedule's slot is: the author of the message whose turn
    /// made it, while the registry still gives them turns, and otherwise the
    /// owner's, as for a schedule the owner set up by hand. A family member's
    /// schedule never fires as the owner's turn.
    fn schedule_author(&self, schedule: &Schedule) -> Result<String> {
        let origin = ScheduleStore::open(&self.paths)?
            .get(&schedule.id)
            .ok()
            .and_then(|stored| stored.origin_seq)
            .and_then(|seq| self.store.event(seq).ok())
            .map(|event| event.author);
        match origin {
            Some(author) if author != self.config.owner => {
                ensure!(
                    self.config.turn_role(&author).is_some(),
                    "schedule {} was made for {author}, who no longer gets turns",
                    schedule.id
                );
                Ok(author)
            }
            _ => Ok(self.config.owner.clone()),
        }
    }

    /// Fire one slot of a schedule into its configured conversation and run
    /// the turn a message from whoever asked for it would get: the owner's,
    /// or the family member's whose turn made it, under the family rules. The
    /// standing instruction is the authority. A slot fires at most once: its recorded event is
    /// reused after a second tick or a restart, the turn marker stops a second
    /// turn, and the command's start record keeps a command that a crash cut
    /// off from running again.
    pub async fn fire_schedule<D: Deliver>(
        &self,
        schedule: &Schedule,
        slot: &DateTime<Local>,
        deliver: &mut D,
    ) -> Result<Option<TurnResult>> {
        ensure!(
            !schedule.wake,
            "schedule {} is a wake, which fires as a look",
            schedule.id
        );
        let identity = schedule.destination(&self.store)?;
        // A schedule kept from before the owner's account became a connection.
        ensure!(
            !self.config.owners_account(&identity.channel),
            "schedule {} names the owner's own account, where the assistant never speaks; cancel it",
            schedule.id
        );
        let _lease = conversation_lease(&self.paths, &identity.id)?
            .context("another turn owns this conversation")?;
        let author = self.schedule_author(schedule)?;
        let standing = self.register(&identity)?;
        let native_id = format!("schedule:{}:{}", schedule.id, slot.to_rfc3339());
        let input = match self.store.inbound_event(&identity.id, &native_id)? {
            // Only the host's own record of this slot is reused.
            Some(event) if event.kind == "schedule" && event.author == author => event,
            Some(_) => bail!("another event already holds this schedule slot's id"),
            None => self.store.append_inbound(&InboundEvent {
                conversation: identity.id.clone(),
                native_id,
                author,
                body: schedule
                    .event_body(slot, &self.paths.runtime.join("runs"))
                    .await?,
                kind: "schedule".into(),
                ..Default::default()
            })?,
        };
        self.answer(&input, &standing, RunCapabilities::Conversation, deliver)
            .await
    }

    /// Fire one heartbeat slot into the heartbeat's conversation and run its
    /// read-only turn, at most once, as a schedule slot is. The heartbeat is
    /// the one serve started with, which chose `deliver`, never this runtime's
    /// own reading of config.toml, which may be newer. Its input tells the
    /// model when the previous heartbeat was, so it can look for what is new;
    /// the turn's marker records the outcome: silent, a reply, or a failure.
    /// `now` is the clock that says, once the lease is held, whether the slot
    /// is still owed.
    pub async fn fire_heartbeat<D: Deliver>(
        &self,
        heartbeat: &crate::heartbeat::Heartbeat,
        slot: &DateTime<Local>,
        now: impl Fn() -> DateTime<Local>,
        deliver: &mut D,
    ) -> Result<Option<TurnResult>> {
        let body = |conversation: &str| {
            let previous = self
                .store
                .latest_inbound(conversation, "heartbeat:")?
                .filter(|event| event.kind == "heartbeat")
                .and_then(|event| {
                    event
                        .native_id?
                        .strip_prefix("heartbeat:")
                        .map(str::to_owned)
                });
            let mut body = serde_json::json!({"heartbeat":slot.to_rfc3339(),"previous":previous});
            if heartbeat.morning(slot.time()) {
                body["morning"] = true.into();
            }
            Ok(Some(body))
        };
        let identity = crate::heartbeat::owners_chat(
            &self.store,
            &heartbeat.conversation,
            &self.config.owner,
        )?;
        let owed = || heartbeat.due_slot(&now()).as_ref() == Some(slot);
        let native_id = format!("heartbeat:{}", slot.to_rfc3339());
        self.look(identity, native_id, owed, body, deliver).await
    }

    /// Fire a wake the assistant set for itself (`wakes.set`) as a look in
    /// its chat, at most once and only while owed, as a heartbeat slot is:
    /// at its time within its window, in `heartbeat`'s waking hours. Its input
    /// is the reason the wake was set with.
    pub async fn fire_wake<D: Deliver>(
        &self,
        wake: &Schedule,
        heartbeat: &crate::heartbeat::Heartbeat,
        slot: &DateTime<Local>,
        now: impl Fn() -> DateTime<Local>,
        deliver: &mut D,
    ) -> Result<Option<TurnResult>> {
        ensure!(wake.wake, "schedule {} is not a wake", wake.id);
        let chat =
            crate::heartbeat::owners_chat(&self.store, &wake.conversation, &self.config.owner);
        let identity = match chat {
            Ok(identity) => identity,
            // Serve hands a due wake over on every tick; one whose chat can no
            // longer take it is cancelled, so it is reported once.
            Err(error) if error.is::<crate::heartbeat::NotOwnersChat>() => {
                ScheduleStore::open(&self.paths)?.cancel(&wake.id)?;
                return Err(error.context(format!("cancelled wake {}", wake.id)));
            }
            Err(error) => return Err(error),
        };
        let owed = || {
            let now = now();
            wake.due_slot(&now).as_ref() == Some(slot) && heartbeat.awake(now.time())
        };
        let body = serde_json::json!({"wake":slot.to_rfc3339(),"reason":wake.prompt,"id":wake.id});
        let native_id = format!("schedule:{}:{}", wake.id, slot.to_rfc3339());
        self.look(identity, native_id, owed, |_| Ok(Some(body)), deliver)
            .await
    }

    /// Fire the weekly digest as a heartbeat of its own, at most once. Its
    /// input holds what the records say about the week's heartbeats; the
    /// model reads what they said and what came of it. A week in which none
    /// spoke or failed has nothing to digest and starts no turn.
    pub async fn fire_digest<D: Deliver>(
        &self,
        heartbeat: &crate::heartbeat::Heartbeat,
        slot: &DateTime<Local>,
        now: impl Fn() -> DateTime<Local>,
        deliver: &mut D,
    ) -> Result<Option<TurnResult>> {
        let identity = crate::heartbeat::owners_chat(
            &self.store,
            &heartbeat.conversation,
            &self.config.owner,
        )?;
        let owed = || heartbeat.due_digest(&now()).as_ref() == Some(slot);
        let body = |conversation: &str| self.week(conversation, slot);
        let native_id = format!("digest:{}", slot.to_rfc3339());
        self.look(identity, native_id, owed, body, deliver).await
    }

    /// Tell the owner, in their direct chat `chat`, about messages in
    /// `source` from someone who gets no turn of their own: an unregistered
    /// sender, or someone known whom the owner has not let ask for things. It
    /// is a read-only look like a heartbeat, once for the newest such message;
    /// the model decides whether anything needs the owner, and may propose
    /// letting that person ask directly so one yes answers both.
    pub async fn fire_newcomer<D: Deliver>(
        &self,
        chat: &str,
        source: &str,
        deliver: &mut D,
    ) -> Result<Option<TurnResult>> {
        let identity = crate::heartbeat::owners_chat(&self.store, chat, &self.config.owner)?;
        let prefix = format!("newcomer:{source}:");
        let previous = self
            .store
            .latest_inbound(&identity.id, &prefix)?
            .and_then(|event| event.native_id?.strip_prefix(&prefix)?.parse().ok())
            .unwrap_or(0);
        let messages: Vec<(i64, String)> = self
            .store
            .messages_after(source, previous, 200)?
            .into_iter()
            .filter(|(_, author)| self.newcomer(author))
            .collect();
        let Some((through, _)) = messages.last().cloned() else {
            return Ok(None);
        };
        let mut from: Vec<&str> = Vec::new();
        for (_, author) in &messages {
            if !from.contains(&author.as_str()) {
                from.push(author);
            }
        }
        let shape = self.store.conversation(source)?;
        let body = serde_json::json!({"newcomer":{"conversation":source,"kind":shape.kind,"title":shape.title,
            "from":from,"message_seqs":messages.iter().map(|(seq, _)| seq).collect::<Vec<_>>(),"after_seq":previous}});
        self.look(
            identity,
            format!("{prefix}{through}"),
            || true,
            |_| Ok(Some(body)),
            deliver,
        )
        .await
    }

    /// Whether a logged author gets no turn of their own and is not another
    /// assistant: someone the owner may want to hear about.
    fn newcomer(&self, author: &str) -> bool {
        let author = self.config.current_author(author);
        author != crate::store::ASSISTANT
            && author != self.config.owner
            && self.config.turn_role(author).is_none()
            && self
                .config
                .person_by_id(author)
                .is_none_or(|person| person.role != crate::config::Role::Agent)
    }

    /// The heartbeats whose slots fall in the week before `until`, from their
    /// events and turn markers: how many looked and ended silent, and which
    /// spoke or failed. None when none spoke or failed.
    fn week(
        &self,
        conversation: &str,
        until: &DateTime<Local>,
    ) -> Result<Option<serde_json::Value>> {
        let from = crate::heartbeat::Heartbeat::week_before(until);
        let (mut looked, mut silent, mut spoke, mut failed) = (0, 0, Vec::new(), Vec::new());
        // A slot's event is logged at or after its slot, so none of the week
        // was logged before it began; older history is never read.
        let since = from.with_timezone(&Utc);
        for event in self
            .store
            .inbound_events_since(conversation, "heartbeat:", since)?
        {
            let slot = event
                .native_id
                .as_deref()
                .and_then(|id| id.strip_prefix("heartbeat:"))
                .filter(|_| event.kind == "heartbeat")
                .and_then(|slot| DateTime::parse_from_rfc3339(slot).ok());
            let Some(slot) = slot.filter(|slot| from <= *slot && slot < until) else {
                continue;
            };
            looked += 1;
            let marker = match fs::read(self.turn_marker(event.seq)) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    serde_json::json!({"state":"not started"})
                }
                marker => serde_json::from_slice(&marker?)?,
            };
            let at = slot.to_rfc3339();
            match (
                marker["state"].as_str(),
                marker["result"]["silent"].as_bool(),
            ) {
                (Some("completed"), Some(true)) => silent += 1,
                (Some("completed"), Some(false)) => {
                    spoke.push(serde_json::json!({
                    "heartbeat_seq":event.seq,"slot":at,"reply_seq":marker["result"]["reply_seq"]}))
                }
                // A delivery that failed or is unknown names its reply,
                // which may have reached the owner.
                _ => failed.push(serde_json::json!({
                    "heartbeat_seq":event.seq,"slot":at,"state":marker["state"],"reply_seq":marker["reply_seq"]})),
            }
        }
        Ok((!spoke.is_empty() || !failed.is_empty()).then(|| {
            serde_json::json!({"digest":{"from":from.to_rfc3339(),"until":until.to_rfc3339(),
                "heartbeats":looked,"silent":silent,"spoke":spoke,"failed":failed}})
        }))
    }

    /// Run the heartbeat's read-only turn for one slot of `name` (a
    /// heartbeat or the digest) while `owed` still says it is due, logging
    /// its input with `body` the first time; a None body starts no turn.
    async fn look<D: Deliver>(
        &self,
        identity: ConversationIdentity,
        native_id: String,
        owed: impl Fn() -> bool,
        body: impl FnOnce(&str) -> Result<Option<serde_json::Value>>,
        deliver: &mut D,
    ) -> Result<Option<TurnResult>> {
        let _lease = conversation_lease(&self.paths, &identity.id)?
            .context("another turn owns this conversation")?;
        // A slot handed over while owed can wait behind a long turn; once the
        // next slot or quiet hours begin, it is no longer owed and never runs.
        if !owed() {
            return Ok(None);
        }
        let standing = self.register(&identity)?;
        let input = match self.store.inbound_event(&identity.id, &native_id)? {
            Some(event) if event.kind == "heartbeat" && event.author == self.config.owner => event,
            Some(_) => bail!("another event already holds the id {native_id}"),
            // A look runs once wherever it ran: a default heartbeat's chat
            // follows the owner, while its slot stays owed in the next chat.
            None if self.store.look_logged(&native_id)? => return Ok(None),
            None => {
                let Some(body) = body(&identity.id)? else {
                    return Ok(None);
                };
                self.store.append_inbound(&InboundEvent {
                    conversation: identity.id.clone(),
                    native_id,
                    author: self.config.owner.clone(),
                    body: body.to_string(),
                    kind: "heartbeat".into(),
                    ..Default::default()
                })?
            }
        };
        self.answer(&input, &standing, RunCapabilities::Heartbeat, deliver)
            .await
    }

    /// The model turn an owner-authored input is due, run at most once. The
    /// caller holds the conversation lease and has appended the input. A
    /// redelivered input already had its turn; its record stays inspectable
    /// and is never run again.
    async fn answer<D: Deliver>(
        &self,
        input: &Event,
        standing: &Standing,
        capabilities: RunCapabilities,
        deliver: &mut D,
    ) -> Result<Option<TurnResult>> {
        let marker_path = self.turn_marker(input.seq);
        if marker_path.try_exists()? {
            return Ok(None);
        }
        atomic_json(
            &marker_path,
            &serde_json::json!({"input_seq":input.seq,"state":"started","at":Utc::now().to_rfc3339()}),
        )?;
        let execution = self
            .execute(
                input,
                input.seq,
                capabilities,
                standing,
                &marker_path,
                deliver,
            )
            .await;
        if execution.is_err() {
            let previous: serde_json::Value = serde_json::from_slice(&fs::read(&marker_path)?)?;
            if previous["state"] == "started" {
                atomic_json(
                    &marker_path,
                    &serde_json::json!({"input_seq":input.seq,"state":"failed","category":"execution",
                    "diagnostic":"turn failed before completion; inspect recorded attempts and delivery state","at":Utc::now().to_rfc3339()}),
                )?;
            }
        }
        execution.map(Some)
    }

    /// Settle the record of the last run in `conversation` (`run_record`). A
    /// run that forgot something holds the text in its native sessions: they
    /// go, with their rows, so the next turn starts fresh. Any other run's
    /// sessions are kept where a forget finds them. This runs after each run
    /// and before each turn, so a turn after a failure or a restart finishes
    /// it. Whether the last run in `conversation` forgot.
    fn settle_sessions(&self, conversation: &str) -> Result<bool> {
        let record = run_record(&self.paths, conversation);
        let Some((forgot, sessions)) = read_run_record(&record)? else {
            return Ok(false);
        };
        for (agent, handle) in &sessions {
            if forgot {
                crate::forget::remove_native_session(handle)?;
                self.store.clear_handle(conversation, handle)?;
            } else if let Some(agent) = agent {
                self.store.keep_session(conversation, agent, handle)?;
            }
        }
        fs::remove_file(record)?;
        Ok(forgot)
    }

    /// What an input's turn marker records: `started` (running, or stopped
    /// mid-turn and never rerun), `completed`, `failed` or `delivery_unknown`.
    /// None before its turn began.
    pub fn turn_state(&self, input_seq: i64) -> Result<Option<String>> {
        turn_state(&self.paths, input_seq)
    }

    fn turn_marker(&self, input_seq: i64) -> PathBuf {
        turn_marker(&self.paths, input_seq)
    }

    /// Send one message a turn queued for another conversation
    /// (`messages.send`). Only a never-attempted entry is sent, so a second
    /// dispatch does nothing and a refused or unknown send is never retried.
    /// One queued too long ago is recorded as not sent instead: what the owner
    /// asked for then may be wrong to say now.
    pub async fn deliver_queued<D: Deliver>(&self, outbox_id: i64, deliver: &mut D) -> Result<()> {
        let entry = self.store.outbox_entry(outbox_id)?;
        if entry.state != DeliveryState::Queued || entry.attempts > 0 {
            return Ok(());
        }
        if send_is_stale(&entry.event.created_at) {
            self.store.withdraw_send(entry.id, STALE_SEND)?;
            return Ok(());
        }
        let observation = self.send(&entry, deliver).await?;
        self.store.finish_send(entry.id, observation)
    }

    /// Record the conversation and return its standing context. The kind only
    /// widens, so a conversation once seen as a group keeps its group policy.
    fn register(&self, identity: &ConversationIdentity) -> Result<Standing> {
        let kind = match self.store.conversation(&identity.id) {
            Ok(existing) if existing.kind == "group" => "group",
            _ => identity.kind.as_str(),
        };
        let standing = self.standing(kind)?;
        self.store.create_conversation(
            &identity.id,
            &identity.channel,
            &identity.account,
            &identity.native_id,
            &standing.policy,
        )?;
        self.store.set_conversation_shape(
            &identity.id,
            &identity.kind,
            identity.title.as_deref(),
        )?;
        if let Some(members) = &identity.members {
            crate::people::Log::open(&self.paths.database())?
                .remember_members(&identity.id, members)?;
        }
        Ok(standing)
    }

    /// The standing context of a conversation the host has already recorded,
    /// as it is now; its shape stays as the channel last reported it.
    fn restanding(&self, conversation: &str) -> Result<Standing> {
        let existing = self.store.conversation(conversation)?;
        let standing = self.standing(&existing.kind)?;
        self.store.create_conversation(
            conversation,
            &existing.channel,
            &existing.account,
            &existing.native_id,
            &standing.policy,
        )?;
        Ok(standing)
    }

    /// Resumed sessions receive only what changed, so everything a fresh session
    /// is told once must be covered by its policy: configuration, manual, host
    /// build, the audience kind and the profiles. The profiles are read once here
    /// and the fresh context sends exactly what was hashed. A change starts a
    /// fresh session with the full context.
    fn standing(&self, kind: &str) -> Result<Standing> {
        let profiles = profiles(&self.paths, &self.config.owner);
        let mut digest = Sha256::new();
        digest.update(self.config.policy_hash(MANUAL)?);
        digest.update(kind);
        digest.update(serde_json::to_vec(&profiles)?);
        Ok(Standing {
            policy: format!("{:x}", digest.finalize()),
            profiles,
        })
    }

    /// Integrate one terminal task under the same lease used by foreground turns.
    /// The worker result is attributed data. Its original owner input supplies
    /// authority; presentation grants allow reads only. Uncertain presentation
    /// is left inspectable and never automatically redelivered.
    pub async fn present_pending<F>(
        &self,
        conversation: &str,
        deliver: F,
    ) -> Result<Option<TurnResult>>
    where
        F: FnMut(&str) -> Result<()>,
    {
        self.present_pending_with(conversation, &mut ConsoleDelivery(deliver))
            .await
    }

    /// `present_pending` for any transport.
    pub async fn present_pending_with<D: Deliver>(
        &self,
        conversation: &str,
        deliver: &mut D,
    ) -> Result<Option<TurnResult>> {
        let Some(_lease) = conversation_lease(&self.paths, conversation)? else {
            return Ok(None);
        };
        // Nothing is presented on the owner's own account.
        if self.on_owners_account(conversation) {
            return Ok(None);
        }
        let tasks = TaskStore::open(&self.paths, &self.config.owner)?;
        let Some(task) = tasks.pending_presentations(conversation, 1)?.pop() else {
            return Ok(None);
        };
        let receipt = tasks.receipt(&task.id)?;
        let standing = self.restanding(conversation)?;
        let body = serde_json::to_string(&serde_json::json!({
            "task_id":task.id,"generation":task.generation,"state":task.state,
            "source_input_seq":task.origin.input_seq,"brief":task.spec.brief,
            "done_when":task.spec.done_when,"stop_requested":task.stop_requested,
            "outcome":receipt.as_ref().map(|r| &r.outcome),"diagnostic":task.diagnostic
        }))?;
        let input = self.store.append_inbound(&InboundEvent {
            conversation: conversation.into(),
            native_id: task.result_key(),
            author: format!("worker:{}", task.spec.agent),
            body,
            kind: "task_result".into(),
            reply_to: Some(task.origin.input_seq.to_string()),
            ..Default::default()
        })?;
        if !tasks.begin_presentation(&task.id, input.seq)? {
            return Ok(None);
        }
        let marker_path = self.turn_marker(input.seq);
        atomic_json(
            &marker_path,
            &serde_json::json!({
                "input_seq":input.seq,"task_id":task.id,"state":"started","at":Utc::now().to_rfc3339()
            }),
        )?;
        let execution = self
            .execute(
                &input,
                task.origin.input_seq,
                RunCapabilities::Presentation,
                &standing,
                &marker_path,
                deliver,
            )
            .await;
        match execution {
            Ok(result) => {
                tasks.finish_presentation(&task.id, input.seq, result.reply_seq)?;
                Ok(Some(result))
            }
            Err(error) => {
                // Preserve an existing unknown-delivery marker. A failed or
                // crashed presentation remains started in tasks for inspection.
                let previous: serde_json::Value = serde_json::from_slice(&fs::read(&marker_path)?)?;
                if previous["state"] == "started" {
                    atomic_json(
                        &marker_path,
                        &serde_json::json!({
                            "input_seq":input.seq,"task_id":task.id,"state":"failed",
                            "diagnostic":"task presentation failed; inspect attempts and outbox"
                        }),
                    )?;
                }
                Err(error)
            }
        }
    }

    async fn execute<D: Deliver>(
        &self,
        input: &Event,
        authority_input_seq: i64,
        capabilities: RunCapabilities,
        standing: &Standing,
        marker_path: &Path,
        deliver: &mut D,
    ) -> Result<TurnResult> {
        let conversation = &input.conversation;
        let policy = standing.policy.as_str();
        // The grant names only this conversation; the rest of its channel
        // account is reachable through the broker's scoped discovery.
        let allowed: Vec<String> = vec![conversation.clone()];
        // Whoever wrote the authorizing input: a task result is presented
        // under its original request.
        let requester = self.store.event(authority_input_seq)?.author;
        let mut failures = Vec::new();
        let mut may_have_acted = false;
        self.settle_sessions(conversation)?;
        // An answer held back because its author wrote again: this turn sees
        // it and answers everything together.
        // Only a message or edit of the same author, within the hour, takes
        // it up; a heartbeat, schedule or another person's turn leaves it.
        let answers_messages = matches!(input.kind.as_str(), "message" | "edit");
        let unsent_path = unsent_draft(&self.paths, conversation);
        let unsent: Option<serde_json::Value> = fs::read(&unsent_path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
            .filter(|draft| {
                answers_messages
                    && draft["author"] == input.author.as_str()
                    && draft["held_at"]
                        .as_str()
                        .and_then(|at| chrono::DateTime::parse_from_rfc3339(at).ok())
                        .is_some_and(|at| Utc::now().signed_duration_since(at).num_minutes() < 60)
            });
        'agents: for agent in &self.config.agents {
            let mut session = self.store.session(conversation, &agent.id, policy)?;
            // An idle or day-old native session is retired; the canonical log
            // rebuilds a fresh one from its recent window and history tools.
            // Each run keeps the sessions it used where a forget finds them;
            // keeping a retiring one covers a session an earlier build never
            // kept.
            let retired = session.take_if(|s| {
                self.config.sessions.retires(
                    s.started_at.as_deref(),
                    s.used_at.as_deref(),
                    chrono::Local::now(),
                )
            });
            if let Some(retired) = retired {
                self.store
                    .keep_session(conversation, &agent.id, &retired.handle)?;
            }
            // A rejected native cache may be rebuilt once, before any effects.
            // The canonical input and transcript are never replayed into storage.
            for attempt in 1..=2 {
                let broker_run = BrokerRun::create_with_capabilities(
                    &self.paths,
                    &requester,
                    authority_input_seq,
                    allowed.clone(),
                    agent.timeout_seconds,
                    &self.broker_executable,
                    capabilities,
                )?;
                let window = context_window(
                    &self.store,
                    conversation,
                    session.as_ref().map(|s| s.cursor),
                )?;
                if !window.resumed {
                    session = None;
                }
                let broker = Broker::open(broker_run.launch.clone())?;
                // Recent attachments, copied where only this run may read them;
                // Codex opens files only with the whole computer open to it.
                let files = readable_copies(
                    &self.store,
                    conversation,
                    &self.paths.runtime.join("attachments"),
                    &broker_run.launch.grant.run_id,
                    agent.family == "anthropic"
                        || matches!(broker_run.launch.grant.access, Access::Computer),
                )?;
                let prompt = turn_prompt(
                    &self.paths,
                    &self.store,
                    &self.config,
                    conversation,
                    &window,
                    &failures,
                    &broker_run.launch.grant,
                    input,
                    &standing.profiles,
                    &files.listed,
                    unsent.as_ref(),
                )?;
                let handle = session.as_ref().map(|s| s.handle.clone());
                // Where a forget in this run would be marked, then the native
                // session it resumes; the harness adds each new one as it
                // arrives.
                let record = run_record(&self.paths, conversation);
                let marker = broker.forgot_marker().display().to_string();
                let resumed = handle.as_ref().map(|resumed| {
                    format!(
                        "{}\n",
                        serde_json::json!({"agent":agent.id,"session":resumed})
                    )
                });
                fs::write(
                    &record,
                    format!("{marker}\n{}", resumed.unwrap_or_default()),
                )?;
                let request = HarnessRequest {
                    protocol_version: 1,
                    conversation: conversation.clone(),
                    prompt,
                    session: handle,
                    session_log: Some(record),
                    instructions: Some(MANUAL.to_owned()),
                    mcp: Some(McpServer {
                        read_tools: broker.connector_tools(),
                        write_tools: broker.connector_write_tools(),
                        ..broker_run.server.clone()
                    }),
                    working_directory: Some(
                        harness_directory(&self.paths, broker_run.launch.grant.access)
                            .to_string_lossy()
                            .into(),
                    ),
                    model: agent.model.clone(),
                    effort: agent.effort.clone(),
                    browser: self.config.browser,
                    access: broker_run.launch.grant.access,
                    readable_dir: files
                        .dir
                        .is_dir()
                        .then(|| files.dir.to_string_lossy().into()),
                };
                // Someone wrote in a direct chat, so the bubble shows while the
                // turn composes. A group turn often ends silent, and so may a
                // schedule or heartbeat: those show none.
                let typing = matches!(input.kind.as_str(), "message" | "edit")
                    && self.store.conversation(conversation)?.kind == "dm";
                let run = harness::run(agent, &request);
                tokio::pin!(run);
                let outcome = loop {
                    if typing {
                        deliver.typing(true).await;
                    }
                    tokio::select! {
                        outcome = &mut run => break outcome,
                        _ = tokio::time::sleep(TYPING_REFRESH) => {}
                    }
                };
                if typing {
                    deliver.typing(false).await;
                }
                drop(files);
                // Before anything else can fail.
                let forgot = self.settle_sessions(conversation)?;
                let mut outcome = outcome?;
                outcome.activity = outcome.activity.max(broker.activity()?);
                may_have_acted |= outcome.activity.may_have_effects();
                let observations = broker.observations()?;
                record_run(
                    &self.paths,
                    input.seq,
                    &agent.id,
                    attempt,
                    policy,
                    &outcome,
                    &observations,
                )?;
                if let Some(failure) = &outcome.failure {
                    failures.push(serde_json::json!({"agent":agent.id,"attempt":attempt,"failure":failure,"activity":outcome.activity,
                    "telemetry_complete":outcome.telemetry_complete,"broker":observations}));
                    if failure.category == FailureCategory::SessionInvalid
                        && attempt == 1
                        && outcome.telemetry_complete
                        && outcome.answer.is_none()
                        && !outcome.activity.may_have_effects()
                    {
                        if let Some(handle) = &request.session {
                            self.store
                                .clear_session(conversation, &agent.id, policy, handle)?;
                            session = None;
                            continue;
                        }
                    }
                    // A continuation sees receipts first. This is not a replay of tool
                    // calls; each new effect still passes through the broker's CAS.
                    let available_elsewhere = matches!(
                        failure.category,
                        FailureCategory::Quota
                            | FailureCategory::Auth
                            | FailureCategory::Unavailable
                    );
                    // A continuation sees memory receipts and their revision
                    // check refuses a repeat. Nothing refuses a repeated
                    // connector write, and an adapter's error does not prove
                    // nothing changed, so any connector write (it has a tier)
                    // ends the turn like a write whose outcome is unknown.
                    let pending_write = observations["writes"].as_array().is_some_and(|writes| {
                        writes.iter().any(|write| {
                            write["status"] == "pending" || write.get("tier").is_some()
                        })
                    });
                    may_have_acted |= pending_write;
                    // A fallback cannot infer whether this turn needed an
                    // account that only the failing harness could read.
                    if available_elsewhere
                        && outcome.telemetry_complete
                        && outcome.answer.is_none()
                        && outcome.activity != Activity::Unmediated
                        && !pending_write
                    {
                        continue 'agents;
                    }
                    return Err(self
                        .fail_turn(
                            input,
                            capabilities,
                            marker_path,
                            &failures,
                            may_have_acted,
                            deliver,
                            "harness failed; recorded cause in private run history",
                        )
                        .await);
                }
                let answer = outcome
                    .answer
                    .as_deref()
                    .context("harness completed without an answer or silence marker")?;
                // Most heartbeats end in nothing, so one must choose silence
                // through NO_REPLY; a blank answer is a failed turn.
                if capabilities == RunCapabilities::Heartbeat && answer.trim().is_empty() {
                    failures.push(serde_json::json!({"agent":agent.id,"attempt":attempt,
                    "failure":"empty answer; a heartbeat is silent only through NO_REPLY"}));
                    return Err(self
                        .fail_turn(
                            input,
                            capabilities,
                            marker_path,
                            &failures,
                            may_have_acted,
                            deliver,
                            "a heartbeat ended without a reply or NO_REPLY",
                        )
                        .await);
                }
                // The author wrote again while this turn ran: hold the answer
                // and let the turn for what they added answer everything. Only
                // in a direct chat: in a group the next message may be for
                // someone else, and a silent turn would lose the answer.
                let held = !chooses_silence(answer)
                    && capabilities == RunCapabilities::Conversation
                    && answers_messages
                    && self.store.conversation(conversation)?.kind == "dm"
                    && self.followups.as_ref().is_some_and(|pending| {
                        pending(
                            &input.author,
                            input.native_id.as_deref().unwrap_or_default(),
                            &input.body,
                        )
                    });
                if held {
                    atomic_json(
                        &unsent_path,
                        &serde_json::json!({"text":answer,"for_input_seq":input.seq,
                            "author":input.author,"held_at":Utc::now().to_rfc3339()}),
                    )?;
                } else if unsent.is_some() {
                    let _ = fs::remove_file(&unsent_path);
                }
                let silent = held || chooses_silence(answer);
                let reply_seq = if silent {
                    None
                } else {
                    let entry = self.store.enqueue_reply(
                        conversation,
                        &format!("reply:{}", input.seq),
                        answer,
                        vec![],
                        Some(input.seq.to_string()),
                    )?;
                    let observation = self.send(&entry, deliver).await?;
                    match observation {
                        DeliveryObservation::Sent { .. } => {
                            if self.store.finish_send(entry.id, observation).is_err() {
                                // The store already holds the entry as unknown.
                                atomic_json(
                                    marker_path,
                                    &serde_json::json!({"input_seq":input.seq,"state":"delivery_unknown",
                                    "agent":agent.id,"attempt":attempt,"outbox_id":entry.id,
                                    "reply_seq":entry.event.seq,"at":Utc::now().to_rfc3339()}),
                                )?;
                                bail!("delivery receipt could not be recorded; delivery remains unknown");
                            }
                        }
                        DeliveryObservation::Unknown { .. } => {
                            self.store.finish_send(entry.id, observation)?;
                            atomic_json(
                                marker_path,
                                &serde_json::json!({"input_seq":input.seq,"state":"delivery_unknown",
                                "agent":agent.id,"attempt":attempt,"outbox_id":entry.id,
                                "reply_seq":entry.event.seq,"at":Utc::now().to_rfc3339()}),
                            )?;
                            bail!("delivery is unknown; inspect history instead of repeating the message");
                        }
                        DeliveryObservation::ConfirmedNotSent { .. } => {
                            self.store.finish_send(entry.id, observation)?;
                            atomic_json(
                                marker_path,
                                &serde_json::json!({"input_seq":input.seq,"state":"failed","category":"delivery",
                                "agent":agent.id,"attempt":attempt,"outbox_id":entry.id,
                                "reply_seq":entry.event.seq,"at":Utc::now().to_rfc3339()}),
                            )?;
                            bail!("the channel refused the reply; it remains queued in the outbox");
                        }
                    }
                    Some(entry.event.seq)
                };
                if let Some(handle) = outcome.session.as_ref().filter(|_| !forgot) {
                    // A turn that waited may answer an input older than what a
                    // turn meanwhile saw, and was itself shown events after it.
                    let shown = window.events.last().map_or(0, |event| event.seq);
                    let saved = self
                        .store
                        .session(conversation, &agent.id, policy)?
                        .map_or(0, |session| session.cursor);
                    self.store.save_session(
                        conversation,
                        &agent.id,
                        policy,
                        handle,
                        reply_seq.unwrap_or(input.seq).max(shown).max(saved),
                    )?;
                }
                let result = TurnResult {
                    input_seq: input.seq,
                    reply_seq,
                    agent: agent.id.clone(),
                    silent,
                };
                let mut completed = serde_json::json!({"state":"completed","result":result});
                if held {
                    completed["held_for_followups"] = true.into();
                }
                atomic_json(marker_path, &completed)?;
                return Ok(result);
            }
        }
        Err(self
            .fail_turn(
                input,
                capabilities,
                marker_path,
                &failures,
                may_have_acted,
                deliver,
                "no configured harness is available; inspect oppa doctor and private run records",
            )
            .await)
    }

    /// Record a failed turn. A person waiting in a direct conversation gets the
    /// one fixed notice when no attempt may have acted, so a failure is never
    /// hidden by silence. In a group, silence is ordinary and nothing is sent.
    /// The notice asks for the message again, so only a message gets it: a
    /// schedule was sent by nobody, and its failed slot shows in `oppa schedules`.
    async fn fail_turn<D: Deliver>(
        &self,
        input: &Event,
        capabilities: RunCapabilities,
        marker_path: &Path,
        failures: &[serde_json::Value],
        may_have_acted: bool,
        deliver: &mut D,
        reason: &str,
    ) -> anyhow::Error {
        if let Err(error) = atomic_json(
            marker_path,
            &serde_json::json!({"input_seq":input.seq,"state":"failed","attempts":failures}),
        ) {
            return error;
        }
        if capabilities != RunCapabilities::Conversation
            || !matches!(input.kind.as_str(), "message" | "edit")
            || may_have_acted
        {
            return anyhow!("{reason}");
        }
        let notice = async {
            if self.store.conversation(&input.conversation)?.kind != "dm" {
                return Ok(());
            }
            let entry = self.store.enqueue_reply(
                &input.conversation,
                &format!("notice:{}", input.seq),
                UNREACHABLE_NOTICE,
                vec![],
                Some(input.seq.to_string()),
            )?;
            let observation = self.send(&entry, deliver).await?;
            self.store.finish_send(entry.id, observation)
        };
        match notice.await {
            Ok(()) => anyhow!("{reason}"),
            Err(error) => anyhow!("{reason}; the unreachable notice failed: {error}"),
        }
    }

    /// Claim a queued entry and hand it to the transport. The caller records the
    /// observation; a transport error is an unknown delivery, never a retry.
    async fn send<D: Deliver>(
        &self,
        entry: &OutboxEntry,
        deliver: &mut D,
    ) -> Result<DeliveryObservation> {
        if !self.store.claim_send(entry.id)? {
            bail!("reply already claimed; inspect its delivery state");
        }
        Ok(match deliver.deliver(entry).await {
            Ok(observation) => observation,
            Err(error) => DeliveryObservation::Unknown {
                evidence: format!("transport failed during send: {error}")
                    .chars()
                    .take(1024)
                    .collect(),
            },
        })
    }
}

/// Where a harness runs. A `computer` run can write files there, and a run
/// below it may Read its working directory, so the two never share one. Runs
/// below `computer` use a directory that no build ever let anything write to;
/// `harness`, which every run used before, now serves `computer` alone.
pub(crate) fn harness_directory(paths: &Paths, access: Access) -> PathBuf {
    let name = if matches!(access, Access::Computer) {
        "harness"
    } else {
        "harness-restricted"
    };
    paths.runtime.join(name)
}

/// What a fresh session is told once, and the policy key that covers it.
struct Standing {
    policy: String,
    profiles: Vec<serde_json::Value>,
}

/// Why a queued message was never sent: serve did not deliver it in time.
pub const STALE_SEND: &str =
    "Not sent: oppa serve could not deliver it within ten minutes of the request.";

/// Whether a message queued at `queued_at` is too old to send: over ten
/// minutes, or a time that cannot be read.
pub fn send_is_stale(queued_at: &str) -> bool {
    DateTime::parse_from_rfc3339(queued_at).map_or(true, |at| {
        Utc::now().signed_duration_since(at) > chrono::Duration::minutes(10)
    })
}

/// Native sessions of runs that forgot something which could not be deleted
/// yet (`settle_sessions`): they still hold the text, and turns in their
/// conversation fail until they go.
pub fn undeleted_forgetting_sessions(paths: &Paths) -> Result<Vec<String>> {
    let mut left = Vec::new();
    for record in fs::read_dir(paths.runtime.join("runs"))? {
        let record = record?.path();
        let name = record.file_name().unwrap_or_default().to_string_lossy();
        if name.starts_with("sessions-") && name.ends_with(".txt") {
            if let Some((true, sessions)) = read_run_record(&record)? {
                left.extend(sessions.into_iter().map(|(_, handle)| handle));
            }
        }
    }
    Ok(left)
}

/// The record of the run under way in `conversation` (`execute`): the marker
/// a forget in it would leave; then a JSON line `{"agent":..,"session":..}` for the native
/// session it resumed and for each one it reports, as it arrives. A record
/// left by an earlier build holds bare session ids, with no harness.
/// Where `conversation`'s held answer waits for the turn that answers its
/// author's follow-ups.
fn unsent_draft(paths: &Paths, conversation: &str) -> PathBuf {
    let key = format!("{:x}", Sha256::digest(conversation.as_bytes()));
    paths
        .runtime
        .join("runs")
        .join(format!("unsent-{key}.json"))
}

fn run_record(paths: &Paths, conversation: &str) -> PathBuf {
    let key = format!("{:x}", Sha256::digest(conversation.as_bytes()));
    paths
        .runtime
        .join("runs")
        .join(format!("sessions-{key}.txt"))
}

/// Whether the run of `record` forgot something, and each native session it
/// names, with its harness when the record says; None without a record.
fn read_run_record(record: &Path) -> Result<Option<(bool, Vec<(Option<String>, String)>)>> {
    let lines = match fs::read_to_string(record) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        lines => lines?,
    };
    let mut lines = lines.lines();
    let forgot = lines
        .next()
        .is_some_and(|marker| Path::new(marker).exists());
    let mut sessions = Vec::new();
    for line in lines.filter(|line| !line.is_empty()) {
        if !line.starts_with('{') {
            sessions.push((None, line.to_owned()));
            continue;
        }
        // A line cut short when serve stopped mid-write names no whole session.
        let named: serde_json::Value = serde_json::from_str(line).unwrap_or_default();
        if let Some(session) = named["session"].as_str() {
            let agent = named["agent"].as_str().map(str::to_owned);
            sessions.push((agent, session.to_owned()));
        }
    }
    Ok(Some((forgot, sessions)))
}

/// The native sessions, each with its harness, that the last run in
/// `conversation` named and that are not settled yet: those of a run under
/// way, or of one serve did not see end, until the conversation's next turn.
/// A forget reads them here until then.
pub(crate) fn unsettled_sessions(
    paths: &Paths,
    conversation: &str,
) -> Result<Vec<(String, String)>> {
    let sessions = read_run_record(&run_record(paths, conversation))?
        .unwrap_or_default()
        .1;
    let named = sessions.into_iter();
    Ok(named
        .filter_map(|(agent, handle)| Some((agent?, handle)))
        .collect())
}

fn turn_marker(paths: &Paths, input_seq: i64) -> PathBuf {
    paths
        .runtime
        .join("runs")
        .join(format!("turn-{input_seq}.json"))
}

/// What an input's turn marker records, or None before its turn began.
pub fn turn_state(paths: &Paths, input_seq: i64) -> Result<Option<String>> {
    let marker = turn_marker(paths, input_seq);
    if !marker.try_exists()? {
        return Ok(None);
    }
    let marker: serde_json::Value = serde_json::from_slice(&fs::read(marker)?)?;
    Ok(marker["state"].as_str().map(str::to_owned))
}

pub fn read_all_events(store: &Store, conversation: &str, after: i64) -> Result<Vec<Event>> {
    let mut all = Vec::new();
    let mut cursor = after;
    loop {
        let batch = store.transcript(conversation, cursor, 500)?;
        if batch.is_empty() {
            break;
        }
        cursor = batch.last().unwrap().seq;
        all.extend(batch);
    }
    Ok(all)
}

/// Whether a turn's answer chooses silence: blank, or NO_REPLY alone on its
/// last line. Reasoning a model writes before NO_REPLY is never for anyone
/// in the chat to read.
fn chooses_silence(answer: &str) -> bool {
    answer
        .trim()
        .lines()
        .last()
        .is_none_or(|line| line.trim() == "NO_REPLY")
}

/// The lease of `conversation`, or None while another turn holds it. A turn
/// waits out a forget or a preview's check of the leases, and a forget that
/// starts after this finds the lease, recorded conversation or not.
fn conversation_lease(paths: &Paths, conversation: &str) -> Result<Option<File>> {
    let forgetting = forget_lock(paths)?;
    forgetting.lock_shared()?;
    try_lease(&lease_path(paths, conversation))
}

/// Held whole by a forget, or a preview checking the leases (`forget::turns_running`).
pub(crate) fn forget_lock(paths: &Paths) -> Result<File> {
    let path = paths.runtime.join("locks").join("forget.lock");
    Ok(OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)?)
}

/// Where the lease of `conversation` is: a 64-digit name in `locks`.
pub(crate) fn lease_path(paths: &Paths, conversation: &str) -> PathBuf {
    let key = format!("{:x}", Sha256::digest(conversation.as_bytes()));
    paths.runtime.join("locks").join(format!("{key}.lock"))
}

pub(crate) fn try_lease(path: &Path) -> Result<Option<File>> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    // A child forked by another thread holds a copy of this descriptor until it
    // execs, so a just-released lease can look held for a few milliseconds.
    for attempt in 0..5 {
        match file.try_lock() {
            Ok(()) => return Ok(Some(file)),
            Err(std::fs::TryLockError::WouldBlock) if attempt < 4 => {
                std::thread::sleep(std::time::Duration::from_millis(20))
            }
            Err(std::fs::TryLockError::WouldBlock) => return Ok(None),
            Err(error) => return Err(error.into()),
        }
    }
    Ok(None)
}

fn record_run(
    paths: &Paths,
    seq: i64,
    agent: &str,
    attempt: usize,
    policy: &str,
    outcome: &RunOutcome,
    broker: &serde_json::Value,
) -> Result<()> {
    let key = format!("{:x}", Sha256::digest(agent.as_bytes()));
    let path = paths
        .runtime
        .join("runs")
        .join(format!("{seq}-{key}-{attempt}.json"));
    anyhow::ensure!(!path.try_exists()?, "attempt record already exists");
    atomic_json(
        &path,
        &serde_json::json!({
        "input_seq":seq,"agent":agent,"attempt":attempt,"policy_hash":policy,"core_version":env!("CARGO_PKG_VERSION"),
        "ended_at":Utc::now().to_rfc3339(),"outcome":outcome,"broker":broker}),
    )
}

pub fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let temporary = path.with_extension(format!("{}.tmp", std::process::id()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary)?;
    let result = (|| -> Result<()> {
        serde_json::to_writer_pretty(&mut file, value)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        File::open(path.parent().context("missing parent")?)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

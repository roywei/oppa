//! Canonical conversation log and transactional outbox.
//!
//! A transport may send only an entry it successfully claimed. Unknown delivery
//! requires adapter evidence; it is never changed to queued by a timeout.
use anyhow::{bail, ensure, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension, Row, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{path::Path, time::Duration};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attachment {
    pub path: String,
    #[serde(default)]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InboundEvent {
    pub conversation: String,
    pub native_id: String,
    pub author: String,
    pub body: String,
    #[serde(default = "message_kind")]
    pub kind: String,
    #[serde(default)]
    pub attachments: Vec<Attachment>,
    #[serde(default)]
    pub reply_to: Option<String>,
    #[serde(default)]
    pub spoken: bool,
    #[serde(default)]
    pub audio_ref: Option<String>,
    #[serde(default)]
    pub device: Option<String>,
}
fn message_kind() -> String {
    "message".into()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    In,
    Out,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryState {
    Queued,
    Sending,
    Unknown,
    Sent,
}
impl DeliveryState {
    fn parse(value: &str) -> rusqlite::Result<Self> {
        match value {
            "queued" => Ok(Self::Queued),
            "sending" => Ok(Self::Sending),
            "unknown" => Ok(Self::Unknown),
            "sent" => Ok(Self::Sent),
            _ => Err(rusqlite::Error::InvalidQuery),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    pub seq: i64,
    pub conversation: String,
    pub created_at: String,
    pub direction: Direction,
    pub author: String,
    pub kind: String,
    pub body: String,
    pub attachments: Vec<Attachment>,
    pub reply_to: Option<String>,
    pub native_id: Option<String>,
    pub spoken: bool,
    pub audio_ref: Option<String>,
    pub device: Option<String>,
    /// Outbound text is an intent until this is `Sent`.
    pub delivery: Option<DeliveryState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Conversation {
    pub id: String,
    pub channel: String,
    pub account: String,
    pub native_id: String,
    pub policy_hash: String,
    /// `dm` or `group`, as the channel adapter reported it.
    pub kind: String,
    pub title: Option<String>,
}

/// Discovery listing for a model: identity plus the newest event, no bodies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConversationSummary {
    pub id: String,
    pub channel: String,
    pub account: String,
    pub native_id: String,
    pub kind: String,
    pub title: Option<String>,
    pub last_event_seq: Option<i64>,
    pub last_event_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    pub handle: String,
    pub cursor: i64,
    /// When this native handle was first saved and last used (RFC 3339 UTC).
    /// Sessions migrated from version 2 have neither and are retired.
    pub started_at: Option<String>,
    pub used_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboxEntry {
    pub id: i64,
    pub destination: Conversation,
    pub event: Event,
    pub idempotency_key: String,
    pub state: DeliveryState,
    pub attempts: u32,
    pub native_id: Option<String>,
    pub evidence: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum DeliveryObservation {
    Sent { native_id: String },
    Unknown { evidence: String },
    ConfirmedNotSent { evidence: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchHit {
    pub event: Event,
    pub context: Vec<Event>,
}

pub struct Store {
    connection: Connection,
}

/// Author of the assistant's own outbound messages in the canonical log.
pub const ASSISTANT: &str = "assistant";

/// Inbound kinds that carry the authority of their owner author: a message,
/// its edit, and a schedule firing the owner's standing instruction from
/// private config.
pub const OWNER_INPUT_KINDS: [&str; 3] = ["message", "edit", "schedule"];

/// Version 2 adds conversation kind and title; 3 adds session start and use times.
pub const SCHEMA_VERSION: i64 = 3;

const EVENT_COLUMNS: &str = "e.seq,e.conversation,e.created_at,e.direction,e.author,e.kind,e.body,e.attachments,e.reply_to,e.native_id,e.spoken,e.audio_ref,e.device,o.state";

fn inbound_since() -> String {
    format!("SELECT {EVENT_COLUMNS} FROM events e LEFT JOIN outbox o ON o.event_seq=e.seq WHERE e.conversation=?1 AND e.created_at>=?3 AND e.direction='in' AND substr(e.native_id,1,length(?2))=?2 ORDER BY e.created_at,e.seq")
}

/// Reads by time, such as the weekly digest's, walk only the rows in range.
/// A database of this version from before the index gains it on open.
const EVENTS_CREATED: &str =
    "CREATE INDEX IF NOT EXISTS events_created ON events(conversation,created_at);";

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let connection = Connection::open(path).context("open state database")?;
        connection.busy_timeout(Duration::from_secs(5))?;
        let version: i64 = connection.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        ensure!(
            (0..=SCHEMA_VERSION).contains(&version),
            "unsupported state database version"
        );
        connection.execute_batch(
            "PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;",
        )?;
        if version == SCHEMA_VERSION {
            connection.execute_batch(EVENTS_CREATED)?;
            return Ok(Self { connection });
        }
        if version >= 1 {
            // Recheck under the write lock: another process may have migrated.
            let tx = Transaction::new_unchecked(&connection, TransactionBehavior::Immediate)?;
            let current: i64 = tx.query_row("PRAGMA user_version", [], |r| r.get(0))?;
            if current == 1 {
                // Version 2 records whether a conversation is shared and its title.
                tx.execute_batch(
                    "ALTER TABLE conversations ADD COLUMN kind TEXT NOT NULL DEFAULT 'dm' CHECK(kind IN ('dm','group'));
                    ALTER TABLE conversations ADD COLUMN title TEXT;",
                )?;
            }
            if current <= 2 {
                // Version 3 records when each native session started and was last used.
                tx.execute_batch(
                    "ALTER TABLE sessions ADD COLUMN started_at TEXT;
                    ALTER TABLE sessions ADD COLUMN used_at TEXT;
                    PRAGMA user_version=3;",
                )?;
            }
            tx.execute_batch(EVENTS_CREATED)?;
            tx.commit()?;
            return Ok(Self { connection });
        }
        connection.execute_batch(
            "BEGIN IMMEDIATE;
            CREATE TABLE IF NOT EXISTS conversations (
                id TEXT PRIMARY KEY,
                channel TEXT NOT NULL,
                account TEXT NOT NULL,
                native_id TEXT NOT NULL,
                policy_hash TEXT NOT NULL,
                kind TEXT NOT NULL DEFAULT 'dm' CHECK(kind IN ('dm','group')),
                title TEXT,
                UNIQUE(channel,account,native_id)
            );
            CREATE TABLE IF NOT EXISTS events (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation TEXT NOT NULL REFERENCES conversations(id),
                created_at TEXT NOT NULL,
                direction TEXT NOT NULL CHECK(direction IN ('in','out')),
                author TEXT NOT NULL,
                kind TEXT NOT NULL,
                body TEXT NOT NULL,
                attachments TEXT NOT NULL,
                reply_to TEXT,
                native_id TEXT,
                spoken INTEGER NOT NULL DEFAULT 0,
                audio_ref TEXT,
                device TEXT,
                payload_digest TEXT NOT NULL,
                UNIQUE(conversation,direction,native_id)
            );
            CREATE INDEX IF NOT EXISTS events_conversation ON events(conversation,seq);
            CREATE INDEX IF NOT EXISTS events_created ON events(conversation,created_at);
            CREATE TABLE IF NOT EXISTS sessions (
                conversation TEXT NOT NULL REFERENCES conversations(id),
                agent TEXT NOT NULL,
                policy_hash TEXT NOT NULL,
                handle TEXT NOT NULL,
                cursor INTEGER NOT NULL CHECK(cursor>=0),
                started_at TEXT,
                used_at TEXT,
                PRIMARY KEY(conversation,agent,policy_hash)
            );
            CREATE TABLE IF NOT EXISTS outbox (
                id INTEGER PRIMARY KEY,
                event_seq INTEGER NOT NULL UNIQUE REFERENCES events(seq),
                idempotency_key TEXT NOT NULL UNIQUE,
                state TEXT NOT NULL CHECK(state IN ('queued','sending','unknown','sent')),
                attempts INTEGER NOT NULL DEFAULT 0,
                native_id TEXT,
                evidence TEXT,
                updated_at TEXT NOT NULL
            );
            CREATE VIRTUAL TABLE IF NOT EXISTS events_fts USING fts5(body,content='events',content_rowid='seq',tokenize='trigram');
            CREATE TRIGGER IF NOT EXISTS events_fts_insert AFTER INSERT ON events BEGIN
                INSERT INTO events_fts(rowid,body) VALUES(new.seq,new.body);
            END;
            CREATE TRIGGER IF NOT EXISTS events_fts_delete AFTER DELETE ON events BEGIN
                INSERT INTO events_fts(events_fts,rowid,body) VALUES('delete',old.seq,old.body);
            END;
            CREATE TRIGGER IF NOT EXISTS events_fts_update AFTER UPDATE OF body ON events BEGIN
                INSERT INTO events_fts(events_fts,rowid,body) VALUES('delete',old.seq,old.body);
                INSERT INTO events_fts(rowid,body) VALUES(new.seq,new.body);
            END;
            PRAGMA user_version=3;
            COMMIT;",
        )?;
        Ok(Self { connection })
    }

    /// Reusing an ID cannot silently redirect a conversation to another account.
    /// A changed audience/policy hash makes prior sessions inaccessible.
    pub fn create_conversation(
        &self,
        id: &str,
        channel: &str,
        account: &str,
        native_id: &str,
        policy_hash: &str,
    ) -> Result<()> {
        for value in [id, channel, account, native_id, policy_hash] {
            ensure!(
                !value.trim().is_empty(),
                "conversation identity fields must be nonempty"
            );
        }
        let tx = Transaction::new_unchecked(&self.connection, TransactionBehavior::Immediate)?;
        let existing: Option<(String, String, String)> = tx
            .query_row(
                "SELECT channel,account,native_id FROM conversations WHERE id=?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        if let Some(existing) = existing {
            ensure!(
                existing == (channel.into(), account.into(), native_id.into()),
                "conversation identity cannot be rebound"
            );
            tx.execute(
                "UPDATE conversations SET policy_hash=?2 WHERE id=?1",
                params![id, policy_hash],
            )?;
        } else {
            tx.execute("INSERT INTO conversations(id,channel,account,native_id,policy_hash) VALUES(?1,?2,?3,?4,?5)", params![id,channel,account,native_id,policy_hash])?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Record the adapter's latest view of a conversation's shape. The kind
    /// only widens: a conversation once seen as a group stays a group.
    pub fn set_conversation_shape(&self, id: &str, kind: &str, title: Option<&str>) -> Result<()> {
        ensure!(
            kind == "dm" || kind == "group",
            "conversation kind must be dm or group"
        );
        let title = title.map(str::trim).filter(|title| !title.is_empty());
        let changed = self.connection.execute(
            "UPDATE conversations SET kind=CASE WHEN kind='group' THEN 'group' ELSE ?2 END,title=?3 WHERE id=?1",
            params![id, kind, title],
        )?;
        ensure!(changed == 1, "unknown conversation");
        Ok(())
    }

    /// Host inventory used to grant the local owner access to existing console chats.
    pub fn conversations(&self) -> Result<Vec<Conversation>> {
        let mut statement = self.connection.prepare(
            "SELECT id,channel,account,native_id,policy_hash,kind,title FROM conversations ORDER BY id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(Conversation {
                id: row.get(0)?,
                channel: row.get(1)?,
                account: row.get(2)?,
                native_id: row.get(3)?,
                policy_hash: row.get(4)?,
                kind: row.get(5)?,
                title: row.get(6)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Conversations of one channel account, ordered by id after `after_id`.
    /// `query` is a case-insensitive substring match on id, native id or title.
    pub fn conversations_in_scope(
        &self,
        channel: &str,
        account: &str,
        after_id: &str,
        query: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ConversationSummary>> {
        ensure!(
            (1..=100).contains(&limit),
            "invalid conversation page limit"
        );
        let pattern = query.map(|query| {
            format!(
                "%{}%",
                query
                    .replace('\\', "\\\\")
                    .replace('%', "\\%")
                    .replace('_', "\\_")
            )
        });
        let mut statement = self.connection.prepare(
            "SELECT c.id,c.channel,c.account,c.native_id,c.kind,c.title,
                (SELECT MAX(seq) FROM events WHERE conversation=c.id),
                (SELECT created_at FROM events WHERE conversation=c.id ORDER BY seq DESC LIMIT 1)
             FROM conversations c
             WHERE c.channel=?1 AND c.account=?2 AND c.id>?3
               AND (?4 IS NULL OR c.id LIKE ?4 ESCAPE '\\' OR c.native_id LIKE ?4 ESCAPE '\\' OR c.title LIKE ?4 ESCAPE '\\')
             ORDER BY c.id LIMIT ?5",
        )?;
        let rows = statement.query_map(
            params![channel, account, after_id, pattern, limit as i64],
            |row| {
                Ok(ConversationSummary {
                    id: row.get(0)?,
                    channel: row.get(1)?,
                    account: row.get(2)?,
                    native_id: row.get(3)?,
                    kind: row.get(4)?,
                    title: row.get(5)?,
                    last_event_seq: row.get(6)?,
                    last_event_at: row.get(7)?,
                })
            },
        )?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn conversation(&self, id: &str) -> Result<Conversation> {
        Ok(self.connection.query_row(
            "SELECT id,channel,account,native_id,policy_hash,kind,title FROM conversations WHERE id=?1",
            [id],
            |row| {
                Ok(Conversation {
                    id: row.get(0)?,
                    channel: row.get(1)?,
                    account: row.get(2)?,
                    native_id: row.get(3)?,
                    policy_hash: row.get(4)?,
                    kind: row.get(5)?,
                    title: row.get(6)?,
                })
            },
        )?)
    }

    /// Native redelivery is idempotent only when its complete payload agrees,
    /// or when a forget left the event with no payload to compare.
    pub fn append_inbound(&self, input: &InboundEvent) -> Result<Event> {
        let tx = Transaction::new_unchecked(&self.connection, TransactionBehavior::Immediate)?;
        let seq = insert_inbound(&tx, input)?;
        tx.commit()?;
        self.event(seq)
    }

    /// The latest text of the inbound message with `native_id` in
    /// `conversation`: its last edit's, or its own; None before it is logged.
    pub fn latest_text(&self, conversation: &str, native_id: &str) -> Result<Option<String>> {
        Ok(self.connection.query_row(
            "SELECT e.body FROM events o JOIN events e ON e.conversation=o.conversation AND e.direction='in' \
             AND (e.seq=o.seq OR (e.kind='edit' AND e.reply_to=CAST(o.seq AS TEXT))) \
             WHERE o.conversation=?1 AND o.direction='in' AND o.native_id=?2 AND o.kind='message' \
             ORDER BY e.seq DESC LIMIT 1",
            params![conversation, native_id],
            |row| row.get(0),
        ).optional()?)
    }

    /// A channel message. One read again under its native id and author with
    /// other text than its latest version was edited on its channel: the edit is
    /// an event of its own, kind `edit`, replying to the original. An edit has
    /// no native id, so no message a channel delivers can take its place. The
    /// original stays as it was, a re-read of the latest text is that version
    /// again, and text that goes back to an earlier version is a new edit. A
    /// forgotten original or latest version never yields an edit, so a re-read
    /// cannot bring its text back.
    pub fn append_channel_message(&self, input: &InboundEvent) -> Result<Event> {
        let tx = Transaction::new_unchecked(&self.connection, TransactionBehavior::Immediate)?;
        let original: Option<(i64, String)> = tx.query_row("SELECT seq,payload_digest FROM events WHERE conversation=?1 AND direction='in' AND native_id=?2 AND kind='message' AND author=?3",params![input.conversation,input.native_id,input.author],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
        let seq = match original {
            Some((original, original_digest)) if original_digest != FORGOTTEN => {
                let (latest, body, stored): (i64, String, String) = tx.query_row("SELECT seq,body,payload_digest FROM events WHERE conversation=?1 AND direction='in' AND (seq=?2 OR (kind='edit' AND reply_to=?3)) ORDER BY seq DESC LIMIT 1",params![input.conversation,original,original.to_string()],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
                let edit = InboundEvent {
                    kind: "edit".into(),
                    reply_to: Some(original.to_string()),
                    ..input.clone()
                };
                if latest == original && body == input.body {
                    insert_inbound(&tx, input)?
                } else if stored == FORGOTTEN || body == input.body {
                    ensure!(
                        stored == FORGOTTEN || digest(&edit)? == stored,
                        "native event id reused with changed payload"
                    );
                    latest
                } else {
                    tx.execute("INSERT INTO events(conversation,created_at,direction,author,kind,body,attachments,reply_to,native_id,spoken,audio_ref,device,payload_digest) VALUES(?1,?2,'in',?3,'edit',?4,?5,?6,NULL,?7,?8,?9,?10)",
                        params![edit.conversation,now(),edit.author,edit.body,serde_json::to_string(&edit.attachments)?,edit.reply_to,edit.spoken,edit.audio_ref,edit.device,digest(&edit)?])?;
                    tx.last_insert_rowid()
                }
            }
            _ => insert_inbound(&tx, input)?,
        };
        tx.commit()?;
        self.event(seq)
    }

    /// The version of a message an edit replaced: the original or the edit
    /// before it.
    pub fn edited_version(&self, edit: &Event) -> Result<Event> {
        let original = edit
            .reply_to
            .as_deref()
            .context("an edit names its original")?;
        let seq: i64 = self.connection.query_row("SELECT seq FROM events WHERE conversation=?1 AND direction='in' AND seq<?2 AND (seq=?3 OR (kind='edit' AND reply_to=?4)) ORDER BY seq DESC LIMIT 1",params![edit.conversation,edit.seq,original.parse::<i64>()?,original],|r|r.get(0))?;
        self.event(seq)
    }

    /// A message the account itself sent elsewhere (for example from the
    /// owner's phone). It is logged as outbound with no delivery intent.
    pub fn append_observed_outbound(&self, input: &InboundEvent) -> Result<Event> {
        ensure!(
            !input.native_id.trim().is_empty() && !input.author.trim().is_empty(),
            "native id and resolved author are required"
        );
        let digest = digest(input)?;
        let attachments = serde_json::to_string(&input.attachments)?;
        let tx = Transaction::new_unchecked(&self.connection, TransactionBehavior::Immediate)?;
        tx.execute("INSERT INTO events(conversation,created_at,direction,author,kind,body,attachments,reply_to,native_id,spoken,audio_ref,device,payload_digest) VALUES(?1,?2,'out',?3,'message',?4,?5,?6,?7,?8,?9,?10,?11) ON CONFLICT(conversation,direction,native_id) DO NOTHING",
            params![input.conversation,now(),input.author,input.body,attachments,input.reply_to,input.native_id,input.spoken,input.audio_ref,input.device,digest])?;
        let (seq, stored_digest, delivered): (i64,String,bool) = tx.query_row("SELECT e.seq,e.payload_digest,o.id IS NOT NULL FROM events e LEFT JOIN outbox o ON o.event_seq=e.seq WHERE e.conversation=?1 AND e.direction='out' AND e.native_id=?2",params![input.conversation,input.native_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
        // A channel echoes the host's own delivered replies under their native
        // receipt; that echo is the same event, not a changed payload.
        ensure!(
            digest == stored_digest || delivered || stored_digest == FORGOTTEN,
            "native event id reused with changed payload"
        );
        tx.commit()?;
        self.event(seq)
    }

    pub fn event(&self, seq: i64) -> Result<Event> {
        Ok(self.connection.query_row(&format!("SELECT {EVENT_COLUMNS} FROM events e LEFT JOIN outbox o ON o.event_seq=e.seq WHERE e.seq=?1"),[seq],event_row)?)
    }

    /// The inbound event a conversation recorded under a native id.
    pub fn inbound_event(&self, conversation: &str, native_id: &str) -> Result<Option<Event>> {
        Ok(self.connection.query_row(&format!("SELECT {EVENT_COLUMNS} FROM events e LEFT JOIN outbox o ON o.event_seq=e.seq WHERE e.conversation=?1 AND e.direction='in' AND e.native_id=?2"),params![conversation,native_id],event_row).optional()?)
    }

    /// The newest inbound event whose native id starts with `prefix`.
    pub fn latest_inbound(&self, conversation: &str, prefix: &str) -> Result<Option<Event>> {
        Ok(self.connection.query_row(&format!("SELECT {EVENT_COLUMNS} FROM events e LEFT JOIN outbox o ON o.event_seq=e.seq WHERE e.conversation=?1 AND e.direction='in' AND substr(e.native_id,1,length(?2))=?2 ORDER BY e.seq DESC LIMIT 1"),params![conversation,prefix],event_row).optional()?)
    }

    /// The inbound events whose native id starts with `prefix` logged since
    /// `since`, oldest first. The read walks the time index from `since`, so
    /// older history is never read. Every event's time comes from `now()`,
    /// the same UTC RFC 3339 form as `since` here, so the strings sort as times.
    pub fn inbound_events_since(
        &self,
        conversation: &str,
        prefix: &str,
        since: DateTime<Utc>,
    ) -> Result<Vec<Event>> {
        let mut statement = self.connection.prepare_cached(&inbound_since())?;
        let rows =
            statement.query_map(params![conversation, prefix, since.to_rfc3339()], event_row)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Ascending canonical events after a harness-specific cursor.
    pub fn transcript(
        &self,
        conversation: &str,
        after_seq: i64,
        limit: usize,
    ) -> Result<Vec<Event>> {
        ensure!(
            after_seq >= 0 && limit <= 10_000,
            "invalid transcript range"
        );
        let mut statement = self.connection.prepare(&format!("SELECT {EVENT_COLUMNS} FROM events e LEFT JOIN outbox o ON o.event_seq=e.seq WHERE e.conversation=?1 AND e.seq>?2 ORDER BY e.seq LIMIT ?3"))?;
        let rows =
            statement.query_map(params![conversation, after_seq, limit as i64], event_row)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// A fresh session needs the tail, not the oldest messages in a long chat.
    pub fn recent_transcript(&self, conversation: &str, limit: usize) -> Result<Vec<Event>> {
        ensure!(limit <= 10_000, "transcript limit too large");
        let mut statement = self.connection.prepare(&format!("SELECT {EVENT_COLUMNS} FROM events e LEFT JOIN outbox o ON o.event_seq=e.seq WHERE e.conversation=?1 ORDER BY e.seq DESC LIMIT ?2"))?;
        let rows = statement.query_map(params![conversation, limit as i64], event_row)?;
        let mut events = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        events.reverse();
        Ok(events)
    }

    /// Everyone who has written in a conversation, by host-resolved author, in
    /// the order they first did.
    pub fn authors(&self, conversation: &str) -> Result<Vec<String>> {
        let mut statement = self.connection.prepare("SELECT author FROM events WHERE conversation=?1 AND direction='in' GROUP BY author ORDER BY MIN(seq)")?;
        let rows = statement.query_map(params![conversation], |row| row.get(0))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Everyone whose message came in to a conversation from its channel,
    /// leaving out host events and the account's own messages.
    pub fn message_authors(&self, conversation: &str) -> Result<Vec<String>> {
        let mut statement = self.connection.prepare("SELECT DISTINCT author FROM events WHERE conversation=?1 AND direction='in' AND kind='message'")?;
        let rows = statement.query_map(params![conversation], |row| row.get(0))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// The seq of the newest event, or 0 before any.
    pub fn last_seq(&self) -> Result<i64> {
        Ok(self
            .connection
            .query_row("SELECT COALESCE(MAX(seq),0) FROM events", [], |row| {
                row.get(0)
            })?)
    }

    /// Whether `author` wrote a message containing `text` after event
    /// `after`, through a channel: `oppa chat` at the terminal, which any
    /// shell here can run as the owner with no secret, does not count. Web
    /// chat does, though a shell that reads its login token could post too.
    pub fn wrote_after(&self, author: &str, after: i64, text: &str) -> Result<bool> {
        Ok(self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM events e JOIN conversations c ON c.id=e.conversation
             WHERE e.direction='in' AND e.kind='message' AND e.author=?1 AND e.seq>?2
             AND instr(e.body,?3)>0 AND c.channel<>'console')",
            params![author, after, text],
            |row| row.get(0),
        )?)
    }

    /// Whether a reply of the assistant's to a message from `author`, in any
    /// conversation, may have reached them: one still queued never did.
    pub fn replied_to(&self, author: &str) -> Result<bool> {
        Ok(self.connection.query_row("SELECT EXISTS(SELECT 1 FROM events i JOIN outbox o ON o.idempotency_key='reply:'||i.seq WHERE i.direction='in' AND i.author=?1 AND o.state<>'queued')", params![author], |row| row.get(0))?)
    }

    /// Whether a message came in from `author`, in any conversation: an
    /// unregistered sender is logged as `<channel>:<handle>`.
    pub fn wrote_in(&self, author: &str) -> Result<bool> {
        // A handle as people are matched by it (`config::handle_key`): an
        // email address in any case.
        Ok(self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM events WHERE (CASE WHEN instr(author,'@')>0 THEN lower(author) ELSE author END)=?1 AND direction='in' AND kind='message')",
            params![crate::config::handle_key(author)],
            |row| row.get(0),
        )?)
    }

    /// The outbox keys that start with `prefix`.
    pub fn send_keys(&self, prefix: &str) -> Result<Vec<String>> {
        let mut statement = self.connection.prepare(
            "SELECT idempotency_key FROM outbox WHERE substr(idempotency_key,1,length(?1))=?1",
        )?;
        let rows = statement.query_map(params![prefix], |row| row.get(0))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// The owner's direct chat on one channel account, most recently used
    /// first: a direct conversation in which every message that came in was
    /// the owner's. What only the owner may read is said there.
    pub fn owner_chat(&self, channel: &str, account: &str, owner: &str) -> Result<Option<String>> {
        Ok(self
            .connection
            .query_row(
                "SELECT c.id FROM conversations c
                 WHERE c.channel=?1 AND c.account=?2 AND c.kind='dm'
                 AND EXISTS(SELECT 1 FROM events e WHERE e.conversation=c.id AND e.direction='in' AND e.kind='message' AND e.author=?3)
                 AND NOT EXISTS(SELECT 1 FROM events e WHERE e.conversation=c.id AND e.direction='in' AND e.kind='message' AND e.author<>?3)
                 ORDER BY (SELECT MAX(e.seq) FROM events e WHERE e.conversation=c.id) DESC LIMIT 1",
                params![channel, account, owner],
                |row| row.get(0),
            )
            .optional()?)
    }

    /// Whether an input with `native_id` was logged in any direct chat, where
    /// every look speaks (`heartbeat::owners_chat`). Each direct chat is
    /// looked up through its unique native id index.
    pub fn look_logged(&self, native_id: &str) -> Result<bool> {
        Ok(self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM conversations c WHERE c.kind='dm' AND EXISTS(
               SELECT 1 FROM events e WHERE e.conversation=c.id AND e.direction='in' AND e.native_id=?1))",
            [native_id],
            |row| row.get(0),
        )?)
    }

    /// The seq of the newest message `author` wrote in `conversation`, or 0
    /// before any.
    pub fn last_message_from(&self, conversation: &str, author: &str) -> Result<i64> {
        Ok(self.connection.query_row(
            "SELECT COALESCE(MAX(seq),0) FROM events
             WHERE conversation=?1 AND direction='in' AND kind='message' AND author=?2",
            [conversation, author],
            |row| row.get(0),
        )?)
    }

    /// The messages that came in to `conversation` after event `after`, each
    /// as its seq and author, oldest first, at most `limit`.
    pub fn messages_after(
        &self,
        conversation: &str,
        after: i64,
        limit: usize,
    ) -> Result<Vec<(i64, String)>> {
        let mut statement = self.connection.prepare(
            "SELECT seq,author FROM events WHERE conversation=?1 AND direction='in' AND kind='message' AND seq>?2 ORDER BY seq LIMIT ?3",
        )?;
        let rows = statement.query_map(params![conversation, after, limit as i64], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Whether a message the assistant wrote in `conversation`, after event
    /// `after` and before event `before`, may have reached its readers: one
    /// still queued never did.
    pub fn assistant_wrote_between(
        &self,
        conversation: &str,
        after: i64,
        before: i64,
    ) -> Result<bool> {
        Ok(self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM events e LEFT JOIN outbox o ON o.event_seq=e.seq
             WHERE e.conversation=?1 AND e.direction='out' AND e.author=?2 AND e.kind='message'
             AND e.seq>?3 AND e.seq<?4 AND (o.state IS NULL OR o.state<>'queued'))",
            params![conversation, ASSISTANT, after, before],
            |row| row.get(0),
        )?)
    }

    /// OR-ranked terms and surrounding turns. Short terms (including two-character
    /// Chinese words) use a Unicode substring fallback because trigrams cannot index them.
    pub fn search(&self, conversation: &str, query: &str, around: usize) -> Result<Vec<SearchHit>> {
        ensure!(around <= 100, "search context too large");
        let terms: Vec<String> = query
            .split(|c: char| !c.is_alphanumeric())
            .filter(|x| !x.is_empty())
            .map(str::to_lowercase)
            .collect();
        if terms.is_empty() {
            return Ok(Vec::new());
        }
        ensure!(
            terms.len() <= 32 && query.len() <= 4096,
            "search query too large"
        );
        let mut candidates: Vec<Event> = if terms.iter().all(|t| t.chars().count() >= 3) {
            let expression = terms
                .iter()
                .map(|t| format!("\"{}\"", t.replace('"', "\"\"")))
                .collect::<Vec<_>>()
                .join(" OR ");
            let mut statement = self.connection.prepare(&format!("SELECT {EVENT_COLUMNS} FROM events_fts JOIN events e ON e.seq=events_fts.rowid LEFT JOIN outbox o ON o.event_seq=e.seq WHERE events_fts MATCH ?1 AND e.conversation=?2 ORDER BY rank,e.seq DESC LIMIT 200"))?;
            let rows = statement.query_map(params![expression, conversation], event_row)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        } else {
            let mut statement = self.connection.prepare(&format!("SELECT {EVENT_COLUMNS} FROM events e LEFT JOIN outbox o ON o.event_seq=e.seq WHERE e.conversation=?1"))?;
            let rows = statement.query_map([conversation], event_row)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        let score = |event: &Event| {
            let body = event.body.to_lowercase();
            terms
                .iter()
                .filter(|term| body.contains(term.as_str()))
                .count()
        };
        candidates.retain(|event| score(event) > 0);
        candidates.sort_by_key(|event| {
            (
                std::cmp::Reverse(score(event)),
                std::cmp::Reverse(event.seq),
            )
        });
        candidates.truncate(20);
        candidates.into_iter().map(|event| {
            let mut statement=self.connection.prepare(&format!("SELECT {EVENT_COLUMNS} FROM events e LEFT JOIN outbox o ON o.event_seq=e.seq WHERE e.conversation=?1 AND e.seq<?2 ORDER BY e.seq DESC LIMIT ?3"))?;
            let rows=statement.query_map(params![conversation,event.seq,around as i64],event_row)?;
            let mut context=rows.collect::<rusqlite::Result<Vec<_>>>()?;
            context.reverse();
            context.push(event.clone());
            context.extend(self.transcript(conversation,event.seq,around)?);
            Ok(SearchHit{event,context})
        }).collect()
    }

    pub fn session(
        &self,
        conversation: &str,
        agent: &str,
        policy_hash: &str,
    ) -> Result<Option<Session>> {
        Ok(self.connection.query_row("SELECT s.handle,s.cursor,s.started_at,s.used_at FROM sessions s JOIN conversations c ON c.id=s.conversation WHERE s.conversation=?1 AND s.agent=?2 AND s.policy_hash=?3 AND c.policy_hash=s.policy_hash", params![conversation,agent,policy_hash], |r|Ok(Session{handle:r.get(0)?,cursor:r.get(1)?,started_at:r.get(2)?,used_at:r.get(3)?})).optional()?)
    }

    /// Remove only the rejected cache entry, never a replacement saved later.
    pub fn clear_session(
        &self,
        conversation: &str,
        agent: &str,
        policy_hash: &str,
        handle: &str,
    ) -> Result<()> {
        self.connection.execute(
            "DELETE FROM sessions WHERE conversation=?1 AND agent=?2 AND policy_hash=?3 AND handle=?4",
            params![conversation, agent, policy_hash, handle],
        )?;
        Ok(())
    }

    /// Keep the native session `handle` that a run in `conversation` used, so
    /// a forget finds it after the conversation moves on to another session.
    /// Its policy, `kept:<handle>`, is no conversation's, so `session` never
    /// returns it and no turn resumes it.
    pub fn keep_session(&self, conversation: &str, agent: &str, handle: &str) -> Result<()> {
        ensure!(!agent.is_empty() && !handle.is_empty(), "invalid session");
        self.connection.execute(
            "INSERT OR IGNORE INTO sessions(conversation,agent,policy_hash,handle,cursor,started_at,used_at) VALUES(?1,?2,?3,?4,0,?5,?5)",
            params![conversation, agent, format!("kept:{handle}"), handle, now()],
        )?;
        Ok(())
    }

    /// Drop the row of the native session `handle` in `conversation`.
    pub fn clear_handle(&self, conversation: &str, handle: &str) -> Result<()> {
        self.connection.execute(
            "DELETE FROM sessions WHERE conversation=?1 AND handle=?2",
            params![conversation, handle],
        )?;
        Ok(())
    }

    pub fn save_session(
        &self,
        conversation: &str,
        agent: &str,
        policy_hash: &str,
        handle: &str,
        cursor: i64,
    ) -> Result<()> {
        ensure!(
            !agent.is_empty() && !handle.is_empty() && cursor >= 0,
            "invalid session"
        );
        let tx = Transaction::new_unchecked(&self.connection, TransactionBehavior::Immediate)?;
        let active_policy: String = tx.query_row(
            "SELECT policy_hash FROM conversations WHERE id=?1",
            [conversation],
            |r| r.get(0),
        )?;
        ensure!(
            active_policy == policy_hash,
            "cannot save session under an obsolete audience policy"
        );
        if cursor > 0 {
            let valid: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM events WHERE seq=?1 AND conversation=?2)",
                params![cursor, conversation],
                |r| r.get(0),
            )?;
            ensure!(
                valid,
                "session cursor must name an event in this conversation"
            );
        }
        // A new native handle restarts the session clock; reuse only refreshes use.
        let changed=tx.execute("INSERT INTO sessions(conversation,agent,policy_hash,handle,cursor,started_at,used_at) VALUES(?1,?2,?3,?4,?5,?6,?6) ON CONFLICT(conversation,agent,policy_hash) DO UPDATE SET handle=excluded.handle,cursor=excluded.cursor,used_at=excluded.used_at,started_at=CASE WHEN sessions.handle=excluded.handle THEN sessions.started_at ELSE excluded.started_at END WHERE excluded.cursor>=sessions.cursor",params![conversation,agent,policy_hash,handle,cursor,now()])?;
        ensure!(changed == 1, "session cursor cannot move backwards");
        tx.commit()?;
        Ok(())
    }

    /// Persist the reply and delivery intent together before calling an adapter.
    /// Idempotency keys are global to the instance, not just one channel.
    pub fn enqueue_reply(
        &self,
        conversation: &str,
        idempotency_key: &str,
        body: &str,
        attachments: Vec<Attachment>,
        reply_to: Option<String>,
    ) -> Result<OutboxEntry> {
        self.enqueue(
            "message",
            conversation,
            idempotency_key,
            body,
            attachments,
            reply_to,
        )
    }

    /// A tapback on message `target`: an outbound `reaction` event whose body
    /// names the reaction, delivered through the outbox like a reply. A message
    /// takes one: the first queued under `idempotency_key` wins, whatever it is.
    pub fn enqueue_reaction(
        &self,
        conversation: &str,
        idempotency_key: &str,
        target: i64,
        reaction: &str,
    ) -> Result<OutboxEntry> {
        self.enqueue(
            "reaction",
            conversation,
            idempotency_key,
            reaction,
            vec![],
            Some(target.to_string()),
        )
    }

    /// The assistant's contact card: an outbound `contact` event whose body is
    /// the name on the card and whose attachment, if any, is its face. The
    /// channel adds the address it is reached at when it sends the card.
    pub fn enqueue_contact(
        &self,
        conversation: &str,
        idempotency_key: &str,
        name: &str,
        face: Vec<Attachment>,
    ) -> Result<OutboxEntry> {
        self.enqueue("contact", conversation, idempotency_key, name, face, None)
    }

    fn enqueue(
        &self,
        kind: &str,
        conversation: &str,
        idempotency_key: &str,
        body: &str,
        attachments: Vec<Attachment>,
        reply_to: Option<String>,
    ) -> Result<OutboxEntry> {
        ensure!(
            !idempotency_key.trim().is_empty(),
            "idempotency key is required"
        );
        // A message keeps its old digest, so keys queued before reactions still match.
        let digest = if kind == "message" {
            digest(&(conversation, body, &attachments, &reply_to))?
        } else {
            digest(&(kind, conversation, body, &attachments, &reply_to))?
        };
        let tx = Transaction::new_unchecked(&self.connection, TransactionBehavior::Immediate)?;
        let existing:Option<(i64,String)>=tx.query_row("SELECT o.id,e.payload_digest FROM outbox o JOIN events e ON e.seq=o.event_seq WHERE o.idempotency_key=?1",[idempotency_key],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
        let id = if let Some((id, stored_digest)) = existing {
            // A reaction's key is its message's, and a card's is its chat, name
            // and face: the first one queued stands.
            ensure!(
                stored_digest == digest || kind == "reaction" || kind == "contact",
                "idempotency key reused with changed reply payload"
            );
            id
        } else {
            tx.execute("INSERT INTO events(conversation,created_at,direction,author,kind,body,attachments,reply_to,payload_digest) VALUES(?1,?2,'out',?3,?4,?5,?6,?7,?8)",params![conversation,now(),ASSISTANT,kind,body,serde_json::to_string(&attachments)?,reply_to,digest])?;
            let seq = tx.last_insert_rowid();
            tx.execute("INSERT INTO outbox(event_seq,idempotency_key,state,updated_at) VALUES(?1,?2,'queued',?3)",params![seq,idempotency_key,now()])?;
            tx.last_insert_rowid()
        };
        tx.commit()?;
        self.outbox_entry(id)
    }

    pub fn outbox_entry(&self, id: i64) -> Result<OutboxEntry> {
        let (event_seq,key,state,attempts,native_id,evidence):(i64,String,String,u32,Option<String>,Option<String>)=self.connection.query_row("SELECT event_seq,idempotency_key,state,attempts,native_id,evidence FROM outbox WHERE id=?1",[id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?)))?;
        let event = self.event(event_seq)?;
        let destination = self.conversation(&event.conversation)?;
        Ok(OutboxEntry {
            id,
            destination,
            event,
            idempotency_key: key,
            state: DeliveryState::parse(&state)?,
            attempts,
            native_id,
            evidence,
        })
    }

    /// Exactly one dispatcher can transition this queued attempt to sending.
    pub fn claim_send(&self, id: i64) -> Result<bool> {
        Ok(self.connection.execute("UPDATE outbox SET state='sending',attempts=attempts+1,updated_at=?2 WHERE id=?1 AND state='queued'",params![id,now()])?==1)
    }

    pub fn finish_send(&self, id: i64, observation: DeliveryObservation) -> Result<()> {
        self.apply_observation(id, "sending", observation)
    }

    /// Only adapter verification, never timeout or absence of a receipt, may
    /// confirm that an unknown attempt did not send.
    pub fn mark_verified(&self, id: i64, observation: DeliveryObservation) -> Result<()> {
        self.apply_observation(id, "unknown", observation)
    }

    fn apply_observation(
        &self,
        id: i64,
        expected: &str,
        observation: DeliveryObservation,
    ) -> Result<()> {
        let tx = Transaction::new_unchecked(&self.connection, TransactionBehavior::Immediate)?;
        let (state, old_native, event_seq): (String, Option<String>, i64) = tx.query_row(
            "SELECT state,native_id,event_seq FROM outbox WHERE id=?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        // A repeated native receipt is harmless. Contradictory evidence is not.
        if let DeliveryObservation::Sent { native_id } = &observation {
            if state == "sent" && old_native.as_ref() == Some(native_id) {
                return Ok(());
            }
        }
        ensure!(
            state == expected,
            "delivery observation requires {expected}, found {state}"
        );
        match observation {
            DeliveryObservation::Sent { native_id } => {
                let invalid = native_id.trim().is_empty()
                    || native_id.len() > 4096
                    || native_id.contains('\0');
                let duplicate: bool = if invalid {
                    false
                } else {
                    tx.query_row(
                        "SELECT EXISTS(SELECT 1 FROM events WHERE conversation=(SELECT conversation FROM events WHERE seq=?1) AND direction='out' AND native_id=?2 AND seq!=?1)",
                        params![event_seq, native_id],
                        |row| row.get(0),
                    )?
                };
                if invalid || duplicate {
                    let evidence = if duplicate {
                        "Delivery receipt duplicates another outbound event; adapter verification required."
                    } else {
                        "Delivery receipt is invalid; adapter verification required."
                    };
                    tx.execute(
                        "UPDATE outbox SET state='unknown',evidence=?2,updated_at=?3 WHERE id=?1",
                        params![id, evidence, now()],
                    )?;
                    tx.commit()?;
                    bail!("delivery receipt could not be recorded; delivery remains unknown");
                }
                tx.execute(
                    "UPDATE events SET native_id=?2 WHERE seq=?1",
                    params![event_seq, native_id],
                )?;
                tx.execute("UPDATE outbox SET state='sent',native_id=?2,evidence=NULL,updated_at=?3 WHERE id=?1",params![id,native_id,now()])?;
            }
            DeliveryObservation::Unknown { evidence } => {
                validate_evidence(&evidence)?;
                tx.execute(
                    "UPDATE outbox SET state='unknown',evidence=?2,updated_at=?3 WHERE id=?1",
                    params![id, evidence, now()],
                )?;
            }
            DeliveryObservation::ConfirmedNotSent { evidence } => {
                validate_evidence(&evidence)?;
                tx.execute(
                    "UPDATE outbox SET state='queued',evidence=?2,updated_at=?3 WHERE id=?1",
                    params![id, evidence, now()],
                )?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Messages a turn queued for another conversation (`messages.send`) that
    /// no dispatcher has tried yet, oldest first, with their conversations and
    /// when each was queued.
    pub fn queued_sends(&self) -> Result<Vec<(i64, String, String)>> {
        let mut statement = self.connection.prepare(
            "SELECT o.id,e.conversation,e.created_at FROM outbox o JOIN events e ON e.seq=o.event_seq
             WHERE o.state='queued' AND o.attempts=0 AND o.idempotency_key LIKE 'send:%'
             ORDER BY o.id LIMIT 100",
        )?;
        let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Record a queued message as never sent, with why, so nothing sends it
    /// later. Only an entry no dispatcher has tried can be withdrawn.
    pub fn withdraw_send(&self, id: i64, evidence: &str) -> Result<bool> {
        if !self.claim_send(id)? {
            return Ok(false);
        }
        self.finish_send(
            id,
            DeliveryObservation::ConfirmedNotSent {
                evidence: evidence.into(),
            },
        )?;
        Ok(true)
    }

    /// Call only after the supervisor has established that the old dispatcher
    /// stopped. Opening another reader must never invalidate live sends.
    pub fn recover_interrupted_sends(&self) -> Result<usize> {
        Ok(self.connection.execute("UPDATE outbox SET state='unknown',evidence='Dispatcher stopped before a delivery receipt was persisted; adapter verification required.',updated_at=?1 WHERE state='sending'",[now()])?)
    }

    /// Includes queued, in-flight, and unknown entries for honest inspection.
    /// Dispatchers must still call `claim_send`, which accepts only queued rows.
    pub fn pending_outbox(&self) -> Result<Vec<OutboxEntry>> {
        let mut statement = self
            .connection
            .prepare("SELECT id FROM outbox WHERE state!='sent' ORDER BY id")?;
        let ids = statement
            .query_map([], |r| r.get::<_, i64>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        ids.into_iter().map(|id| self.outbox_entry(id)).collect()
    }
}

fn now() -> String {
    Utc::now().to_rfc3339()
}
/// The payload digest of an event a forget tombstoned. No hash of its old
/// payload is kept, and a channel's replay of its native id is taken as
/// already logged.
pub const FORGOTTEN: &str = "forgotten";

/// One inbound event under its native id, or the one already there when its
/// payload agrees.
fn insert_inbound(tx: &Transaction, input: &InboundEvent) -> Result<i64> {
    ensure!(
        !input.native_id.trim().is_empty() && !input.author.trim().is_empty(),
        "native id and resolved author are required"
    );
    let digest = digest(input)?;
    let attachments = serde_json::to_string(&input.attachments)?;
    let kind = if input.kind.is_empty() {
        "message"
    } else {
        &input.kind
    };
    tx.execute("INSERT INTO events(conversation,created_at,direction,author,kind,body,attachments,reply_to,native_id,spoken,audio_ref,device,payload_digest) VALUES(?1,?2,'in',?3,?4,?5,?6,?7,?8,?9,?10,?11,?12) ON CONFLICT(conversation,direction,native_id) DO NOTHING",
        params![input.conversation,now(),input.author,kind,input.body,attachments,input.reply_to,input.native_id,input.spoken,input.audio_ref,input.device,digest])?;
    let (seq, stored_digest): (i64,String) = tx.query_row("SELECT seq,payload_digest FROM events WHERE conversation=?1 AND direction='in' AND native_id=?2",params![input.conversation,input.native_id],|r|Ok((r.get(0)?,r.get(1)?)))?;
    ensure!(
        digest == stored_digest || stored_digest == FORGOTTEN,
        "native event id reused with changed payload"
    );
    Ok(seq)
}

fn digest(value: &impl Serialize) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(value)?)))
}
fn validate_evidence(evidence: &str) -> Result<()> {
    if evidence.trim().is_empty() || evidence.len() > 4096 {
        bail!("delivery evidence must be nonempty and at most 4096 bytes");
    }
    Ok(())
}
fn event_row(row: &Row<'_>) -> rusqlite::Result<Event> {
    let attachments: String = row.get(7)?;
    let attachments = serde_json::from_str(&attachments).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(7, rusqlite::types::Type::Text, Box::new(e))
    })?;
    let direction: String = row.get(3)?;
    let direction = match direction.as_str() {
        "in" => Direction::In,
        "out" => Direction::Out,
        _ => return Err(rusqlite::Error::InvalidQuery),
    };
    let delivery: Option<String> = row.get(13)?;
    Ok(Event {
        seq: row.get(0)?,
        conversation: row.get(1)?,
        created_at: row.get(2)?,
        direction,
        author: row.get(4)?,
        kind: row.get(5)?,
        body: row.get(6)?,
        attachments,
        reply_to: row.get(8)?,
        native_id: row.get(9)?,
        spoken: row.get(10)?,
        audio_ref: row.get(11)?,
        device: row.get(12)?,
        delivery: delivery.as_deref().map(DeliveryState::parse).transpose()?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_digest_with_no_heartbeat_before_its_week_reads_none_of_the_older_history() {
        // P-5 review finding of 2026-10-11: with no heartbeat older than the
        // week to stop at, the digest's read walked every message ever logged
        // in the chat. It now steps through the week's rows only.
        // A database from before the time index gains it when it is opened.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let store = Store::open(&path).unwrap();
        store
            .connection
            .execute_batch("DROP INDEX events_created;")
            .unwrap();
        drop(store);
        let store = Store::open(&path).unwrap();
        store
            .create_conversation("room", "web", "local", "native-room", "adult-v1")
            .unwrap();
        let month_ago = (Utc::now() - chrono::Duration::days(30)).to_rfc3339();
        for n in 0..10_000 {
            store
                .append_inbound(&InboundEvent {
                    conversation: "room".into(),
                    native_id: format!("m{n}"),
                    author: "person-a".into(),
                    body: "An old message.".into(),
                    kind: "message".into(),
                    ..Default::default()
                })
                .unwrap();
        }
        store
            .connection
            .execute("UPDATE events SET created_at=?1", [&month_ago])
            .unwrap();
        store
            .append_inbound(&InboundEvent {
                conversation: "room".into(),
                native_id: "heartbeat:this-week".into(),
                author: "heartbeat".into(),
                body: "{}".into(),
                kind: "heartbeat".into(),
                ..Default::default()
            })
            .unwrap();
        let since = Utc::now() - chrono::Duration::days(7);
        let events = store
            .inbound_events_since("room", "heartbeat:", since)
            .unwrap();
        let ids: Vec<_> = events.iter().map(|e| e.native_id.as_deref()).collect();
        assert_eq!(ids, [Some("heartbeat:this-week")]);
        let steps = store
            .connection
            .prepare_cached(&inbound_since())
            .unwrap()
            .get_status(rusqlite::StatementStatus::VmStep);
        assert!(steps > 0 && steps < 1_000, "{steps} steps");
    }
}

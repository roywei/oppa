//! Message tool: at the owner's request the assistant says something in
//! another of its conversations, such as telling a family group what the owner
//! asked it to. The host queues the message in that conversation's outbox,
//! `oppa serve` delivers it through its channel, and the tool waits briefly for
//! the receipt. It reaches only conversations the assistant is already in, and
//! never at `read` access. At `computer` access a message may carry files the
//! run made, such as a picture, to this conversation as well. Another family
//! member's turn may also ask the owner, in the owner's direct chat, for what
//! the owner's rules leave to the owner.

use super::{
    denied, digest, invalid,
    schedules::{not_deliverable, owners_account},
    tool, unavailable, Broker, Fault, RunGrant, TaskAccess, ToolCall,
};
use crate::{
    config::private_directory,
    harness::Access,
    store::{Attachment, DeliveryState, Store},
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    fs,
    path::Path,
    time::{Duration, Instant},
};

/// How long a send waits for `oppa serve` to deliver before it reports the
/// message as queued.
const DELIVERY_WAIT: Duration = Duration::from_secs(20);

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MessageSend {
    context: String,
    conversation: String,
    text: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    files: Vec<String>,
    idempotency_key: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MessageReact {
    context: String,
    event_seq: i64,
    reaction: String,
}

/// The tapbacks a channel takes; iMessage names these six.
const REACTIONS: [&str; 6] = ["love", "like", "dislike", "laugh", "emphasize", "question"];

/// Files one message may carry, each at most this large: what the channels
/// take as a photo or a document.
const FILES_PER_MESSAGE: usize = 8;
const FILE_BYTES: u64 = 20 * 1024 * 1024;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AskOwner {
    context: String,
    text: String,
    idempotency_key: String,
}

/// Questions a family member's turn may put to the owner: enough to ask and to
/// follow up, never a flood.
const ASKS_PER_TURN: usize = 2;

fn asked_enough() -> Fault {
    Fault {
        code: "asked_enough",
        message: "This turn already asked the owner twice. Tell the person the owner has been asked, and wait for the answer.",
    }
}

fn no_owner_chat() -> Fault {
    Fault {
        code: "no_owner_chat",
        message: "The owner has no direct chat with the assistant on this account yet, so they cannot be asked this way. Say so, and suggest asking the owner.",
    }
}

pub(super) fn definitions(grant: &RunGrant, context: &Value) -> Vec<Value> {
    let mut tools = Vec::new();
    if grant.task_access == TaskAccess::Manage && grant.role != crate::config::Role::Owner {
        tools.push(tool("messages.ask_owner", "Ask the owner, in their direct chat with you, about something this family member asked for that the owner's rules say needs the owner's yes, such as building or hosting something new, acting as the owner, or sharing something private of the owner's. Say who asked and what exactly. The owner answers in their own chat; tell this person you asked, and do it once the owner says yes.", json!({"context":context,"text":{"type":"string","minLength":1,"maxLength":8192},"idempotency_key":{"type":"string","minLength":1,"maxLength":256}}), &["context","text","idempotency_key"], false));
    }
    // A conversation turn reacts; a delegated task or heartbeat has no one to react to.
    if grant.task_access == TaskAccess::Manage {
        tools.push(tool("messages.react", "React to a message in the current conversation with a tapback (love ❤️, like 👍, dislike 👎, laugh 😂, emphasize ‼️, question ❓), as a friend would: instead of a reply that would say only as much, or beside one. Name the message by its event_seq. A message takes one reaction from you, and the first is final, even one the channel refused: asking again returns that one. It goes out when this turn ends; a channel without reactions refuses it.", json!({"context":context,"event_seq":{"type":"integer"},"reaction":{"type":"string","enum":REACTIONS}}), &["context","event_seq","reaction"], false));
    }
    if grant.task_access != TaskAccess::Manage || grant.access.is_read() {
        return tools;
    }
    let mut properties = json!({"context":context,"conversation":{"type":"string","maxLength":4096},"text":{"type":"string","minLength":1,"maxLength":65536},"idempotency_key":{"type":"string","minLength":1,"maxLength":256}});
    let mut description = "Send a message now, in your own voice, to another conversation you are in (a group, or someone's direct chat with you). Name it from history.conversations. Answer in the current conversation with your reply, not with this tool. Reuse the idempotency_key to get the same message's receipt back; never send again a message whose delivery is unknown.".to_string();
    if grant.access == Access::Computer {
        properties["files"] = json!({"type":"array","items":{"type":"string","maxLength":4096},"maxItems":FILES_PER_MESSAGE,"description":"Absolute paths of files to send with the text, each at most 20 MiB, such as pictures you made."});
        description.push_str(" To send files, such as a picture you made, name them in files; with files it may go to the current conversation too, ahead of your reply. Not every channel takes files; a refused send says so.");
    }
    tools.push(tool(
        "messages.send",
        &description,
        properties,
        &["context", "conversation", "text", "idempotency_key"],
        false,
    ));
    tools
}

impl Broker {
    /// A family member's question to the owner, in the owner's direct chat on
    /// the account they wrote on.
    pub(super) fn ask_owner(&self, args: AskOwner) -> ToolCall {
        self.authorize(&args.context)?;
        let grant = &self.launch.grant;
        if grant.role == crate::config::Role::Owner || grant.task_access != TaskAccess::Manage {
            return Err(denied());
        }
        if args.text.trim().is_empty()
            || args.text.len() > 8192
            || args.text.contains('\0')
            || args.idempotency_key.trim().is_empty()
            || args.idempotency_key.len() > 256
        {
            return Err(invalid());
        }
        let owner = self
            .paths()
            .and_then(|paths| paths.load())
            .map_err(|_| unavailable())?
            .owner;
        let store = Store::open(&self.launch.database).map_err(|_| unavailable())?;
        let origin = grant
            .conversations
            .first()
            .and_then(|conversation| store.conversation(conversation).ok())
            .ok_or_else(denied)?;
        let chat = store
            .owner_chat(&origin.channel, &origin.account, &owner)
            .map_err(|_| unavailable())?
            .ok_or_else(no_owner_chat)?;
        let asked = format!("send:ask:{}:", grant.input_seq);
        let key = format!(
            "{asked}{}",
            digest(&args.idempotency_key).map_err(|_| unavailable())?
        );
        let earlier = store.send_keys(&asked).map_err(|_| unavailable())?;
        if earlier.len() >= ASKS_PER_TURN && !earlier.contains(&key) {
            return Err(asked_enough());
        }
        let mut result = self.journaled_effect("messages.ask_owner", "outward", &args, || {
            let entry = store
                .enqueue_reply(&chat, &key, &args.text, vec![], None)
                .map_err(|_| invalid())?;
            Ok(json!({"conversation":chat,"outbox_id":entry.id,"event_seq":entry.event.seq}))
        })?;
        let outbox_id = result["receipt"]["outbox_id"]
            .as_i64()
            .ok_or_else(unavailable)?;
        result["delivery"] = self.await_delivery(&store, outbox_id)?;
        Ok(result)
    }

    pub(super) fn message_send(&self, args: MessageSend) -> ToolCall {
        self.authorize(&args.context)?;
        let grant = &self.launch.grant;
        if grant.task_access != TaskAccess::Manage || grant.access.is_read() {
            return Err(denied());
        }
        if args.text.trim().is_empty()
            || args.text.len() > 65536
            || args.text.contains('\0')
            || args.idempotency_key.trim().is_empty()
            || args.idempotency_key.len() > 256
            || args.files.len() > FILES_PER_MESSAGE
        {
            return Err(invalid());
        }
        if !args.files.is_empty() && grant.access != Access::Computer {
            return Err(denied());
        }
        if args.files.is_empty() && grant.conversations.contains(&args.conversation) {
            return Err(Fault {
                code: "current_conversation",
                message: "This is the current conversation: say it in your reply instead.",
            });
        }
        self.conversation_allowed(&args.conversation)?;
        let store = Store::open(&self.launch.database).map_err(|_| unavailable())?;
        let target = store
            .conversation(&args.conversation)
            .map_err(|_| denied())?;
        if target.channel == "console" {
            return Err(not_deliverable());
        }
        if self.on_owners_account(&target.channel, &target.account) {
            return Err(owners_account());
        }
        let key = format!(
            "send:{}:{}",
            grant.input_seq,
            digest(&args.idempotency_key).map_err(|_| unavailable())?
        );
        let mut result = self.journaled_effect("messages.send", "outward", &args, || {
            let files = self.outgoing_files(&args.files, &key)?;
            let entry = store
                .enqueue_reply(&args.conversation, &key, &args.text, files, None)
                .map_err(|_| invalid())?;
            Ok(json!({"conversation":args.conversation,"outbox_id":entry.id,"event_seq":entry.event.seq}))
        })?;
        let outbox_id = result["receipt"]["outbox_id"]
            .as_i64()
            .ok_or_else(unavailable)?;
        result["delivery"] = self.await_delivery(&store, outbox_id)?;
        Ok(result)
    }

    /// A tapback on someone's message in the current conversation. Its worker
    /// is running this turn, so it is queued and goes out when the turn ends.
    pub(super) fn message_react(&self, args: MessageReact) -> ToolCall {
        self.authorize(&args.context)?;
        if self.launch.grant.task_access != TaskAccess::Manage {
            return Err(denied());
        }
        if !REACTIONS.contains(&args.reaction.as_str()) {
            return Err(invalid());
        }
        let grant = &self.launch.grant;
        let store = Store::open(&self.launch.database).map_err(|_| unavailable())?;
        let mut target = store.event(args.event_seq).map_err(|_| invalid())?;
        // An edit stands for the message it changed.
        if target.kind == "edit" {
            let original = target.reply_to.as_deref().and_then(|seq| seq.parse().ok());
            target = original
                .and_then(|seq| store.event(seq).ok())
                .ok_or_else(invalid)?;
        }
        if !grant.conversations.contains(&target.conversation) {
            return Err(denied());
        }
        if target.kind != "message" || target.native_id.is_none() {
            return Err(Fault {
                code: "not_a_message",
                message: "That event is not a channel message that can take a reaction.",
            });
        }
        let conversation = store
            .conversation(&target.conversation)
            .map_err(|_| denied())?;
        if conversation.channel == "console" {
            return Err(not_deliverable());
        }
        if self.on_owners_account(&conversation.channel, &conversation.account) {
            return Err(owners_account());
        }
        // One reaction per message, ever: the key is the message's own, and
        // the store keeps the first queued under it.
        let key = format!("send:react:{}", target.seq);
        let receipt = self.journaled_effect("messages.react", "outward", &args, || {
            let entry = store
                .enqueue_reaction(&target.conversation, &key, target.seq, &args.reaction)
                .map_err(|_| unavailable())?;
            Ok(json!({"outbox_id":entry.id}))
        })?;
        let id = receipt["receipt"]["outbox_id"]
            .as_i64()
            .ok_or_else(unavailable)?;
        let entry = store.outbox_entry(id).map_err(|_| unavailable())?;
        let delivery = match entry.state {
            DeliveryState::Sent => json!({"state":"sent"}),
            DeliveryState::Unknown => {
                json!({"state":"unknown","meaning":"It may have reached them. Do not react again."})
            }
            _ if entry.attempts > 0 => {
                json!({"state":"refused","meaning":"The channel did not send it.","evidence":entry.evidence})
            }
            _ => json!({"state":"queued","meaning":"It goes out when this turn ends."}),
        };
        Ok(json!({"reaction":entry.event.body,"event_seq":entry.event.seq,"delivery":delivery}))
    }

    /// Copies of the files a message carries, where the host keeps them until
    /// the channel has sent them. A key's copies are made once, so a retried
    /// send matches the queued one and never rewrites a file being sent.
    /// Serve sends nothing older than ten minutes, so copies a day old go.
    fn outgoing_files(
        &self,
        files: &[String],
        key: &str,
    ) -> std::result::Result<Vec<Attachment>, Fault> {
        if files.is_empty() {
            return Ok(vec![]);
        }
        let paths = self.paths().map_err(|_| unavailable())?;
        let outgoing = paths.runtime.join("outgoing");
        private_directory(&outgoing).map_err(|_| unavailable())?;
        sweep(&outgoing, Duration::from_secs(24 * 3600));
        let folder = outgoing.join(digest(&key).map_err(|_| unavailable())?);
        private_directory(&folder).map_err(|_| unavailable())?;
        let mut copies = Vec::new();
        for (index, file) in files.iter().enumerate() {
            let source = Path::new(file);
            let Some(name) = source
                .file_name()
                .and_then(|name| name.to_str())
                .filter(|_| source.is_absolute())
            else {
                return Err(invalid());
            };
            let copy = folder.join(format!("{index}-{name}"));
            if !copy.exists() {
                copy_capped(source, &copy).map_err(|_| invalid())?;
            }
            copies.push(Attachment {
                path: copy.to_string_lossy().into(),
                mime_type: mime_type(name).map(str::to_owned),
                name: Some(name.to_owned()),
            });
        }
        Ok(copies)
    }

    /// Wait for serve to deliver the message, so the reply can say what
    /// happened. Serve never sends one it finds too old, so a message queued
    /// while it was down is not delivered long after it was asked for.
    fn await_delivery(&self, store: &Store, outbox_id: i64) -> std::result::Result<Value, Fault> {
        let deadline = Instant::now() + DELIVERY_WAIT;
        loop {
            let entry = store.outbox_entry(outbox_id).map_err(|_| unavailable())?;
            let settled = match (entry.state, entry.attempts) {
                (DeliveryState::Sent, _) => {
                    Some(json!({"state":"sent","native_id":entry.native_id}))
                }
                (DeliveryState::Unknown, _) => Some(
                    json!({"state":"unknown","meaning":"It may have been delivered. Do not send it again; tell the owner it is unconfirmed."}),
                ),
                (DeliveryState::Queued, attempts) if attempts > 0 => Some(
                    json!({"state":"refused","meaning":"The channel refused it; it was not sent.","evidence":entry.evidence}),
                ),
                _ => None,
            };
            if let Some(settled) = settled {
                return Ok(settled);
            }
            if Instant::now() >= deadline {
                return Ok(match entry.state {
                    DeliveryState::Sending => {
                        json!({"state":"sending","meaning":"It is being sent; its outcome is not known yet. Do not send it again."})
                    }
                    _ => {
                        json!({"state":"queued","meaning":"Not delivered yet. It goes out when that conversation is free, if within ten minutes of now, and otherwise never: say it is on its way, not that it was sent."})
                    }
                });
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }
}

/// The media type a channel needs to show a file as a photo or video.
fn mime_type(name: &str) -> Option<&'static str> {
    let extension = name.rsplit_once('.')?.1.to_ascii_lowercase();
    Some(match extension.as_str() {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "heic" => "image/heic",
        "pdf" => "application/pdf",
        "html" | "htm" => "text/html",
        "mp4" => "video/mp4",
        "mov" => "video/quicktime",
        _ => return None,
    })
}

/// Copy a regular file of at most `FILE_BYTES`, read from the file as opened:
/// a pipe in its place is refused without waiting, and one that grows past the
/// limit meanwhile is refused too. Nothing is left on a refusal.
fn copy_capped(source: &Path, copy: &Path) -> std::io::Result<()> {
    use std::io::{Error, ErrorKind, Read};
    use std::os::unix::fs::OpenOptionsExt;
    let file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(source)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > FILE_BYTES {
        return Err(Error::from(ErrorKind::InvalidInput));
    }
    let partial = copy.with_extension("partial");
    let copied = std::io::copy(
        &mut file.take(FILE_BYTES + 1),
        &mut fs::File::create(&partial)?,
    )
    .and_then(|bytes| match bytes > FILE_BYTES {
        true => Err(Error::from(ErrorKind::InvalidInput)),
        false => fs::rename(&partial, copy),
    });
    if copied.is_err() {
        let _ = fs::remove_file(&partial);
    }
    copied
}

/// Remove the folders of copies older than `age`, as far as the folder can be
/// read; a failure here never stops a send.
fn sweep(outgoing: &Path, age: Duration) {
    for entry in fs::read_dir(outgoing).into_iter().flatten().flatten() {
        let old = entry
            .metadata()
            .and_then(|metadata| metadata.modified())
            .is_ok_and(|modified| modified.elapsed().is_ok_and(|elapsed| elapsed > age));
        if old {
            let _ = fs::remove_dir_all(entry.path());
        }
    }
}

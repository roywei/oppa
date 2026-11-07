//! Schedule tools: standing work the owner asks for in conversation. A
//! schedule fires its instruction into a conversation at a local time, once or
//! on repeating days, as a turn with the owner's authority, where the model
//! does the work and writes whatever is said. Only an owner conversation turn
//! may make or cancel one, and at `read` access only in its own conversation.

use super::{denied, invalid, tool, unavailable, Broker, Fault, RunGrant, TaskAccess, ToolCall};
use crate::{
    schedule::{self, Schedule, ScheduleStore},
    store::Store,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ScheduleCreate {
    context: String,
    conversation: String,
    at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    date: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    days: Vec<String>,
    prompt: String,
    idempotency_key: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ScheduleList {
    context: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ScheduleCancel {
    context: String,
    id: String,
}

pub(super) fn owners_account() -> Fault {
    Fault {
        code: "owners_account",
        message: "This conversation is on the owner's own account. You read it, but nothing is sent there, since it would speak as the owner. Tell the owner in your reply instead.",
    }
}

pub(super) fn not_deliverable() -> Fault {
    Fault {
        code: "not_deliverable",
        message: "Nothing is delivered in the console. Choose a conversation on a messaging channel, such as the owner's direct chat.",
    }
}

fn bad_time() -> Fault {
    Fault {
        code: "invalid_schedule",
        message: "Give at as HH:MM and a one-off's date as YYYY-MM-DD in the host's time zone (this turn's now), not in the past, with either a date or days, and a prompt that fits.",
    }
}

pub(super) fn definitions(grant: &RunGrant, context: &Value) -> Vec<Value> {
    let list = tool("schedules.list", "List the owner's schedules in conversations this run may read, with each one's next slot and the last slot it fired.", json!({"context":context}), &["context"], true);
    match grant.task_access {
        TaskAccess::None => return vec![],
        TaskAccess::Read => return vec![list],
        TaskAccess::Manage => {}
    }
    vec![
        tool("schedules.create", "Schedule standing work for the owner, such as a reminder or a message due later: at a local time (HH:MM in the host's time zone) on one date (YYYY-MM-DD) or on repeating days (every day when none are given), the host fires `prompt` into the conversation as an owner turn, and you then do the work and write the message there. Name the conversation from history.conversations: where the owner asked, their own direct chat, or a group you are in. Reuse the idempotency_key to get the same schedule back.", json!({"context":context,"conversation":{"type":"string","maxLength":4096},"at":{"type":"string","pattern":"^[0-2][0-9]:[0-5][0-9]$"},"date":{"type":"string","pattern":"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"},"days":{"type":"array","items":{"type":"string","enum":["mon","tue","wed","thu","fri","sat","sun"]},"maxItems":7},"prompt":{"type":"string","minLength":1,"maxLength":8000,"description":"The instruction for that later turn in plain words, with everything it needs: for whom, and what to say or do."},"idempotency_key":{"type":"string","minLength":1,"maxLength":256}}), &["context","conversation","at","prompt","idempotency_key"], false),
        list,
        tool("schedules.cancel", "Cancel a schedule by id so it never fires again; slots it already fired keep their records.", json!({"context":context,"id":{"type":"string","maxLength":64}}), &["context","id"], false),
    ]
}

impl Broker {
    /// Changing schedules is an owner turn's; a read-only run may list them.
    fn schedule_authorize(&self, context: &str, change: bool) -> std::result::Result<(), Fault> {
        self.authorize(context)?;
        let needed = if change {
            TaskAccess::Manage
        } else {
            TaskAccess::Read
        };
        if self.launch.grant.task_access < needed {
            return Err(denied());
        }
        Ok(())
    }

    /// A conversation a schedule may fire into: one this run may read, on a
    /// channel that delivers, and at `read` access only the run's own.
    fn schedule_target(&self, conversation: &str) -> std::result::Result<(), Fault> {
        self.conversation_allowed(conversation)?;
        let store = Store::open(&self.launch.database).map_err(|_| unavailable())?;
        let target = store.conversation(conversation).map_err(|_| denied())?;
        if target.channel == "console" {
            return Err(not_deliverable());
        }
        if self.on_owners_account(&target.channel, &target.account) {
            return Err(owners_account());
        }
        let grant = &self.launch.grant;
        if grant.access.is_read() && !grant.conversations.iter().any(|c| c == conversation) {
            return Err(Fault {
                code: "access_denied",
                message: "At read access a schedule can only fire in this conversation.",
            });
        }
        Ok(())
    }

    pub(super) fn schedule_create(&self, args: ScheduleCreate) -> ToolCall {
        self.schedule_authorize(&args.context, true)?;
        if args.idempotency_key.trim().is_empty() || args.idempotency_key.len() > 256 {
            return Err(invalid());
        }
        self.schedule_target(&args.conversation)?;
        let seed = format!("{}\n{}", self.launch.grant.input_seq, args.idempotency_key);
        let schedule = Schedule {
            id: schedule::new_id(Some(&seed)).map_err(|_| unavailable())?,
            at: args.at.clone(),
            date: args.date.clone(),
            days: schedule::parse_days(&args.days).map_err(|_| bad_time())?,
            conversation: args.conversation.clone(),
            prompt: args.prompt.clone(),
            command: vec![],
            timeout_seconds: 300,
            window_minutes: 60,
            enabled: true,
            wake: false,
        };
        let paths = self.paths().map_err(|_| unavailable())?;
        let store = ScheduleStore::open(&paths).map_err(|_| unavailable())?;
        // Refusals that change nothing come before the journal.
        store
            .check(&schedule)
            .and_then(|()| schedule::ensure_future(&schedule))
            .map_err(|_| bad_time())?;
        let origin = Some(self.launch.grant.input_seq);
        // Outward: at its time it acts with the owner's authority, sends included.
        self.journaled_effect("schedules.create", "outward", &args, || {
            let (stored, _) = store
                .add(&schedule, "assistant", origin)
                .map_err(|_| invalid())?;
            let events = Store::open(&self.launch.database).map_err(|_| unavailable())?;
            schedule::report(&paths, &events, &stored).map_err(|_| unavailable())
        })
    }

    pub(super) fn schedule_list(&self, args: ScheduleList) -> ToolCall {
        self.schedule_authorize(&args.context, false)?;
        self.note_read()?;
        let paths = self.paths().map_err(|_| unavailable())?;
        let store = ScheduleStore::open(&paths).map_err(|_| unavailable())?;
        let events = Store::open(&self.launch.database).map_err(|_| unavailable())?;
        let mut listed = Vec::new();
        // Wakes are the assistant's own looks, listed by wakes.list.
        for stored in store.list(false).map_err(|_| unavailable())? {
            if !stored.schedule.wake
                && self
                    .conversation_allowed(&stored.schedule.conversation)
                    .is_ok()
            {
                listed.push(schedule::report(&paths, &events, &stored).map_err(|_| unavailable())?);
            }
        }
        Ok(
            json!({"source":self.source(),"now":chrono::Local::now().to_rfc3339(),"schedules":listed}),
        )
    }

    pub(super) fn schedule_cancel(&self, args: ScheduleCancel) -> ToolCall {
        self.schedule_authorize(&args.context, true)?;
        let paths = self.paths().map_err(|_| unavailable())?;
        let store = ScheduleStore::open(&paths).map_err(|_| unavailable())?;
        let stored = store.get(&args.id).map_err(|_| invalid())?;
        self.conversation_allowed(&stored.schedule.conversation)?;
        let grant = &self.launch.grant;
        if grant.access.is_read()
            && !grant
                .conversations
                .iter()
                .any(|c| *c == stored.schedule.conversation)
        {
            return Err(Fault {
                code: "access_denied",
                message: "At read access only this conversation's schedules can be cancelled.",
            });
        }
        self.journaled_effect("schedules.cancel", "reversible", &args, || {
            let cancelled = store.cancel(&args.id).map_err(|_| unavailable())?;
            let events = Store::open(&self.launch.database).map_err(|_| unavailable())?;
            schedule::report(&paths, &events, &cancelled).map_err(|_| unavailable())
        })
    }
}

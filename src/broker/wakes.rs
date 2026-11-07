//! Wake tools: looks the assistant sets for itself. A wake fires once, at a
//! local time in the heartbeat's waking hours, as a read-only look in the
//! heartbeat's chat with the reason it was set for as its input, and speaks
//! only when the owner needs it. The model owns when and why; the host owns
//! how far: ten minutes to fourteen days ahead, eight pending, and twelve set
//! in any day, so a chain of wakes that each set the next looks at most twelve
//! times a day. Only the owner's conversation turns and the heartbeat's own
//! looks set wakes, and none runs while no heartbeat does.

use super::{denied, invalid, tool, unavailable, Broker, Fault, RunGrant, ToolCall};
use crate::{
    heartbeat::Heartbeat,
    schedule::{self, Schedule, ScheduleStore, StoredSchedule},
    store::Store,
};
use chrono::{Local, TimeDelta, TimeZone};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

const SOONEST_MINUTES: i64 = 10;
const LATEST_DAYS: i64 = 14;
const PENDING: usize = 8;
const IN_A_DAY: usize = 12;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WakeSet {
    context: String,
    date: String,
    at: String,
    reason: String,
    idempotency_key: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WakeList {
    context: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WakeCancel {
    context: String,
    id: String,
}

fn bad_time() -> Fault {
    Fault {
        code: "invalid_wake",
        message: "Give date as YYYY-MM-DD and at as HH:MM in the host's time zone (this turn's now): at least 10 minutes from now, at most 14 days ahead, in the heartbeat's waking hours, with a reason that fits.",
    }
}

fn too_many() -> Fault {
    Fault {
        code: "too_many_wakes",
        message: "At most 8 wakes may be pending, and 12 set in a day. Fold this check into a wake already set, or cancel one that no longer matters.",
    }
}

fn no_heartbeat() -> Fault {
    Fault {
        code: "no_heartbeat",
        message: "A wake looks in the heartbeat's chat, and no heartbeat runs there: the owner turned it off, nothing is connected, the owner has not written to you directly yet, or the chat the owner named is not their direct chat. Anything the owner asked for at a set time is a schedule.",
    }
}

pub(super) fn definitions(grant: &RunGrant, context: &Value) -> Vec<Value> {
    if !grant.wakes {
        return vec![];
    }
    vec![
        tool("wakes.set", "Wake yourself for a look at a time you choose, when something you saw will need checking then: a sale that opens, a delivery window, a reply someone promised by noon. At date and at (local, in the host's time zone and the heartbeat's waking hours, 10 minutes to 14 days ahead) the host starts a read-only look like a heartbeat in the owner's direct chat, with reason as its input. Anything the owner asked to happen at a set time is a schedule instead. Reuse the idempotency_key to get the same wake back.", json!({"context":context,"date":{"type":"string","pattern":"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"},"at":{"type":"string","pattern":"^[0-2][0-9]:[0-5][0-9]$"},"reason":{"type":"string","minLength":1,"maxLength":2000,"description":"What to check then and why, with everything that look needs: where to look, what would be worth telling the owner, and when to stop looking."},"idempotency_key":{"type":"string","minLength":1,"maxLength":256}}), &["context","date","at","reason","idempotency_key"], false),
        tool("wakes.list", "List the wakes still to come, each with its id, time and reason.", json!({"context":context}), &["context"], true),
        tool("wakes.cancel", "Cancel a wake by id when what it was set for no longer matters.", json!({"context":context,"id":{"type":"string","maxLength":64}}), &["context","id"], false),
    ]
}

/// Whether a wake is still to come at `now`.
fn pending(stored: &StoredSchedule, now: &chrono::DateTime<Local>) -> bool {
    stored.schedule.wake
        && stored.cancelled_at.is_none()
        && stored.schedule.next_slot(now).is_some()
}

impl Broker {
    fn wake_authorize(&self, context: &str) -> std::result::Result<(), Fault> {
        self.authorize(context)?;
        if !self.launch.grant.wakes {
            return Err(denied());
        }
        Ok(())
    }

    /// The heartbeat a wake looks with: its chat, one it may speak in, and
    /// its waking hours.
    fn wake_heartbeat(&self) -> std::result::Result<Heartbeat, Fault> {
        let config = self
            .paths()
            .and_then(|paths| paths.load())
            .map_err(|_| unavailable())?;
        let store = Store::open(&self.launch.database).map_err(|_| unavailable())?;
        let heartbeat = Heartbeat::running(&config, &store)
            .map_err(|_| unavailable())?
            .ok_or_else(no_heartbeat)?;
        crate::heartbeat::owners_chat(&store, &heartbeat.conversation, &config.owner)
            .map_err(|_| no_heartbeat())?;
        Ok(heartbeat)
    }

    pub(super) fn wake_set(&self, args: WakeSet) -> ToolCall {
        self.wake_authorize(&args.context)?;
        let heartbeat = self.wake_heartbeat()?;
        if args.idempotency_key.trim().is_empty() || args.idempotency_key.len() > 256 {
            return Err(invalid());
        }
        let seed = format!(
            "wake\n{}\n{}",
            self.launch.grant.input_seq, args.idempotency_key
        );
        let id = schedule::new_id(Some(&seed)).map_err(|_| unavailable())?;
        let paths = self.paths().map_err(|_| unavailable())?;
        let store = ScheduleStore::open(&paths).map_err(|_| unavailable())?;
        let now = Local::now();
        // A wake set before is that wake again, whatever the clock or the
        // heartbeat's chat says now.
        let wake = match store.get(&id) {
            Ok(stored) if stored.schedule.wake => stored.schedule,
            Ok(_) => return Err(invalid()),
            Err(_) => {
                let wake = Schedule {
                    id,
                    at: args.at.clone(),
                    date: Some(args.date.clone()),
                    days: vec![],
                    conversation: heartbeat.conversation.clone(),
                    prompt: args.reason.clone(),
                    command: vec![],
                    timeout_seconds: 300,
                    window_minutes: 30,
                    enabled: true,
                    wake: true,
                };
                let slot = wake
                    .one_off_date()
                    .zip(wake.time())
                    .and_then(|(date, at)| Local.from_local_datetime(&date.and_time(at)).earliest())
                    .ok_or_else(bad_time)?;
                if slot < now + TimeDelta::minutes(SOONEST_MINUTES)
                    || slot > now + TimeDelta::days(LATEST_DAYS)
                    || !heartbeat.awake(slot.time())
                {
                    return Err(bad_time());
                }
                store.check(&wake).map_err(|_| bad_time())?;
                wake
            }
        };
        // Counted in the add's own transaction, so two runs cannot both take
        // the last room; cancelled wakes count toward the day's twelve.
        let room = |kept: &[StoredSchedule]| {
            let day_ago = now - TimeDelta::days(1);
            let wakes = || kept.iter().filter(|stored| stored.schedule.wake);
            let recent = wakes()
                .filter(|stored| {
                    chrono::DateTime::parse_from_rfc3339(&stored.created_at)
                        .is_ok_and(|created| created >= day_ago)
                })
                .count();
            let waiting = wakes().filter(|stored| pending(stored, &now)).count();
            waiting < PENDING && recent < IN_A_DAY
        };
        let origin = Some(self.launch.grant.input_seq);
        // Reversible: it only looks, and is cancelled before it fires.
        self.journaled_effect("wakes.set", "reversible", &args, || {
            let (stored, _) = store
                .add_within(&wake, "assistant", origin, room)
                .map_err(|_| invalid())?
                .ok_or_else(too_many)?;
            let events = Store::open(&self.launch.database).map_err(|_| unavailable())?;
            schedule::report(&paths, &events, &stored).map_err(|_| unavailable())
        })
    }

    pub(super) fn wake_list(&self, args: WakeList) -> ToolCall {
        self.wake_authorize(&args.context)?;
        self.note_read()?;
        let paths = self.paths().map_err(|_| unavailable())?;
        let store = ScheduleStore::open(&paths).map_err(|_| unavailable())?;
        let events = Store::open(&self.launch.database).map_err(|_| unavailable())?;
        let now = Local::now();
        let mut listed = Vec::new();
        for stored in store.list(false).map_err(|_| unavailable())? {
            if pending(&stored, &now) {
                listed.push(schedule::report(&paths, &events, &stored).map_err(|_| unavailable())?);
            }
        }
        Ok(json!({"source":self.source(),"now":now.to_rfc3339(),"wakes":listed}))
    }

    pub(super) fn wake_cancel(&self, args: WakeCancel) -> ToolCall {
        self.wake_authorize(&args.context)?;
        let paths = self.paths().map_err(|_| unavailable())?;
        let store = ScheduleStore::open(&paths).map_err(|_| unavailable())?;
        let stored = store.get(&args.id).map_err(|_| invalid())?;
        if !stored.schedule.wake {
            return Err(invalid());
        }
        self.journaled_effect("wakes.cancel", "reversible", &args, || {
            let cancelled = store.cancel(&args.id).map_err(|_| unavailable())?;
            let events = Store::open(&self.launch.database).map_err(|_| unavailable())?;
            schedule::report(&paths, &events, &cancelled).map_err(|_| unavailable())
        })
    }
}

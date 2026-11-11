//! Standing instructions: the owner's, made in conversation, the dashboard or
//! the CLI, and kept in the instance's state database. The host decides when a
//! slot is due and runs an owner's command; the conversation model does the
//! work and writes any message.
use crate::{
    config::{ChannelManifest, Paths},
    harness::OwnedProcess,
    prompt::BODY_CHARS,
    runtime::{atomic_json, ConversationIdentity},
    store::Store,
};
use anyhow::{bail, ensure, Context, Result};
use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, TimeDelta, TimeZone, Utc, Weekday};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::Digest;
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncReadExt};

const MAX_STDOUT: usize = 64 * 1024;
const MAX_STDERR: usize = 8 * 1024;
/// How much of a watch run's output its record keeps.
const RECORD_CHARS: usize = 1000;
/// The failure a slot reports when its command started and left no result.
const CUT_OFF: &str = "it started for this slot before and recorded no result, so it was not \
    run again; oppa may have stopped while it ran";
/// A watch's quiet time after a turn when none is given.
pub const DEFAULT_COOLDOWN: u32 = 30;

/// A watch: the schedule's command runs every `every_minutes` from its `at`
/// until `until` on its days, with no turn, and a run that asks for one or
/// fails starts the schedule's turn, at most one per `cooldown_minutes`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Watch {
    pub every_minutes: u32,
    /// Host local time, `HH:MM`: the first one after `at`, so a time not
    /// later than `at` closes the window the next day.
    pub until: String,
    #[serde(default = "default_cooldown")]
    pub cooldown_minutes: u32,
}
fn default_cooldown() -> u32 {
    DEFAULT_COOLDOWN
}

impl Watch {
    pub fn end(&self) -> Option<NaiveTime> {
        NaiveTime::parse_from_str(&self.until, "%H:%M").ok()
    }
}

/// One schedule: a local time, on one date or on repeating days.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Schedule {
    pub id: String,
    /// Host local time, `HH:MM`.
    pub at: String,
    /// A one-off's local date, `YYYY-MM-DD`; absent for a repeating schedule.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub date: Option<String>,
    /// The days it runs; empty means every day. A one-off has none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub days: Vec<Weekday>,
    /// The stored conversation id: `console` or `<channel>/<account>/<native id>`.
    pub conversation: String,
    /// The owner's standing instruction.
    pub prompt: String,
    /// Argv run without a shell before the turn; empty means none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command: Vec<String>,
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,
    /// A slot missed by more than this is not run late.
    #[serde(default = "default_window")]
    pub window_minutes: u32,
    #[serde(default = "enabled")]
    pub enabled: bool,
    /// A look the assistant set for itself (`wakes.set`): a one-off in the
    /// heartbeat's chat that fires read-only like a heartbeat, its prompt the
    /// reason, instead of an owner turn.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub wake: bool,
    /// Set for a watch, whose command runs every few minutes without a turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watch: Option<Watch>,
}
fn default_timeout() -> u64 {
    300
}
fn default_window() -> u32 {
    60
}
fn enabled() -> bool {
    true
}

impl Schedule {
    pub fn validate(&self, channels: &[ChannelManifest]) -> Result<()> {
        let id = &self.id;
        ensure!(
            !id.is_empty()
                && id.len() <= 64
                && id
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
            "a schedule id must contain letters, digits, '-' or '_'"
        );
        ensure!(
            self.time().is_some(),
            "schedule {id}: at must be a local HH:MM time"
        );
        ensure!(
            self.date.is_none() || (self.one_off_date().is_some() && self.days.is_empty()),
            "schedule {id}: date must be a YYYY-MM-DD local date, without days"
        );
        let destination = match self.channel_destination() {
            None => self.conversation == "console",
            Some(_) => crate::config::is_channel_conversation(&self.conversation, channels),
        };
        ensure!(
            destination,
            "schedule {id}: conversation must be \"console\" or <channel id>/<account>/<native id> of a channel on the assistant's own account"
        );
        // A turn shows the first BODY_CHARS characters of an event, so the
        // instruction must fit there whole, ahead of any command output. A
        // slot is whole minutes with an offset, as long as this sample.
        let head = EventBody {
            schedule: id,
            slot: "2026-01-01T00:00:00+00:00".into(),
            prompt: &self.prompt,
            command: None,
        };
        ensure!(
            !self.prompt.trim().is_empty()
                && !self.prompt.contains('\0')
                && serde_json::to_string(&head)?.chars().count() <= BODY_CHARS,
            "schedule {id}: prompt must be text that fits in the first {BODY_CHARS} characters of its event"
        );
        ensure!(
            self.command
                .first()
                .is_none_or(|program| !program.is_empty())
                && self.command.iter().all(|arg| !arg.contains('\0')),
            "schedule {id}: command must be an argv that starts with a program"
        );
        ensure!(
            !self.wake
                || (self.one_off_date().is_some()
                    && self.command.is_empty()
                    && self.channel_destination().is_some()),
            "schedule {id}: a wake is a one-off in a channel conversation, without a command"
        );
        if let Some(watch) = &self.watch {
            ensure!(
                !self.command.is_empty(),
                "schedule {id}: a watch needs a command to run"
            );
            ensure!(
                (1..=60).contains(&watch.every_minutes),
                "schedule {id}: a watch's every_minutes must be 1..=60"
            );
            ensure!(
                watch.end().is_some(),
                "schedule {id}: a watch's until must be a local HH:MM time"
            );
            ensure!(
                (5..=1440).contains(&watch.cooldown_minutes),
                "schedule {id}: a watch's cooldown_minutes must be 5..=1440"
            );
        }
        ensure!(
            (1..=3600).contains(&self.timeout_seconds),
            "schedule {id}: timeout_seconds must be 1..=3600"
        );
        ensure!(
            (1..=1440).contains(&self.window_minutes),
            "schedule {id}: window_minutes must be 1..=1440"
        );
        Ok(())
    }

    pub fn time(&self) -> Option<NaiveTime> {
        NaiveTime::parse_from_str(&self.at, "%H:%M").ok()
    }

    pub fn one_off_date(&self) -> Option<NaiveDate> {
        NaiveDate::parse_from_str(self.date.as_deref()?, "%Y-%m-%d").ok()
    }

    /// Channel id, account and native conversation id; `None` for the console.
    pub fn channel_destination(&self) -> Option<(&str, &str, &str)> {
        channel_conversation(&self.conversation)
    }

    /// The conversation this schedule names, as the host recorded it.
    pub fn destination(&self, store: &Store) -> Result<ConversationIdentity> {
        if self.channel_destination().is_none() {
            return Ok(ConversationIdentity::console(&self.conversation));
        }
        known_conversation(store, &self.conversation)
    }

    /// The event body for one slot: the standing instruction and, when there
    /// is a command, what it printed or why it could not. A command runs at
    /// most once per slot: a durable record in `runs` precedes it, and a slot
    /// that has the record but no event was cut off while its command ran
    /// (the process stopped or its event could not be recorded), so the body
    /// reports that instead of running the command again.
    pub async fn event_body(&self, slot: &DateTime<impl TimeZone>, runs: &Path) -> Result<String> {
        let started = runs.join(format!("schedule-{}-{}.json", self.id, slot.timestamp()));
        let command = if self.command.is_empty() {
            None
        } else if started.try_exists()? {
            Some(CommandReport {
                argv: self.command.clone(),
                failure: Some(CUT_OFF.into()),
                ..Default::default()
            })
        } else {
            let record = json!({"schedule":self.id,"slot":slot.to_rfc3339(),"argv":self.command,
                "state":"started","at":chrono::Utc::now().to_rfc3339()});
            atomic_json(&started, &record)?;
            Some(run_command(&self.command, self.timeout_seconds).await)
        };
        Ok(serde_json::to_string(&EventBody {
            schedule: &self.id,
            slot: slot.to_rfc3339(),
            prompt: &self.prompt,
            command,
        })?)
    }

    /// Run a watch's command for `slot`, with no turn, and return the event
    /// body its turn gets when the run asks for one or fails; None when it
    /// was quiet. A run may take its interval at most, so runs never overlap.
    /// Its record, `runs/watch-<id>.json`, holds the latest run and is written
    /// before the command starts, so a slot runs once, even across a restart.
    pub async fn watch_run(
        &self,
        slot: &DateTime<impl TimeZone>,
        runs: &Path,
    ) -> Result<Option<String>> {
        let every = self.watch.as_ref().context("not a watch")?.every_minutes;
        let record = watch_record(runs, &self.id);
        let slot_text = slot.to_rfc3339();
        let last: Option<Value> = std::fs::read(&record)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok());
        if last.is_some_and(|last| last["slot"] == slot_text.as_str()) {
            return Ok(None);
        }
        let started_at = Utc::now().to_rfc3339();
        atomic_json(
            &record,
            &json!({"schedule":self.id,"slot":slot_text,"state":"started","started_at":started_at}),
        )?;
        let timeout = self.timeout_seconds.min(u64::from(every) * 60);
        let report = run_command(&self.command, timeout).await;
        let stdout = report.stdout.as_deref().unwrap_or_default();
        let state = match (report.ok, asks_for_turn(stdout)) {
            (false, _) => "failed",
            (true, true) => "triggered",
            (true, false) => "quiet",
        };
        // The head of the output and the tail of the errors, as a turn gets them.
        let stderr = report.stderr.as_deref().unwrap_or_default();
        let skipped = stderr.chars().count().saturating_sub(RECORD_CHARS);
        atomic_json(
            &record,
            &json!({"schedule":self.id,"slot":slot_text,"state":state,"started_at":started_at,
                "finished_at":Utc::now().to_rfc3339(),"exit_status":report.exit_status,
                "failure":report.failure,"stdout":stdout.chars().take(RECORD_CHARS).collect::<String>(),
                "stderr":stderr.chars().skip(skipped).collect::<String>()}),
        )?;
        if state == "quiet" {
            return Ok(None);
        }
        Ok(Some(serde_json::to_string(&EventBody {
            schedule: &self.id,
            slot: slot_text,
            prompt: &self.prompt,
            command: Some(report),
        })?))
    }

    /// The slot owed at `now`: the latest configured time on an allowed day at
    /// or before `now`, if `now` is at most `window_minutes` past it. A
    /// watch's is its latest run time in the window `now` is in, owed until
    /// the next.
    pub fn due_slot<Tz: TimeZone>(&self, now: &DateTime<Tz>) -> Option<DateTime<Tz>> {
        if let Some(watch) = &self.watch {
            let (start, _) = self.window(watch, now)?;
            let every = i64::from(watch.every_minutes);
            let runs = now.clone().signed_duration_since(&start).num_minutes() / every;
            return Some(start + TimeDelta::minutes(runs * every));
        }
        let latest = self.slots(now, -1).find(|slot| slot <= now)?;
        let late = now.clone().signed_duration_since(&latest);
        (late <= TimeDelta::minutes(self.window_minutes.into())).then_some(latest)
    }

    /// The first slot after `now`.
    pub fn next_slot<Tz: TimeZone>(&self, now: &DateTime<Tz>) -> Option<DateTime<Tz>> {
        if let Some(watch) = &self.watch {
            let every = TimeDelta::minutes(watch.every_minutes.into());
            let in_window = self.window(watch, now).and_then(|(_, end)| {
                let next = self.due_slot(now)? + every;
                (next < end).then_some(next)
            });
            return in_window.or_else(|| {
                self.windows(watch, now, 1)
                    .map(|(start, _)| start)
                    .find(|start| start > now)
            });
        }
        self.slots(now, 1).find(|slot| slot > now)
    }

    /// The watch window `now` is in, as its start and end.
    fn window<Tz: TimeZone>(
        &self,
        watch: &Watch,
        now: &DateTime<Tz>,
    ) -> Option<(DateTime<Tz>, DateTime<Tz>)> {
        self.windows(watch, now, -1)
            .find(|(start, end)| start <= now && now < end)
    }

    /// A watch's windows, in the direction of `step`: each from a slot that
    /// `slots` gives until the next `until`, which ends a window that runs
    /// past midnight on the next day. Clock changes move neither end, and
    /// runs keep their interval in real time across them. A day whose `until`
    /// does not exist (clocks spring forward) has no window, as one whose
    /// `at` does not.
    fn windows<'a, Tz: TimeZone>(
        &'a self,
        watch: &'a Watch,
        now: &'a DateTime<Tz>,
        step: i64,
    ) -> impl Iterator<Item = (DateTime<Tz>, DateTime<Tz>)> + 'a {
        self.slots(now, step).filter_map(move |start| {
            let until = watch.end()?;
            let date = if until > self.time()? {
                start.date_naive()
            } else {
                start.date_naive().succ_opt()?
            };
            let end = now
                .timezone()
                .from_local_datetime(&date.and_time(until))
                .earliest()?;
            Some((start, end))
        })
    }

    /// Slots on the fifteen local days from `now`'s date in the direction of
    /// `step`, enough to pass a weekly slot that a clock change skipped. A time
    /// that does not exist on a day (clocks spring forward) skips that day; an
    /// ambiguous one (clocks fall back) is its earlier instant, so it fires once.
    /// A one-off has its one slot however far away it is.
    fn slots<'a, Tz: TimeZone>(
        &'a self,
        now: &'a DateTime<Tz>,
        step: i64,
    ) -> impl Iterator<Item = DateTime<Tz>> + 'a {
        let today = now.date_naive();
        let one_off = self.one_off_date();
        let days = match one_off {
            Some(_) => 0..1,
            None => 0..15,
        };
        days.filter_map(move |offset| {
            let date = match one_off {
                Some(date) => date,
                None => today.checked_add_signed(TimeDelta::days(offset * step))?,
            };
            if !self.days.is_empty() && !self.days.contains(&date.weekday()) {
                return None;
            }
            now.timezone()
                .from_local_datetime(&date.and_time(self.time()?))
                .earliest()
        })
    }
}

/// Channel id, account and native id of a stored channel conversation id;
/// `None` for the console.
pub fn channel_conversation(conversation: &str) -> Option<(&str, &str, &str)> {
    let mut parts = conversation.splitn(3, '/');
    Some((parts.next()?, parts.next()?, parts.next()?))
}

/// A channel conversation as the host recorded it. It must already be known
/// from its channel: the host never guesses whether a chat it has not seen is
/// direct or a group.
pub fn known_conversation(store: &Store, conversation: &str) -> Result<ConversationIdentity> {
    let (channel, account, native_id) =
        channel_conversation(conversation).context("not a channel conversation")?;
    let known = store
        .conversation(conversation)
        .ok()
        .filter(|known| {
            (known.channel.as_str(), known.account.as_str(), known.native_id.as_str())
                == (channel, account, native_id)
        })
        .with_context(|| {
            format!(
                "{conversation} has not been seen on its channel yet; nothing fires there until a message arrives"
            )
        })?;
    Ok(ConversationIdentity {
        id: known.id,
        channel: known.channel,
        account: known.account,
        native_id: known.native_id,
        kind: known.kind,
        title: known.title,
        members: None,
    })
}

/// A schedule as the instance keeps it: who made it, at whose request, and
/// when it was cancelled. A cancelled schedule never fires and stays listed.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct StoredSchedule {
    #[serde(flatten)]
    pub schedule: Schedule,
    /// `owner` (the CLI or dashboard) or `assistant` (asked in conversation).
    pub created_by: String,
    /// The owner input a schedule made in conversation answers.
    pub origin_seq: Option<i64>,
    pub created_at: String,
    pub cancelled_at: Option<String>,
}

/// The instance's schedules, in the state database beside the canonical log.
/// `oppa serve` reads them on every tick, so a change needs no restart.
pub struct ScheduleStore {
    connection: Connection,
    channels: Vec<ChannelManifest>,
}

impl ScheduleStore {
    pub fn open(paths: &Paths) -> Result<Self> {
        let channels = paths.load()?.channels;
        // The state store owns migration; a schedule command may run first.
        Store::open(&paths.database())?;
        let connection = Connection::open(paths.database())?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch(
            "PRAGMA foreign_keys=ON;
            CREATE TABLE IF NOT EXISTS schedules (
                id TEXT PRIMARY KEY,
                spec TEXT NOT NULL,
                created_by TEXT NOT NULL CHECK(created_by IN ('owner','assistant')),
                origin_seq INTEGER REFERENCES events(seq),
                created_at TEXT NOT NULL,
                cancelled_at TEXT
            );",
        )?;
        Ok(Self {
            connection,
            channels,
        })
    }

    /// Whether `schedule` is one this instance can keep: a valid shape for a
    /// configured channel, or the console.
    pub fn check(&self, schedule: &Schedule) -> Result<()> {
        schedule.validate(&self.channels)
    }

    /// Add a schedule, and say whether it is new. The same id with the same
    /// schedule and origin is that schedule again; anything else is refused.
    pub fn add(
        &self,
        schedule: &Schedule,
        created_by: &str,
        origin_seq: Option<i64>,
    ) -> Result<(StoredSchedule, bool)> {
        self.check(schedule)?;
        if let Some(existing) = self.find(&schedule.id)? {
            ensure!(
                existing.schedule == *schedule
                    && existing.created_by == created_by
                    && existing.origin_seq == origin_seq,
                "schedule id {} is already taken",
                schedule.id
            );
            return Ok((existing, false));
        }
        self.connection.execute(
            "INSERT INTO schedules(id,spec,created_by,origin_seq,created_at) VALUES(?1,?2,?3,?4,?5)",
            params![
                schedule.id,
                serde_json::to_string(schedule)?,
                created_by,
                origin_seq,
                Utc::now().to_rfc3339()
            ],
        )?;
        Ok((self.get(&schedule.id)?, true))
    }

    /// Add a schedule as `add` does, in one write transaction with the check
    /// `room` makes of every schedule kept, cancelled ones included, so two
    /// runs cannot both take the last room. None when there is none; the
    /// same schedule again is returned without a check.
    pub fn add_within(
        &self,
        schedule: &Schedule,
        created_by: &str,
        origin_seq: Option<i64>,
        room: impl FnOnce(&[StoredSchedule]) -> bool,
    ) -> Result<Option<(StoredSchedule, bool)>> {
        self.connection.execute_batch("BEGIN IMMEDIATE")?;
        let added = (|| {
            if self.find(&schedule.id)?.is_none() && !room(&self.list(true)?) {
                return Ok(None);
            }
            self.add(schedule, created_by, origin_seq).map(Some)
        })();
        let end = if added.is_ok() { "COMMIT" } else { "ROLLBACK" };
        self.connection.execute_batch(end)?;
        added
    }

    pub fn get(&self, id: &str) -> Result<StoredSchedule> {
        self.find(id)?.with_context(|| format!("no schedule {id}"))
    }

    fn find(&self, id: &str) -> Result<Option<StoredSchedule>> {
        let row = self
            .connection
            .query_row(
                "SELECT spec,created_by,origin_seq,created_at,cancelled_at FROM schedules WHERE id=?1",
                [id],
                stored_row,
            )
            .optional()?;
        row.map(parse_stored).transpose()
    }

    /// Schedules oldest first: those not cancelled, or with `cancelled` all.
    pub fn list(&self, cancelled: bool) -> Result<Vec<StoredSchedule>> {
        let mut statement = self.connection.prepare(
            "SELECT spec,created_by,origin_seq,created_at,cancelled_at FROM schedules
             WHERE ?1 OR cancelled_at IS NULL ORDER BY created_at,id",
        )?;
        let rows = statement.query_map([cancelled], stored_row)?;
        rows.map(|row| parse_stored(row?)).collect()
    }

    /// Cancel a schedule. A slot it already fired keeps its record; a second
    /// cancel changes nothing.
    pub fn cancel(&self, id: &str) -> Result<StoredSchedule> {
        self.get(id)?;
        self.connection.execute(
            "UPDATE schedules SET cancelled_at=?2 WHERE id=?1 AND cancelled_at IS NULL",
            params![id, Utc::now().to_rfc3339()],
        )?;
        self.get(id)
    }
}

type StoredRow = (String, String, Option<i64>, String, Option<String>);

fn stored_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredRow> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
    ))
}

fn parse_stored(
    (spec, created_by, origin_seq, created_at, cancelled_at): StoredRow,
) -> Result<StoredSchedule> {
    Ok(StoredSchedule {
        schedule: serde_json::from_str(&spec).context("stored schedule is unreadable")?,
        created_by,
        origin_seq,
        created_at,
        cancelled_at,
    })
}

/// A schedule as `oppa schedules`, the dashboard and the schedule tools show
/// it: with the next slot serve will fire, or what blocks it, and the last
/// slot fired with its turn's state from the turn marker.
pub fn report(paths: &Paths, store: &Store, stored: &StoredSchedule) -> Result<Value> {
    let schedule = &stored.schedule;
    let prefix = format!("schedule:{}:", schedule.id);
    let last = store.latest_inbound(&schedule.conversation, &prefix)?;
    // A recorded slot whose turn has not begun has no marker yet.
    let state = match &last {
        Some(event) => {
            Some(crate::runtime::turn_state(paths, event.seq)?.unwrap_or_else(|| "recorded".into()))
        }
        None => None,
    };
    let last = last.and_then(|event| event.native_id?.strip_prefix(&prefix).map(str::to_owned));
    // Serve fires only into a conversation its channel has reported.
    // Serve also never fires one on the owner's own account.
    let owners = schedule
        .channel_destination()
        .is_some_and(|(channel, _, _)| {
            paths
                .load()
                .is_ok_and(|config| config.owners_account(channel))
        });
    let blocked = if owners {
        Some("it names the owner's own account, where the assistant never speaks; cancel it".into())
    } else {
        schedule
            .channel_destination()
            .and_then(|_| schedule.destination(store).err())
            .map(|error| error.to_string())
    };
    let live = stored.cancelled_at.is_none() && schedule.enabled;
    let next = (live && schedule.channel_destination().is_some() && blocked.is_none())
        .then(|| schedule.next_slot(&chrono::Local::now()))
        .flatten()
        .map(|slot| slot.to_rfc3339());
    let mut value = serde_json::to_value(stored)?;
    let fields = value.as_object_mut().context("schedule report")?;
    fields.entry("date").or_insert(Value::Null);
    fields.entry("days").or_insert(json!([]));
    fields.entry("command").or_insert(json!([]));
    fields.insert("next_slot".into(), json!(next));
    fields.insert("blocked".into(), json!(blocked));
    fields.insert("last_fired_slot".into(), json!(last));
    fields.insert("last_state".into(), json!(state));
    // A watch's runs start no turn unless they ask: its record shows the latest.
    if schedule.watch.is_some() {
        let record = watch_record(&paths.runtime.join("runs"), &schedule.id);
        let last_run = std::fs::read(record)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
        fields.insert("last_run".into(), json!(last_run));
    }
    Ok(value)
}

/// The id a new schedule gets when nobody names one: a stable digest for a
/// request that may be repeated, or fresh random bytes.
pub fn new_id(seed: Option<&str>) -> Result<String> {
    let digest = match seed {
        Some(seed) => format!("{:x}", sha2::Sha256::digest(seed.as_bytes())),
        None => {
            let mut bytes = [0u8; 8];
            std::io::Read::read_exact(&mut std::fs::File::open("/dev/urandom")?, &mut bytes)?;
            bytes.iter().map(|b| format!("{b:02x}")).collect()
        }
    };
    Ok(format!("s-{}", &digest[..12]))
}

/// Parse `mon,wed` (or `Monday`) into weekdays; empty means every day.
pub fn parse_days(days: &[String]) -> Result<Vec<Weekday>> {
    days.iter()
        .flat_map(|day| day.split(','))
        .map(str::trim)
        .filter(|day| !day.is_empty())
        .map(|day| {
            day.parse::<Weekday>()
                .map_err(|_| anyhow::anyhow!("{day} is not a weekday"))
        })
        .collect()
}

/// Refuse a one-off whose slot has already passed, since it would never fire.
pub fn ensure_future(schedule: &Schedule) -> Result<()> {
    if schedule.one_off_date().is_none() {
        return Ok(());
    }
    let now = chrono::Local::now();
    match schedule.next_slot(&(now - TimeDelta::minutes(1))) {
        Some(_) => Ok(()),
        None => bail!(
            "{} {} has already passed (it is now {})",
            schedule.date.as_deref().unwrap_or_default(),
            schedule.at,
            now.format("%Y-%m-%d %H:%M")
        ),
    }
}

/// A slot's event body. Fields serialize in declaration order, unlike a JSON
/// map, so the instruction always precedes output that a turn's view clips.
#[derive(Serialize)]
struct EventBody<'a> {
    schedule: &'a str,
    slot: String,
    prompt: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    command: Option<CommandReport>,
}

/// What a slot's turn learns about its command: the outcome first, then the
/// error tail, then the output, which is the longest and is clipped first.
#[derive(Default, Serialize)]
struct CommandReport {
    argv: Vec<String>,
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    failure: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exit_status: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stderr_truncated: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stderr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stdout_truncated: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stdout: Option<String>,
}

/// Where a watch keeps the record of its latest run.
fn watch_record(runs: &Path, id: &str) -> PathBuf {
    runs.join(format!("watch-{id}.json"))
}

/// Whether a watch run's output asks for a turn: a line that is a JSON
/// object whose `trigger` is true, such as `{"trigger": true, "why": "..."}`.
fn asks_for_turn(stdout: &str) -> bool {
    stdout
        .lines()
        .any(|line| serde_json::from_str::<Value>(line).is_ok_and(|value| value["trigger"] == true))
}

/// Run a schedule's command without a shell and report what its turn needs:
/// the output, or why there is none. A failure is data for the model, like a
/// failed source read; it never skips the turn.
async fn run_command(argv: &[String], timeout_seconds: u64) -> CommandReport {
    let mut report = CommandReport {
        argv: argv.to_vec(),
        ..Default::default()
    };
    let mut command = tokio::process::Command::new(&argv[0]);
    command
        .args(&argv[1..])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(false);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.as_std_mut().process_group(0);
    }
    let mut process = match crate::process::spawning(|| command.spawn()) {
        Ok(child) => OwnedProcess::new(child),
        Err(error) => {
            report.failure = Some(format!("could not start: {error}"));
            return report;
        }
    };
    let (Some(stdout), Some(stderr)) = (process.child.stdout.take(), process.child.stderr.take())
    else {
        report.failure = Some("its output could not be read".into());
        return report;
    };
    // The run ends when the command exits: whatever it left in its group is
    // stopped then, so a background child cannot hold its output open.
    let output = tokio::time::timeout(Duration::from_secs(timeout_seconds), async {
        let exited = async {
            process.wait_until_exit().await?;
            process.kill_group().context("process group cleanup failed")
        };
        tokio::try_join!(
            read_bounded(stdout, MAX_STDOUT, Keep::Head),
            read_bounded(stderr, MAX_STDERR, Keep::Tail),
            exited
        )
    })
    .await;
    let status = process.finish().await;
    let Ok(output) = output else {
        let late = format!("did not finish within its {timeout_seconds}-second timeout");
        report.failure = Some(match status {
            Ok(_) => format!("{late} and was stopped"),
            Err(error) => format!("{late}, and stopping it could not be confirmed: {error:#}"),
        });
        return report;
    };
    let (((stdout, stdout_truncated), (stderr, stderr_truncated), ()), status) =
        match (output, status) {
            (Ok(output), Ok(status)) => (output, status),
            (Err(error), _) | (_, Err(error)) => {
                report.failure = Some(format!("its exit could not be observed: {error:#}"));
                return report;
            }
        };
    report.stdout = Some(stdout);
    report.stdout_truncated = Some(stdout_truncated);
    report.exit_status = status.code();
    if status.success() {
        report.ok = true;
        return report;
    }
    report.failure = Some(match status.code() {
        Some(code) => format!("exited with status {code}"),
        None => format!("stopped by {status}"),
    });
    report.stderr = Some(stderr);
    report.stderr_truncated = Some(stderr_truncated);
    report
}

enum Keep {
    Head,
    Tail,
}

/// Read a pipe to its end, keeping `limit` bytes: the head of the output, or
/// the tail of an error stream, where the cause usually is. The flag says
/// whether anything was dropped.
async fn read_bounded(
    mut pipe: impl AsyncRead + Unpin,
    limit: usize,
    keep: Keep,
) -> Result<(String, bool)> {
    let (mut kept, mut dropped) = (Vec::new(), false);
    let mut buffer = [0u8; 8192];
    loop {
        let count = pipe.read(&mut buffer).await.context("read its output")?;
        if count == 0 {
            break;
        }
        kept.extend_from_slice(&buffer[..count]);
        if kept.len() > limit {
            dropped = true;
            match keep {
                Keep::Head => kept.truncate(limit),
                Keep::Tail => {
                    kept.drain(..kept.len() - limit);
                }
            }
        }
    }
    Ok((String::from_utf8_lossy(&kept).into_owned(), dropped))
}

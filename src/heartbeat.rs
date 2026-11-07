//! The heartbeat. On its cadence, in waking hours only, the host starts a
//! read-only turn in one conversation; the model reviews what is new since the
//! last heartbeat and decides whether anything is worth suggesting, and may
//! set wakes for a closer look at a time of its choosing (`broker/wakes.rs`).
//! Without a `[heartbeat]` table it runs by default once anything is
//! connected, twice a day in the owner's direct chat; `off = true` stops it.
//! The host owns when and how far; the model owns whether and what.
use crate::{
    config::{AccountHolder, ChannelManifest, Config},
    runtime::ConversationIdentity,
    store::Store,
};
use anyhow::{ensure, Result};
use chrono::{DateTime, Datelike, NaiveTime, TimeDelta, TimeZone, Weekday};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Heartbeat {
    /// Minutes between heartbeats, counted each day from `waking_from`.
    #[serde(default = "default_every")]
    pub every_minutes: u32,
    /// Local `HH:MM` when waking hours begin, and the day's first heartbeat.
    #[serde(default = "default_from")]
    pub waking_from: String,
    /// Local `HH:MM` when waking hours end; no heartbeat starts at or after it.
    #[serde(default = "default_until")]
    pub waking_until: String,
    /// The one conversation it may speak in: `<channel>/<account>/<native id>`;
    /// empty for the owner's latest direct chat with the assistant.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub conversation: String,
    /// When the weekly digest is due, as local `<weekday> HH:MM` in waking
    /// hours, for example `Sun 18:00`; none when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
    /// No heartbeat at all, and none by default.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub off: bool,
}

/// How far the nightly review may set the pace: every half hour to every
/// eight hours.
pub const PACE_MINUTES: std::ops::RangeInclusive<u32> = 30..=480;

fn pace_file(paths: &crate::config::Paths) -> std::path::PathBuf {
    paths.runtime.join("heartbeat-pace")
}

/// The shell command that sets the pace to `<minutes>`, for the nightly
/// review's instruction.
pub fn pace_command(paths: &crate::config::Paths) -> String {
    format!(
        "echo <minutes> > {}",
        crate::prompt::shell_quote(&pace_file(paths).display().to_string())
    )
}

fn default_every() -> u32 {
    480
}
fn default_from() -> String {
    "08:30".into()
}
fn default_until() -> String {
    "21:00".into()
}

/// The default: 08:30 and 16:30, in the owner's latest direct chat.
impl Default for Heartbeat {
    fn default() -> Self {
        Self {
            every_minutes: default_every(),
            waking_from: default_from(),
            waking_until: default_until(),
            conversation: String::new(),
            digest: None,
            off: false,
        }
    }
}

impl Heartbeat {
    /// The heartbeat a configuration asks for: its own `[heartbeat]` unless
    /// that is off, or without one the default once anything is connected to
    /// look at (a script connector, or one of the owner's own
    /// accounts). With nothing connected it would only reread the chats, so
    /// replies and schedules carry the conversation instead.
    pub fn of(config: &Config) -> Option<Self> {
        match &config.heartbeat {
            Some(heartbeat) => (!heartbeat.off).then(|| heartbeat.clone()),
            None => {
                let owners = |c: &ChannelManifest| c.account_holder == AccountHolder::Owner;
                let connected = !config.connectors.is_empty() || config.channels.iter().any(owners);
                connected.then(Self::default)
            }
        }
    }

    /// The heartbeat serve runs now, with the chat it speaks in named: the
    /// configured one, or else the owner's direct chat with the assistant
    /// that the owner wrote in last; a look or a reply logged there does not
    /// count. None while it is off, or before the owner has written to the
    /// assistant directly.
    pub fn running(config: &Config, store: &Store) -> Result<Option<Self>> {
        let Some(mut heartbeat) = Self::of(config) else {
            return Ok(None);
        };
        if heartbeat.conversation.is_empty() {
            let mut latest = None;
            let assistants = config
                .channels
                .iter()
                .filter(|c| c.account_holder == AccountHolder::Assistant);
            for channel in assistants {
                if let Some(chat) =
                    store.owner_chat(&channel.id, &channel.account, &config.owner)?
                {
                    let last = store.last_message_from(&chat, &config.owner)?;
                    if latest.as_ref().is_none_or(|(seq, _)| last > *seq) {
                        latest = Some((last, chat));
                    }
                }
            }
            let Some((_, chat)) = latest else {
                return Ok(None);
            };
            heartbeat.conversation = chat;
        }
        Ok(Some(heartbeat))
    }

    /// The heartbeat serve runs now, at the pace the nightly review last set:
    /// what serve fires and doctor reports.
    pub fn serving(
        config: &Config,
        store: &Store,
        paths: &crate::config::Paths,
    ) -> Result<Option<Self>> {
        Ok(Self::running(config, store)?.map(|heartbeat| heartbeat.paced(paths)))
    }

    /// This heartbeat at the pace the nightly review last set, if it set one
    /// (`pace_command` writes it): minutes between heartbeats, held to
    /// PACE_MINUTES, in place of `every_minutes`. The owner's `[heartbeat]`
    /// keeps the chat, the hours and `off`; deleting the file restores
    /// `every_minutes`.
    pub fn paced(mut self, paths: &crate::config::Paths) -> Self {
        let set = std::fs::read_to_string(pace_file(paths))
            .ok()
            .and_then(|text| text.trim().parse::<u32>().ok());
        if let Some(minutes) = set {
            self.every_minutes = minutes.clamp(*PACE_MINUTES.start(), *PACE_MINUTES.end());
        }
        self
    }

    /// Whether `at` falls in waking hours.
    pub fn awake(&self, at: NaiveTime) -> bool {
        self.hours()
            .is_some_and(|(from, until)| from <= at && at < until)
    }

    pub fn validate(&self, channels: &[ChannelManifest]) -> Result<()> {
        ensure!(
            (5..=1440).contains(&self.every_minutes),
            "heartbeat.every_minutes must be 5..=1440"
        );
        let hours = self.hours();
        ensure!(
            hours.is_some_and(|(from, until)| from < until),
            "heartbeat.waking_from and waking_until must be local HH:MM times, the first earlier in the day"
        );
        ensure!(
            self.digest.is_none()
                || self.digest_time().zip(hours).is_some_and(
                    |((_, at), (from, until))| from <= at && at < until
                ),
            "heartbeat.digest must be a weekday and a local HH:MM time in waking hours, such as \"Sun 18:00\""
        );
        ensure!(
            self.conversation.is_empty()
                || crate::config::is_channel_conversation(&self.conversation, channels),
            "heartbeat.conversation must be <channel id>/<account>/<native id> of a channel on the assistant's own account, or left out for the owner's direct chat"
        );
        Ok(())
    }

    /// Whether `slot` is the day's first heartbeat, the morning one.
    pub fn morning(&self, slot: NaiveTime) -> bool {
        self.hours().is_some_and(|(from, _)| from == slot)
    }

    fn hours(&self) -> Option<(NaiveTime, NaiveTime)> {
        let time = |text: &str| NaiveTime::parse_from_str(text, "%H:%M").ok();
        Some((time(&self.waking_from)?, time(&self.waking_until)?))
    }

    fn digest_time(&self) -> Option<(Weekday, NaiveTime)> {
        let (day, time) = self.digest.as_deref()?.split_once(' ')?;
        Some((
            day.parse().ok()?,
            NaiveTime::parse_from_str(time, "%H:%M").ok()?,
        ))
    }

    /// The weekly digest owed at `now`: its time today, on its weekday, from
    /// then until waking hours end. Like a heartbeat slot, a restart still
    /// fires it once, and a time a clock change skipped is not owed.
    pub fn due_digest<Tz: TimeZone>(&self, now: &DateTime<Tz>) -> Option<DateTime<Tz>> {
        let ((day, at), (_, until)) = self.digest_time().zip(self.hours())?;
        let local = now.naive_local();
        if local.weekday() != day || local.time() < at || local.time() >= until {
            return None;
        }
        now.timezone()
            .from_local_datetime(&local.date().and_time(at))
            .earliest()
            .filter(|slot| slot <= now)
    }

    /// The start of the digest's week: the same local time seven calendar
    /// days before `until`, so weeks meet across a clock change. A time the
    /// change skipped starts at the instant seven days of 24 hours before.
    pub fn week_before<Tz: TimeZone>(until: &DateTime<Tz>) -> DateTime<Tz> {
        let local = until.naive_local() - chrono::Days::new(7);
        until
            .timezone()
            .from_local_datetime(&local)
            .earliest()
            .unwrap_or_else(|| until.clone() - TimeDelta::days(7))
    }

    /// The heartbeat owed at `now`: the latest slot today at or before `now`,
    /// while `now` is in waking hours. A slot stays owed until the next one, so
    /// a restart still fires it once; in quiet hours nothing is owed. A slot
    /// time that a clock change skipped is not owed, and one that happens
    /// twice is its earlier instant both times.
    pub fn due_slot<Tz: TimeZone>(&self, now: &DateTime<Tz>) -> Option<DateTime<Tz>> {
        let (from, until) = self.hours()?;
        let local = now.naive_local();
        if local.time() < from || local.time() >= until {
            return None;
        }
        let every = i64::from(self.every_minutes);
        let elapsed = (local.time() - from).num_minutes();
        let slot = local.date().and_time(from) + TimeDelta::minutes(elapsed / every * every);
        now.timezone()
            .from_local_datetime(&slot)
            .earliest()
            .filter(|slot| slot <= now)
    }
}

/// The owner's direct chat `conversation`, where a heartbeat, a wake or a
/// newcomer look speaks. A look reads every conversation and all of memory,
/// so it speaks only where the owner alone listens: a direct chat already
/// seen on its channel, in which every message that came in was the owner's.
/// Serve checks this before each look; `oppa doctor` reports it for the
/// heartbeat. A chat refused for its kind or its senders is `NotOwnersChat`.
pub fn owners_chat(store: &Store, conversation: &str, owner: &str) -> Result<ConversationIdentity> {
    let identity = crate::schedule::known_conversation(store, conversation)?;
    if identity.kind != "dm" {
        let why = format!(
            "a look speaks only in a direct chat, and {} is a group",
            identity.id
        );
        return Err(NotOwnersChat(why).into());
    }
    let senders = store.message_authors(&identity.id)?;
    if senders.is_empty() || senders.iter().any(|sender| sender != owner) {
        let why = format!(
            "a look speaks only in the owner's direct chat, and {} has messages from someone else or none from the owner",
            identity.id
        );
        return Err(NotOwnersChat(why).into());
    }
    Ok(identity)
}

/// Why a chat is not one a look may speak in.
#[derive(Debug)]
pub struct NotOwnersChat(String);

impl std::fmt::Display for NotOwnersChat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for NotOwnersChat {}

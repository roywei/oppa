//! The memory sync. OPPA's memory is the source
//! of truth, and the owner also uses Claude Code and Codex every day, each with
//! a memory of its own. Once a day the host starts an owner turn in the owner's
//! direct chat whose command lists what changed in those memories; the model
//! takes in what should last and asks about what contradicts OPPA memory.
//! `oppa memory sync` runs the same turn at the console. The first import is
//! the owner's yes in chat (`memory.import`): the host then runs the turn every
//! ten minutes until nothing is left, and the nightly sync runs only once
//! something was taken in, so a no stays a no.
//!
//! The same night, whether or not anything was imported, a second turn
//! reviews the conversations the assistant can read since the last review and keeps
//! what they taught as skills (procedure) and memory (facts), so a lesson the
//! owner had to teach once is not taught again. The host hands each review the
//! span of history it covers, by event seq, and a mark command the review runs
//! once that span is done, so a review that fails or times out is read again
//! at the next slot, wherever the owner last wrote; the model judges what is
//! worth keeping.
use crate::{config::Paths, schedule::Schedule, store::Store};
use anyhow::{ensure, Result};
use chrono::{DateTime, Local, TimeZone};
use serde::{Deserialize, Serialize};
use std::fs;

pub const ID: &str = "memory-sync";
pub const IMPORT_ID: &str = "memory-import";
pub const REVIEW_ID: &str = "skill-review";
/// An import's turns come this often, and stop this long after the owner's yes;
/// what is left then comes in with the nightly sync.
const IMPORT_EVERY_MINUTES: u32 = 10;
const IMPORT_HOURS: i64 = 3;
/// The most events one review turn is given; how long after the nightly
/// review further turns come while more than REVIEW_LEFTOVER events are left
/// (fewer, such as the last review's own turn, wait for the next night).
const REVIEW_EVENTS: i64 = 100;
const REVIEW_HOURS: u32 = 3;
const REVIEW_LEFTOVER: i64 = 10;

const PROMPT: &str = "Memory sync. The command output lists what changed in the owner's Claude Code and Codex memory, and any other source they named, since the last sync; the first sync lists everything. OPPA memory is the source of truth. Read each changed file whole (its text is included while it fits; otherwise open its path), and bring what should last into OPPA memory with memory.commit: facts about the owner and the people around them, preferences, decisions and standing rules, each in the note where it belongs, with the source path and date. Leave out what is only about one code project or one session. Where a source contradicts OPPA memory, keep OPPA's note and tell the owner in one short message what differs and ask which is right. When the changes are in memory, run the output's mark command so they are not offered again. Return NO_REPLY unless something needs the owner.";

const IMPORT_PROMPT: &str = "This is one batch of the import the owner asked for; the host runs the next batch in ten minutes until a mark leaves nothing. Run the mark command even when this batch offers nothing. Only when this batch offered changes and its not_yet_offered is 0, after the mark, tell the owner in one short message that the import is finished and what kinds of things you took in; a batch that offered nothing returns NO_REPLY.";

/// The review's standing instruction; its span and mark follow it.
const REVIEW_PROMPT: &str = "Nightly self-improvement. The owner is asleep: never message anyone from this turn, and end with NO_REPLY; what you did reaches them in the morning through the note below. Read the conversations you can read for work done in several steps that the owner will want again, such as finding hotel award nights, comparing flights or checking out one item from a full cart, and for what went wrong in it: an answer the owner corrected, a site that blocked you, a search that misled you, a worker that stopped and lost its results, a rule you misread. Keep each lesson in the skill for that kind of work, fixing the one that covers it or writing a new one: its description line says what it does and when to use it, its body the steps that worked and the mistakes to avoid, each with the date it was learned. A skill may be steps alone, without a script. Facts about people, their accounts, cards, perks and preferences go in memory with memory.commit, never in a skill, and never a secret anywhere; what you read in someone's private chat keeps the discretion it has in any turn. Where your own code or instructions caused a mistake or could serve the owner better and the change is clear, delegate a background task that makes it as own_source says, on a self/<topic> branch with its tests and gate, and whose brief says its result is for the morning: presenting it returns NO_REPLY. Set tomorrow's heartbeat pace from how much the owner's mail, chats and other channels have needed them lately and how they took the heartbeat's suggestions: minutes between heartbeats, 30 to 480, with {pace}. Then add to assistant/morning.md, under a heading with today's date, one line for each skill you wrote or fixed and its lesson, the pace and why, each branch you started and what it changes, and each improvement that only the owner can make (a rule, an access, an account); drop sections older than two days. Change nothing else: book, buy, send and ask no one.";

/// The ten-minute boundary at or before `now`, from the instant, so an hour
/// the clocks repeat never makes an ambiguous local time; such an hour's second
/// pass waits.
fn ten_minute_slot(now: DateTime<Local>) -> DateTime<Local> {
    let every = i64::from(IMPORT_EVERY_MINUTES) * 60;
    let instant = now.timestamp() - now.timestamp().rem_euclid(every);
    Local.timestamp_opt(instant, 0).single().unwrap_or(now)
}

/// The seqs finished reviews stopped at, one a line, appended by their mark
/// commands; the largest counts, so a slow or repeated review never moves the
/// next one back; a line cut short, or past the newest event, is skipped.
fn reviewed(paths: &Paths) -> std::path::PathBuf {
    paths.runtime.join("skill-reviewed")
}

fn reviewed_through(paths: &Paths, latest: i64) -> i64 {
    fs::read_to_string(reviewed(paths))
        .unwrap_or_default()
        .lines()
        .filter_map(|line| line.trim().parse::<i64>().ok())
        .filter(|through| *through <= latest)
        .max()
        .unwrap_or(0)
}

/// The owner's yes to an import, kept while it runs.
fn import_marker(paths: &Paths) -> std::path::PathBuf {
    paths.runtime.join("memory-import.json")
}

/// What `memory_changes.py` last marked.
fn state(paths: &Paths) -> std::path::PathBuf {
    paths.runtime.join("memory-sync.json")
}

#[derive(Deserialize, Serialize)]
struct Import {
    /// Unix milliseconds.
    started: i64,
}

#[derive(Deserialize)]
struct Marked {
    #[serde(default)]
    marked_at_ms: Option<i64>,
    #[serde(default)]
    remaining: Option<u64>,
}

fn import(paths: &Paths) -> Option<Import> {
    fs::read(import_marker(paths))
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
}

/// Begin an import: the owner said yes. One still running keeps its start;
/// one past its three hours, or a marker that cannot be read, starts afresh.
pub fn start_import(paths: &Paths) -> Result<()> {
    let now = Local::now().timestamp_millis();
    if import(paths).is_some_and(|import| now <= import.started + IMPORT_HOURS * 3_600_000) {
        return Ok(());
    }
    let marker = import_marker(paths);
    let temporary = marker.with_extension("json.tmp");
    fs::write(&temporary, serde_json::to_vec(&Import { started: now })?)?;
    fs::rename(&temporary, &marker)?;
    Ok(())
}

/// Where Claude Code and Codex keep what they remember about the owner.
pub fn default_sources() -> Vec<String> {
    [
        "~/.claude/CLAUDE.md",
        "~/.claude/projects/*/memory/**/*.md",
        "~/.codex/AGENTS.md",
        "~/.codex/memories/**/*.md",
    ]
    .map(String::from)
    .to_vec()
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MemorySync {
    /// Local `HH:MM` each day.
    pub at: String,
    /// Files or globs to take in: Claude Code's and Codex's own memory unless
    /// the owner lists others.
    #[serde(default = "default_sources")]
    pub sources: Vec<String>,
    /// The chat it runs in; the owner's latest direct chat when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation: Option<String>,
}

impl MemorySync {
    pub fn validate(&self, channels: &[crate::config::ChannelManifest]) -> Result<()> {
        ensure!(
            chrono::NaiveTime::parse_from_str(&self.at, "%H:%M").is_ok(),
            "memory_sync.at must be a local HH:MM time"
        );
        ensure!(
            !self.sources.is_empty()
                && self
                    .sources
                    .iter()
                    .all(|source| !source.trim().is_empty() && !source.contains('\0')),
            "memory_sync.sources must be file paths or globs"
        );
        ensure!(
            self.conversation
                .as_ref()
                .is_none_or(|c| crate::config::is_channel_conversation(c, channels)),
            "memory_sync.conversation must be <channel id>/<account>/<native id> of a channel on the assistant's own account"
        );
        Ok(())
    }

    /// The turns serve fires in `conversation` at `now`: the review of what
    /// came after the last finished one (`latest` is the newest event's seq),
    /// the nightly sync once anything was taken in, and every ten minutes
    /// while an import the owner asked for runs. An import ends, and its
    /// marker goes, when a mark made since it began left nothing, or after
    /// three hours.
    pub fn due(
        &self,
        paths: &Paths,
        latest: i64,
        conversation: &str,
        now: DateTime<Local>,
    ) -> Vec<Schedule> {
        let marked: Option<Marked> = fs::read(state(paths))
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok());
        let mut due: Vec<Schedule> = self
            .review(paths, latest, conversation, now)
            .into_iter()
            .collect();
        if marked.is_some() {
            due.push(self.schedule(paths, conversation));
        }
        if let Some(import) = import(paths) {
            let finished = marked.is_some_and(|marked| {
                marked.remaining == Some(0)
                    && marked.marked_at_ms.is_some_and(|at| at > import.started)
            });
            let expired = now.timestamp_millis() > import.started + IMPORT_HOURS * 3_600_000;
            if finished || expired {
                let _ = fs::remove_file(import_marker(paths));
            } else {
                let slot = ten_minute_slot(now);
                let mut turn = self.schedule(paths, conversation);
                turn.id = IMPORT_ID.into();
                turn.at = slot.format("%H:%M").to_string();
                turn.date = Some(slot.format("%Y-%m-%d").to_string());
                turn.window_minutes = IMPORT_EVERY_MINUTES - 1;
                turn.prompt = format!("{PROMPT} {IMPORT_PROMPT}");
                due.push(turn);
            }
        }
        due
    }

    /// The day's turn as a schedule in `conversation`.
    pub fn schedule(&self, paths: &Paths, conversation: &str) -> Schedule {
        let mut command = vec![
            "python3".to_string(),
            paths
                .runtime
                .join("adapters/memory_changes.py")
                .display()
                .to_string(),
            "--state".into(),
            state(paths).display().to_string(),
        ];
        for source in &self.sources {
            command.extend(["--source".into(), source.clone()]);
        }
        Schedule {
            id: ID.into(),
            at: self.at.clone(),
            date: None,
            days: vec![],
            conversation: conversation.into(),
            prompt: PROMPT.into(),
            command,
            timeout_seconds: 60,
            window_minutes: 60,
            enabled: true,
            wake: false,
        }
    }

    /// The review of the next events after the last finished review, at most
    /// REVIEW_EVENTS of them so one turn can read them: nightly, half an hour
    /// before the nightly sync, and while more than REVIEW_LEFTOVER are left
    /// (an instance's first review of a long history), every ten minutes for
    /// REVIEW_HOURS after it, queueing with the sync and any import in the one
    /// chat. A slot queued behind a slow turn may carry a span already done;
    /// its turn checks the marks first. None when nothing came after them.
    pub fn review(
        &self,
        paths: &Paths,
        latest: i64,
        conversation: &str,
        now: DateTime<Local>,
    ) -> Option<Schedule> {
        let after = reviewed_through(paths, latest);
        if latest <= after {
            return None;
        }
        let through = latest.min(after + REVIEW_EVENTS);
        let at = chrono::NaiveTime::parse_from_str(&self.at, "%H:%M")
            .map(|at| {
                (at - chrono::TimeDelta::minutes(30))
                    .format("%H:%M")
                    .to_string()
            })
            .unwrap_or_else(|_| self.at.clone());
        let file = crate::prompt::shell_quote(&reviewed(paths).display().to_string());
        let mark = format!("echo {through} >> {file}");
        let mut review = Schedule {
            id: REVIEW_ID.into(),
            at,
            date: None,
            days: vec![],
            conversation: conversation.into(),
            prompt: format!("{} This review covers the events with seq after {after} through {through}. First run cat {file}: if a line there is {through} or more, another review already did this span; return NO_REPLY. Otherwise read this conversation and every one history.conversations lists (page it with after_id) with a last_event_seq past {after}, each with history.read from after_seq {after}, stopping at seq {through}; the briefs you gave workers and what they returned are among them. When all of it is reviewed, and only then, run this so the next review starts after it: {mark}", REVIEW_PROMPT.replace("{pace}", &crate::heartbeat::pace_command(paths))),
            command: vec![],
            timeout_seconds: 60,
            window_minutes: 60,
            enabled: true,
            wake: false,
        };
        let catching_up = Schedule {
            window_minutes: REVIEW_HOURS * 60,
            ..review.clone()
        };
        if latest - after > REVIEW_LEFTOVER && catching_up.due_slot(&now).is_some() {
            let slot = ten_minute_slot(now);
            review.at = slot.format("%H:%M").to_string();
            review.date = Some(slot.format("%Y-%m-%d").to_string());
            review.window_minutes = 9;
        }
        Some(review)
    }

    /// Where it runs: the configured chat, else the owner's direct chat with
    /// the newest event, one in which every message that came in was theirs.
    pub fn chat(&self, store: &Store, owner: &str) -> Result<Option<String>> {
        if let Some(conversation) = &self.conversation {
            return Ok(Some(conversation.clone()));
        }
        let mut best = None;
        for conversation in store.conversations()? {
            if conversation.kind != "dm" || conversation.channel == "console" {
                continue;
            }
            let senders = store.message_authors(&conversation.id)?;
            if senders.is_empty() || senders.iter().any(|sender| sender != owner) {
                continue;
            }
            let last = store
                .recent_transcript(&conversation.id, 1)?
                .last()
                .map_or(0, |event| event.seq);
            if best.as_ref().is_none_or(|(seq, _)| last > *seq) {
                best = Some((last, conversation.id));
            }
        }
        Ok(best.map(|(_, id)| id))
    }
}

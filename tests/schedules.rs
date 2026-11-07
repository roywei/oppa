//! When a schedule or a heartbeat is due, and which ones a configuration may
//! hold. Turns for fired schedules and heartbeats are covered in
//! tests/runtime.rs and tests/channel.rs.
use chrono::{
    DateTime, FixedOffset, MappedLocalTime, NaiveDate, NaiveDateTime, TimeDelta, TimeZone, Weekday,
};
use oppa::{
    broker::{Broker, BrokerLaunch, ConversationScope, RunGrant, TaskAccess, ToolResult},
    config::{AccountHolder, ChannelManifest, Config, ConnectorKind, ConnectorManifest, Paths},
    harness::{Access, AgentManifest},
    heartbeat::Heartbeat,
    runtime::{ConsoleDelivery, Runtime},
    schedule::{Schedule, ScheduleStore},
    store::{DeliveryState, InboundEvent, Store},
    vault::Vault,
};
use serde_json::{json, Value};
use std::fs;

/// A synthetic zone at UTC-8 that springs forward at 02:00 on 2026-03-08
/// (02:00 to 02:59 does not exist) and falls back at 02:00 on 2026-11-01
/// (01:00 to 01:59 happens twice), so the test does not depend on the host zone.
#[derive(Clone, Copy, Debug)]
struct Synthetic;

fn standard() -> FixedOffset {
    FixedOffset::west_opt(8 * 3600).unwrap()
}
fn daylight() -> FixedOffset {
    FixedOffset::west_opt(7 * 3600).unwrap()
}
fn naive(month: u32, day: u32, hour: u32, minute: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, month, day)
        .unwrap()
        .and_hms_opt(hour, minute, 0)
        .unwrap()
}

impl TimeZone for Synthetic {
    type Offset = FixedOffset;
    fn from_offset(_: &FixedOffset) -> Self {
        Synthetic
    }
    fn offset_from_local_date(&self, local: &NaiveDate) -> MappedLocalTime<FixedOffset> {
        self.offset_from_local_datetime(&local.and_hms_opt(12, 0, 0).unwrap())
    }
    fn offset_from_local_datetime(&self, local: &NaiveDateTime) -> MappedLocalTime<FixedOffset> {
        let (spring, fall) = (naive(3, 8, 2, 0), naive(11, 1, 1, 0));
        let hour = TimeDelta::hours(1);
        if *local < spring {
            MappedLocalTime::Single(standard())
        } else if *local < spring + hour {
            MappedLocalTime::None
        } else if *local < fall {
            MappedLocalTime::Single(daylight())
        } else if *local < fall + hour {
            MappedLocalTime::Ambiguous(daylight(), standard())
        } else {
            MappedLocalTime::Single(standard())
        }
    }
    fn offset_from_utc_date(&self, utc: &NaiveDate) -> FixedOffset {
        self.offset_from_utc_datetime(&utc.and_hms_opt(12, 0, 0).unwrap())
    }
    fn offset_from_utc_datetime(&self, utc: &NaiveDateTime) -> FixedOffset {
        if *utc >= naive(3, 8, 10, 0) && *utc < naive(11, 1, 9, 0) {
            daylight()
        } else {
            standard()
        }
    }
}

fn at(month: u32, day: u32, hour: u32, minute: u32) -> DateTime<Synthetic> {
    Synthetic
        .from_local_datetime(&naive(month, day, hour, minute))
        .single()
        .unwrap()
}

fn schedule(at: &str, days: &[Weekday], window_minutes: u32) -> Schedule {
    Schedule {
        id: "review".into(),
        at: at.into(),
        date: None,
        days: days.to_vec(),
        conversation: "console".into(),
        prompt: "Review the synthetic source.".into(),
        command: vec![],
        timeout_seconds: 300,
        window_minutes,
        enabled: true,
        wake: false,
    }
}

#[test]
fn a_slot_is_due_on_its_days_within_its_window_and_once_across_clock_changes() {
    use Weekday::{Fri, Mon, Wed};
    let slot = |schedule: &Schedule, now| schedule.due_slot(&now).map(|slot| slot.to_rfc3339());
    // 2026-09-28 is a Monday.
    let review = schedule("07:15", &[Mon, Wed, Fri], 60);
    let monday = Some("2026-09-28T07:15:00-07:00".to_owned());
    assert_eq!(slot(&review, at(9, 28, 7, 14)), None, "before at");
    assert_eq!(slot(&review, at(9, 28, 7, 15)), monday);
    assert_eq!(
        slot(&review, at(9, 28, 8, 15)),
        monday,
        "late within window"
    );
    assert_eq!(slot(&review, at(9, 28, 8, 16)), None, "missed by more");
    assert_eq!(slot(&review, at(9, 29, 7, 20)), None, "not a Tuesday job");
    assert_eq!(
        review.next_slot(&at(9, 28, 8, 0)).unwrap().to_rfc3339(),
        "2026-09-30T07:15:00-07:00"
    );
    // A missed slot stays due until its window closes, even into the next day.
    let daily = schedule("07:00", &[], 1440);
    assert_eq!(
        slot(&daily, at(9, 29, 6, 30)),
        Some("2026-09-28T07:00:00-07:00".to_owned())
    );

    // 02:30 does not exist on 2026-03-08: that day has no slot.
    let early = schedule("02:30", &[], 60);
    assert_eq!(slot(&early, at(3, 8, 3, 10)), None);
    assert_eq!(
        early.next_slot(&at(3, 7, 3, 0)).unwrap().to_rfc3339(),
        "2026-03-09T02:30:00-07:00"
    );
    // 01:30 happens twice on 2026-11-01: both passes owe the earlier slot.
    let night = schedule("01:30", &[], 120);
    let local = naive(11, 1, 1, 45);
    let first = Synthetic.from_local_datetime(&local).earliest().unwrap();
    let second = Synthetic.from_local_datetime(&local).latest().unwrap();
    assert_ne!(first, second);
    for now in [first, second] {
        assert_eq!(
            slot(&night, now),
            Some("2026-11-01T01:30:00-07:00".to_owned())
        );
    }
}

/// A configuration with one channel account, `imsg/assistant`.
fn base() -> Config {
    Config {
        access: Default::default(),
        version: 1,
        connectors: vec![],
        sessions: Default::default(),
        source: None,
        owner: "synthetic-owner".into(),
        agents: vec![AgentManifest {
            id: "harness".into(),
            family: "synthetic".into(),
            command: vec!["/never/run".into()],
            timeout_seconds: 5,
            task_timeout_seconds: 5,
            model: None,
            effort: None,
        }],
        channels: vec![ChannelManifest {
            id: "imsg".into(),
            account: "assistant".into(),
            command: vec!["/never/run".into()],
            timeout_seconds: 5,
            account_holder: Default::default(),
        }],
        people: vec![],
        groups: vec![],
        heartbeat: None,
        browser: false,
        memory_sync: None,
    }
}

#[test]
fn schedules_must_address_the_console_or_a_configured_channel_account() {
    let channels = base().channels;
    let to = |conversation: &str| Schedule {
        conversation: conversation.into(),
        ..schedule("07:00", &[], 60)
    };
    for conversation in ["console", "imsg/assistant/any;-;chat/with/slashes"] {
        to(conversation).validate(&channels).unwrap();
    }
    for invalid in [
        to("other/assistant/chat"),
        to("imsg/owner/chat"),
        to("imsg/assistant/ "),
        to("imsg/assistant"),
        to("family"),
        Schedule {
            at: "7am".into(),
            ..to("console")
        },
        Schedule {
            id: "no spaces".into(),
            ..to("console")
        },
        Schedule {
            prompt: " ".into(),
            ..to("console")
        },
        // Short in bytes, but longer than a turn shows once escaped as JSON.
        Schedule {
            prompt: "\"".repeat(8_100),
            ..to("console")
        },
        Schedule {
            command: vec![String::new()],
            ..to("console")
        },
        Schedule {
            window_minutes: 0,
            ..to("console")
        },
        Schedule {
            timeout_seconds: 3601,
            ..to("console")
        },
        // A one-off has a real date and no repeating days.
        Schedule {
            date: Some("2026-02-30".into()),
            ..to("console")
        },
        Schedule {
            date: Some("2026-10-04".into()),
            days: vec![Weekday::Mon],
            ..to("console")
        },
    ] {
        assert!(invalid.validate(&channels).is_err(), "{invalid:?}");
    }
}

#[test]
fn a_one_off_has_one_slot_however_far_away_it_is() {
    // P-6: "remind me on the 19th at 9": one slot, due within its window only.
    let once = Schedule {
        date: Some("2026-11-19".into()),
        ..schedule("09:00", &[], 60)
    };
    let slot = Some("2026-11-19T09:00:00-08:00".to_owned());
    assert_eq!(
        once.next_slot(&at(9, 28, 7, 0)).map(|s| s.to_rfc3339()),
        slot,
        "seven weeks ahead"
    );
    assert_eq!(once.due_slot(&at(11, 19, 8, 59)), None);
    assert_eq!(
        once.due_slot(&at(11, 19, 9, 30)).map(|s| s.to_rfc3339()),
        slot
    );
    assert_eq!(once.due_slot(&at(11, 19, 10, 1)), None, "missed by more");
    assert_eq!(once.next_slot(&at(11, 19, 9, 0)), None, "never again");
}

fn heartbeat(every_minutes: u32, waking_from: &str, waking_until: &str) -> Heartbeat {
    Heartbeat {
        every_minutes,
        waking_from: waking_from.into(),
        waking_until: waking_until.into(),
        conversation: "imsg/assistant/owner-chat".into(),
        digest: None,
        off: false,
    }
}

#[test]
fn a_heartbeat_is_owed_on_its_cadence_in_waking_hours_only() {
    // P-1, P-5: every 90 minutes from 08:00, and nothing from 21:00 until the
    // next morning. Each slot stays owed until the next, so a restart fires it.
    let day = heartbeat(90, "08:00", "21:00");
    let slot = |beat: &Heartbeat, now| beat.due_slot(&now).map(|slot| slot.to_rfc3339());
    let owed = |time: &str| Some(format!("2026-09-28T{time}:00-07:00"));
    assert_eq!(slot(&day, at(9, 28, 7, 59)), None, "quiet before waking");
    assert_eq!(slot(&day, at(9, 28, 8, 0)), owed("08:00"));
    assert_eq!(slot(&day, at(9, 28, 9, 29)), owed("08:00"));
    assert_eq!(slot(&day, at(9, 28, 9, 30)), owed("09:30"));
    assert_eq!(slot(&day, at(9, 28, 20, 59)), owed("20:00"));
    assert_eq!(
        slot(&day, at(9, 28, 21, 0)),
        None,
        "quiet from waking_until"
    );
    assert_eq!(slot(&day, at(9, 29, 0, 30)), None, "quiet overnight");

    // 02:00 does not exist on 2026-03-08: that slot is skipped, not owed late.
    let night = heartbeat(90, "00:30", "05:00");
    assert_eq!(slot(&night, at(3, 8, 3, 10)), None);
    assert_eq!(
        slot(&night, at(3, 8, 3, 30)),
        Some("2026-03-08T03:30:00-07:00".into())
    );
    // 01:30 happens twice on 2026-11-01: both passes owe the earlier slot.
    let late = heartbeat(30, "00:00", "05:00");
    let local = naive(11, 1, 1, 45);
    for now in [
        Synthetic.from_local_datetime(&local).earliest().unwrap(),
        Synthetic.from_local_datetime(&local).latest().unwrap(),
    ] {
        assert_eq!(slot(&late, now), Some("2026-11-01T01:30:00-07:00".into()));
    }
}

#[test]
fn the_nightly_review_sets_the_heartbeat_pace_within_bounds_and_the_first_slot_is_the_morning() {
    // P-1: the nightly review sets tomorrow's
    // pace with the command its prompt gives; the host holds it to every half
    // hour through every eight hours, and the owner's hours and chat stay. The
    // day's first slot carries the morning report.
    let dir = tempfile::tempdir().unwrap();
    let paths = Paths::new(
        dir.path().join("run time's"),
        Some(dir.path().join("private")),
    )
    .unwrap();
    paths.prepare().unwrap();
    let configured = heartbeat(120, "08:30", "21:00");
    assert_eq!(
        configured.clone().paced(&paths).every_minutes,
        120,
        "no pace set"
    );
    let set = |minutes: &str| {
        let command = oppa::heartbeat::pace_command(&paths).replace("<minutes>", minutes);
        assert!(std::process::Command::new("sh")
            .args(["-c", &command])
            .status()
            .unwrap()
            .success());
        configured.clone().paced(&paths)
    };
    let paced = set("60");
    assert_eq!(paced.every_minutes, 60);
    assert_eq!(
        (paced.waking_from.as_str(), paced.waking_until.as_str()),
        ("08:30", "21:00")
    );
    assert_eq!(
        set("5").every_minutes,
        30,
        "never faster than every half hour"
    );
    assert_eq!(
        set("1440").every_minutes,
        480,
        "never slower than every eight hours"
    );
    assert_eq!(
        set("soon").every_minutes,
        120,
        "an unreadable pace leaves the owner's"
    );
    let time = |text| chrono::NaiveTime::parse_from_str(text, "%H:%M").unwrap();
    assert!(configured.morning(time("08:30")));
    assert!(!configured.morning(time("10:30")));
}

#[test]
fn the_weekly_digest_is_owed_on_its_day_from_its_time_until_waking_hours_end() {
    // P-5: the digest is owed on its weekday from its time, and stays owed
    // until waking hours end that day, so a restart still fires it once.
    // 2026-09-27 is a Sunday.
    let weekly = Heartbeat {
        digest: Some("Sun 18:00".into()),
        ..heartbeat(60, "08:00", "21:00")
    };
    let digest = |now| weekly.due_digest(&now).map(|slot| slot.to_rfc3339());
    let owed = Some("2026-09-27T18:00:00-07:00".to_string());
    assert_eq!(digest(at(9, 27, 17, 59)), None, "before its time");
    assert_eq!(digest(at(9, 27, 18, 0)), owed);
    assert_eq!(digest(at(9, 27, 20, 59)), owed);
    assert_eq!(digest(at(9, 27, 21, 0)), None, "quiet from waking_until");
    assert_eq!(digest(at(9, 28, 18, 30)), None, "another day");
    assert_eq!(
        digest(at(10, 4, 18, 0)),
        Some("2026-10-04T18:00:00-07:00".into()),
        "a week later"
    );
    assert_eq!(
        heartbeat(60, "08:00", "21:00").due_digest(&at(9, 27, 18, 0)),
        None
    );
}

#[test]
fn digest_weeks_meet_across_clock_changes() {
    // P-5: a digest reports the heartbeats since the one before it, by local
    // calendar, so no heartbeat falls between two weeks or into both when the
    // clock changes (2026-11-01 falls back, 2026-03-08 springs forward).
    let start = |month, day| Heartbeat::week_before(&at(month, day, 18, 0)).to_rfc3339();
    assert_eq!(start(11, 1), "2026-10-25T18:00:00-07:00");
    assert_eq!(start(11, 8), at(11, 1, 18, 0).to_rfc3339());
    assert_eq!(start(3, 8), "2026-03-01T18:00:00-08:00");
    assert_eq!(start(3, 15), at(3, 8, 18, 0).to_rfc3339());
}

#[test]
fn a_heartbeat_needs_a_cadence_waking_hours_and_a_configured_chat() {
    // P-1: off unless configured, and then only as the owner wrote it.
    let mut config = base();
    config.validate().unwrap();
    config.heartbeat = Some(heartbeat(60, "08:00", "21:00"));
    config.validate().unwrap();
    for digest in ["Sun 18:00", "monday 08:00", "Fri 20:59"] {
        config.heartbeat.as_mut().unwrap().digest = Some(digest.into());
        config.validate().unwrap();
    }
    for invalid in [
        heartbeat(4, "08:00", "21:00"),
        heartbeat(1441, "08:00", "21:00"),
        heartbeat(60, "21:00", "08:00"),
        heartbeat(60, "08:00", "08:00"),
        heartbeat(60, "8am", "21:00"),
        Heartbeat {
            conversation: "console".into(),
            ..heartbeat(60, "08:00", "21:00")
        },
        Heartbeat {
            conversation: "imsg/owner/owner-chat".into(),
            ..heartbeat(60, "08:00", "21:00")
        },
        Heartbeat {
            conversation: "imsg/assistant/ ".into(),
            ..heartbeat(60, "08:00", "21:00")
        },
    ]
    .into_iter()
    .chain(
        [
            "Sun 07:59",
            "Sun 21:00",
            "Sun",
            "18:00",
            "Someday 18:00",
            "Sun 6pm",
        ]
        .map(|digest| Heartbeat {
            digest: Some(digest.into()),
            ..heartbeat(60, "08:00", "21:00")
        }),
    ) {
        config.heartbeat = Some(invalid.clone());
        assert!(config.validate().is_err(), "{invalid:?}");
    }
}

/// A disposable instance holding `config` and an empty log.
fn instance(config: &Config) -> (tempfile::TempDir, Paths) {
    let dir = tempfile::tempdir().unwrap();
    let paths = Paths::new(dir.path().join("runtime"), Some(dir.path().join("private"))).unwrap();
    paths.prepare().unwrap();
    fs::write(paths.config_file(), toml::to_string(config).unwrap()).unwrap();
    Store::open(&paths.database()).unwrap();
    (dir, paths)
}

fn oppa(paths: &Paths, args: &[&str]) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_oppa"))
        .arg("--home")
        .arg(&paths.runtime)
        .arg("--private-dir")
        .arg(&paths.private)
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn a_channel_schedule_is_listed_with_what_holds_it_and_fires_only_from_serve() {
    // Review: the listing showed a next slot for a chat serve will not fire into.
    let (_dir, paths) = instance(&base());
    let added = oppa(
        &paths,
        &[
            "schedules",
            "add",
            "--id",
            "review",
            "--conversation",
            "imsg/assistant/unseen-chat",
            "--at",
            "07:00",
            "--days",
            "mon,wed,fri",
            "--prompt",
            "Review the synthetic source.",
        ],
    );
    assert!(
        added.status.success(),
        "{}",
        String::from_utf8_lossy(&added.stderr)
    );
    let added: Value = serde_json::from_slice(&added.stdout).unwrap();
    assert_eq!(
        (
            &added["created_by"],
            &added["days"],
            &added["timeout_seconds"],
            &added["window_minutes"]
        ),
        (
            &json!("owner"),
            &json!(["Mon", "Wed", "Fri"]),
            &json!(300),
            &json!(60)
        )
    );
    let listed = oppa(&paths, &["schedules"]);
    assert!(listed.status.success());
    let entry = &serde_json::from_slice::<Value>(&listed.stdout).unwrap()[0];
    assert!(entry["next_slot"].is_null(), "{entry}");
    assert!(
        entry["blocked"]
            .as_str()
            .is_some_and(|reason| reason.contains("has not been seen on its channel yet")),
        "{entry}"
    );

    let run = oppa(&paths, &["schedules", "run", "review"]);
    assert!(!run.status.success());
    assert!(
        String::from_utf8_lossy(&run.stderr).contains("oppa serve runs it at its time"),
        "{}",
        String::from_utf8_lossy(&run.stderr)
    );
    let store = Store::open(&paths.database()).unwrap();
    assert!(store.conversations().unwrap().is_empty(), "nothing fired");
}

#[test]
fn a_cancelled_schedule_stays_listed_only_with_all_and_never_fires() {
    // P-6: standing work can be stopped; the record of it stays.
    let (_dir, paths) = instance(&base());
    let add = |id: &str| {
        oppa(
            &paths,
            &[
                "schedules",
                "add",
                "--id",
                id,
                "--conversation",
                "console",
                "--at",
                "07:00",
                "--prompt",
                "Review the synthetic source.",
            ],
        )
    };
    assert!(add("review").status.success());
    assert!(!add("review").status.success(), "an id is taken once");
    let cancelled = oppa(&paths, &["schedules", "cancel", "review"]);
    assert!(cancelled.status.success());
    let cancelled: Value = serde_json::from_slice(&cancelled.stdout).unwrap();
    assert!(cancelled["cancelled_at"].is_string() && cancelled["next_slot"].is_null());
    let listed =
        |args: &[&str]| -> Value { serde_json::from_slice(&oppa(&paths, args).stdout).unwrap() };
    assert_eq!(listed(&["schedules"]), json!([]));
    assert_eq!(listed(&["schedules", "--all"])[0]["id"], "review");
    let run = oppa(&paths, &["schedules", "run", "review"]);
    assert!(
        !run.status.success(),
        "a cancelled schedule cannot be run either"
    );
    assert!(String::from_utf8_lossy(&run.stderr).contains("cancelled"));
    // A past one-off would never fire, so it is refused.
    let past = oppa(
        &paths,
        &[
            "schedules",
            "add",
            "--conversation",
            "console",
            "--at",
            "07:00",
            "--date",
            "2020-01-01",
            "--prompt",
            "Too late.",
        ],
    );
    assert!(!past.status.success());
    assert!(String::from_utf8_lossy(&past.stderr).contains("has already passed"));
}

#[test]
fn a_configuration_that_still_holds_schedules_says_where_they_went() {
    // Config-file schedules were replaced by stored ones; an old file must not
    // load with them silently ignored.
    let (_dir, paths) = instance(&base());
    let mut text = fs::read_to_string(paths.config_file()).unwrap();
    text.push_str("\n[[schedules]]\nid = \"old\"\nat = \"07:00\"\nconversation = \"console\"\nprompt = \"Old.\"\n");
    fs::write(paths.config_file(), text).unwrap();
    let error = paths.load().unwrap_err().to_string();
    assert!(error.contains("oppa schedules add"), "{error}");
}

#[test]
fn oppa_conversations_lists_the_ids_a_schedule_can_name() {
    let (_dir, paths) = instance(&base());
    let store = Store::open(&paths.database()).unwrap();
    let id = "imsg/assistant/any;+;chat-guid";
    store
        .create_conversation(id, "imsg", "assistant", "any;+;chat-guid", "synthetic")
        .unwrap();
    store
        .set_conversation_shape(id, "group", Some("Synthetic Family"))
        .unwrap();
    let event = store
        .append_inbound(&InboundEvent {
            conversation: id.into(),
            native_id: "message-1".into(),
            author: "imsg:+15550001".into(),
            body: "Hello.".into(),
            ..Default::default()
        })
        .unwrap();
    let output = oppa(&paths, &["conversations"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap(),
        json!([{"id":id,"channel":"imsg","account":"assistant","kind":"group",
            "title":"Synthetic Family","last_event_at":event.created_at}])
    );
}

/// A broker for an owner turn in `fake/acct/dm-1`, with a family group and the
/// console in reach, at `access`; `manage` is false for a worker's grant.
fn tool_fixture(access: Access, manage: bool) -> (tempfile::TempDir, Paths, Broker, i64) {
    let mut config = fake_config();
    config.access = access;
    owner_turn(&config, manage, false)
}

/// The base configuration with its channel named `fake` on account `acct`.
fn fake_config() -> Config {
    let mut config = base();
    config.channels[0].id = "fake".into();
    config.channels[0].account = "acct".into();
    config
}

/// That owner turn's broker on an instance holding `config`, offered the wake
/// tools when `wakes`.
fn owner_turn(
    config: &Config,
    manage: bool,
    wakes: bool,
) -> (tempfile::TempDir, Paths, Broker, i64) {
    let access = config.access;
    let (dir, paths) = instance(config);
    let store = Store::open(&paths.database()).unwrap();
    for (id, native, kind) in [
        ("fake/acct/dm-1", "dm-1", "dm"),
        ("fake/acct/family", "family", "group"),
    ] {
        store
            .create_conversation(id, "fake", "acct", native, "policy")
            .unwrap();
        store.set_conversation_shape(id, kind, None).unwrap();
    }
    store
        .create_conversation("console", "console", "local", "console", "policy")
        .unwrap();
    let input = store
        .append_inbound(&InboundEvent {
            conversation: "fake/acct/dm-1".into(),
            native_id: "m1".into(),
            author: "synthetic-owner".into(),
            body: "Remind the family group to water the lawn at 7.".into(),
            ..Default::default()
        })
        .unwrap();
    Vault::init(&paths.vault()).unwrap();
    let launch = BrokerLaunch {
        private_dir: paths.private.clone(),
        grant: RunGrant {
            wakes,
            access,
            context: "synthetic-context".into(),
            requester: "synthetic-owner".into(),
            role: oppa::config::Role::Owner,
            run_id: "synthetic-run".into(),
            input_seq: input.seq,
            conversations: vec!["fake/acct/dm-1".into()],
            conversation_scopes: vec![
                ConversationScope {
                    channel: "fake".into(),
                    account: "acct".into(),
                    owners_account: false,
                },
                ConversationScope {
                    channel: "console".into(),
                    account: "local".into(),
                    owners_account: false,
                },
            ],
            read_scopes: vec!["*".into()],
            write_scopes: vec![],
            task_access: if manage {
                TaskAccess::Manage
            } else {
                TaskAccess::None
            },
            eligible_agents: vec![],
            expires_at: chrono::Utc::now().timestamp() + 300,
        },
        database: paths.database(),
        vault: paths.vault(),
        journal: dir.path().join("journal"),
    };
    let broker = Broker::open(launch).unwrap();
    (dir, paths, broker, input.seq)
}

/// A one-off slot a few minutes from now, as a date and a time.
fn soon() -> (String, String) {
    let at = chrono::Local::now() + TimeDelta::minutes(5);
    (
        at.format("%Y-%m-%d").to_string(),
        at.format("%H:%M").to_string(),
    )
}

fn create(broker: &Broker, conversation: &str, key: &str, date: &str, at: &str) -> ToolResult {
    broker.call(
        "schedules.create",
        json!({"context":"synthetic-context","conversation":conversation,"date":date,"at":at,
            "prompt":"Remind everyone here to water the lawn.","idempotency_key":key}),
    )
}

fn fault(result: ToolResult) -> String {
    assert!(result.is_error, "{}", result.value);
    result.value["error"]["code"].as_str().unwrap().to_owned()
}

#[test]
fn the_owner_can_schedule_a_message_in_conversation_and_cancel_it() {
    // The owner's request from 2026-10-03 that failed: "remind us at 7pm by
    // sending a message here". P-6: standing work made and stopped by chat.
    let (_dir, paths, broker, input_seq) = tool_fixture(Access::Connectors, true);
    let (date, at) = soon();
    let made = create(&broker, "fake/acct/family", "lawn", &date, &at);
    assert!(!made.is_error, "{}", made.value);
    let schedule = &made.value["receipt"];
    assert_eq!(schedule["created_by"], "assistant");
    assert_eq!(schedule["origin_seq"], input_seq);
    assert_eq!(schedule["conversation"], "fake/acct/family");
    assert!(schedule["next_slot"].is_string(), "{schedule}");
    let id = schedule["id"].as_str().unwrap().to_owned();
    // The same request again is the same schedule, not a second one.
    let again = create(&broker, "fake/acct/family", "lawn", &date, &at);
    assert_eq!(again.value["replayed"], true);
    assert_eq!(again.value["receipt"]["id"], id.as_str());
    assert_eq!(
        ScheduleStore::open(&paths)
            .unwrap()
            .list(false)
            .unwrap()
            .len(),
        1
    );
    let listed = broker.call("schedules.list", json!({"context":"synthetic-context"}));
    assert_eq!(listed.value["schedules"][0]["id"], id.as_str());
    // Serve reads the same store, so the schedule needs no restart to fire.
    let stored = ScheduleStore::open(&paths).unwrap().get(&id).unwrap();
    assert_eq!(
        stored.schedule.prompt,
        "Remind everyone here to water the lawn."
    );
    assert!(
        stored.schedule.command.is_empty(),
        "a model never sets a command"
    );

    let cancelled = broker.call(
        "schedules.cancel",
        json!({"context":"synthetic-context","id":id}),
    );
    assert!(!cancelled.is_error, "{}", cancelled.value);
    assert!(cancelled.value["receipt"]["cancelled_at"].is_string());
    assert!(ScheduleStore::open(&paths)
        .unwrap()
        .list(false)
        .unwrap()
        .is_empty());
    // Both changes are effects of this run, so no other harness repeats them.
    let observations = broker.observations().unwrap();
    assert_eq!(observations["activity"], "effect");
    let tiers: Vec<_> = observations["writes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|write| (write["tool"].clone(), write["status"].clone()))
        .collect();
    assert_eq!(tiers.len(), 2, "{tiers:?}");
}

#[test]
fn a_schedule_stays_here_at_read_access_and_never_targets_the_console() {
    // S-3: read access keeps the earlier boundary, so content the model read
    // cannot make it speak in another conversation; a past or malformed time,
    // a console target and a worker's grant are refused before anything is kept.
    let (_dir, paths, broker, _) = tool_fixture(Access::Read, true);
    let (date, at) = soon();
    assert_eq!(
        fault(create(&broker, "fake/acct/family", "a", &date, &at)),
        "access_denied"
    );
    assert!(!create(&broker, "fake/acct/dm-1", "b", &date, &at).is_error);
    assert_eq!(
        fault(create(&broker, "console", "c", &date, &at)),
        "not_deliverable"
    );
    assert_eq!(
        fault(create(
            &broker,
            "fake/acct/dm-1",
            "d",
            "2020-01-01",
            "07:00"
        )),
        "invalid_schedule"
    );
    assert_eq!(
        fault(create(
            &broker,
            "fake/acct/dm-1",
            "e",
            "2026-02-30",
            "07:00"
        )),
        "invalid_schedule"
    );
    assert_eq!(
        fault(create(&broker, "fake/acct/unknown", "f", &date, &at)),
        "access_denied"
    );
    assert_eq!(
        ScheduleStore::open(&paths)
            .unwrap()
            .list(true)
            .unwrap()
            .len(),
        1
    );
    // Nor can it stop what the owner set up in another conversation.
    let elsewhere = Schedule {
        id: "family-briefing".into(),
        conversation: "fake/acct/family".into(),
        ..schedule("07:00", &[], 60)
    };
    ScheduleStore::open(&paths)
        .unwrap()
        .add(&elsewhere, "owner", None)
        .unwrap();
    let cancel = |id: &str| {
        broker.call(
            "schedules.cancel",
            json!({"context":"synthetic-context","id":id}),
        )
    };
    assert_eq!(fault(cancel("family-briefing")), "access_denied");
    let kept = ScheduleStore::open(&paths)
        .unwrap()
        .get("family-briefing")
        .unwrap();
    assert!(kept.cancelled_at.is_none());
    // A read turn cannot message another conversation at all.
    assert!(!broker
        .definitions()
        .iter()
        .any(|tool| tool["name"] == "messages.send"));

    let (_dir, _paths, worker, _) = tool_fixture(Access::Computer, false);
    assert!(!worker
        .definitions()
        .iter()
        .any(|tool| tool["name"].as_str().unwrap().starts_with("schedules.")));
    assert_eq!(
        fault(create(&worker, "fake/acct/dm-1", "g", &date, &at)),
        "access_denied"
    );
}

#[test]
fn a_message_to_another_conversation_waits_for_its_receipt_and_is_never_sent_twice() {
    // The owner's request from 2026-10-03 that failed: "send the parents group
    // a message". G-6: one outbox entry, its receipt reported, no second send.
    let (_dir, paths, broker, _) = tool_fixture(Access::Connectors, true);
    let send = |conversation: &str, key: &str| {
        broker.call(
            "messages.send",
            json!({"context":"synthetic-context","conversation":conversation,
                "text":"Lawn at 7 tonight.","idempotency_key":key}),
        )
    };
    assert_eq!(fault(send("fake/acct/dm-1", "x")), "current_conversation");
    assert_eq!(fault(send("console", "x")), "not_deliverable");
    assert_eq!(fault(send("fake/acct/unknown", "x")), "access_denied");
    // Stand in for serve: deliver the entry once it is queued.
    let database = paths.database();
    let serve = std::thread::spawn(move || {
        let store = Store::open(&database).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while std::time::Instant::now() < deadline {
            if let Some((id, conversation, _)) = store.queued_sends().unwrap().pop() {
                assert_eq!(conversation, "fake/acct/family");
                assert!(store.claim_send(id).unwrap());
                store
                    .finish_send(
                        id,
                        oppa::store::DeliveryObservation::Sent {
                            native_id: "native-1".into(),
                        },
                    )
                    .unwrap();
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        panic!("nothing was queued");
    });
    let sent = send("fake/acct/family", "lawn");
    serve.join().unwrap();
    assert!(!sent.is_error, "{}", sent.value);
    assert_eq!(sent.value["delivery"]["state"], "sent");
    let again = send("fake/acct/family", "lawn");
    assert_eq!(again.value["replayed"], true);
    let store = Store::open(&paths.database()).unwrap();
    let events = store.transcript("fake/acct/family", 0, 10).unwrap();
    assert_eq!(events.len(), 1, "one message, however often it is asked");
    assert_eq!(events[0].author, "assistant");
    assert!(store.queued_sends().unwrap().is_empty());
}

#[test]
fn a_picture_the_run_made_goes_out_with_a_message_here_or_elsewhere() {
    // Incident 2026-10-12: a child asked the family group for two pictures to
    // learn drawing from, and no reply could carry one. At computer access a
    // message carries files the run made, to this conversation too, from a
    // copy the host keeps until the channel sends it.
    let (dir, paths, broker, _) = tool_fixture(Access::Computer, true);
    let picture = dir.path().join("bluey idea.png");
    fs::write(&picture, b"synthetic png bytes").unwrap();
    let send = |files: Value, key: &str| {
        broker.call(
            "messages.send",
            json!({"context":"synthetic-context","conversation":"fake/acct/dm-1",
                "text":"Idea 1 to sketch.","files":files,"idempotency_key":key}),
        )
    };
    assert!(broker
        .definitions()
        .iter()
        .any(|tool| tool["name"] == "messages.send"
            && tool["inputSchema"]["properties"].get("files").is_some()));
    let folder = dir.path().to_string_lossy().to_string();
    // A pipe in a file's place is refused without waiting for a writer.
    let pipe = dir.path().join("pipe.png");
    assert!(std::process::Command::new("mkfifo")
        .arg(&pipe)
        .status()
        .unwrap()
        .success());
    for refused in [
        json!(["bluey idea.png"]),
        json!([folder]),
        json!([pipe]),
        json!([dir.path().join("missing.png")]),
        json!(vec![picture.clone(); 9]),
    ] {
        assert_eq!(
            fault(send(refused.clone(), "refused")),
            "invalid_arguments",
            "{refused}"
        );
    }
    let database = paths.database();
    let serve = std::thread::spawn(move || {
        let store = Store::open(&database).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while std::time::Instant::now() < deadline {
            if let Some((id, conversation, _)) = store.queued_sends().unwrap().pop() {
                assert_eq!(conversation, "fake/acct/dm-1");
                assert!(store.claim_send(id).unwrap());
                store
                    .finish_send(
                        id,
                        oppa::store::DeliveryObservation::Sent {
                            native_id: "native-picture".into(),
                        },
                    )
                    .unwrap();
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        panic!("nothing was queued");
    });
    let sent = send(json!([picture]), "idea-1");
    serve.join().unwrap();
    assert!(!sent.is_error, "{}", sent.value);
    assert_eq!(sent.value["delivery"]["state"], "sent");
    // The run's file may go; the copy the outbox names stays, and the same
    // send again is the same message.
    fs::remove_file(&picture).unwrap();
    let again = send(json!([picture]), "idea-1");
    assert_eq!(again.value["replayed"], true, "{}", again.value);
    let events = Store::open(&paths.database())
        .unwrap()
        .transcript("fake/acct/dm-1", 0, 10)
        .unwrap();
    let out: Vec<_> = events.iter().filter(|e| e.author == "assistant").collect();
    assert_eq!(out.len(), 1);
    let attachment = &out[0].attachments[0];
    assert_eq!(
        (attachment.name.as_deref(), attachment.mime_type.as_deref()),
        (Some("bluey idea.png"), Some("image/png"))
    );
    assert!(attachment
        .path
        .starts_with(&paths.runtime.join("outgoing").to_string_lossy().to_string()));
    assert_eq!(fs::read(&attachment.path).unwrap(), b"synthetic png bytes");
    // Below computer access a run has no files of its own to send.
    let (dir, _paths, connectors, _) = tool_fixture(Access::Connectors, true);
    let picture = dir.path().join("idea.png");
    fs::write(&picture, b"synthetic").unwrap();
    assert!(!connectors
        .definitions()
        .iter()
        .any(|tool| tool["name"] == "messages.send"
            && tool["inputSchema"]["properties"].get("files").is_some()));
    assert_eq!(
        fault(connectors.call(
            "messages.send",
            json!({"context":"synthetic-context","conversation":"fake/acct/family",
                "text":"Idea.","files":[picture],"idempotency_key":"k"}),
        )),
        "access_denied"
    );
}

#[test]
fn the_owner_sets_access_from_the_cli_and_the_rest_of_the_file_is_kept() {
    // S-3: access is the owner's protected setting; the dashboard changes it
    // through this command, which rewrites only its line and checks the result.
    let (_dir, paths) = instance(&base());
    let original = fs::read_to_string(paths.config_file()).unwrap();
    let edited = format!("# The owner's own comment stays.\n{original}");
    fs::write(paths.config_file(), &edited).unwrap();
    // The change takes the configuration lock, whose folder it makes if needed.
    fs::remove_dir_all(paths.runtime.join("locks")).unwrap();
    let set = oppa(&paths, &["settings", "access", "computer"]);
    assert!(
        set.status.success(),
        "{}",
        String::from_utf8_lossy(&set.stderr)
    );
    let set: Value = serde_json::from_slice(&set.stdout).unwrap();
    assert_eq!(
        (&set["access"], &set["previous"]),
        (&json!("computer"), &json!("read"))
    );
    let text = fs::read_to_string(paths.config_file()).unwrap();
    assert!(text.starts_with("# The owner's own comment stays.\n"));
    assert_eq!(text.matches("access = ").count(), 1, "{text}");
    assert_eq!(paths.load().unwrap().access, Access::Computer);
    let shown: Value = serde_json::from_slice(&oppa(&paths, &["settings"]).stdout).unwrap();
    assert_eq!(shown["access"], "computer");
    assert_eq!(shown["notes"]["rules"], "assistant/rules.md");
    assert!(!oppa(&paths, &["settings", "access", "everything"])
        .status
        .success());
    assert_eq!(paths.load().unwrap().access, Access::Computer);
}

#[tokio::test]
async fn a_message_serve_could_not_deliver_within_ten_minutes_is_never_sent() {
    // Review of feat/full-agency: a message queued while serve was down must not
    // go out long after the owner asked for it; it is recorded as not sent.
    let (_dir, paths, _broker, _) = tool_fixture(Access::Connectors, true);
    let store = Store::open(&paths.database()).unwrap();
    let stale = store
        .enqueue_reply(
            "fake/acct/family",
            "send:1:stale",
            "Too late.",
            vec![],
            None,
        )
        .unwrap();
    let fresh = store
        .enqueue_reply("fake/acct/family", "send:1:fresh", "In time.", vec![], None)
        .unwrap();
    let eleven_minutes_ago = (chrono::Utc::now() - TimeDelta::minutes(11)).to_rfc3339();
    rusqlite::Connection::open(paths.database())
        .unwrap()
        .execute(
            "UPDATE events SET created_at=?1 WHERE seq=?2",
            rusqlite::params![eleven_minutes_ago, stale.event.seq],
        )
        .unwrap();
    let runtime = Runtime::open(paths.clone()).unwrap();
    let mut sent = Vec::new();
    for entry in [&stale, &fresh] {
        runtime
            .deliver_queued(
                entry.id,
                &mut ConsoleDelivery(|text: &str| {
                    sent.push(text.to_owned());
                    Ok(())
                }),
            )
            .await
            .unwrap();
    }
    assert_eq!(sent, ["In time."], "only the fresh message went out");
    let withdrawn = store.outbox_entry(stale.id).unwrap();
    assert_eq!(
        (withdrawn.state, withdrawn.attempts),
        (DeliveryState::Queued, 1)
    );
    assert!(withdrawn.evidence.unwrap().contains("ten minutes"));
    assert!(
        store.queued_sends().unwrap().is_empty(),
        "nothing is offered again"
    );
}

#[test]
fn a_model_run_cannot_change_access_through_the_cli() {
    // S-3: every harness run carries the run marker, so a model told to raise
    // its own reach with `oppa settings access` is refused and nothing changes.
    let (_dir, paths) = instance(&base());
    let refused = std::process::Command::new(env!("CARGO_BIN_EXE_oppa"))
        .arg("--home")
        .arg(&paths.runtime)
        .arg("--private-dir")
        .arg(&paths.private)
        .args(["settings", "access", "computer"])
        .env(oppa::harness::RUN_MARKER, "1")
        .output()
        .unwrap();
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("owner's to change"));
    assert_eq!(paths.load().unwrap().access, Access::Read);
}

/// `config` with a script connector, which is enough to count as connected.
fn connected(mut config: Config) -> Config {
    config.connectors.push(ConnectorManifest {
        id: "mail".into(),
        kind: ConnectorKind::Script,
        command: vec!["/never/run".into()],
        timeout_seconds: 5,
        owner_only: false,
    });
    config
}

#[test]
fn the_heartbeat_runs_by_default_twice_a_day_once_anything_is_connected() {
    // P-1: with nothing connected, replies
    // and schedules carry the conversation; once mail or anything else is
    // connected, the assistant looks around at 08:30 and 16:30 by default.
    assert_eq!(Heartbeat::of(&base()), None);
    let default = Heartbeat::of(&connected(base())).unwrap();
    assert_eq!(default, Heartbeat::default());
    assert!(default.conversation.is_empty(), "the owner's chat is found");
    let slot = |hour, minute| default.due_slot(&at(9, 28, hour, minute));
    assert_eq!(slot(8, 29), None);
    assert_eq!(slot(8, 30), Some(at(9, 28, 8, 30)));
    assert_eq!(slot(16, 29), Some(at(9, 28, 8, 30)));
    assert_eq!(slot(16, 30), Some(at(9, 28, 16, 30)));
    assert_eq!(slot(20, 59), Some(at(9, 28, 16, 30)));
    assert_eq!(slot(21, 0), None);
    // One of the owner's own accounts counts as connected too.
    let mut owners = base();
    owners.channels.push(ChannelManifest {
        id: "mine".into(),
        account: "owner".into(),
        command: vec!["/never/run".into()],
        timeout_seconds: 5,
        account_holder: AccountHolder::Owner,
    });
    assert_eq!(Heartbeat::of(&owners), Some(Heartbeat::default()));
    // A [heartbeat] the owner wrote is used as written, and `off` stops any.
    let mut configured = connected(base());
    configured.heartbeat = Some(heartbeat(60, "08:00", "21:00"));
    assert_eq!(Heartbeat::of(&configured), configured.heartbeat);
    let text = toml::to_string(&connected(base())).unwrap() + "\n[heartbeat]\noff = true\n";
    let off: Config = toml::from_str(&text).unwrap();
    off.validate().unwrap();
    assert_eq!(Heartbeat::of(&off), None);
}

#[test]
fn a_heartbeat_that_names_no_chat_speaks_in_the_owners_latest_direct_one() {
    // P-1: it waits for the owner's first direct message to the assistant,
    // then follows the direct chat the owner wrote in last, on any of the
    // assistant's channels, never a group or someone else's chat.
    let mut config = connected(base());
    config.channels.push(ChannelManifest {
        id: "web".into(),
        account: "local".into(),
        command: vec!["/never/run".into()],
        timeout_seconds: 5,
        account_holder: Default::default(),
    });
    let (_dir, paths) = instance(&config);
    let store = Store::open(&paths.database()).unwrap();
    for (id, kind) in [
        ("imsg/assistant/family", "group"),
        ("imsg/assistant/sam", "dm"),
        ("imsg/assistant/owner-chat", "dm"),
        ("web/local/main", "dm"),
    ] {
        let mut parts = id.splitn(3, '/');
        let (channel, account, native) = (
            parts.next().unwrap(),
            parts.next().unwrap(),
            parts.next().unwrap(),
        );
        store
            .create_conversation(id, channel, account, native, "policy")
            .unwrap();
        store.set_conversation_shape(id, kind, None).unwrap();
    }
    let says = |conversation: &str, native_id: &str, author: &str| {
        store
            .append_inbound(&InboundEvent {
                conversation: conversation.into(),
                native_id: native_id.into(),
                author: author.into(),
                body: "Hello.".into(),
                ..Default::default()
            })
            .unwrap();
    };
    let chat = |config: &Config| {
        Heartbeat::running(config, &store)
            .unwrap()
            .map(|heartbeat| heartbeat.conversation)
    };
    assert_eq!(chat(&config), None);
    says("imsg/assistant/family", "m1", "synthetic-owner");
    says("imsg/assistant/sam", "m2", "sam");
    assert_eq!(chat(&config), None, "a group or someone else's chat");
    says("imsg/assistant/owner-chat", "m3", "synthetic-owner");
    assert_eq!(chat(&config).as_deref(), Some("imsg/assistant/owner-chat"));
    says("web/local/main", "m4", "synthetic-owner");
    assert_eq!(chat(&config).as_deref(), Some("web/local/main"));
    says("imsg/assistant/owner-chat", "m5", "synthetic-owner");
    assert_eq!(chat(&config).as_deref(), Some("imsg/assistant/owner-chat"));
    // A look logged in the other chat since, like a wake's, is not the owner
    // writing there.
    store
        .append_inbound(&InboundEvent {
            conversation: "web/local/main".into(),
            native_id: "schedule:wake:slot".into(),
            author: "synthetic-owner".into(),
            body: "{}".into(),
            kind: "heartbeat".into(),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(chat(&config).as_deref(), Some("imsg/assistant/owner-chat"));
    // A chat the owner named stays the one.
    config.heartbeat = Some(Heartbeat {
        conversation: "web/local/main".into(),
        ..Heartbeat::default()
    });
    assert_eq!(chat(&config).as_deref(), Some("web/local/main"));
}

/// A date `days` from today, as the tools take it.
fn day(days: u64) -> String {
    (chrono::Local::now().date_naive() + chrono::Days::new(days))
        .format("%Y-%m-%d")
        .to_string()
}

fn wake(broker: &Broker, key: &str, date: &str, at: &str) -> ToolResult {
    broker.call(
        "wakes.set",
        json!({"context":"synthetic-context","date":date,"at":at,"idempotency_key":key,
            "reason":"Read shopping/kite-stock.md and tell the owner if the Comet kit is back in stock."}),
    )
}

#[test]
fn the_assistant_sets_its_own_wakes_within_the_hosts_bounds() {
    // P-1: the model chooses when to look
    // again; the host keeps a wake in the owner's direct chat, in waking
    // hours, 10 minutes to 14 days ahead, with 8 pending and 12 set in a day.
    let (_dir, paths, broker, input) = owner_turn(&fake_config(), true, true);
    let tomorrow = day(1);
    assert_eq!(
        fault(wake(&broker, "a", &tomorrow, "10:00")),
        "no_heartbeat",
        "nothing is connected"
    );
    fs::write(
        paths.config_file(),
        toml::to_string(&connected(fake_config())).unwrap(),
    )
    .unwrap();
    let early = chrono::Local::now() + TimeDelta::minutes(5);
    let too_soon = (
        early.format("%Y-%m-%d").to_string(),
        early.format("%H:%M").to_string(),
    );
    for (date, at) in [
        (too_soon.0.as_str(), too_soon.1.as_str()),
        (tomorrow.as_str(), "07:00"),
        (tomorrow.as_str(), "21:00"),
        (day(15).as_str(), "10:00"),
        ("2026-02-30", "10:00"),
    ] {
        assert_eq!(
            fault(wake(&broker, "a", date, at)),
            "invalid_wake",
            "{date} {at}"
        );
    }
    let set = wake(&broker, "a", &tomorrow, "10:00");
    assert!(!set.is_error, "{}", set.value);
    let receipt = &set.value["receipt"];
    let id = receipt["id"].as_str().unwrap().to_owned();
    assert_eq!(
        (
            &receipt["wake"],
            &receipt["conversation"],
            &receipt["created_by"],
            &receipt["origin_seq"],
            &receipt["date"],
            &receipt["at"],
        ),
        (
            &json!(true),
            &json!("fake/acct/dm-1"),
            &json!("assistant"),
            &json!(input),
            &json!(tomorrow),
            &json!("10:00"),
        )
    );
    assert!(receipt["prompt"].as_str().unwrap().contains("Comet kit"));
    let again = wake(&broker, "a", &tomorrow, "10:00");
    assert_eq!(
        (&again.value["receipt"]["id"], &again.value["replayed"]),
        (&json!(id), &json!(true))
    );
    // The same key is that wake again, whatever else the call asks.
    let same = wake(&broker, "a", &tomorrow, "11:30");
    assert_eq!(
        (&same.value["receipt"]["id"], &same.value["receipt"]["at"]),
        (&json!(id), &json!("10:00"))
    );
    // A wake is the assistant's own: listed by wakes.list, not schedules.list.
    let listed = broker.call("wakes.list", json!({"context":"synthetic-context"}));
    assert_eq!(listed.value["wakes"][0]["id"], id.as_str());
    let schedules = broker.call("schedules.list", json!({"context":"synthetic-context"}));
    assert_eq!(schedules.value["schedules"], json!([]));

    for key in 1..8 {
        let at = format!("10:{key:02}");
        assert!(!wake(&broker, &format!("k{key}"), &tomorrow, &at).is_error);
    }
    assert_eq!(
        fault(wake(&broker, "ninth", &tomorrow, "11:00")),
        "too_many_wakes",
        "eight are pending"
    );
    // Cancelling makes room among the pending, never among the day's twelve.
    let ids: Vec<String> = broker
        .call("wakes.list", json!({"context":"synthetic-context"}))
        .value["wakes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|wake| wake["id"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(ids.len(), 8);
    for id in &ids {
        let cancelled = broker.call(
            "wakes.cancel",
            json!({"context":"synthetic-context","id":id}),
        );
        assert!(cancelled.value["receipt"]["cancelled_at"].is_string());
    }
    for key in 8..12 {
        let at = format!("10:{key:02}");
        assert!(!wake(&broker, &format!("k{key}"), &tomorrow, &at).is_error);
    }
    assert_eq!(
        fault(wake(&broker, "thirteenth", &tomorrow, "11:00")),
        "too_many_wakes",
        "twelve were set today"
    );
    // wakes.cancel stops only wakes; a schedule is the owner's standing work.
    let (date, at) = soon();
    let schedule = create(&broker, "fake/acct/dm-1", "s", &date, &at);
    let schedule = schedule.value["receipt"]["id"].as_str().unwrap().to_owned();
    assert_eq!(
        fault(broker.call(
            "wakes.cancel",
            json!({"context":"synthetic-context","id":schedule}),
        )),
        "invalid_arguments"
    );
    // Each wake is an effect of this run, so no other harness repeats it.
    let observations = broker.observations().unwrap();
    assert_eq!(observations["activity"], "effect");

    // A grant without wakes is offered none and refused them.
    let (_dir, _, broker, _) = owner_turn(&connected(fake_config()), true, false);
    assert!(!broker
        .definitions()
        .iter()
        .any(|tool| tool["name"].as_str().unwrap().starts_with("wakes.")));
    assert_eq!(
        fault(wake(&broker, "a", &tomorrow, "10:00")),
        "access_denied"
    );
}

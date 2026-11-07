//! The memory sync: what changed in the owner's
//! other agents' memory reaches an owner turn as data, once marked it is not
//! offered again, and the turn runs in the owner's direct chat. Every source is
//! a synthetic file in a temporary folder, never the real ~/.claude or ~/.codex.

use chrono::TimeZone;
use oppa::{
    config::{Config, Paths},
    harness::AgentManifest,
    memory_sync::MemorySync,
    runtime::{ConsoleDelivery, Runtime},
    store::{InboundEvent, Store},
    vault::Vault,
};
use serde_json::Value;
use std::{fs, path::Path, process::Command};

const CHANGES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/resources/adapters/memory_changes.py"
);

fn changes(state: &Path, sources: &[String], mark: Option<&str>) -> (bool, String) {
    let mut command = Command::new("python3");
    command.arg(CHANGES).arg("--state").arg(state);
    for source in sources {
        command.args(["--source", source]);
    }
    if let Some(digest) = mark {
        command.args(["--mark", digest]);
    }
    let output = command.output().unwrap();
    let text =
        String::from_utf8(output.stdout).unwrap() + &String::from_utf8(output.stderr).unwrap();
    (output.status.success(), text)
}

#[test]
fn changed_memory_is_offered_until_marked_and_removals_are_named() {
    let dir = tempfile::tempdir().unwrap();
    let claude = dir.path().join("claude/projects/a/memory");
    fs::create_dir_all(&claude).unwrap();
    fs::write(claude.join("prefs.md"), "Sam prefers tea.\n").unwrap();
    fs::write(dir.path().join("codex.md"), "Sam's dentist is Dr. Lee.\n").unwrap();
    let sources = vec![
        format!("{}/claude/projects/*/memory/**/*.md", dir.path().display()),
        format!("{}/codex.md", dir.path().display()),
    ];
    let state = dir.path().join("sync.json");
    let (ok, first) = changes(&state, &sources, None);
    assert!(ok, "{first}");
    let first: Value = serde_json::from_str(&first).unwrap();
    assert_eq!(first["first_sync"], true);
    let texts: Vec<_> = first["changed"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| (entry["status"].clone(), entry["text"].clone()))
        .collect();
    assert_eq!(texts.len(), 2);
    assert!(texts.iter().all(|(status, _)| status == "new"));
    assert!(texts.iter().any(|(_, text)| text == "Sam prefers tea.\n"));
    // Not marked: the same changes come again, which is safe.
    let (_, again) = changes(&state, &sources, None);
    assert_eq!(
        serde_json::from_str::<Value>(&again).unwrap()["changed"],
        first["changed"]
    );
    let digest = first["mark"]
        .as_str()
        .unwrap()
        .rsplit(' ')
        .next()
        .unwrap()
        .to_owned();
    assert!(first["mark"].as_str().unwrap().contains("--source"));
    let (ok, marked) = changes(&state, &sources, Some(&digest));
    assert!(ok && marked.starts_with("marked"), "{marked}");
    let (_, quiet) = changes(&state, &sources, None);
    let quiet: Value = serde_json::from_str(&quiet).unwrap();
    assert_eq!(
        (&quiet["first_sync"], &quiet["changed"], &quiet["unchanged"]),
        (
            &Value::Bool(false),
            &serde_json::json!([]),
            &serde_json::json!(2)
        )
    );
    fs::write(claude.join("prefs.md"), "Sam prefers coffee now.\n").unwrap();
    fs::remove_file(dir.path().join("codex.md")).unwrap();
    let (_, later) = changes(&state, &sources, None);
    let later: Value = serde_json::from_str(&later).unwrap();
    assert_eq!(later["changed"][0]["status"], "changed");
    assert_eq!(later["changed"][0]["text"], "Sam prefers coffee now.\n");
    assert_eq!(later["removed"].as_array().unwrap().len(), 1);
    // A digest from before the change is refused rather than hiding it.
    let (ok, refused) = changes(&state, &sources, Some(&digest));
    assert!(!ok && refused.contains("changed since"), "{refused}");
    // Many changes come 25 at a time, newest first, until all are in.
    for index in 0..35 {
        fs::write(
            claude.join(format!("note-{index}.md")),
            format!("Fact {index}.\n"),
        )
        .unwrap();
    }
    let mut offered = 0;
    loop {
        let (_, batch) = changes(&state, &sources, None);
        let batch: Value = serde_json::from_str(&batch).unwrap();
        let count = batch["changed"].as_array().unwrap().len();
        if count == 0 && batch["removed"].as_array().unwrap().is_empty() {
            break;
        }
        assert!(count <= 25);
        offered += count;
        let digest = batch["mark"]
            .as_str()
            .unwrap()
            .rsplit(' ')
            .next()
            .unwrap()
            .to_owned();
        assert!(changes(&state, &sources, Some(&digest)).0);
    }
    assert_eq!(offered, 36, "35 new notes and the changed one, each once");
}

fn config(root: &Path, sources: Vec<String>) -> Config {
    let adapter = r#"
import json, pathlib, sys
capture = pathlib.Path(sys.argv[1])
capture.write_text(json.dumps(json.load(sys.stdin)))
print(json.dumps({"type":"session","handle":"sync-session"}))
print(json.dumps({"type":"answer","text":"NO_REPLY"}))
print(json.dumps({"type":"completed"}))
"#;
    Config {
        access: oppa::harness::Access::Computer,
        version: 1,
        connectors: vec![],
        sessions: Default::default(),
        source: None,
        channels: vec![],
        people: vec![],
        owner: "synthetic-owner".into(),
        agents: vec![AgentManifest {
            id: "first".into(),
            family: "synthetic".into(),
            command: vec![
                "python3".into(),
                "-c".into(),
                adapter.into(),
                root.join("request.json").display().to_string(),
            ],
            timeout_seconds: 30,
            task_timeout_seconds: 30,
            model: None,
            effort: None,
        }],
        groups: vec![],
        heartbeat: None,
        browser: false,
        memory_sync: Some(MemorySync {
            at: "03:30".into(),
            sources,
            conversation: None,
        }),
    }
}

#[tokio::test]
async fn the_sync_is_an_owner_turn_given_the_changes_as_data() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("notes.md"), "Sam's sister is Ana.\n").unwrap();
    let paths = Paths::new(dir.path().join("runtime"), Some(dir.path().join("private"))).unwrap();
    paths.prepare().unwrap();
    let config = config(
        dir.path(),
        vec![format!("{}/notes.md", dir.path().display())],
    );
    fs::write(paths.config_file(), toml::to_string(&config).unwrap()).unwrap();
    Vault::init(&paths.vault()).unwrap();
    Store::open(&paths.database()).unwrap();
    oppa::harness::install_bundled_adapters(&paths.runtime).unwrap();
    let schedule = config
        .memory_sync
        .as_ref()
        .unwrap()
        .schedule(&paths, "console");
    let runtime = Runtime::open(paths).unwrap();
    let slot = chrono::Local::now();
    runtime
        .fire_schedule(&schedule, &slot, &mut ConsoleDelivery(|_: &str| Ok(())))
        .await
        .unwrap();
    let request: Value =
        serde_json::from_str(&fs::read_to_string(dir.path().join("request.json")).unwrap())
            .unwrap();
    assert_eq!(request["access"], "computer");
    let prompt = request["prompt"].as_str().unwrap();
    assert!(prompt.contains("Memory sync."), "{prompt}");
    assert!(prompt.contains("source of truth"));
    assert!(
        prompt.contains("Sam's sister is Ana."),
        "the change reached the turn"
    );
    assert!(prompt.contains("owner-schedule"));
}

#[test]
fn the_sync_runs_in_the_owners_own_direct_chat() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("state.db")).unwrap();
    let sync = MemorySync {
        at: "03:30".into(),
        sources: oppa::memory_sync::default_sources(),
        conversation: None,
    };
    assert_eq!(sync.chat(&store, "synthetic-owner").unwrap(), None);
    for (native, kind, author) in [
        ("owner-chat", "dm", "synthetic-owner"),
        ("sam-chat", "dm", "sam"),
        ("family", "group", "synthetic-owner"),
    ] {
        let id = format!("imsg/assistant/{native}");
        store
            .create_conversation(&id, "imsg", "assistant", native, "policy")
            .unwrap();
        store.set_conversation_shape(&id, kind, None).unwrap();
        store
            .append_inbound(&InboundEvent {
                conversation: id.clone(),
                author: author.into(),
                body: "Hello.".into(),
                native_id: format!("{native}-1"),
                kind: "message".into(),
                ..Default::default()
            })
            .unwrap();
    }
    assert_eq!(
        sync.chat(&store, "synthetic-owner").unwrap().as_deref(),
        Some("imsg/assistant/owner-chat")
    );
    let configured = MemorySync {
        conversation: Some("imsg/assistant/elsewhere".into()),
        ..sync
    };
    assert_eq!(
        configured
            .chat(&store, "synthetic-owner")
            .unwrap()
            .as_deref(),
        Some("imsg/assistant/elsewhere")
    );
}

#[test]
fn an_import_runs_every_ten_minutes_until_nothing_is_left_and_a_no_keeps_the_nightly_sync_off() {
    // Onboarding: the owner's yes in chat starts the import. Before it nothing
    // runs, not even the nightly sync; during it a batch comes every ten
    // minutes; it ends when a mark leaves nothing, or after three hours.
    let dir = tempfile::tempdir().unwrap();
    let notes = dir.path().join("notes");
    fs::create_dir_all(&notes).unwrap();
    for n in 0..30 {
        fs::write(notes.join(format!("{n}.md")), format!("Fact {n}.\n")).unwrap();
    }
    let sources = vec![format!("{}/*.md", notes.display())];
    let paths = Paths::new(dir.path().join("runtime"), Some(dir.path().join("private"))).unwrap();
    paths.prepare().unwrap();
    let sync = MemorySync {
        at: "03:30".into(),
        sources: sources.clone(),
        conversation: None,
    };
    let chat = "imsg/assistant/owner-chat";
    let at = |hour, minute| {
        chrono::Local
            .with_ymd_and_hms(2026, 10, 13, hour, minute, 0)
            .unwrap()
    };
    let ids = |due: &[oppa::schedule::Schedule]| -> Vec<String> {
        due.iter().map(|schedule| schedule.id.clone()).collect()
    };
    assert!(
        sync.due(&paths, 0, chat, at(9, 0)).is_empty(),
        "no yes, no sync"
    );

    oppa::memory_sync::start_import(&paths).unwrap();
    // A minute into the current ten-minute slot: in a slot's last minute the
    // clock is past its nine-minute window, and the next slot takes over.
    let now = chrono::Local::now();
    let now = now - chrono::TimeDelta::seconds(now.timestamp().rem_euclid(600) - 60);
    let due = sync.due(&paths, 0, chat, now);
    assert_eq!(ids(&due), ["memory-import"]);
    let slot_start = {
        use chrono::Timelike;
        now.with_second(0)
            .and_then(|t| t.with_nanosecond(0))
            .and_then(|t| t.with_minute(t.minute() / 10 * 10))
            .unwrap()
    };
    let import = &due[0];
    assert_eq!(import.conversation, chat);
    assert_eq!(import.window_minutes, 9);
    assert_eq!(
        // Asked at the start of now's ten minutes: past nine minutes into
        // them, now itself is outside the window.
        import
            .due_slot(&slot_start)
            .unwrap()
            .format("%M")
            .to_string()
            .parse::<u32>()
            .unwrap()
            % 10,
        0,
        "one slot each ten minutes"
    );
    assert!(import.prompt.contains("not_yet_offered is 0"));

    // Each batch marks what it took in, the way the turn does.
    let state = paths.runtime.join("memory-sync.json");
    let batch = || {
        let (ok, listed) = changes(&state, &sources, None);
        assert!(ok, "{listed}");
        let listed: Value = serde_json::from_str(&listed).unwrap();
        let digest = listed["mark"]
            .as_str()
            .unwrap()
            .rsplit(' ')
            .next()
            .unwrap()
            .to_owned();
        let (ok, marked) = changes(&state, &sources, Some(&digest));
        assert!(ok, "{marked}");
    };
    batch();
    assert_eq!(
        ids(&sync.due(&paths, 0, chat, now)),
        ["memory-sync", "memory-import"],
        "five notes are left, so the import goes on"
    );
    batch();
    assert_eq!(
        ids(&sync.due(&paths, 0, chat, now)),
        ["memory-sync"],
        "nothing is left: the import ends and the nightly sync stays"
    );
    assert!(!paths.runtime.join("memory-import.json").exists());

    // An import that never finishes stops after three hours.
    oppa::memory_sync::start_import(&paths).unwrap();
    fs::write(notes.join("new.md"), "Fact new.\n").unwrap();
    let later = chrono::Local::now() + chrono::TimeDelta::hours(3) + chrono::TimeDelta::minutes(1);
    assert_eq!(ids(&sync.due(&paths, 0, chat, later)), ["memory-sync"]);
    assert!(!paths.runtime.join("memory-import.json").exists());

    // A marker that cannot be read never blocks a later yes.
    fs::write(paths.runtime.join("memory-import.json"), "not json").unwrap();
    oppa::memory_sync::start_import(&paths).unwrap();
    assert_eq!(
        ids(&sync.due(&paths, 0, chat, chrono::Local::now())),
        ["memory-sync", "memory-import"]
    );
}

#[tokio::test]
async fn each_night_a_review_is_given_what_came_after_the_last_finished_one() {
    // M-11: reused procedures become skills. A lesson the owner taught once in
    // chat (a misleading search, a blocked site, a corrected answer) should
    // reach a skill even when nobody asked for it to be kept, and even when the
    // owner never said yes to the memory import. A review that stops before
    // its mark leaves its span for the next one, and a long history is read a
    // slice at a time. The runtime path needs quoting in the mark command.
    let dir = tempfile::tempdir().unwrap();
    let paths = Paths::new(
        dir.path().join("run time's"),
        Some(dir.path().join("private")),
    )
    .unwrap();
    paths.prepare().unwrap();
    let mut config = config(dir.path(), oppa::memory_sync::default_sources());
    config.memory_sync.as_mut().unwrap().at = "00:10".into();
    let sync = config.memory_sync.clone().unwrap();
    let at = |day, hour, minute| {
        chrono::Local
            .with_ymd_and_hms(2026, 10, day, hour, minute, 0)
            .unwrap()
    };
    let night = at(13, 23, 50);
    assert!(
        sync.due(&paths, 0, "console", night).is_empty(),
        "no history, no review"
    );

    let due = sync.due(&paths, 5, "console", night);
    assert_eq!(due.len(), 1, "no import, so only the review");
    let review = due[0].clone();
    assert_eq!(review.id, oppa::memory_sync::REVIEW_ID);
    assert_eq!(
        review.at, "23:40",
        "half an hour before the sync, across midnight"
    );
    assert!(review.command.is_empty() && review.date.is_none());
    assert_eq!(review.due_slot(&at(14, 0, 5)), Some(at(13, 23, 40)));
    review.validate(&config.channels).unwrap();
    // It sets tomorrow's heartbeat pace with the host's own command and
    // messages nobody at night.
    let pace = oppa::heartbeat::pace_command(&paths);
    assert!(review.prompt.contains(&pace) && !review.prompt.contains("{pace}"));
    assert!(review
        .prompt
        .contains("never message anyone from this turn"));
    assert!(
        review.prompt.contains("seq after 0 through 5"),
        "{}",
        review.prompt
    );

    // Unmarked, as when the turn failed or timed out: the span stays.
    assert!(sync.due(&paths, 9, "console", night)[0]
        .prompt
        .contains("seq after 0 through 9"));

    // The turn runs the mark command the prompt gives it, through a shell.
    let mark = review
        .prompt
        .split("run this so the next review starts after it: ")
        .nth(1)
        .unwrap();
    let ran = Command::new("sh").args(["-c", mark]).status().unwrap();
    assert!(ran.success(), "{mark}");
    assert!(
        sync.due(&paths, 5, "console", night).is_empty(),
        "nothing new since the mark"
    );
    assert!(sync.due(&paths, 9, "console", night)[0]
        .prompt
        .contains("seq after 5 through 9"));
    // A slot queued behind that turn still carries the done span: the check
    // its prompt starts with shows a mark at or past the span's end.
    let check = review.prompt.split("First run ").nth(1).unwrap();
    let check = check
        .split(": if a line there is 5 or more")
        .next()
        .unwrap();
    let shown = Command::new("sh").args(["-c", check]).output().unwrap();
    assert!(shown.status.success(), "{check}");
    assert!(String::from_utf8_lossy(&shown.stdout)
        .lines()
        .any(|line| line.trim() == "5"));
    assert!(review
        .prompt
        .contains("another review already did this span; return NO_REPLY"));
    // A mark past the newest event, such as a mistyped one, is ignored.
    let typo = review.prompt.replace("echo 5 >>", "echo 9999 >>");
    let typo = typo.rsplit("starts after it: ").next().unwrap();
    assert!(Command::new("sh")
        .args(["-c", typo])
        .status()
        .unwrap()
        .success());
    assert!(sync.due(&paths, 9, "console", night)[0]
        .prompt
        .contains("seq after 5 through 9"));
    // A slower review of an older span marks after it: the marks never go back.
    let older = review.prompt.replace("echo 5 >>", "echo 3 >>");
    let older = older.rsplit("starts after it: ").next().unwrap();
    assert!(Command::new("sh")
        .args(["-c", older])
        .status()
        .unwrap()
        .success());
    assert!(sync.due(&paths, 9, "console", night)[0]
        .prompt
        .contains("seq after 5 through 9"));

    // A long history: a hundred events a turn, every ten minutes for three
    // hours after the nightly time while more is left, then nightly again.
    let slice = |now| sync.due(&paths, 450, "console", now).remove(0);
    let catching_up = slice(at(14, 1, 17));
    assert!(catching_up.prompt.contains("seq after 5 through 105"));
    assert_eq!(catching_up.date.as_deref(), Some("2026-10-14"));
    assert_eq!(catching_up.at, "01:10");
    assert_eq!(catching_up.due_slot(&at(14, 1, 19)), Some(at(14, 1, 10)));
    for outside in [at(13, 23, 30), at(14, 2, 41)] {
        let nightly = slice(outside);
        assert!(nightly.date.is_none() && nightly.at == "23:40", "{outside}");
    }
    // The last part slice still comes that night; a few events wait for the next.
    let last = sync.due(&paths, 55, "console", at(14, 1, 17)).remove(0);
    assert!(last.prompt.contains("seq after 5 through 55") && last.date.is_some());
    let few = sync.due(&paths, 12, "console", at(14, 1, 17)).remove(0);
    assert!(
        few.date.is_none(),
        "seven events wait for the nightly review"
    );

    // The review is the owner's turn at full access, told where skills live.
    fs::write(paths.config_file(), toml::to_string(&config).unwrap()).unwrap();
    Vault::init(&paths.vault()).unwrap();
    Store::open(&paths.database()).unwrap();
    oppa::harness::install_bundled_adapters(&paths.runtime).unwrap();
    let skills = paths.skills();
    let runtime = Runtime::open(paths).unwrap();
    runtime
        .fire_schedule(
            &review,
            &chrono::Local::now(),
            &mut ConsoleDelivery(|_: &str| Ok(())),
        )
        .await
        .unwrap();
    let request = fs::read_to_string(dir.path().join("request.json")).unwrap();
    let request: Value = serde_json::from_str(&request).unwrap();
    assert_eq!(request["access"], "computer");
    let prompt = request["prompt"].as_str().unwrap();
    assert!(prompt.contains(&skills.display().to_string()), "{prompt}");
    assert!(prompt.contains("seq after 0 through 5"));
}

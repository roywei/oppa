//! `oppa memory forget` (M-10, scenario 10): a text leaves every store a
//! forget covers, is reported where it stays, and nothing unrelated changes.

use oppa::store::{Attachment, DeliveryObservation, InboundEvent, Store};
use oppa::vault::{Edit, Provenance, Vault};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

const KEPT: &str = "11111111-2222-4333-8444-555555555555";
const CLAUDE: &str = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
const CODEX: &str = "0199aaaa-bbbb-7ccc-8ddd-ffffffffffff";

/// Whether this process runs only the test `name`. Elsewhere it runs `name`
/// by itself in a fresh process of this test binary, fails if it fails there,
/// and answers false. A test that sets process-wide environment, which every
/// test of a binary shares, runs this way.
fn alone(name: &str) -> bool {
    if std::env::var_os("OPPA_TEST_ALONE").is_some() {
        return true;
    }
    let output = oppa::process::output(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([name, "--exact", "--nocapture"])
            .env("OPPA_TEST_ALONE", name),
    )
    .unwrap();
    let printed = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() && printed.contains("test result: ok. 1 passed"),
        "{name} alone:\n{printed}{}",
        String::from_utf8_lossy(&output.stderr)
    );
    false
}

fn oppa(root: &Path, args: &[&str]) -> Output {
    oppa_after(root, "", args)
}

/// `oppa`, started by a shell after the line `setup`, such as a `ulimit`.
fn oppa_after(root: &Path, setup: &str, args: &[&str]) -> Output {
    let mut command = Command::new("sh");
    let script = format!("{setup}\nexec \"$@\"");
    command.args(["-c", &script, "sh", env!("CARGO_BIN_EXE_oppa")]);
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("GIT_")
            || key == "OPPA_HOME"
            || key == "OPPA_PRIVATE_DIR"
        {
            command.env_remove(key);
        }
    }
    command
        .env("CLAUDE_CONFIG_DIR", root.join("claude"))
        .env("CODEX_HOME", root.join("codex"))
        .arg("--home")
        .arg(root.join("runtime"))
        .arg("--private-dir")
        .arg(root.join("private"))
        .args(args)
        .output()
        .unwrap()
}

fn report(output: Output) -> Value {
    assert!(
        output.status.success(),
        "command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn git(repository: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .current_dir(repository)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

fn marker() -> String {
    let mut bytes = [0u8; 8];
    fs::File::open("/dev/urandom")
        .and_then(|mut file| std::io::Read::read_exact(&mut file, &mut bytes))
        .unwrap();
    format!(
        "zq{}",
        bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
    )
}

fn holds(path: &Path, marker: &str) -> bool {
    fs::read(path).is_ok_and(|bytes| bytes.windows(marker.len()).any(|w| w == marker.as_bytes()))
}

/// Every file below `root`, by path, except those `skip` names.
fn snapshot(root: &Path, skip: impl Fn(&Path) -> bool) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut files = BTreeMap::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(folder) = pending.pop() {
        for entry in fs::read_dir(&folder).unwrap() {
            let path = entry.unwrap().path();
            if skip(&path) {
                continue;
            } else if path.is_dir() {
                pending.push(path);
            } else {
                files.insert(path.clone(), fs::read(&path).unwrap());
            }
        }
    }
    files
}

fn commit(vault: &Vault, edits: Vec<Edit>, reason: &str) {
    let provenance = Provenance {
        author: "synthetic-owner".into(),
        source: "test".into(),
        run_id: "owner-console".into(),
        reason: reason.into(),
    };
    vault
        .commit(&vault.revision().unwrap(), &edits, &provenance)
        .unwrap();
}

fn write(path: PathBuf, content: &str) -> PathBuf {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, content).unwrap();
    path
}

struct Planted {
    root: tempfile::TempDir,
    marker: String,
    room: String,
    removed_sessions: Vec<PathBuf>,
}

/// An instance holding a fresh marker in each covered store, in each store a
/// forget only reports, and beside it unrelated notes, events and sessions.
fn planted() -> Planted {
    let root = tempfile::tempdir().unwrap();
    let base = root.path();
    report_ok(oppa(
        base,
        &["init", "--agent", "codex", "--owner", "synthetic-owner"],
    ));
    let marker = marker();
    let vault = Vault::open(&base.join("private/vault")).unwrap();
    let note = |text: &str| Edit::Write {
        path: "people/alex.md".into(),
        content: text.into(),
    };
    commit(
        &vault,
        vec![note(&format!("Alex's locker code is {marker}.\n"))],
        "Save the locker code",
    );
    commit(
        &vault,
        vec![
            note(&format!(
                "Alex likes tea.\nAlex's locker code is {marker}.\n"
            )),
            Edit::Write {
                path: "people/sam.md".into(),
                content: "Sam walks the dog at seven.\n".into(),
            },
        ],
        &format!("Note that {marker} is the code"),
    );
    // A note that held it only in history, and an owner commit whose subject the reflog keeps.
    let vault_root = base.join("private/vault");
    write(
        vault_root.join("trips/old.md"),
        &format!("Old plan {marker}\n"),
    );
    git(&vault_root, &["add", "trips/old.md"]);
    git(
        &vault_root,
        &[
            "-c",
            "user.name=Owner",
            "-c",
            "user.email=owner@example.test",
            "commit",
            "-qm",
            &format!("Plan {marker}"),
        ],
    );
    write(vault_root.join("trips/old.md"), "Old plan, done\n");
    git(
        &vault_root,
        &[
            "-c",
            "user.name=Owner",
            "-c",
            "user.email=owner@example.test",
            "commit",
            "-qam",
            "Finish the plan",
        ],
    );

    let store = Store::open(&base.join("runtime/state.db")).unwrap();
    let room = "console:local:room".to_string();
    store
        .create_conversation(&room, "console", "local", "room", "policy")
        .unwrap();
    store
        .set_conversation_shape(&room, "group", Some(&format!("Club {marker}")))
        .unwrap();
    let event = |id: &str, body: String, attachments| InboundEvent {
        conversation: room.clone(),
        native_id: id.into(),
        author: "synthetic-owner".into(),
        body,
        kind: "message".into(),
        attachments,
        ..Default::default()
    };
    store
        .append_inbound(&event("1", "Good morning".into(), vec![]))
        .unwrap();
    store
        .append_inbound(&event("2", format!("My locker code is {marker}"), vec![]))
        .unwrap();
    let reply = store
        .enqueue_reply(&room, "reply:2", &format!("Saved {marker}."), vec![], None)
        .unwrap();
    store.claim_send(reply.id).unwrap();
    store
        .finish_send(
            reply.id,
            DeliveryObservation::Sent {
                native_id: "out-1".into(),
            },
        )
        .unwrap();
    let attachment = Attachment {
        path: "attachments/photo.jpg".into(),
        mime_type: None,
        name: Some(format!("{marker}.jpg")),
    };
    store
        .append_inbound(&event("3", "A photo".into(), vec![attachment]))
        .unwrap();
    let last = store
        .append_inbound(&event("4", "See you later".into(), vec![]))
        .unwrap();
    store
        .save_session(&room, "claude", "policy", CLAUDE, last.seq)
        .unwrap();
    store
        .save_session(&room, "codex", "policy", CODEX, last.seq)
        .unwrap();
    let other = "console:local:other".to_string();
    store
        .create_conversation(&other, "console", "local", "other", "policy")
        .unwrap();
    let unrelated = store
        .append_inbound(&InboundEvent {
            conversation: other.clone(),
            ..event("5", "Unrelated".into(), vec![])
        })
        .unwrap();
    store
        .save_session(&other, "claude", "policy", KEPT, unrelated.seq)
        .unwrap();

    let json = serde_json::to_string(&format!("he said \"{marker}\"")).unwrap();
    let removed_sessions = vec![
        write(
            base.join(format!("claude/projects/-runtime-harness/{CLAUDE}.jsonl")),
            &format!("{{\"text\":{json}}}\n"),
        ),
        write(
            base.join(format!(
                "claude/projects/-runtime-harness/{CLAUDE}/subagents/agent-1.jsonl"
            )),
            "{}\n",
        ),
        write(
            base.join(format!(
                "claude/projects/-runtime-harness/{CLAUDE}/subagents/agent-2.jsonl"
            )),
            "{}\n",
        ),
        write(
            base.join(format!(
                "codex/sessions/2026/10/05/rollout-2026-10-05T07-00-00-{CODEX}.jsonl"
            )),
            &format!("{marker}\n"),
        ),
    ];
    // A session the table does not name, though it holds the text, and one it names that does not.
    write(
        base.join("claude/projects/-elsewhere/99999999-2222-4333-8444-555555555555.jsonl"),
        &marker,
    );
    write(
        base.join(format!("claude/projects/-runtime-harness/{KEPT}.jsonl")),
        "{\"text\":\"tea\"}\n",
    );
    write(
        base.join("runtime/runs/7-abc/attempt-1.json"),
        &format!("{{\"answer\":\"Saved {marker}.\"}}"),
    );
    write(
        base.join("runtime/tasks/t1/board.md"),
        &format!("- remember {marker}\n"),
    );
    Planted {
        root,
        marker,
        room,
        removed_sessions,
    }
}

fn report_ok(output: Output) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn places(report: &Value, field: &str) -> Vec<String> {
    report[field]
        .as_array()
        .unwrap()
        .iter()
        .map(|place| place.as_str().unwrap().to_owned())
        .collect()
}

#[test]
fn a_forget_removes_the_text_from_every_covered_store_and_reports_the_rest() {
    let Planted {
        root,
        marker,
        room,
        removed_sessions,
    } = planted();
    let base = root.path();
    let vault_root = base.join("private/vault");
    let unrelated = |path: &Path| {
        path.ends_with("state.db")
            || path.ends_with("state.db-wal")
            || path.ends_with("state.db-shm")
            || path.ends_with(".git")
            || path.ends_with("runtime/locks")
            // The instance's secret for digests, made by its first preview.
            || path.ends_with("private/forget.key")
            || path.ends_with("people/alex.md")
            || removed_sessions
                .iter()
                .any(|removed| path.starts_with(removed.with_extension("")) || path == removed)
    };
    let before = snapshot(base, unrelated);
    let store = Store::open(&base.join("runtime/state.db")).unwrap();
    let events_before = store.transcript(&room, 0, 100).unwrap();
    let other_before = store.transcript("console:local:other", 0, 100).unwrap();
    let sam_blob = git(&vault_root, &["rev-parse", "HEAD:people/sam.md"]);

    let preview = report(oppa(base, &["memory", "forget", &marker]));
    let covered = places(&preview, "covered");
    for place in [
        "note people/alex.md".to_string(),
        "vault history of people/alex.md".into(),
        "vault history of trips/old.md".into(),
        format!("message 2 in {room} by synthetic-owner"),
        format!("message 3 in {room} by assistant"),
        format!("title of {room}"),
        format!("native session of {room} with claude"),
        format!("native session of {room} with codex"),
    ] {
        assert!(covered.contains(&place), "{place} missing from {covered:?}");
    }
    assert_eq!(
        covered
            .iter()
            .filter(|place| place.starts_with("vault message of commit"))
            .count(),
        2
    );
    assert_eq!(covered.len(), 10, "{covered:?}");
    let left = places(&preview, "uncovered");
    for changed in ["database events.body", "database conversations.title"] {
        assert!(
            !left.iter().any(|place| place.starts_with(changed)),
            "{left:?}"
        );
    }
    assert!(
        !preview.to_string().contains(&marker),
        "a report never holds the text"
    );
    // A preview changes nothing, and a forget needs its digest.
    assert_eq!(snapshot(base, unrelated), before);
    let secret = fs::metadata(base.join("private/forget.key")).unwrap();
    assert_eq!(secret.len(), 32);
    let private = git(
        &base.join("private"),
        &["status", "--porcelain", "--ignored"],
    );
    assert!(private.contains("!! forget.key"), "{private}");
    assert!(
        !oppa(base, &["memory", "forget", &marker, "--confirm", "0000"])
            .status
            .success()
    );
    assert_eq!(snapshot(base, unrelated), before);

    let digest = preview["digest"].as_str().unwrap();
    let done = report(oppa(
        base,
        &["memory", "forget", &marker, "--confirm", digest],
    ));
    assert_eq!(places(&done, "covered"), covered);

    // Gone from every covered store.
    assert!(!holds(&vault_root.join("people/alex.md"), &marker));
    let tombstone = format!("[forgotten {}]", chrono::Local::now().format("%Y-%m-%d"));
    assert_eq!(
        fs::read_to_string(vault_root.join("people/alex.md")).unwrap(),
        format!("Alex likes tea.\nAlex's locker code is {tombstone}.\n")
    );
    let history = git(&vault_root, &["log", "--branches", "-p", "--format=%H%n%B"]);
    assert!(!history.contains(&marker));
    assert!(
        history.contains("Finish the plan"),
        "owner commits keep their place"
    );
    assert_eq!(git(&vault_root, &["status", "--porcelain"]), "");
    assert_eq!(
        git(&vault_root, &["rev-parse", "HEAD:people/sam.md"]),
        sam_blob
    );
    for removed in &removed_sessions {
        assert!(!removed.exists(), "{} remains", removed.display());
    }
    assert!(!base
        .join(format!("claude/projects/-runtime-harness/{CLAUDE}"))
        .exists());
    let store = Store::open(&base.join("runtime/state.db")).unwrap();
    let events_after = store.transcript(&room, 0, 100).unwrap();
    assert_eq!(events_after.len(), events_before.len());
    for (before, after) in events_before.iter().zip(&events_after) {
        if before.body.contains(&marker) {
            assert_eq!(after.body, tombstone);
            assert_eq!(
                (after.seq, &after.author, after.direction),
                (before.seq, &before.author, before.direction)
            );
        } else {
            assert_eq!(after, before);
        }
    }
    assert_eq!(
        store.transcript("console:local:other", 0, 100).unwrap(),
        other_before
    );
    assert_eq!(
        store.conversation(&room).unwrap().title,
        Some(tombstone.clone())
    );
    assert!(store.search(&room, &marker, 0).unwrap().is_empty());
    assert!(store.session(&room, "claude", "policy").unwrap().is_none());
    assert!(store.session(&room, "codex", "policy").unwrap().is_none());
    assert_eq!(
        store
            .session("console:local:other", "claude", "policy")
            .unwrap()
            .unwrap()
            .handle,
        KEPT
    );
    drop(store);
    // No page keeps an old body or title; the attachment name is reported below.
    for file in ["state.db", "state.db-wal"] {
        for copy in [
            format!("code is {marker}"),
            format!("Saved {marker}"),
            format!("Club {marker}"),
        ] {
            assert!(
                !holds(&base.join("runtime").join(file), &copy),
                "{file} still holds a copy"
            );
        }
    }

    // Reported where it stays, and nothing there or anywhere unrelated changed.
    let uncovered = places(&done, "uncovered");
    for place in [
        "database events.attachments, 1 rows",
        "file state.db",
        "file runs/7-abc/attempt-1.json",
        "file tasks/t1/board.md",
        "vault reflog HEAD",
    ] {
        assert!(
            uncovered.iter().any(|found| found == place),
            "{place} missing from {uncovered:?}"
        );
    }
    assert!(uncovered.iter().any(|place| place
        .ends_with("Git objects no branch reaches (reflogs, stashes, tags or unreferenced)")));
    assert_eq!(snapshot(base, unrelated), before);
    let again = report(oppa(base, &["memory", "forget", &marker]));
    assert!(places(&again, "covered").is_empty());
}

#[test]
fn a_reply_still_being_sent_blocks_the_forget() {
    let Planted {
        root, marker, room, ..
    } = planted();
    let base = root.path();
    let store = Store::open(&base.join("runtime/state.db")).unwrap();
    store
        .enqueue_reply(&room, "reply:9", &format!("Again {marker}"), vec![], None)
        .unwrap();
    let preview = report(oppa(base, &["memory", "forget", &marker]));
    assert_eq!(places(&preview, "blocked").len(), 1);
    let digest = preview["digest"].as_str().unwrap();
    let refused = oppa(base, &["memory", "forget", &marker, "--confirm", digest]);
    assert!(!refused.status.success());
    assert!(
        holds(&base.join("private/vault/people/alex.md"), &marker),
        "nothing was forgotten"
    );
}

/// Holds the lease a turn in `conversation` holds while it runs.
fn running_turn(base: &Path, conversation: &str) -> fs::File {
    use sha2::{Digest, Sha256};
    let key = format!("{:x}", Sha256::digest(conversation.as_bytes()));
    let lease = fs::File::create(base.join(format!("runtime/locks/{key}.lock"))).unwrap();
    lease.lock().unwrap();
    lease
}

#[test]
fn a_turn_running_in_another_conversation_blocks_the_forget() {
    let Planted { root, marker, .. } = planted();
    let base = root.path();
    let turn = running_turn(base, "console:local:other");
    let preview = report(oppa(base, &["memory", "forget", &marker]));
    assert_eq!(
        places(&preview, "blocked"),
        ["a turn is running in console:local:other"]
    );
    let digest = preview["digest"].as_str().unwrap();
    let refused = oppa(base, &["memory", "forget", &marker, "--confirm", digest]);
    assert!(!refused.status.success());
    assert!(holds(&base.join("private/vault/people/alex.md"), &marker));
    drop(turn);
    let preview = report(oppa(base, &["memory", "forget", &marker]));
    assert!(preview.get("blocked").is_none(), "{preview}");
}

/// A turn takes its lease before its conversation is recorded.
#[test]
fn a_turn_in_a_conversation_not_yet_recorded_blocks_the_forget() {
    let Planted { root, marker, .. } = planted();
    let base = root.path();
    let _turn = running_turn(base, "console:local:new");
    let preview = report(oppa(base, &["memory", "forget", &marker]));
    assert_eq!(
        places(&preview, "blocked"),
        ["a turn is running in a conversation not yet recorded"]
    );
    let digest = preview["digest"].as_str().unwrap();
    assert!(
        !oppa(base, &["memory", "forget", &marker, "--confirm", digest])
            .status
            .success()
    );
    assert!(holds(&base.join("private/vault/people/alex.md"), &marker));
}

/// A session that replaced the one the owner saw is not deleted on that
/// preview's digest, though its place reads the same.
#[test]
fn a_replaced_session_needs_a_new_preview() {
    let Planted {
        root, marker, room, ..
    } = planted();
    let base = root.path();
    let preview = report(oppa(base, &["memory", "forget", &marker]));
    let replacement = "bbbbbbbb-bbbb-4ccc-8ddd-eeeeeeeeeeee";
    let native = write(
        base.join(format!(
            "claude/projects/-runtime-harness/{replacement}.jsonl"
        )),
        &marker,
    );
    let store = Store::open(&base.join("runtime/state.db")).unwrap();
    let last = store.transcript(&room, 0, 100).unwrap().last().unwrap().seq;
    store
        .save_session(&room, "claude", "policy", replacement, last)
        .unwrap();
    let again = report(oppa(base, &["memory", "forget", &marker]));
    assert_eq!(places(&again, "covered"), places(&preview, "covered"));
    let digest = preview["digest"].as_str().unwrap();
    let refused = oppa(base, &["memory", "forget", &marker, "--confirm", digest]);
    assert!(!refused.status.success());
    assert!(native.exists(), "the replacement was deleted");
    assert!(holds(&base.join("private/vault/people/alex.md"), &marker));
}

/// Any file in the vault folder but a note, tracked or not, is reported.
#[test]
fn files_in_the_vault_that_are_not_notes_are_reported() {
    let Planted { root, marker, .. } = planted();
    let vault_root = root.path().join("private/vault");
    write(vault_root.join("scratch.txt"), &marker);
    write(vault_root.join(".obsidian/cache.json"), &marker);
    write(vault_root.join("people/.draft.md"), &marker);
    let preview = report(oppa(root.path(), &["memory", "forget", &marker]));
    let uncovered = places(&preview, "uncovered");
    for file in ["scratch.txt", ".obsidian/cache.json", "people/.draft.md"] {
        let place = format!("vault file {file}, not a note");
        assert!(
            uncovered.contains(&place),
            "{place} missing from {uncovered:?}"
        );
    }
    assert!(!places(&preview, "covered").contains(&"note scratch.txt".into()));
}

/// A name, an author or a conversation id that holds the text is reported
/// with the text left out, and a forget leaves the names it cannot rewrite.
#[test]
fn names_holding_the_text_are_reported_without_it() {
    let Planted { root, marker, .. } = planted();
    let base = root.path();
    let vault_root = base.join("private/vault");
    write(
        vault_root.join(format!("people/{marker}.md")),
        &format!("Code {marker}\n"),
    );
    git(&vault_root, &["add", "."]);
    let author = format!("user.name=Owner {marker}");
    git(
        &vault_root,
        &[
            "-c",
            &author,
            "-c",
            "user.email=o@example.test",
            "commit",
            "-qm",
            "Add",
        ],
    );
    let store = Store::open(&base.join("runtime/state.db")).unwrap();
    let named = format!("console:local:{marker}");
    store
        .create_conversation(&named, "console", "local", &marker, "policy")
        .unwrap();
    let seq = store
        .append_inbound(&InboundEvent {
            conversation: named.clone(),
            native_id: "1".into(),
            author: "synthetic-owner".into(),
            body: format!("It is {marker}"),
            kind: "message".into(),
            ..Default::default()
        })
        .unwrap()
        .seq;
    drop(store);

    let preview = report(oppa(base, &["memory", "forget", &marker]));
    assert!(!preview.to_string().contains(&marker), "{preview}");
    let covered = places(&preview, "covered");
    for place in [
        "note people/[the text].md".to_string(),
        "vault history of people/[the text].md".into(),
        format!("message {seq} in console:local:[the text] by synthetic-owner"),
    ] {
        assert!(covered.contains(&place), "{place} missing from {covered:?}");
    }
    let digest = preview["digest"].as_str().unwrap();
    let done = report(oppa(
        base,
        &["memory", "forget", &marker, "--confirm", digest],
    ));
    assert!(!done.to_string().contains(&marker), "{done}");
    let uncovered = places(&done, "uncovered");
    assert!(
        uncovered.contains(&"vault file names in history of /people".into()),
        "{uncovered:?}"
    );
    assert!(
        uncovered
            .iter()
            .any(|place| place.starts_with("vault author or committer of commit ")),
        "{uncovered:?}"
    );
    let note = fs::read_to_string(vault_root.join(format!("people/{marker}.md"))).unwrap();
    assert!(!note.contains(&marker), "{note}");
}

/// What arrives after the owner saw the preview is reported, not removed.
#[test]
fn a_forget_changes_only_what_its_preview_named() {
    if !alone("a_forget_changes_only_what_its_preview_named") {
        return;
    }
    let Planted {
        root, marker, room, ..
    } = planted();
    let base = root.path();
    // The forget runs in this process, so it must find sessions where the
    // command does; this process runs no other test.
    std::env::set_var("CLAUDE_CONFIG_DIR", base.join("claude"));
    std::env::set_var("CODEX_HOME", base.join("codex"));
    let paths = oppa::config::Paths::new(base.join("runtime"), Some(base.join("private"))).unwrap();
    let digest = oppa::forget::preview(&paths, &marker, &[]).unwrap().digest;
    let late = std::cell::Cell::new(0);
    let done = oppa::forget::forget(&paths, &marker, &digest, &[], || {
        // No turn can start meanwhile: every turn takes this lock, shared.
        let forgetting = fs::File::open(paths.runtime.join("locks/forget.lock"))?;
        assert!(forgetting.try_lock_shared().is_err(), "a turn could start");
        let store = Store::open(&paths.database())?;
        let event = store.append_inbound(&InboundEvent {
            conversation: room.clone(),
            native_id: "late".into(),
            author: "synthetic-owner".into(),
            body: format!("Late {marker}"),
            kind: "message".into(),
            ..Default::default()
        })?;
        late.set(event.seq);
        Ok(())
    })
    .unwrap();
    let store = Store::open(&paths.database()).unwrap();
    let events = store.transcript(&room, 0, 100).unwrap();
    let kept = events.iter().find(|event| event.seq == late.get()).unwrap();
    assert_eq!(kept.body, format!("Late {marker}"));
    assert!(
        places(&serde_json::to_value(&done).unwrap(), "uncovered").contains(&format!(
            "message {} in {room} by synthetic-owner, new since the preview",
            late.get()
        )),
        "{done:?}"
    );
}

/// Git run as the owner, as an owner's own commit or merge would be.
fn as_owner(vault: &Path, args: &[&str]) {
    let owner = "-c user.name=Owner -c user.email=owner@example.test";
    git(
        vault,
        &[&owner.split(' ').collect::<Vec<_>>(), args].concat(),
    );
}

/// A copied or renamed note keeps one Git object for all its paths, and the
/// rewrite changes it under each of them, so the preview names every path.
#[test]
fn every_path_a_note_had_in_history_is_previewed() {
    let Planted { root, marker, .. } = planted();
    let base = root.path();
    let vault = base.join("private/vault");
    write(vault.join("trips/a.md"), &format!("Copy {marker}\n"));
    git(&vault, &["add", "trips/a.md"]);
    as_owner(&vault, &["commit", "-qm", "Add"]);
    fs::copy(vault.join("trips/a.md"), vault.join("trips/b.md")).unwrap();
    git(&vault, &["add", "trips/b.md"]);
    as_owner(&vault, &["commit", "-qm", "Copy"]);
    git(&vault, &["mv", "trips/a.md", "trips/c.md"]);
    as_owner(&vault, &["commit", "-qm", "Rename"]);
    git(&vault, &["rm", "-q", "trips/b.md", "trips/c.md"]);
    as_owner(&vault, &["commit", "-qm", "Remove"]);
    let preview = report(oppa(base, &["memory", "forget", &marker]));
    let covered = places(&preview, "covered");
    for path in ["trips/a.md", "trips/b.md", "trips/c.md"] {
        let place = format!("vault history of {path}");
        assert!(covered.contains(&place), "{place} missing from {covered:?}");
    }
    let digest = preview["digest"].as_str().unwrap();
    let done = report(oppa(
        base,
        &["memory", "forget", &marker, "--confirm", digest],
    ));
    assert_eq!(places(&done, "covered"), covered);
    assert!(!git(&vault, &["log", "--branches", "-p", "--format=%B"]).contains(&marker));
}

/// A commit on `parent`'s tree whose header carries a signature. Git checks
/// no signature when it stores or exports a commit, so a made-up one serves.
fn signed(vault: &Path, parent: &str, message: &str) -> String {
    let parent = git(vault, &["rev-parse", parent]).trim().to_owned();
    let tree = git(vault, &["rev-parse", &format!("{parent}^{{tree}}")]);
    let owner = "Owner <owner@example.test> 1700000000 +0000";
    let signature = "-----BEGIN SSH SIGNATURE-----\n U1NIU0lH\n -----END SSH SIGNATURE-----";
    let object = format!(
        "tree {}\nparent {parent}\nauthor {owner}\ncommitter {owner}\ngpgsig {signature}\n\n{message}\n",
        tree.trim()
    );
    let mut command = Command::new("git");
    let mut child = command
        .current_dir(vault)
        .args(["hash-object", "-t", "commit", "-w", "--stdin"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    std::io::Write::write_all(&mut child.stdin.take().unwrap(), object.as_bytes()).unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

/// A forget rewrites only the commits that hold the text and those after
/// them, on the branches that hold it. Every other commit keeps its id and
/// its signature: before the text, on another branch, or merged in from a
/// side branch. The preview names each signed commit it rewrites, which
/// loses its signature.
#[test]
fn a_forget_keeps_every_commit_it_need_not_rewrite() {
    let root = tempfile::tempdir().unwrap();
    let base = root.path();
    report_ok(oppa(
        base,
        &["init", "--agent", "codex", "--owner", "synthetic-owner"],
    ));
    let vault = base.join("private/vault");
    let marker = marker();
    let before = signed(&vault, "HEAD", "Signed before the text");
    git(&vault, &["update-ref", "HEAD", &before]);
    let elsewhere = signed(&vault, &before, "Signed on a branch without it");
    git(&vault, &["branch", "other", &elsewhere]);
    let side = signed(&vault, &before, "Signed on a side branch");
    write(vault.join("people/alex.md"), &format!("Code {marker}\n"));
    git(&vault, &["add", "people/alex.md"]);
    as_owner(&vault, &["commit", "-qm", "Add the code"]);
    as_owner(
        &vault,
        &["merge", "--no-ff", "-qm", "Merge the side", &side],
    );
    let after = signed(&vault, "HEAD", "Signed after the text");
    git(&vault, &["update-ref", "HEAD", &after]);

    let preview = report(oppa(base, &["memory", "forget", &marker]));
    let covered = places(&preview, "covered");
    let signature = |id: &str| format!("vault signature of commit {}", &id[..12]);
    assert!(covered.contains(&signature(&after)), "{covered:?}");
    for kept in [&before, &elsewhere, &side] {
        assert!(!covered.contains(&signature(kept)), "{covered:?}");
    }
    let digest = preview["digest"].as_str().unwrap();
    report(oppa(
        base,
        &["memory", "forget", &marker, "--confirm", digest],
    ));
    assert_eq!(git(&vault, &["rev-parse", "other"]).trim(), elsewhere);
    for kept in [&before, &side] {
        let reached = Command::new("git")
            .current_dir(&vault)
            .args(["merge-base", "--is-ancestor", kept, "HEAD"])
            .status()
            .unwrap();
        assert!(reached.success(), "{kept} was rewritten");
    }
    let head = git(&vault, &["cat-file", "commit", "HEAD"]);
    assert!(head.ends_with("\n\nSigned after the text\n") && !head.contains("gpgsig"));
    assert!(!git(&vault, &["log", "--branches", "-p"]).contains(&marker));
}

/// A preview and a forget check each conversation's lease one at a time, so
/// an owner with hundreds of conversations needs only a few open files.
#[test]
fn hundreds_of_conversations_need_only_a_few_open_files() {
    let Planted { root, marker, .. } = planted();
    let base = root.path();
    let store = Store::open(&base.join("runtime/state.db")).unwrap();
    for n in 0..300 {
        let (id, native) = (format!("console:local:chat-{n}"), format!("chat-{n}"));
        store
            .create_conversation(&id, "console", "local", &native, "policy")
            .unwrap();
    }
    drop(store);
    let limited = "ulimit -n 64";
    let preview = report(oppa_after(base, limited, &["memory", "forget", &marker]));
    let digest = preview["digest"].as_str().unwrap();
    let confirm = ["memory", "forget", &marker, "--confirm", digest];
    let done = report(oppa_after(base, limited, &confirm));
    assert_eq!(places(&done, "covered"), places(&preview, "covered"));
    assert!(!holds(&base.join("private/vault/people/alex.md"), &marker));
}

/// A Git too old to export signed commits is refused before the history
/// rewrite starts, and nothing changes.
#[test]
fn a_git_too_old_to_rewrite_history_changes_nothing() {
    use std::os::unix::fs::PermissionsExt;
    let Planted { root, marker, .. } = planted();
    let base = root.path();
    let preview = report(oppa(base, &["memory", "forget", &marker]));
    let digest = preview["digest"].as_str().unwrap();
    // Git 2.49, as far as the forget can tell: it has no --signed-commits.
    let real = Command::new("sh").args(["-c", "command -v git"]).output();
    let real = String::from_utf8(real.unwrap().stdout).unwrap();
    let log = base.join("git.log");
    let old = write(
        base.join("old-git/git"),
        &format!(
            "#!/bin/sh\necho \"$*\" >> '{}'\nfor argument in \"$@\"; do case \"$argument\" in\n\
             version) echo 'git version 2.49.0'; exit 0;;\n\
             --signed-commits=*) echo 'error: unknown option' >&2; exit 129;;\n\
             esac; done\nexec '{}' \"$@\"\n",
            log.display(),
            real.trim()
        ),
    );
    fs::set_permissions(&old, fs::Permissions::from_mode(0o755)).unwrap();
    let path = format!(
        "export PATH='{}':\"$PATH\"",
        old.parent().unwrap().display()
    );
    let confirm = ["memory", "forget", &marker, "--confirm", digest];
    let refused = oppa_after(base, &path, &confirm);
    let error = String::from_utf8_lossy(&refused.stderr);
    assert!(
        !refused.status.success() && error.contains("needs Git 2.50"),
        "{error}"
    );
    let calls = fs::read_to_string(&log).unwrap();
    assert!(
        !calls.contains("fast-export"),
        "the rewrite started: {calls}"
    );
    assert!(holds(&base.join("private/vault/people/alex.md"), &marker));
}

/// A forgotten message or edit that its channel reads again, as an iMessage
/// restart does, is never recorded again, not even as an edit of its tombstone.
#[test]
fn a_forgotten_message_read_again_is_not_an_edit() {
    let Planted {
        root, marker, room, ..
    } = planted();
    let base = root.path();
    let lunch = |body: String| InboundEvent {
        conversation: room.clone(),
        native_id: "lunch".into(),
        author: "synthetic-owner".into(),
        body,
        kind: "message".into(),
        ..Default::default()
    };
    let store = Store::open(&base.join("runtime/state.db")).unwrap();
    store
        .append_channel_message(&lunch("Lunch at noon".into()))
        .unwrap();
    let edit = store
        .append_channel_message(&lunch(format!("Lunch at noon, code {marker}")))
        .unwrap();
    assert_eq!(edit.kind, "edit");
    let preview = report(oppa(base, &["memory", "forget", &marker]));
    let digest = preview["digest"].as_str().unwrap();
    report(oppa(
        base,
        &["memory", "forget", &marker, "--confirm", digest],
    ));
    let before = store.transcript(&room, 0, 100).unwrap();
    let again = store
        .append_channel_message(&InboundEvent {
            conversation: room.clone(),
            native_id: "2".into(),
            author: "synthetic-owner".into(),
            body: format!("My locker code is {marker}"),
            kind: "message".into(),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(again.kind, "message");
    assert!(!again.body.contains(&marker));
    let edit_again = store
        .append_channel_message(&lunch(format!("Lunch at noon, code {marker}")))
        .unwrap();
    assert_eq!(edit_again.seq, edit.seq);
    assert!(!edit_again.body.contains(&marker));
    assert_eq!(store.transcript(&room, 0, 100).unwrap(), before);
}

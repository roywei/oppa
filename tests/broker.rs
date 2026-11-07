use chrono::Utc;
use oppa::{
    broker::{Broker, BrokerLaunch, RunGrant, TaskAccess, ToolResult},
    config::{Config, Paths},
    harness::{Access, Activity, AgentManifest},
    store::{InboundEvent, Store},
    vault::{Edit, Provenance, Vault},
};
use serde_json::{json, Value};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};
use tempfile::TempDir;

struct Fixture {
    _temporary: TempDir,
    launch: BrokerLaunch,
    broker: Broker,
}

fn fixture(read_scopes: &[&str], write_scopes: &[&str]) -> Fixture {
    let temporary = TempDir::new().unwrap();
    let paths = Paths::new(
        temporary.path().join("runtime"),
        Some(temporary.path().join("private")),
    )
    .unwrap();
    paths.prepare().unwrap();
    fs::write(
        paths.config_file(),
        toml::to_string(&Config {
            access: Default::default(),
            version: 1,
            connectors: vec![],
            sessions: Default::default(),
            source: None,
            channels: vec![],
            people: vec![],
            owner: "synthetic-owner".into(),
            agents: vec![AgentManifest {
                id: "synthetic".into(),
                family: "fixture".into(),
                command: vec!["/never/launch".into()],
                timeout_seconds: 30,
                task_timeout_seconds: 30,
                model: None,
                effort: None,
            }],
            groups: vec![],
            heartbeat: None,
            browser: false,
            memory_sync: None,
        })
        .unwrap(),
    )
    .unwrap();
    let database = paths.database();
    let store = Store::open(&database).unwrap();
    for conversation in ["owner-chat", "other-chat"] {
        store
            .create_conversation(conversation, "console", "local", conversation, "policy-1")
            .unwrap();
    }
    let input = store
        .append_inbound(&InboundEvent {
            conversation: "owner-chat".into(),
            native_id: "synthetic-input-1".into(),
            author: "synthetic-owner".into(),
            body: "Remember the synthetic example.".into(),
            ..Default::default()
        })
        .unwrap();
    store
        .append_inbound(&InboundEvent {
            conversation: "other-chat".into(),
            native_id: "synthetic-input-2".into(),
            author: "other-person".into(),
            body: "Private other conversation sentinel.".into(),
            ..Default::default()
        })
        .unwrap();
    let vault = paths.vault();
    Vault::init(&vault).unwrap();
    let launch = BrokerLaunch {
        private_dir: paths.private.clone(),
        grant: RunGrant {
            wakes: false,
            access: Default::default(),
            context: "synthetic-opaque-current-context".into(),
            requester: "synthetic-owner".into(),
            role: oppa::config::Role::Owner,
            run_id: "synthetic-run".into(),
            input_seq: input.seq,
            conversations: vec!["owner-chat".into()],
            conversation_scopes: vec![],
            read_scopes: read_scopes.iter().map(|scope| scope.to_string()).collect(),
            write_scopes: write_scopes.iter().map(|scope| scope.to_string()).collect(),
            task_access: TaskAccess::None,
            eligible_agents: vec![],
            expires_at: Utc::now().timestamp() + 300,
        },
        database,
        vault,
        journal: temporary.path().join("broker-journal"),
    };
    let broker = Broker::open(launch.clone()).unwrap();
    Fixture {
        _temporary: temporary,
        launch,
        broker,
    }
}

fn success(result: ToolResult) -> Value {
    assert!(!result.is_error, "{}", result.value);
    result.value
}
fn code(result: ToolResult) -> String {
    assert!(result.is_error, "{}", result.value);
    result.value["error"]["code"].as_str().unwrap().to_owned()
}

fn commit_args(fixture: &Fixture, path: &str, content: Value) -> Value {
    json!({"context":fixture.launch.grant.context,"expected_revision":Vault::open(&fixture.launch.vault).unwrap().revision().unwrap(),"edits":[{"path":path,"content":content}],"reason":"Save synthetic preference"})
}

fn seed(fixture: &Fixture, edits: &[(&str, &str)]) {
    let vault = Vault::open(&fixture.launch.vault).unwrap();
    vault
        .commit(
            &vault.revision().unwrap(),
            &edits
                .iter()
                .map(|(path, content)| Edit::Write {
                    path: path.to_string(),
                    content: content.to_string(),
                })
                .collect::<Vec<_>>(),
            &Provenance {
                author: "synthetic-owner".into(),
                source: "fixture".into(),
                run_id: "seed".into(),
                reason: "Seed synthetic notes".into(),
            },
        )
        .unwrap();
}

fn records(fixture: &Fixture) -> Vec<PathBuf> {
    fs::read_dir(&fixture.launch.journal)
        .unwrap()
        .flat_map(|entry| fs::read_dir(entry.unwrap().path()).unwrap())
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("write-")
        })
        .collect()
}

fn set_pending(path: &Path) {
    let mut record: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    record["state"] = json!({"status":"pending"});
    fs::write(path, serde_json::to_vec(&record).unwrap()).unwrap();
}

fn git(root: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .current_dir(root)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

#[test]
fn definitions_expose_context_and_host_owned_attribution() {
    let fixture = fixture(&["*"], &["*"]);
    let definitions = fixture.broker.definitions();
    // History 4, memory 4, and people.list for a run of the owner's.
    assert_eq!(definitions.len(), 9);
    for tool in &definitions {
        assert_eq!(tool["inputSchema"]["additionalProperties"], false);
        assert!(tool["inputSchema"]["required"]
            .as_array()
            .unwrap()
            .contains(&json!("context")));
        for forbidden in ["author", "source", "run_id", "requester"] {
            assert!(tool["inputSchema"]["properties"].get(forbidden).is_none());
        }
    }
    let read_only = self::fixture(&["*"], &[]);
    assert!(read_only
        .broker
        .definitions()
        .iter()
        .all(|tool| tool["name"] != "memory.commit" && tool["name"] != "memory.forget"));
}

#[test]
fn invalid_context_expired_grant_and_changed_canonical_author_fail_closed() {
    let fixture = fixture(&["*"], &["*"]);
    assert_eq!(
        code(fixture.broker.call(
            "memory.search",
            json!({"context":"wrong","query":"sentinel"})
        )),
        "access_denied"
    );
    assert_eq!(fixture.broker.activity().unwrap(), Activity::None);
    let mut expired = fixture.launch.clone();
    expired.grant.run_id = "expired-run".into();
    expired.grant.expires_at = Utc::now().timestamp() - 1;
    let expired = Broker::open(expired).unwrap();
    assert_eq!(
        code(expired.call(
            "memory.search",
            json!({"context":fixture.launch.grant.context,"query":"sentinel"})
        )),
        "access_denied"
    );
    assert_eq!(expired.activity().unwrap(), Activity::None);
    let connection = rusqlite::Connection::open(&fixture.launch.database).unwrap();
    connection
        .execute(
            "UPDATE events SET author='changed-person' WHERE seq=?1",
            [fixture.launch.grant.input_seq],
        )
        .unwrap();
    assert_eq!(
        code(fixture.broker.call(
            "memory.search",
            json!({"context":fixture.launch.grant.context,"query":"sentinel"})
        )),
        "access_denied"
    );
}

#[test]
fn grant_cannot_bind_another_authors_input_or_reuse_a_run_with_more_scopes() {
    let fixture = fixture(&["household"], &[]);
    let mut changed = fixture.launch.clone();
    changed.grant.read_scopes = vec!["*".into()];
    assert!(Broker::open(changed).is_err());
    let mut wrong = fixture.launch.clone();
    wrong.grant.run_id = "wrong-author".into();
    wrong.grant.requester = "another-person".into();
    assert!(Broker::open(wrong).is_err());
}

#[test]
fn history_scope_prevents_search_and_direct_event_id_disclosure() {
    let fixture = fixture(&["*"], &[]);
    let forbidden=fixture.broker.call("history.search",json!({"context":fixture.launch.grant.context,"conversation":"other-chat","query":"sentinel"}));
    assert_eq!(code(forbidden), "access_denied");
    assert_eq!(code(fixture.broker.call("history.read",json!({"context":fixture.launch.grant.context,"conversation":"owner-chat","event_seq":2}))),"access_denied");
    let visible=success(fixture.broker.call("history.search",json!({"context":fixture.launch.grant.context,"conversation":"owner-chat","query":"synthetic"})));
    assert_eq!(visible["hits"].as_array().unwrap().len(), 1);
    assert!(!visible
        .to_string()
        .contains("Private other conversation sentinel"));
    assert_eq!(fixture.broker.activity().unwrap(), Activity::Read);
}

#[test]
fn a_turns_record_holds_its_state_attempts_and_effects_but_none_of_their_content() {
    // B-11, B-5: asked why it did or did not do something, the model reads
    // what the host recorded about that turn (its state, each attempt's
    // harness, failure and journaled effects), never its prompt, reply,
    // arguments, receipts or native session, and only in a conversation it
    // may read.
    let fixture = fixture(&["*"], &[]);
    let runs = fixture.launch.database.parent().unwrap().join("runs");
    let seq = fixture.launch.grant.input_seq;
    let attempt = |harness: &str, number: u32, record: Value| {
        let digest = format!("{:064x}", harness.len() as u128 * 7 + number as u128);
        fs::write(
            runs.join(format!("{seq}-{digest}-{number}.json")),
            record.to_string(),
        )
        .unwrap();
    };
    attempt(
        "codex",
        1,
        json!({"input_seq":seq,"agent":"codex","attempt":1,"ended_at":"2026-10-10T10:00:00Z",
            "outcome":{"activity":"none","answer":null,"session":"SESSION-SENTINEL","telemetry_complete":true,
                "failure":{"category":"quota","diagnostic":"Synthetic quota exhausted before work."}},
            "broker":{"source":{"run_id":"RUN-SENTINEL"},"activity":"none","read_calls":0,"writes":[]}}),
    );
    attempt(
        "claude",
        1,
        json!({"input_seq":seq,"agent":"claude","attempt":1,"ended_at":"2026-10-10T10:00:05Z",
            "outcome":{"activity":"effect","answer":"REPLY-SENTINEL","session":"SESSION-SENTINEL","failure":null,"telemetry_complete":true},
            "broker":{"activity":"effect","read_calls":1,"writes":[{"tool":"memory.commit","tier":"reversible","status":"committed",
                "request_digest":"DIGEST-SENTINEL","receipt":{"paths":["RECEIPT-SENTINEL"]}}]}}),
    );
    // A broker journal folder and a file that is no attempt record share the prefix.
    fs::create_dir_all(runs.join(format!("{seq}-run-journal"))).unwrap();
    fs::write(runs.join(format!("{seq}-elsewhere-1.json")), "{}").unwrap();
    fs::write(
        runs.join(format!("turn-{seq}.json")),
        json!({"state":"completed","result":{"agent":"claude","input_seq":seq,"reply_seq":seq + 9,"silent":false}}).to_string(),
    )
    .unwrap();
    let context = &fixture.launch.grant.context;
    let record = success(fixture.broker.call(
        "history.turn",
        json!({"context":context,"conversation":"owner-chat","input_seq":seq}),
    ));
    assert_eq!(
        (
            &record["turn"]["state"],
            &record["turn"]["agent"],
            &record["turn"]["reply_seq"]
        ),
        (&json!("completed"), &json!("claude"), &json!(seq + 9))
    );
    let attempts = record["attempts"].as_array().unwrap();
    assert_eq!(attempts.len(), 2, "{record}");
    assert_eq!(attempts[0]["agent"], "codex");
    assert_eq!(
        attempts[0]["failure"],
        json!({"category":"quota","cause":"Synthetic quota exhausted before work."})
    );
    assert_eq!(attempts[1]["failure"], Value::Null);
    assert_eq!(
        attempts[1]["effects"],
        json!([{"tool":"memory.commit","tier":"reversible","status":"committed"}])
    );
    for sentinel in ["SESSION-", "RUN-", "REPLY-", "DIGEST-", "RECEIPT-"] {
        assert!(
            !record.to_string().contains(sentinel),
            "{sentinel}: {record}"
        );
    }
    // Before its turn began a message has no record.
    let store = Store::open(&fixture.launch.database).unwrap();
    let later = store
        .append_inbound(&InboundEvent {
            conversation: "owner-chat".into(),
            native_id: "synthetic-input-3".into(),
            author: "synthetic-owner".into(),
            body: "A later message.".into(),
            ..Default::default()
        })
        .unwrap();
    let none = success(fixture.broker.call(
        "history.turn",
        json!({"context":context,"conversation":"owner-chat","input_seq":later.seq}),
    ));
    assert_eq!(
        (&none["turn"], &none["attempts"]),
        (&Value::Null, &json!([]))
    );
    // Not in a conversation the run may not read, nor under another conversation's name.
    for (conversation, input_seq) in [("other-chat", 2), ("owner-chat", 2)] {
        let denied = fixture.broker.call(
            "history.turn",
            json!({"context":context,"conversation":conversation,"input_seq":input_seq}),
        );
        assert_eq!(code(denied), "access_denied");
    }
}

#[test]
fn a_failed_turns_record_holds_the_causes_only_its_marker_kept() {
    // B-11: a failure the runtime notes only in the turn's marker, such as an
    // empty answer where only NO_REPLY is silence, reaches the record beside
    // the attempts; a cause is cut at 500 characters.
    let fixture = fixture(&["*"], &[]);
    let runs = fixture.launch.database.parent().unwrap().join("runs");
    let seq = fixture.launch.grant.input_seq;
    let digest = "c".repeat(64);
    fs::write(
        runs.join(format!("{seq}-{digest}-1.json")),
        json!({"input_seq":seq,"agent":"codex","attempt":1,"ended_at":"2026-10-10T10:00:00Z",
            "outcome":{"activity":"none","answer":"","failure":null,"telemetry_complete":true},
            "broker":{"activity":"none","read_calls":0,"writes":[]}})
        .to_string(),
    )
    .unwrap();
    fs::write(
        runs.join(format!("turn-{seq}.json")),
        json!({"input_seq":seq,"state":"failed","attempts":[
            {"agent":"codex","attempt":1,"failure":{"category":"timeout","diagnostic":"t".repeat(800)}},
            {"agent":"claude","attempt":1,"failure":"empty answer; a heartbeat is silent only through NO_REPLY"}]})
        .to_string(),
    )
    .unwrap();
    let record = success(fixture.broker.call(
        "history.turn",
        json!({"context":fixture.launch.grant.context,"conversation":"owner-chat","input_seq":seq}),
    ));
    assert_eq!(record["turn"]["state"], "failed");
    let attempts = record["attempts"].as_array().unwrap();
    let of = |agent: &str| attempts.iter().find(|a| a["agent"] == agent).unwrap();
    assert_eq!(of("codex")["failure"]["category"], "timeout");
    assert_eq!(
        of("codex")["failure"]["cause"]
            .as_str()
            .unwrap()
            .chars()
            .count(),
        500
    );
    assert_eq!(
        of("claude")["failure"],
        json!({"category":null,"cause":"empty answer; a heartbeat is silent only through NO_REPLY"})
    );
}

#[test]
fn history_pages_and_unicode_event_slices_have_explicit_continuation() {
    let fixture = fixture(&["*"], &[]);
    let store = Store::open(&fixture.launch.database).unwrap();
    let long = "汉🙂a".repeat(2000);
    let event = store
        .append_inbound(&InboundEvent {
            conversation: "owner-chat".into(),
            native_id: "long-message".into(),
            author: "synthetic-owner".into(),
            body: long.clone(),
            ..Default::default()
        })
        .unwrap();
    let first = success(fixture.broker.call(
        "history.read",
        json!({"context":fixture.launch.grant.context,"conversation":"owner-chat","limit":1}),
    ));
    assert_eq!(first["pagination"]["has_more"], true);
    assert_eq!(
        first["pagination"]["next_after_seq"],
        fixture.launch.grant.input_seq
    );
    let second=success(fixture.broker.call("history.read",json!({"context":fixture.launch.grant.context,"conversation":"owner-chat","after_seq":fixture.launch.grant.input_seq,"limit":1})));
    assert_eq!(second["events"][0]["body_char_count"], 6000);
    assert_eq!(second["events"][0]["body_truncated"], true);
    assert_eq!(second["events"][0]["next_offset"], 4000);
    let sliced=success(fixture.broker.call("history.read",json!({"context":fixture.launch.grant.context,"conversation":"owner-chat","event_seq":event.seq,"offset":1,"max_chars":4})));
    assert_eq!(sliced["event"]["body"], "🙂a汉🙂");
    assert_eq!(sliced["next_offset"], 5);
    assert_eq!(code(fixture.broker.call("history.read",json!({"context":fixture.launch.grant.context,"conversation":"owner-chat","event_seq":event.seq,"limit":1}))),"invalid_arguments");
}

#[test]
fn memory_search_reads_only_exact_permitted_components() {
    let fixture = fixture(&["household"], &[]);
    seed(
        &fixture,
        &[
            ("household/profile.md", "苹果 AI permitted"),
            ("household-extra/private.md", "苹果 forbidden sibling"),
            ("people/other/private.md", "苹果 forbidden person"),
        ],
    );
    // An invalid UTF-8 note outside the scope must neither be read nor prevent
    // permitted results from being returned.
    fs::write(
        fixture.launch.vault.join("people/other/binary.md"),
        [255, 254],
    )
    .unwrap();
    let result = success(fixture.broker.call(
        "memory.search",
        json!({"context":fixture.launch.grant.context,"query":"苹果"}),
    ));
    assert_eq!(result["hits"].as_array().unwrap().len(), 1);
    assert_eq!(result["hits"][0]["path"], "household/profile.md");
    assert!(!result.to_string().contains("forbidden"));
    let existing = code(fixture.broker.call(
        "memory.read",
        json!({"context":fixture.launch.grant.context,"path":"people/other/private.md"}),
    ));
    let absent = code(fixture.broker.call(
        "memory.read",
        json!({"context":fixture.launch.grant.context,"path":"people/missing/private.md"}),
    ));
    assert_eq!(existing, absent);
    for path in [
        "household/../people/other/private.md",
        "household/.git/config.md",
        "/household/profile.md",
        "household-extra/private.md",
    ] {
        assert_eq!(
            code(fixture.broker.call(
                "memory.read",
                json!({"context":fixture.launch.grant.context,"path":path})
            )),
            "access_denied"
        );
    }
}

#[test]
fn commit_provenance_is_host_owned_and_missing_content_never_deletes() {
    let fixture = fixture(&["*"], &["household"]);
    let args = commit_args(
        &fixture,
        "household/preference.md",
        json!("Synthetic preference"),
    );
    for forbidden in ["author", "source", "run_id", "requester"] {
        let mut injected = args.clone();
        injected[forbidden] = json!("another-person");
        assert_eq!(
            code(fixture.broker.call("memory.commit", injected)),
            "invalid_arguments"
        );
    }
    let mut missing = args.clone();
    missing["edits"][0]
        .as_object_mut()
        .unwrap()
        .remove("content");
    assert_eq!(
        code(fixture.broker.call("memory.commit", missing)),
        "invalid_arguments"
    );
    assert!(records(&fixture).is_empty());
    let result = success(fixture.broker.call("memory.commit", args));
    let provenance = &result["receipt"]["provenance"];
    assert_eq!(provenance["author"], fixture.launch.grant.requester);
    assert_eq!(provenance["run_id"], fixture.launch.grant.run_id);
    assert!(provenance["source"]
        .as_str()
        .unwrap()
        .starts_with(&format!("broker:{}:", fixture.launch.grant.input_seq)));
    assert_eq!(fixture.broker.activity().unwrap(), Activity::Effect);
    assert_eq!(records(&fixture).len(), 1);
}

#[test]
fn every_edit_is_authorized_before_any_write_or_journal_admission() {
    let fixture = fixture(&["*"], &["household"]);
    let mut args = commit_args(&fixture, "household/allowed.md", json!("allowed"));
    args["edits"]
        .as_array_mut()
        .unwrap()
        .push(json!({"path":"people/other/denied.md","content":"denied"}));
    let before = Vault::open(&fixture.launch.vault)
        .unwrap()
        .revision()
        .unwrap();
    assert_eq!(
        code(fixture.broker.call("memory.commit", args)),
        "access_denied"
    );
    assert_eq!(
        Vault::open(&fixture.launch.vault)
            .unwrap()
            .revision()
            .unwrap(),
        before
    );
    assert!(records(&fixture).is_empty());
}

#[test]
fn repeated_write_returns_same_receipt_and_survives_broker_restart() {
    let fixture = fixture(&["*"], &["*"]);
    let args = commit_args(
        &fixture,
        "household/note.md",
        json!("durable synthetic note"),
    );
    let first = success(fixture.broker.call("memory.commit", args.clone()));
    let reopened = Broker::open(fixture.launch.clone()).unwrap();
    let replay = success(reopened.call("memory.commit", args));
    assert_eq!(first["receipt"], replay["receipt"]);
    assert_eq!(replay["replayed"], true);
    assert_eq!(
        Vault::open(&fixture.launch.vault)
            .unwrap()
            .history(10)
            .unwrap()
            .len(),
        1
    );
    let observation = reopened.observations().unwrap();
    assert_eq!(observation["writes"][0]["status"], "committed");
    assert!(!observation.to_string().contains("durable synthetic note"));
}

#[test]
fn simultaneous_identical_calls_share_one_durable_receipt() {
    let fixture = fixture(&["*"], &["*"]);
    let args = commit_args(&fixture, "household/note.md", json!("concurrent synthetic"));
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
    let threads: Vec<_> = (0..2)
        .map(|_| {
            let launch = fixture.launch.clone();
            let args = args.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let broker = Broker::open(launch).unwrap();
                barrier.wait();
                success(broker.call("memory.commit", args))
            })
        })
        .collect();
    barrier.wait();
    let results: Vec<_> = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    assert_eq!(results[0]["receipt"], results[1]["receipt"]);
    assert_ne!(results[0]["replayed"], results[1]["replayed"]);
    assert_eq!(
        git(&fixture.launch.vault, &["rev-list", "--count", "HEAD"]),
        "2"
    );
    assert_eq!(records(&fixture).len(), 1);
}

#[test]
fn explicit_null_optional_history_arguments_cannot_bypass_exclusive_modes() {
    let fixture = fixture(&["*"], &[]);
    assert_eq!(
        code(fixture.broker.call(
            "history.read",
            json!({
                "context": fixture.launch.grant.context,
                "conversation": "owner-chat",
                "event_seq": fixture.launch.grant.input_seq,
                "limit": null
            })
        )),
        "invalid_arguments"
    );
}

#[test]
fn commit_then_receipt_loss_is_reconciled_from_git_without_second_write() {
    let fixture = fixture(&["*"], &["*"]);
    let args = commit_args(
        &fixture,
        "household/note.md",
        json!("synthetic receipt recovery"),
    );
    let first = success(fixture.broker.call("memory.commit", args.clone()));
    set_pending(&records(&fixture)[0]);
    let reopened = Broker::open(fixture.launch.clone()).unwrap();
    let recovered = success(reopened.call("memory.commit", args));
    assert_eq!(recovered["receipt"], first["receipt"]);
    assert_eq!(
        git(&fixture.launch.vault, &["rev-list", "--count", "HEAD"]),
        "2"
    );
}

#[test]
fn unmatched_pending_write_is_never_repeated_even_at_original_revision() {
    let fixture = fixture(&["*"], &["*"]);
    let args = commit_args(&fixture, "household/note.md", json!("synthetic"));
    let first = success(fixture.broker.call("memory.commit", args.clone()));
    git(
        &fixture.launch.vault,
        &[
            "reset",
            "--hard",
            first["receipt"]["parent_revision"].as_str().unwrap(),
        ],
    );
    set_pending(&records(&fixture)[0]);
    assert_eq!(
        code(fixture.broker.call("memory.commit", args)),
        "write_pending"
    );
    assert!(!fixture.launch.vault.join("household/note.md").exists());
    assert_eq!(fixture.broker.activity().unwrap(), Activity::Effect);
    assert_eq!(
        fixture.broker.observations().unwrap()["writes"][0]["status"],
        "pending"
    );
}

#[test]
fn pending_vault_transaction_recovers_receipt_after_index_lock_clears() {
    let fixture = fixture(&["*"], &["*"]);
    let args = commit_args(&fixture, "household/note.md", json!("synthetic"));
    let lock = fixture.launch.vault.join(".git/index.lock");
    fs::write(&lock, "synthetic external index lock").unwrap();
    assert_eq!(
        code(fixture.broker.call("memory.commit", args.clone())),
        "write_pending"
    );
    let revision = Vault::open(&fixture.launch.vault)
        .unwrap()
        .revision()
        .unwrap();
    fs::remove_file(lock).unwrap();
    let result = success(fixture.broker.call("memory.commit", args));
    assert_eq!(result["receipt"]["revision"], revision);
    assert_eq!(
        git(&fixture.launch.vault, &["rev-list", "--count", "HEAD"]),
        "2"
    );
}

#[test]
fn owner_dirty_conflict_is_recorded_rejected_without_overwrite() {
    let fixture = fixture(&["*"], &["*"]);
    seed(&fixture, &[("household/note.md", "base")]);
    fs::write(
        fixture.launch.vault.join("household/note.md"),
        "owner draft",
    )
    .unwrap();
    let args = commit_args(&fixture, "household/note.md", json!("overwrite"));
    assert_eq!(
        code(fixture.broker.call("memory.commit", args.clone())),
        "write_rejected"
    );
    assert_eq!(
        code(fixture.broker.call("memory.commit", args)),
        "write_rejected"
    );
    assert_eq!(
        fs::read_to_string(fixture.launch.vault.join("household/note.md")).unwrap(),
        "owner draft"
    );
    assert_eq!(
        fixture.broker.observations().unwrap()["writes"][0]["status"],
        "rejected"
    );
}

#[test]
fn logical_delete_retains_history_and_is_explicit_in_tool_contract() {
    let fixture = fixture(&["*"], &["*"]);
    seed(
        &fixture,
        &[("household/note.md", "synthetic retained history")],
    );
    let result = success(fixture.broker.call(
        "memory.commit",
        commit_args(&fixture, "household/note.md", Value::Null),
    ));
    assert!(!fixture.launch.vault.join("household/note.md").exists());
    let parent = result["receipt"]["parent_revision"].as_str().unwrap();
    assert_eq!(
        git(
            &fixture.launch.vault,
            &["show", &format!("{parent}:household/note.md")]
        ),
        "synthetic retained history"
    );
    assert!(fixture
        .broker
        .definitions()
        .iter()
        .find(|tool| tool["name"] == "memory.commit")
        .unwrap()["description"]
        .as_str()
        .unwrap()
        .contains("not full forgetting"));
}

#[cfg(unix)]
#[test]
fn replaced_database_symlink_is_rejected_on_the_next_call() {
    let fixture = fixture(&["*"], &[]);
    let moved = fixture.launch.database.with_extension("saved");
    fs::rename(&fixture.launch.database, &moved).unwrap();
    std::os::unix::fs::symlink(&moved, &fixture.launch.database).unwrap();
    assert_eq!(
        code(fixture.broker.call(
            "history.read",
            json!({"context":fixture.launch.grant.context,"conversation":"owner-chat"})
        )),
        "storage_unavailable"
    );
}

#[test]
fn unrelated_vault_recovery_record_rejects_without_inventing_a_pending_write() {
    let fixture = fixture(&["*"], &["*"]);
    let args = commit_args(&fixture, "household/new.md", json!("synthetic"));
    let base = Vault::open(&fixture.launch.vault)
        .unwrap()
        .revision()
        .unwrap();
    let journal = fixture
        .launch
        .vault
        .join(".git/oppa-vault-transaction.json");
    fs::write(
        &journal,
        br#"{"foreign":"recovery belongs to another request"}"#,
    )
    .unwrap();
    assert_eq!(
        code(fixture.broker.call("memory.commit", args.clone())),
        "write_rejected"
    );
    assert_eq!(
        code(fixture.broker.call("memory.commit", args)),
        "write_rejected"
    );
    assert_eq!(
        fixture.broker.observations().unwrap()["writes"][0]["status"],
        "rejected"
    );
    assert_eq!(fixture.broker.activity().unwrap(), Activity::None);
    assert_eq!(
        Vault::open(&fixture.launch.vault)
            .unwrap()
            .revision()
            .unwrap(),
        base
    );
    assert!(!fixture.launch.vault.join("household/new.md").exists());
    assert_eq!(
        fs::read(&journal).unwrap(),
        br#"{"foreign":"recovery belongs to another request"}"#
    );
}

fn task_fixture(access: TaskAccess) -> Fixture {
    let mut fixture = fixture(
        &["*"],
        if access == TaskAccess::Manage {
            &["*"]
        } else {
            &[]
        },
    );
    fixture.launch.grant.run_id = "task-tools-run".into();
    fixture.launch.grant.task_access = access;
    fixture.launch.grant.eligible_agents = if access == TaskAccess::Manage {
        vec!["synthetic".into()]
    } else {
        vec![]
    };
    fixture.broker = Broker::open(fixture.launch.clone()).unwrap();
    fixture
}

fn task_args(fixture: &Fixture) -> Value {
    json!({"context":fixture.launch.grant.context,"agent":"synthetic",
        "brief":"Read the synthetic source and report its color.","done_when":"A source-backed answer is recorded.",
        "idempotency_key":"synthetic-color-task"})
}

fn task_paths(fixture: &Fixture) -> Paths {
    Paths::new(
        fixture.launch.database.parent().unwrap().into(),
        Some(fixture.launch.private_dir.clone()),
    )
    .unwrap()
}

#[test]
fn delegation_binds_original_input_replays_identical_task_and_rejects_changed_payload() {
    let fixture = task_fixture(TaskAccess::Manage);
    // History 4, memory 4, tasks 4, schedules 3, people 4, messages.react and
    // messages.contact_card, and the owner's soul.avatar; a read run has no
    // messages.send.
    assert_eq!(fixture.broker.definitions().len(), 22);
    let args = task_args(&fixture);
    for name in [
        "requester",
        "input_seq",
        "conversation",
        "worker",
        "command",
    ] {
        let mut injected = args.clone();
        injected[name] = json!("model-owned-authority");
        assert_eq!(
            code(fixture.broker.call("tasks.delegate", injected)),
            "invalid_arguments"
        );
    }
    let mut invalid = args.clone();
    invalid["agent"] = json!("unregistered");
    assert_eq!(
        code(fixture.broker.call("tasks.delegate", invalid)),
        "access_denied"
    );
    assert_eq!(fixture.broker.activity().unwrap(), Activity::None);
    let first = success(fixture.broker.call("tasks.delegate", args.clone()));
    assert_eq!(
        first["task"]["origin"]["input_seq"],
        fixture.launch.grant.input_seq
    );
    assert_eq!(first["task"]["origin"]["requester"], "synthetic-owner");
    assert_eq!(first["task"]["origin"]["conversation"], "owner-chat");
    assert_eq!(first["task"]["state"], "queued");
    let again = success(fixture.broker.call("tasks.delegate", args.clone()));
    assert_eq!(again["task"], first["task"]);
    assert_eq!(again["replayed"], true);
    let mut changed = args.clone();
    changed["brief"] = json!("Changed work with the old key");
    assert_eq!(
        code(fixture.broker.call("tasks.delegate", changed)),
        "invalid_arguments"
    );
    let mut resumed = fixture.launch.clone();
    resumed.grant.run_id = "next-conversation-run".into();
    resumed.grant.context = "next-context".into();
    let next = Broker::open(resumed.clone()).unwrap();
    let mut args = args;
    args["context"] = json!(resumed.grant.context);
    assert_eq!(
        success(next.call("tasks.delegate", args))["task"]["id"],
        first["task"]["id"]
    );
    let tasks = oppa::tasks::TaskStore::open(&task_paths(&fixture), "synthetic-owner").unwrap();
    assert_eq!(tasks.list(Some("owner-chat"), 100).unwrap().len(), 1);
    let observation = fixture.broker.observations().unwrap();
    assert_eq!(observation["activity"], "effect");
    assert_eq!(observation["writes"][0]["tool"], "tasks.delegate");
    assert_eq!(observation["writes"][0]["status"], "committed");
    assert!(!observation.to_string().contains("synthetic source"));
}

#[test]
fn task_scope_stays_at_original_conversation_even_with_wider_history() {
    use oppa::tasks::{TaskOrigin, TaskSpec, TaskStore};
    let mut fixture = task_fixture(TaskAccess::Manage);
    fixture.launch.grant.run_id = "wider-history-run".into();
    fixture.launch.grant.conversations.push("other-chat".into());
    fixture.broker = Broker::open(fixture.launch.clone()).unwrap();
    let store = Store::open(&fixture.launch.database).unwrap();
    let input = store
        .append_inbound(&InboundEvent {
            conversation: "other-chat".into(),
            native_id: "other-owner-input".into(),
            author: "synthetic-owner".into(),
            body: "Other task".into(),
            ..Default::default()
        })
        .unwrap();
    let tasks = TaskStore::open(&task_paths(&fixture), "synthetic-owner").unwrap();
    let other = tasks
        .create(
            &TaskOrigin {
                requester: "synthetic-owner".into(),
                conversation: "other-chat".into(),
                input_seq: input.seq,
            },
            &TaskSpec {
                agent: "synthetic".into(),
                brief: "Other scoped sentinel".into(),
                done_when: "Done".into(),
                idempotency_key: "other".into(),
            },
            &["synthetic".into()],
        )
        .unwrap();
    assert_eq!(
        code(fixture.broker.call(
            "tasks.list",
            json!({"context":fixture.launch.grant.context,"conversation":"other-chat"})
        )),
        "access_denied"
    );
    for tool in ["tasks.read", "tasks.stop"] {
        assert_eq!(
            code(fixture.broker.call(
                tool,
                json!({"context":fixture.launch.grant.context,"task_id":other.id})
            )),
            "access_denied"
        );
    }
    assert!(!tasks.get(&other.id).unwrap().stop_requested);
    assert_eq!(fixture.broker.activity().unwrap(), Activity::None);
}

#[test]
fn task_reads_paginate_actual_outcome_without_native_observations_and_stop_is_only_requested() {
    use oppa::tasks::{AttemptOutcome, TaskStore};
    let fixture = task_fixture(TaskAccess::Manage);
    let created = success(fixture.broker.call("tasks.delegate", task_args(&fixture)));
    let id = created["task"]["id"].as_str().unwrap();
    let tasks = TaskStore::open(&task_paths(&fixture), "synthetic-owner").unwrap();
    let claim = tasks.claim_next("synthetic-worker").unwrap().unwrap();
    let stopped = success(fixture.broker.call(
        "tasks.stop",
        json!({"context":fixture.launch.grant.context,"task_id":id}),
    ));
    assert_eq!(stopped["task"]["state"], "running");
    assert_eq!(stopped["task"]["stop_requested"], true);
    tasks
        .finish(
            &claim,
            AttemptOutcome::Completed {
                result: "汉🙂abcdef".into(),
            },
            json!({"native_session":"private-native-sentinel"}),
        )
        .unwrap();
    let read = success(fixture.broker.call(
        "tasks.read",
        json!({"context":fixture.launch.grant.context,"task_id":id,"offset":1,"max_chars":3}),
    ));
    assert_eq!(read["task"]["state"], "stopped");
    assert_eq!(read["outcome"], "completed");
    assert_eq!(read["result"], "🙂ab");
    assert_eq!(read["next_offset"], 4);
    assert_eq!(read["result_char_count"], 8);
    assert!(!read.to_string().contains("private-native-sentinel"));
    let rest = success(fixture.broker.call(
        "tasks.read",
        json!({"context":fixture.launch.grant.context,"task_id":id,"offset":4,"max_chars":20}),
    ));
    assert_eq!(rest["result"], "cdef");
    assert!(rest["next_offset"].is_null());
    assert_eq!(
        code(fixture.broker.call(
            "tasks.read",
            json!({"context":fixture.launch.grant.context,"task_id":id,"offset":9})
        )),
        "invalid_arguments"
    );
}

#[test]
fn read_only_capabilities_deny_hidden_mutations_and_current_registry_fences_new_tasks() {
    for access in [TaskAccess::None, TaskAccess::Read] {
        let fixture = task_fixture(access);
        let names: Vec<_> = fixture
            .broker
            .definitions()
            .iter()
            .map(|tool| tool["name"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(
            names.contains(&"tasks.read".to_owned()),
            access == TaskAccess::Read
        );
        for name in ["tasks.delegate", "tasks.stop", "memory.commit"] {
            assert!(!names.contains(&name.to_owned()));
        }
        assert_eq!(
            code(fixture.broker.call("tasks.delegate", task_args(&fixture))),
            "access_denied"
        );
        assert_eq!(
            code(fixture.broker.call(
                "tasks.stop",
                json!({"context":fixture.launch.grant.context,"task_id":"a".repeat(64)})
            )),
            "access_denied"
        );
        assert_eq!(
            code(fixture.broker.call(
                "memory.commit",
                commit_args(&fixture, "household/denied.md", json!("denied"))
            )),
            "access_denied"
        );
        // A read-only run (a presentation or a heartbeat) may list schedules
        // and change none.
        let context = &fixture.launch.grant.context;
        for (name, args) in [
            (
                "schedules.create",
                json!({"context":context,"conversation":"console","at":"07:00","prompt":"Synthetic.","idempotency_key":"k"}),
            ),
            ("schedules.cancel", json!({"context":context,"id":"s-0"})),
        ] {
            assert_eq!(code(fixture.broker.call(name, args)), "access_denied");
        }
        assert_eq!(fixture.broker.activity().unwrap(), Activity::None);
        let listed = fixture
            .broker
            .call("schedules.list", json!({"context":context}));
        assert_eq!(
            !listed.is_error,
            access == TaskAccess::Read,
            "schedules.list at {access:?}: {}",
            listed.value
        );
    }
    let fixture = task_fixture(TaskAccess::Manage);
    let paths = task_paths(&fixture);
    let mut config = paths.load().unwrap();
    config.agents[0].id = "replacement-agent".into();
    fs::write(paths.config_file(), toml::to_string(&config).unwrap()).unwrap();
    assert_eq!(
        code(fixture.broker.call("tasks.delegate", task_args(&fixture))),
        "access_denied"
    );
    let mut replacement = task_args(&fixture);
    replacement["agent"] = json!("replacement-agent");
    assert_eq!(
        code(fixture.broker.call("tasks.delegate", replacement)),
        "access_denied"
    );
    config.owner = "replacement-owner".into();
    fs::write(paths.config_file(), toml::to_string(&config).unwrap()).unwrap();
    assert!(
        fixture
            .broker
            .call(
                "tasks.list",
                json!({"context":fixture.launch.grant.context,"conversation":"owner-chat"})
            )
            .is_error
    );
}

#[test]
fn task_admission_recovers_lost_broker_receipt_and_preserves_uncertain_pending_record() {
    let fixture = task_fixture(TaskAccess::Manage);
    let created = success(fixture.broker.call("tasks.delegate", task_args(&fixture)));
    let record = fs::read_dir(&fixture.launch.journal)
        .unwrap()
        .flat_map(|entry| fs::read_dir(entry.unwrap().path()).unwrap())
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("task-")
        })
        .unwrap();
    let mut data: Value = serde_json::from_slice(&fs::read(&record).unwrap()).unwrap();
    data["receipt"] = Value::Null;
    fs::write(&record, serde_json::to_vec(&data).unwrap()).unwrap();
    let recovered = fixture.broker.observations().unwrap();
    assert_eq!(recovered["writes"][0]["status"], "committed");
    assert_eq!(
        recovered["writes"][0]["receipt"]["id"],
        created["task"]["id"]
    );
    data["task_id"] = json!("f".repeat(64));
    fs::write(&record, serde_json::to_vec(&data).unwrap()).unwrap();
    let uncertain = fixture.broker.observations().unwrap();
    assert_eq!(uncertain["writes"][0]["status"], "pending");
    assert_eq!(
        code(fixture.broker.call("tasks.delegate", task_args(&fixture))),
        "write_pending"
    );
}

#[test]
fn task_stopped_before_any_attempt_reads_as_stopped_not_pending() {
    let fixture = task_fixture(TaskAccess::Manage);
    let created = success(fixture.broker.call("tasks.delegate", task_args(&fixture)));
    let id = created["task"]["id"].as_str().unwrap();
    let stopped = success(fixture.broker.call(
        "tasks.stop",
        json!({"context":fixture.launch.grant.context,"task_id":id}),
    ));
    assert_eq!(stopped["task"]["state"], "stopped");
    let read = success(fixture.broker.call(
        "tasks.read",
        json!({"context":fixture.launch.grant.context,"task_id":id}),
    ));
    assert_eq!(read["task"]["state"], "stopped");
    assert_eq!(read["outcome"], "stopped");
    assert_eq!(read["result"], "");
    assert!(read["next_offset"].is_null());
}

#[test]
fn scoped_discovery_lists_and_reads_only_the_granted_channel_account() {
    use oppa::broker::ConversationScope;
    let mut fixture = fixture(&["*"], &[]);
    let context = fixture.launch.grant.context.clone();
    let store = Store::open(&fixture.launch.database).unwrap();
    for id in ["alpha-chat", "beta-chat"] {
        store
            .create_conversation(id, "console", "local", id, "policy-1")
            .unwrap();
    }
    store
        .create_conversation(
            "foreign-chat",
            "imsg",
            "other-account",
            "foreign",
            "policy-1",
        )
        .unwrap();
    for (conversation, body) in [
        ("beta-chat", "Beta sentinel"),
        ("foreign-chat", "Foreign sentinel"),
    ] {
        store
            .append_inbound(&InboundEvent {
                conversation: conversation.into(),
                native_id: format!("{conversation}-1"),
                author: "synthetic-owner".into(),
                body: body.into(),
                ..Default::default()
            })
            .unwrap();
    }
    let listed = success(
        fixture
            .broker
            .call("history.conversations", json!({"context":context})),
    );
    assert_eq!(listed["conversations"].as_array().unwrap().len(), 1);
    assert_eq!(listed["conversations"][0]["id"], "owner-chat");
    assert_eq!(listed["conversations"][0]["current"], true);
    assert_eq!(listed["pagination"]["may_have_more"], false);
    assert_eq!(
        code(fixture.broker.call(
            "history.read",
            json!({"context":context,"conversation":"beta-chat"})
        )),
        "access_denied",
        "without a scope only explicit conversations are readable"
    );

    fixture.launch.grant.run_id = "scoped-run".into();
    fixture.launch.grant.conversation_scopes = vec![ConversationScope {
        channel: "console".into(),
        account: "local".into(),
        owners_account: false,
    }];
    fixture.broker = Broker::open(fixture.launch.clone()).unwrap();
    let first = success(fixture.broker.call(
        "history.conversations",
        json!({"context":context,"limit":2}),
    ));
    let ids = |page: &Value| {
        page["conversations"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["id"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>()
    };
    assert_eq!(ids(&first), ["owner-chat", "alpha-chat", "beta-chat"]);
    assert_eq!(
        first["conversations"][2]["last_event_seq"]
            .as_i64()
            .unwrap()
            > 0,
        true
    );
    assert_eq!(first["pagination"]["may_have_more"], true);
    assert_eq!(first["pagination"]["next_after_id"], "beta-chat");
    let second = success(fixture.broker.call(
        "history.conversations",
        json!({"context":context,"limit":2,"after_id":"beta-chat"}),
    ));
    assert_eq!(ids(&second), ["other-chat", "owner-chat"]);
    let third = success(fixture.broker.call(
        "history.conversations",
        json!({"context":context,"limit":2,"after_id":"owner-chat"}),
    ));
    assert!(ids(&third).is_empty());
    assert_eq!(third["pagination"]["may_have_more"], false);
    let queried = success(fixture.broker.call(
        "history.conversations",
        json!({"context":context,"query":"BETA"}),
    ));
    assert_eq!(ids(&queried), ["beta-chat"]);
    // A native group id is opaque; the model finds a group by the title it knows.
    store
        .set_conversation_shape("alpha-chat", "group", Some("Synthetic Parents"))
        .unwrap();
    let titled = success(fixture.broker.call(
        "history.conversations",
        json!({"context":context,"query":"parents"}),
    ));
    assert_eq!(ids(&titled), ["alpha-chat"]);
    assert_eq!(titled["conversations"][0]["kind"], "group");
    assert_eq!(titled["conversations"][0]["title"], "Synthetic Parents");
    let wildcard = success(fixture.broker.call(
        "history.conversations",
        json!({"context":context,"query":"%"}),
    ));
    assert!(ids(&wildcard).is_empty(), "LIKE wildcards are literal");
    assert!(!wildcard.to_string().contains("foreign"));

    let read = success(fixture.broker.call(
        "history.read",
        json!({"context":context,"conversation":"beta-chat"}),
    ));
    assert_eq!(read["events"][0]["body"], "Beta sentinel");
    for (tool, args) in [
        (
            "history.read",
            json!({"context":context,"conversation":"foreign-chat"}),
        ),
        (
            "history.search",
            json!({"context":context,"conversation":"foreign-chat","query":"sentinel"}),
        ),
        (
            "history.read",
            json!({"context":context,"conversation":"ghost-chat"}),
        ),
    ] {
        assert_eq!(code(fixture.broker.call(tool, args)), "access_denied");
    }
    assert!(
        store.conversation("ghost-chat").is_err(),
        "denial never creates a conversation"
    );
    assert_eq!(fixture.broker.activity().unwrap(), Activity::Read);
}

#[test]
fn connector_reads_reach_every_run_and_writes_only_full_access_with_a_journal() {
    use oppa::config::{ConnectorKind, ConnectorManifest};
    // C-5, C-8: a script connector's read operations become broker tools under
    // the run's grant; its writes reach only a full-access run, journaled.
    const FAKE: &str = r#"
import json, sys
request = json.loads(sys.stdin.read())
if request["method"] == "call" and request["operation"] == "add_event":
    with open(sys.argv[1], "a") as calls:
        calls.write(str(request["arguments"].get("day")) + "\n")
if request["method"] == "call" and request["arguments"].get("hang"):
    import time; time.sleep(30)
if request["method"] == "call" and request["arguments"].get("refuse"):
    print(json.dumps({"status": "error", "error": "Synthetic refusal."})); sys.exit(0)
if request["method"] == "call" and request["arguments"].get("day") == "big":
    print(json.dumps({"status": "observed", "result": {"blob": "x" * 900000}})); sys.exit(0)
if request["method"] == "describe":
    print(json.dumps({"protocol_version": 1, "operations": [
        {"name": "list_events", "tier": "read", "description": "Synthetic events.",
         "input_schema": {"type": "object", "properties": {"day": {"type": "string"}}}},
        {"name": "add_event", "tier": "reversible", "description": "Synthetic write.",
         "input_schema": {"type": "object"}}]}))
else:
    print(json.dumps({"status": "observed", "result": {"day": request["arguments"].get("day"),
                      "keys": sorted(request["arguments"]), "prompt": request["may_prompt"]}}))
"#;
    let fixture = fixture(&[], &[]);
    let calls = fixture.launch.private_dir.parent().unwrap().join("calls");
    let config_file = Paths::new(
        fixture.launch.private_dir.parent().unwrap().join("runtime"),
        Some(fixture.launch.private_dir.clone()),
    )
    .unwrap()
    .config_file();
    let mut config: Config = toml::from_str(&fs::read_to_string(&config_file).unwrap()).unwrap();
    config.connectors = vec![ConnectorManifest {
        id: "synthetic".into(),
        kind: ConnectorKind::Script,
        command: vec![
            "python3".into(),
            "-c".into(),
            FAKE.into(),
            calls.to_string_lossy().into(),
        ],
        timeout_seconds: 3,
        owner_only: false,
    }];
    fs::write(&config_file, toml::to_string(&config).unwrap()).unwrap();
    let broker = Broker::open(fixture.launch.clone()).unwrap();
    assert_eq!(broker.connector_tools(), ["synthetic.list_events"]);
    let names: Vec<String> = broker
        .definitions()
        .iter()
        .map(|tool| tool["name"].as_str().unwrap().to_owned())
        .collect();
    assert!(names.contains(&"synthetic.list_events".to_owned()));
    assert!(!names.iter().any(|name| name.contains("add_event")));
    assert_eq!(
        code(broker.call(
            "synthetic.add_event",
            json!({"context":fixture.launch.grant.context})
        )),
        "unknown_tool"
    );
    assert_eq!(
        code(broker.call(
            "synthetic.list_events",
            json!({"context":"forged","day":"2026-01-02"})
        )),
        "access_denied"
    );
    let value = success(broker.call(
        "synthetic.list_events",
        json!({"context":fixture.launch.grant.context,"day":"2026-01-02"}),
    ));
    assert_eq!(value["observation"]["result"]["day"], "2026-01-02");
    assert_eq!(
        value["observation"]["result"]["keys"],
        json!(["day"]),
        "the grant's context never reaches the adapter"
    );
    assert_eq!(
        value["observation"]["result"]["prompt"], false,
        "no access prompts from a run"
    );
    assert_eq!(broker.activity().unwrap(), Activity::Read);

    let mut full = fixture.launch.clone();
    full.grant.access = Access::Connectors;
    full.grant.run_id = "full-access-run".into();
    let broker = Broker::open(full).unwrap();
    assert_eq!(broker.connector_tools(), ["synthetic.list_events"]);
    assert_eq!(broker.connector_write_tools(), ["synthetic.add_event"]);
    let add = broker
        .definitions()
        .into_iter()
        .find(|tool| tool["name"] == "synthetic.add_event")
        .unwrap();
    assert_eq!(add["annotations"]["readOnlyHint"], false);
    let call = |day: &str| {
        success(broker.call(
            "synthetic.add_event",
            json!({"context":fixture.launch.grant.context,"day":day}),
        ))
    };
    let first = call("2026-01-03");
    assert_eq!(first["observation"]["result"]["day"], "2026-01-03");
    assert_eq!(broker.activity().unwrap(), Activity::Effect);
    // Identical arguments can be two intended changes, or a change back after
    // another one, so every call runs and every call is journaled.
    call("2026-01-04");
    call("2026-01-03");
    let ran = || fs::read_to_string(&calls).unwrap();
    assert_eq!(ran(), "2026-01-03\n2026-01-04\n2026-01-03\n");
    let writes = broker.observations().unwrap()["writes"].clone();
    assert_eq!(writes.as_array().unwrap().len(), 3);
    assert!(writes
        .as_array()
        .unwrap()
        .iter()
        .all(|write| write["tool"] == "synthetic.add_event" && write["status"] == "observed"));
    let refused = json!({"context":fixture.launch.grant.context,"refuse":true});
    success(broker.call("synthetic.add_event", refused.clone()));
    // An error does not prove nothing changed: the identical call is refused.
    assert_eq!(
        code(broker.call("synthetic.add_event", refused)),
        "write_unverified"
    );
    assert!(broker.observations().unwrap()["writes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|write| write["status"] == "failed"));
    // A write whose outcome was lost may have acted, and says so: an identical
    // one is refused, never repeated, while other writes still run.
    let lost = json!({"context":fixture.launch.grant.context,"hang":true});
    let failed = broker.call("synthetic.add_event", lost.clone());
    assert_eq!(code(failed.clone()), "write_unverified");
    assert!(failed.value["error"]["message"]
        .as_str()
        .unwrap()
        .contains("may or may not have been made"));
    assert_eq!(
        code(broker.call("synthetic.add_event", lost)),
        "write_unverified"
    );
    call("2026-01-05");
    // An answer under the broker's limit can still be too large for MCP to
    // carry; the write says how it ended instead, and the journal stays readable.
    let big = json!({"context":fixture.launch.grant.context,"day":"big"});
    let answer = success(broker.call("synthetic.add_event", big));
    assert_eq!(answer["observation"]["status"], "observed");
    assert!(answer["observation"].get("result").is_none());
    assert!(serde_json::to_vec(&answer).unwrap().len() < 1024);
    // The refused write and the lost one each ran once; the identical retry did not.
    assert_eq!(
        ran(),
        "2026-01-03\n2026-01-04\n2026-01-03\nNone\nNone\n2026-01-05\nbig\n"
    );
    assert!(broker.observations().unwrap()["writes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|write| write["status"] == "pending"));
}

#[test]
fn permission_tool_answers_from_the_runs_access_and_journals_names_only() {
    // C-5, S-3: the host decides each native tool call and records that an
    // allowed one may have acted; arguments are never journaled.
    let read = fixture(&["*"], &[]);
    assert!(read
        .broker
        .definitions()
        .iter()
        .all(|tool| tool["name"] != "permission.decide"));
    assert_eq!(
        code(
            read.broker
                .call("permission.decide", json!({"tool_name":"Bash","input":{}}))
        ),
        "unknown_tool"
    );
    let decide = |broker: &Broker, tool: &str| {
        success(broker.call(
            "permission.decide",
            json!({"tool_name":tool,"input":{"to":"SYNTHETIC_ARGUMENT"}}),
        ))
    };
    for (access, tool, allowed) in [
        (
            Access::Connectors,
            "mcp__claude_ai_Gmail__send_message",
            true,
        ),
        (Access::Connectors, "Bash", false),
        (Access::Connectors, "Write", false),
        // Everything the owner configured in the
        // harness, their own MCP servers and the browser included.
        (
            Access::Connectors,
            "mcp__robinhood-trading__place_order",
            true,
        ),
        (Access::Connectors, "mcp__claude-in-chrome__navigate", true),
        (Access::Connectors, "WebFetch", true),
        (Access::Computer, "Bash", true),
    ] {
        let fixture = fixture(&["*"], &["*"]);
        let mut launch = fixture.launch.clone();
        launch.grant.access = access;
        launch.grant.run_id = format!("{access:?}-{tool}").replace(['_', '-'], "");
        let broker = Broker::open(launch).unwrap();
        assert!(broker
            .definitions()
            .iter()
            .any(|tool| tool["name"] == "permission.decide"));
        let decision = decide(&broker, tool);
        if allowed {
            assert_eq!(decision["behavior"], "allow");
            assert_eq!(decision["updatedInput"]["to"], "SYNTHETIC_ARGUMENT");
            assert_eq!(broker.activity().unwrap(), Activity::Unmediated);
        } else {
            assert_eq!(decision["behavior"], "deny");
            assert_eq!(broker.activity().unwrap(), Activity::None);
        }
        let observations = broker.observations().unwrap();
        let list = if allowed { "allowed" } else { "denied" };
        assert_eq!(observations["native_permissions"][list], json!([tool]));
        assert!(!observations.to_string().contains("SYNTHETIC_ARGUMENT"));
    }
    let fixture = fixture(&["*"], &["*"]);
    let mut expired = fixture.launch.clone();
    expired.grant.access = Access::Computer;
    expired.grant.run_id = "expired-permission".into();
    expired.grant.expires_at = Utc::now().timestamp() - 1;
    let expired = Broker::open(expired).unwrap();
    assert_eq!(decide(&expired, "Bash")["behavior"], "deny");
    // A change of owner revokes a live grant, as it does for every broker tool.
    let mut launch = fixture.launch.clone();
    launch.grant.access = Access::Computer;
    launch.grant.run_id = "replaced-owner".into();
    let broker = Broker::open(launch).unwrap();
    assert_eq!(decide(&broker, "Bash")["behavior"], "allow");
    let paths = Paths::new(
        fixture.launch.private_dir.parent().unwrap().join("runtime"),
        Some(fixture.launch.private_dir.clone()),
    )
    .unwrap();
    let mut config = paths.load().unwrap();
    config.owner = "replacement-owner".into();
    fs::write(paths.config_file(), toml::to_string(&config).unwrap()).unwrap();
    assert_eq!(decide(&broker, "Bash")["behavior"], "deny");
}

#[test]
fn a_read_run_budgets_only_the_connector_tools_it_is_offered() {
    // C-8: writes a read run never sees must not crowd its reads out of the
    // harness adapters' 256-tool limit.
    use oppa::config::{ConnectorKind, ConnectorManifest};
    const WIDE: &str = r#"
import json, sys
request = json.loads(sys.stdin.read())
schema = {"type": "object"}
operations = [{"name": "read_one", "tier": "read", "description": "Synthetic read.", "input_schema": schema}]
operations += [{"name": "write_%d" % i, "tier": "reversible", "description": "Synthetic write.",
                "input_schema": schema} for i in range(63)]
print(json.dumps({"protocol_version": 1, "operations": operations}))
"#;
    let fixture = fixture(&[], &[]);
    let paths = Paths::new(
        fixture.launch.private_dir.parent().unwrap().join("runtime"),
        Some(fixture.launch.private_dir.clone()),
    )
    .unwrap();
    let mut config = paths.load().unwrap();
    config.connectors = (0..5)
        .map(|i| ConnectorManifest {
            id: format!("wide{i}"),
            kind: ConnectorKind::Script,
            command: vec!["python3".into(), "-c".into(), WIDE.into()],
            timeout_seconds: 10,
            owner_only: false,
        })
        .collect();
    fs::write(paths.config_file(), toml::to_string(&config).unwrap()).unwrap();
    let read = Broker::open(fixture.launch.clone()).unwrap();
    assert_eq!(read.connector_tools().len(), 5);
    assert!(read.connector_write_tools().is_empty());
    let mut launch = fixture.launch.clone();
    launch.grant.access = Access::Connectors;
    launch.grant.run_id = "full-access-budget".into();
    let full = Broker::open(launch).unwrap();
    assert_eq!(full.connector_tools().len(), 4);
    assert_eq!(full.connector_write_tools().len(), 4 * 63);
}

#[test]
fn an_owner_only_connector_reaches_only_the_owners_runs_above_read() {
    // Review of 2026-10-11: the owner's vault must not reach a heartbeat or a
    // result presentation, both read runs the owner requested, where injected
    // text could ask for a secret; the owner's turn at connectors gets it.
    use oppa::config::{ConnectorKind, ConnectorManifest};
    const FAKE: &str = r#"
import json, sys
request = json.loads(sys.stdin.read())
print(json.dumps({"protocol_version": 1, "operations": [
    {"name": "list_events", "tier": "read", "description": "Synthetic items.",
     "input_schema": {"type": "object"}}]}))
"#;
    let fixture = fixture(&[], &[]);
    let calls = fixture.launch.private_dir.parent().unwrap().join("calls");
    let config_file = Paths::new(
        fixture.launch.private_dir.parent().unwrap().join("runtime"),
        Some(fixture.launch.private_dir.clone()),
    )
    .unwrap()
    .config_file();
    let mut config: Config = toml::from_str(&fs::read_to_string(&config_file).unwrap()).unwrap();
    config.connectors = vec![ConnectorManifest {
        id: "vault".into(),
        kind: ConnectorKind::Script,
        command: vec![
            "python3".into(),
            "-c".into(),
            FAKE.into(),
            calls.to_string_lossy().into(),
        ],
        timeout_seconds: 3,
        owner_only: true,
    }];
    fs::write(&config_file, toml::to_string(&config).unwrap()).unwrap();
    assert_eq!(fixture.launch.grant.access, Access::Read);
    let read = Broker::open(fixture.launch.clone()).unwrap();
    assert!(read.connector_tools().is_empty());
    let mut full = fixture.launch.clone();
    full.grant.access = Access::Connectors;
    full.grant.run_id = "owner-only-full".into();
    let full = Broker::open(full).unwrap();
    assert_eq!(full.connector_tools(), ["vault.list_events"]);
}

#[test]
fn only_the_owners_own_turn_at_full_access_may_start_a_memory_import() {
    // Onboarding: the owner's yes in chat starts the import of their Claude
    // Code and Codex memory; a family member's turn cannot, and an instance
    // without the sync says so instead of starting nothing silently.
    let with = |role, access| {
        let mut fixture = fixture(&["*"], &["*"]);
        fixture.launch.grant.run_id = "memory-import-run".into();
        fixture.launch.grant.role = role;
        fixture.launch.grant.access = access;
        fixture.broker = Broker::open(fixture.launch.clone()).unwrap();
        fixture
    };
    let offered = |fixture: &Fixture| {
        fixture
            .broker
            .definitions()
            .iter()
            .any(|tool| tool["name"] == "memory.import")
    };
    let start = |fixture: &Fixture| {
        fixture.broker.call(
            "memory.import",
            json!({"context":"synthetic-opaque-current-context"}),
        )
    };
    let adult = with(oppa::config::Role::Adult, Access::Computer);
    assert!(!offered(&adult));
    assert_eq!(code(start(&adult)), "access_denied");
    assert!(!offered(&with(oppa::config::Role::Owner, Access::Read)));
    assert!(!offered(&with(oppa::config::Role::Child, Access::Computer)));

    let owner = with(oppa::config::Role::Owner, Access::Computer);
    assert!(offered(&owner));
    assert_eq!(code(start(&owner)), "no_memory_sync");
    let runtime = owner.launch.database.parent().unwrap().to_owned();
    let paths = Paths::new(runtime.clone(), Some(owner.launch.private_dir.clone())).unwrap();
    let mut config = paths.load().unwrap();
    config.access = Access::Computer;
    config.memory_sync = Some(oppa::memory_sync::MemorySync {
        at: "03:30".into(),
        sources: vec!["/nonexistent/synthetic/*.md".into()],
        conversation: None,
    });
    fs::write(paths.config_file(), toml::to_string(&config).unwrap()).unwrap();
    let started = success(start(&owner));
    assert_eq!(started["receipt"]["started"], true, "{started}");
    assert!(runtime.join("memory-import.json").exists());
}

/// A picture `width` by `height` pixels, written by Python, to stand in for a
/// face someone sent.
fn picture(dir: &Path, width: u32, height: u32) -> PathBuf {
    let path = dir.join(format!("picture-{width}x{height}.png"));
    let script = r#"
import struct, sys, zlib
w, h, out = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
raw = b''.join(b'\x00' + bytes([200, 80, 40]) * w for _ in range(h))
def chunk(kind, data):
    return struct.pack('>I', len(data)) + kind + data + struct.pack('>I', zlib.crc32(kind + data) & 0xffffffff)
png = b'\x89PNG\r\n\x1a\n' + chunk(b'IHDR', struct.pack('>IIBBBBB', w, h, 8, 2, 0, 0, 0)) + chunk(b'IDAT', zlib.compress(raw)) + chunk(b'IEND', b'')
open(out, 'wb').write(png)
"#;
    let status = Command::new("python3")
        .args(["-c", script, &width.to_string(), &height.to_string()])
        .arg(&path)
        .status()
        .unwrap();
    assert!(status.success());
    path
}

fn pixels(picture: &Path) -> (u32, u32) {
    let output = Command::new("sips")
        .args(["-g", "pixelWidth", "-g", "pixelHeight"])
        .arg(picture)
        .output()
        .unwrap();
    let text = String::from_utf8(output.stdout).unwrap();
    let field = |name: &str| {
        text.lines()
            .find_map(|line| line.trim().strip_prefix(name)?.trim().parse().ok())
            .unwrap()
    };
    (field("pixelWidth:"), field("pixelHeight:"))
}

#[test]
fn only_the_owners_turn_sets_a_face_which_is_squared_and_keeps_the_one_before() {
    // The owner asked on 2026-11-07 for a face like Muse's and Dot's: the owner's
    // turn sets it from a picture, the host squares it, and earlier faces stay so
    // going back is possible. Nobody else changes who the assistant is.
    let fixture = task_fixture(TaskAccess::Manage);
    let paths = task_paths(&fixture);
    let dir = fixture.launch.private_dir.parent().unwrap().to_path_buf();
    let set = |broker: &Broker, file: &Path| {
        broker.call(
            "soul.avatar",
            json!({"context":fixture.launch.grant.context,"file":file}),
        )
    };
    let first = success(set(&fixture.broker, &picture(&dir, 60, 40)));
    assert_eq!(first["receipt"]["avatar"], json!(paths.avatar()));
    assert_eq!(pixels(&paths.avatar()), (1024, 1024));
    assert_eq!(first["receipt"]["earlier"], json!([]));

    let words = dir.join("not-a-picture.png");
    fs::write(&words, "Hello.").unwrap();
    let before = fs::read(paths.avatar()).unwrap();
    assert_eq!(code(set(&fixture.broker, &words)), "not_a_picture");
    assert_eq!(fs::read(paths.avatar()).unwrap(), before, "nothing changed");

    let second = success(set(&fixture.broker, &picture(&dir, 30, 50)));
    let earlier = second["receipt"]["earlier"].as_array().unwrap();
    assert_eq!(earlier.len(), 1);
    assert_eq!(fs::read(earlier[0].as_str().unwrap()).unwrap(), before);

    for role in [oppa::config::Role::Adult, oppa::config::Role::Child] {
        let mut launch = fixture.launch.clone();
        launch.grant.role = role;
        launch.grant.run_id = format!("family-{role:?}");
        let broker = Broker::open(launch).unwrap();
        assert!(!broker
            .definitions()
            .iter()
            .any(|tool| tool["name"] == "soul.avatar"));
        assert_eq!(code(set(&broker, &picture(&dir, 20, 20))), "access_denied");
    }
}

#[test]
fn a_contact_card_goes_once_per_name_and_face_and_only_to_a_direct_chat() {
    // The owner asked on 2026-11-07 for a contact card with the assistant's
    // name and face, as Dot and Poke send: the first card to a chat is queued
    // with the face, asking again returns it, a new name is a new card, and a
    // group never gets one.
    let fixture = task_fixture(TaskAccess::Manage);
    let paths = task_paths(&fixture);
    let dir = fixture.launch.private_dir.parent().unwrap().to_path_buf();
    oppa::soul::set_avatar(&paths, &picture(&dir, 40, 40)).unwrap();
    let store = Store::open(&fixture.launch.database).unwrap();
    let mut inputs = std::collections::HashMap::new();
    for (id, kind) in [
        ("imsg/assistant/dm", "dm"),
        ("imsg/assistant/club", "group"),
    ] {
        store
            .create_conversation(id, "imsg", "assistant", id, "policy-1")
            .unwrap();
        store.set_conversation_shape(id, kind, None).unwrap();
        let input = store
            .append_inbound(&InboundEvent {
                conversation: id.into(),
                native_id: format!("{id}-hello"),
                author: "synthetic-owner".into(),
                body: "Hello.".into(),
                ..Default::default()
            })
            .unwrap();
        inputs.insert(id, input.seq);
    }
    let in_chat = |conversation: &'static str, run: &str| {
        let mut launch = fixture.launch.clone();
        launch.grant.conversations = vec![conversation.into()];
        launch.grant.input_seq = inputs[conversation];
        launch.grant.run_id = run.into();
        Broker::open(launch).unwrap()
    };
    let card = |broker: &Broker, name: &str| {
        broker.call(
            "messages.contact_card",
            json!({"context":fixture.launch.grant.context,"name":name}),
        )
    };
    let direct = in_chat("imsg/assistant/dm", "card-run");
    let first = success(card(&direct, "Mimi"));
    assert_eq!(
        (&first["name"], &first["face"], &first["delivery"]["state"]),
        (&json!("Mimi"), &json!(true), &json!("queued"))
    );
    let again = in_chat("imsg/assistant/dm", "card-run-again");
    success(card(&again, "Mimi"));
    success(card(&again, "Juniper"));
    let cards: Vec<_> = store
        .pending_outbox()
        .unwrap()
        .into_iter()
        .filter(|entry| entry.event.kind == "contact")
        .map(|entry| (entry.event.body, entry.event.attachments.len()))
        .collect();
    assert_eq!(cards, [("Mimi".into(), 1), ("Juniper".into(), 1)]);
    assert_eq!(
        code(card(&in_chat("imsg/assistant/club", "group-run"), "Mimi")),
        "not_a_direct_chat"
    );
    assert_eq!(code(card(&direct, " ")), "invalid_arguments");
}

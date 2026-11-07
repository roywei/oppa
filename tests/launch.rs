#![cfg(unix)]

use oppa::{
    broker::TaskAccess,
    config::{Config, Paths, Person, Role},
    harness::{Access, AgentManifest},
    launch::{BrokerRun, RunCapabilities},
    mcp::LEGACY_VERSION,
    store::{InboundEvent, Store},
    vault::Vault,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    os::unix::fs::{symlink, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
};
use tempfile::TempDir;

struct Fixture {
    _root: TempDir,
    paths: Paths,
    input_seq: i64,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let paths = Paths::new(
            root.path().join("runtime"),
            Some(root.path().join("private")),
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
                    id: "unused".into(),
                    family: "synthetic".into(),
                    command: vec!["/never/launch/a/provider".into()],
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
        Vault::init(&paths.vault()).unwrap();
        let store = Store::open(&paths.database()).unwrap();
        store
            .create_conversation("chat", "console", "local", "chat", "synthetic-policy")
            .unwrap();
        let input_seq = store
            .append_inbound(&InboundEvent {
                conversation: "chat".into(),
                native_id: "native-1".into(),
                author: "synthetic-owner".into(),
                body: "Synthetic bilingual history: 放学接送。".into(),
                kind: "message".into(),
                ..Default::default()
            })
            .unwrap()
            .seq;
        drop(store);
        Self {
            _root: root,
            paths,
            input_seq,
        }
    }

    fn root(&self) -> &Path {
        self.paths.runtime.parent().unwrap()
    }

    fn create(&self) -> anyhow::Result<BrokerRun> {
        BrokerRun::create(
            &self.paths,
            "synthetic-owner",
            self.input_seq,
            vec!["chat".into()],
            30,
            Path::new(env!("CARGO_BIN_EXE_oppa")),
        )
    }
}

fn grant_file(run: &BrokerRun) -> PathBuf {
    PathBuf::from(run.server.args.last().unwrap())
}

fn requests(run: &BrokerRun, write: bool, revision: &str) -> Vec<Value> {
    let call = if write {
        json!({"name":"memory.commit","arguments":{
            "context":run.launch.grant.context,"expected_revision":revision,
            "edits":[{"path":"household/should-not-exist.md","content":"synthetic forbidden write"}],
            "reason":"synthetic rejected launch"
        }})
    } else {
        json!({"name":"history.read","arguments":{
            "context":run.launch.grant.context,"conversation":"chat","after_seq":0,"limit":10
        }})
    };
    vec![
        json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
            "protocolVersion":LEGACY_VERSION,"capabilities":{},"clientInfo":{"name":"synthetic-cli-test","version":"1"}
        }}),
        json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
        json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":call}),
    ]
}

fn execute(run: &BrokerRun, grant: &Path, requests: &[Value]) -> Output {
    let mut command = Command::new(&run.server.command);
    let mut args = run.server.args.clone();
    *args.last_mut().unwrap() = grant.to_string_lossy().into_owned();
    command
        .args(args)
        .env_remove("HOME")
        .env_remove("OPPA_HOME")
        .env_remove("OPPA_PRIVATE_DIR")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().unwrap();
    let mut input = Vec::new();
    for request in requests {
        serde_json::to_writer(&mut input, request).unwrap();
        input.push(b'\n');
    }
    // A rejected grant may close stdin before accepting the synthetic request.
    let _ = child.stdin.take().unwrap().write_all(&input);
    child.wait_with_output().unwrap()
}

type Snapshot = BTreeMap<PathBuf, (u32, Vec<u8>)>;

fn snapshot(root: &Path) -> Snapshot {
    fn walk(root: &Path, path: &Path, entries: &mut Snapshot) {
        // SQLite READ_ONLY connections still coordinate readers in this volatile
        // shared-memory cache. Durable DB/WAL bytes and all private files remain exact.
        if path.strip_prefix(root).ok() == Some(Path::new("runtime/state.db-shm")) {
            return;
        }
        let metadata = fs::symlink_metadata(path).unwrap();
        let bytes = if metadata.file_type().is_symlink() {
            fs::read_link(path)
                .unwrap()
                .to_string_lossy()
                .as_bytes()
                .to_vec()
        } else if metadata.is_file() {
            fs::read(path).unwrap()
        } else {
            Vec::new()
        };
        entries.insert(
            path.strip_prefix(root).unwrap().into(),
            (metadata.mode(), bytes),
        );
        if metadata.is_dir() {
            for entry in fs::read_dir(path).unwrap() {
                walk(root, &entry.unwrap().path(), entries);
            }
        }
    }
    let mut entries = BTreeMap::new();
    walk(root, root, &mut entries);
    entries
}

fn assert_rejected_without_effect(fixture: &Fixture, run: &BrokerRun, file: &Path) {
    let revision = Vault::open(&fixture.paths.vault())
        .unwrap()
        .revision()
        .unwrap();
    let before = snapshot(fixture.root());
    let output = execute(run, file, &requests(run, true, &revision));
    assert!(
        !output.status.success(),
        "rejected launch succeeded: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        output.stdout.is_empty(),
        "rejected launch emitted protocol output"
    );
    assert!(
        !output.stderr.is_empty(),
        "rejection omitted its diagnostic"
    );
    let after = snapshot(fixture.root());
    let changed: Vec<_> = before
        .keys()
        .chain(after.keys())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .filter(|path| before.get(*path) != after.get(*path))
        .map(|path| {
            use sha2::{Digest, Sha256};
            let summary = |entry: Option<&(u32, Vec<u8>)>| {
                entry.map(|(mode, bytes)| {
                    format!(
                        "mode={mode:o},bytes={},sha256={:x}",
                        bytes.len(),
                        Sha256::digest(bytes)
                    )
                })
            };
            (path, summary(before.get(path)), summary(after.get(path)))
        })
        .collect();
    assert!(
        changed.is_empty(),
        "rejected launch changed storage: {changed:?}"
    );
}

#[test]
fn issued_private_grant_runs_real_stdio_server_and_is_removed_on_drop() {
    let fixture = Fixture::new();
    let run = fixture.create().unwrap();
    let file = grant_file(&run);
    let metadata = fs::metadata(&file).unwrap();
    assert_eq!(metadata.mode() & 0o777, 0o600);
    assert_eq!(metadata.nlink(), 1);
    let private_before = snapshot(&fixture.paths.private);
    let output = execute(&run, &file, &requests(&run, false, ""));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    let replies: Vec<Value> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(
        replies.len(),
        2,
        "stdout must contain only requested JSON-RPC responses"
    );
    assert_eq!(replies[0]["result"]["protocolVersion"], LEGACY_VERSION);
    let result = &replies[1]["result"];
    assert_eq!(result["isError"], false);
    assert_eq!(
        result["structuredContent"]["events"][0]["seq"],
        fixture.input_seq
    );
    assert_eq!(
        result["structuredContent"]["events"][0]["body"],
        "Synthetic bilingual history: 放学接送。"
    );
    assert_eq!(
        result["structuredContent"]["source"]["requester"],
        "synthetic-owner"
    );
    assert_eq!(snapshot(&fixture.paths.private), private_before);
    drop(run);
    assert!(
        !file.exists(),
        "grant must be revoked after the host run exits"
    );
}

#[test]
fn rejects_grants_from_another_runtime_or_source_checkout_without_effects() {
    for other in ["other-runtime", "other-source"] {
        let fixture = Fixture::new();
        let run = fixture.create().unwrap();
        let parent = fixture.root().join(other);
        fs::create_dir_all(parent.join("grants")).unwrap();
        if other == "other-source" {
            fs::create_dir(parent.join(".git")).unwrap();
        }
        let file = parent.join("grants/copied.json");
        fs::copy(grant_file(&run), &file).unwrap();
        assert_rejected_without_effect(&fixture, &run, &file);
    }
}

#[test]
fn rejects_symlink_hardlink_and_public_grant_files_without_effects() {
    for kind in ["symlink", "hardlink", "public"] {
        let fixture = Fixture::new();
        let run = fixture.create().unwrap();
        let original = grant_file(&run);
        let file = if kind == "public" {
            fs::set_permissions(&original, fs::Permissions::from_mode(0o644)).unwrap();
            original
        } else {
            let alias = original.with_file_name(format!("{kind}.json"));
            if kind == "symlink" {
                symlink(&original, &alias).unwrap();
            } else {
                fs::hard_link(&original, &alias).unwrap();
            }
            alias
        };
        assert_rejected_without_effect(&fixture, &run, &file);
    }
}

#[test]
fn rejects_wrong_owner_storage_expiration_and_invalid_grants_without_effects() {
    for kind in [
        "owner",
        "database",
        "private-dir",
        "vault",
        "journal",
        "expired",
        "invalid-json",
        "oversized",
    ] {
        let fixture = Fixture::new();
        let run = fixture.create().unwrap();
        let file = grant_file(&run);
        let mut launch = run.launch.clone();
        match kind {
            "owner" => launch.grant.requester = "another-owner".into(),
            "database" => {
                launch.database = fixture.root().join("other.db");
                drop(Store::open(&launch.database).unwrap());
            }
            "private-dir" => {
                launch.private_dir = fixture.root().join("other-private");
                fs::create_dir(&launch.private_dir).unwrap();
            }
            "vault" => {
                launch.vault = fixture.root().join("other-vault");
                Vault::init(&launch.vault).unwrap();
            }
            "journal" => {
                launch.journal = fixture.root().join("other-journal");
                fs::create_dir(&launch.journal).unwrap();
            }
            "expired" => launch.grant.expires_at = chrono::Utc::now().timestamp() - 1,
            _ => {}
        }
        let bytes = match kind {
            "invalid-json" => b"{ invalid JSON".to_vec(),
            "oversized" => vec![b' '; 65_537],
            _ => serde_json::to_vec(&launch).unwrap(),
        };
        fs::write(&file, bytes).unwrap();
        assert_rejected_without_effect(&fixture, &run, &file);
    }
}

#[test]
fn family_turns_get_the_owners_grant_and_nobody_else_gets_one() {
    // G-5, G-10: authority follows the sender, and
    // a registered adult's or child's own message yields the grant the
    // owner's would, guarded by instructions alone; a friend's or an agent's
    // message yields none, and neither does anyone's claim on another
    // person's message. Removing someone from the registry ends their run's
    // grant.
    let fixture = Fixture::new();
    let file = fixture.paths.config_file();
    let text = fs::read_to_string(&file).unwrap();
    fs::write(
        &file,
        text.replace("access = \"read\"", "access = \"computer\""),
    )
    .unwrap();
    let mut config = fixture.paths.load().unwrap();
    for (id, role) in [
        ("sam", Role::Adult),
        ("kid", Role::Child),
        ("kim", Role::Friend),
        ("robin-assistant", Role::Agent),
    ] {
        config.people.push(Person {
            id: id.into(),
            role,
            handles: vec![],
        });
    }
    fs::write(&file, toml::to_string(&config).unwrap()).unwrap();
    let store = Store::open(&fixture.paths.database()).unwrap();
    let mut seq = BTreeMap::new();
    for author in ["sam", "kid", "kim", "robin-assistant"] {
        let event = store
            .append_inbound(&InboundEvent {
                conversation: "chat".into(),
                native_id: format!("from-{author}"),
                author: author.into(),
                body: "Synthetic request.".into(),
                kind: "message".into(),
                ..Default::default()
            })
            .unwrap();
        seq.insert(author, event.seq);
    }
    drop(store);
    let create = |requester: &str, input_seq: i64, capabilities| {
        BrokerRun::create_with_capabilities(
            &fixture.paths,
            requester,
            input_seq,
            vec!["chat".into()],
            30,
            Path::new(env!("CARGO_BIN_EXE_oppa")),
            capabilities,
        )
    };
    let reach = |run: &BrokerRun| {
        let grant = &run.launch.grant;
        (
            grant.access,
            grant.task_access,
            grant.read_scopes.clone(),
            grant.write_scopes.clone(),
            grant.conversation_scopes.clone(),
        )
    };
    let owner = create(
        "synthetic-owner",
        fixture.input_seq,
        RunCapabilities::Conversation,
    )
    .unwrap();
    assert_eq!(reach(&owner).0, Access::Computer);
    let adult = create("sam", seq["sam"], RunCapabilities::Conversation).unwrap();
    let child = create("kid", seq["kid"], RunCapabilities::Conversation).unwrap();
    for run in [&adult, &child] {
        assert_eq!(reach(run), reach(&owner), "{}", run.launch.grant.requester);
        assert!(!run.launch.grant.wakes, "only the owner's runs set wakes");
        assert!(
            run.launch.grant.eligible_agents.is_empty(),
            "background tasks stay the owner's"
        );
    }
    assert_eq!(child.launch.grant.role, Role::Child);
    // A family member's grant that claims wakes is refused before any call.
    let mut forged = adult.launch.clone();
    forged.grant.wakes = true;
    fs::write(grant_file(&adult), serde_json::to_vec(&forged).unwrap()).unwrap();
    assert_rejected_without_effect(&fixture, &adult, &grant_file(&adult));
    let adult = create("sam", seq["sam"], RunCapabilities::Conversation).unwrap();
    for (requester, input_seq, capabilities) in [
        ("sam", fixture.input_seq, RunCapabilities::Conversation),
        ("synthetic-owner", seq["sam"], RunCapabilities::Conversation),
        ("kim", seq["kim"], RunCapabilities::Conversation),
        (
            "robin-assistant",
            seq["robin-assistant"],
            RunCapabilities::Conversation,
        ),
    ] {
        assert!(
            create(requester, input_seq, capabilities).is_err(),
            "{requester} got a grant for input {input_seq} as {capabilities:?}"
        );
    }
    config.people.retain(|person| person.id != "sam");
    fs::write(&file, toml::to_string(&config).unwrap()).unwrap();
    assert_rejected_without_effect(&fixture, &adult, &grant_file(&adult));
}

#[test]
fn refuses_redirected_managed_directories_before_creating_grants() {
    for child in ["grants", "runs"] {
        let fixture = Fixture::new();
        let foreign = fixture.root().join("other-source");
        fs::create_dir_all(foreign.join(".git")).unwrap();
        fs::write(foreign.join("keep.txt"), "synthetic unrelated source").unwrap();
        let redirected = fixture.paths.runtime.join(child);
        if redirected.exists() {
            fs::remove_dir(&redirected).unwrap();
        }
        symlink(&foreign, &redirected).unwrap();
        let before = snapshot(&foreign);
        assert!(fixture.create().is_err(), "{child} link was accepted");
        assert_eq!(
            snapshot(&foreign),
            before,
            "grant creation followed {child} link"
        );
    }
}

#[test]
fn refuses_managed_directory_redirects_introduced_after_grant_creation() {
    for child in ["grants", "runs", "journal"] {
        let fixture = Fixture::new();
        let run = fixture.create().unwrap();
        let (original, moved) = if child == "journal" {
            // A nested link must be rejected even if its target remains under runs.
            (
                run.launch.journal.clone(),
                fixture.paths.runtime.join("runs/another-journal"),
            )
        } else {
            let foreign = fixture.root().join("other-source");
            fs::create_dir_all(foreign.join(".git")).unwrap();
            (fixture.paths.runtime.join(child), foreign.join(child))
        };
        fs::rename(&original, &moved).unwrap();
        symlink(&moved, &original).unwrap();
        assert_rejected_without_effect(&fixture, &run, &grant_file(&run));
    }
}

#[test]
fn grant_identity_filename_and_journal_must_name_the_same_run() {
    for kind in [
        "renamed",
        "run-id",
        "context",
        "journal-parent",
        "journal-other-run",
    ] {
        let fixture = Fixture::new();
        let run = fixture.create().unwrap();
        let original = grant_file(&run);
        let mut file = original.clone();
        let mut launch = run.launch.clone();
        match kind {
            "renamed" => {
                file = original.with_file_name("different.json");
                fs::rename(&original, &file).unwrap();
            }
            "run-id" => launch.grant.run_id = "different-run".into(),
            "context" => launch.grant.context = "not-a-host-context".into(),
            "journal-parent" => launch.journal = fixture.paths.runtime.join("runs"),
            "journal-other-run" => {
                launch.journal = fixture.paths.runtime.join("runs/another-run");
                fs::create_dir(&launch.journal).unwrap();
            }
            _ => unreachable!(),
        }
        fs::write(&file, serde_json::to_vec(&launch).unwrap()).unwrap();
        assert_rejected_without_effect(&fixture, &run, &file);
    }
}

#[test]
fn factory_persists_restricted_worker_and_presentation_capabilities_before_launch() {
    use oppa::{
        broker::{Broker, TaskAccess},
        launch::RunCapabilities,
    };
    let fixture = Fixture::new();
    // A heartbeat runs: one is configured and the owner has written to the
    // assistant directly on its channel.
    let mut config = fixture.paths.load().unwrap();
    config.heartbeat = Some(oppa::heartbeat::Heartbeat::default());
    config.channels.push(oppa::config::ChannelManifest {
        id: "fake".into(),
        account: "acct".into(),
        command: vec!["/never/run".into()],
        timeout_seconds: 5,
        account_holder: Default::default(),
    });
    fs::write(
        fixture.paths.config_file(),
        toml::to_string(&config).unwrap(),
    )
    .unwrap();
    let store = Store::open(&fixture.paths.database()).unwrap();
    store
        .create_conversation("fake/acct/dm", "fake", "acct", "dm", "synthetic-policy")
        .unwrap();
    store
        .append_inbound(&InboundEvent {
            conversation: "fake/acct/dm".into(),
            native_id: "native-dm".into(),
            author: "synthetic-owner".into(),
            body: "Synthetic hello.".into(),
            kind: "message".into(),
            ..Default::default()
        })
        .unwrap();
    drop(store);
    for (capabilities, access) in [
        (RunCapabilities::Worker, TaskAccess::None),
        (RunCapabilities::Presentation, TaskAccess::Read),
        (RunCapabilities::Conversation, TaskAccess::Manage),
    ] {
        let run = BrokerRun::create_with_capabilities(
            &fixture.paths,
            "synthetic-owner",
            fixture.input_seq,
            vec!["chat".into()],
            30,
            Path::new(env!("CARGO_BIN_EXE_oppa")),
            capabilities,
        )
        .unwrap();
        let persisted = oppa::launch::load(&fixture.paths, &grant_file(&run)).unwrap();
        assert_eq!(persisted.grant.task_access, access);
        assert_eq!(persisted.grant.read_scopes, ["*"]);
        assert_eq!(
            persisted.grant.write_scopes.is_empty(),
            capabilities != RunCapabilities::Conversation
        );
        assert_eq!(
            persisted.grant.eligible_agents.is_empty(),
            capabilities != RunCapabilities::Conversation
        );
        assert_eq!(
            persisted.grant.wakes,
            capabilities == RunCapabilities::Conversation
        );
        let broker = Broker::open(persisted).unwrap();
        let names: Vec<_> = broker
            .definitions()
            .into_iter()
            .map(|tool| tool["name"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(
            names.contains(&"tasks.read".into()),
            access != TaskAccess::None
        );
        assert_eq!(
            names.contains(&"tasks.delegate".into()),
            access == TaskAccess::Manage
        );
        assert_eq!(
            names.contains(&"memory.commit".into()),
            access == TaskAccess::Manage
        );
        // The owner's conversation turn may set wakes for later looks; a
        // worker or a presentation may not.
        assert_eq!(
            names.contains(&"wakes.set".into()),
            capabilities == RunCapabilities::Conversation
        );
    }
    // With the heartbeat off there is nothing to look with, so no turn gets
    // wakes; nor without a direct chat of the owner's to look in.
    let off = oppa::heartbeat::Heartbeat {
        off: true,
        ..Default::default()
    };
    // Nor in a chat someone else has written in, though it is configured.
    let store = Store::open(&fixture.paths.database()).unwrap();
    store
        .create_conversation(
            "fake/acct/other",
            "fake",
            "acct",
            "other",
            "synthetic-policy",
        )
        .unwrap();
    store
        .append_inbound(&InboundEvent {
            conversation: "fake/acct/other".into(),
            native_id: "native-other".into(),
            author: "fake:+15559999".into(),
            body: "Synthetic hello from someone else.".into(),
            kind: "message".into(),
            ..Default::default()
        })
        .unwrap();
    drop(store);
    let shared = oppa::heartbeat::Heartbeat {
        conversation: "fake/acct/other".into(),
        ..Default::default()
    };
    for (heartbeat, channels) in [
        (off, config.channels.clone()),
        (Default::default(), vec![]),
        (shared, config.channels.clone()),
    ] {
        config.heartbeat = Some(heartbeat);
        config.channels = channels;
        fs::write(
            fixture.paths.config_file(),
            toml::to_string(&config).unwrap(),
        )
        .unwrap();
        let run = BrokerRun::create_with_capabilities(
            &fixture.paths,
            "synthetic-owner",
            fixture.input_seq,
            vec!["chat".into()],
            30,
            Path::new(env!("CARGO_BIN_EXE_oppa")),
            RunCapabilities::Conversation,
        )
        .unwrap();
        assert!(!run.launch.grant.wakes);
    }
}

#[test]
fn launch_rejects_result_events_and_foreign_authors_but_accepts_any_channel() {
    for mutation in [
        "UPDATE events SET kind='task_result'",
        "UPDATE events SET author='fake:+15559999'",
    ] {
        let fixture = Fixture::new();
        let run = fixture.create().unwrap();
        let connection = rusqlite::Connection::open(fixture.paths.database()).unwrap();
        connection.execute(mutation, []).unwrap();
        assert!(fixture.create().is_err(), "{mutation}");
        assert_rejected_without_effect(&fixture, &run, &grant_file(&run));
    }
    // Authority follows the host-resolved author, not the surface: an owner
    // message on another channel account binds a grant like a console one.
    for mutation in [
        "UPDATE conversations SET account='remote'",
        "UPDATE conversations SET channel='other'",
    ] {
        let fixture = Fixture::new();
        let connection = rusqlite::Connection::open(fixture.paths.database()).unwrap();
        connection.execute(mutation, []).unwrap();
        let run = fixture.create().unwrap();
        let persisted = oppa::launch::load(&fixture.paths, &grant_file(&run)).unwrap();
        // The origin's own account first, then the instance's others.
        let scopes = &persisted.grant.conversation_scopes;
        let origin = connection
            .query_row("SELECT channel,account FROM conversations", [], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap();
        assert_eq!(
            (scopes[0].channel.clone(), scopes[0].account.clone()),
            origin,
            "{mutation}"
        );
        assert!(scopes
            .iter()
            .any(|scope| (scope.channel.as_str(), scope.account.as_str()) == ("console", "local")));
    }
}

#[test]
fn owner_turns_and_their_tasks_receive_the_owners_access() {
    // S-3: reach comes from the owner's configuration and the host-chosen run
    // kind. A task the owner's turn delegates does the work with that reach,
    // such as writing a program; a presentation only reports, so it reads.
    let fixture = Fixture::new();
    let file = fixture.paths.config_file();
    let config = fs::read_to_string(&file).unwrap();
    fs::write(
        &file,
        config.replace("access = \"read\"", "access = \"computer\""),
    )
    .unwrap();
    for (capabilities, expected) in [
        (RunCapabilities::Conversation, Access::Computer),
        (RunCapabilities::Worker, Access::Computer),
        (RunCapabilities::Presentation, Access::Read),
    ] {
        let run = BrokerRun::create_with_capabilities(
            &fixture.paths,
            "synthetic-owner",
            fixture.input_seq,
            vec!["chat".into()],
            30,
            Path::new(env!("CARGO_BIN_EXE_oppa")),
            capabilities,
        )
        .unwrap();
        assert_eq!(run.launch.grant.access, expected, "{capabilities:?}");
    }
}

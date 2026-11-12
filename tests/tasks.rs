use oppa::{
    config::{Config, Paths},
    harness::AgentManifest,
    store::{DeliveryObservation, InboundEvent, Store},
    tasks::{
        AttemptOutcome, ProcessStoppedEvidence, Task, TaskOrigin, TaskSpec, TaskState, TaskStore,
    },
};
use rusqlite::Connection;
use serde_json::json;
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Barrier},
};
use tempfile::TempDir;

struct Fixture {
    _root: TempDir,
    paths: Paths,
    store: Store,
    origin: TaskOrigin,
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
                    id: "worker".into(),
                    family: "synthetic".into(),
                    command: vec!["/never/run/a/provider".into()],
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
        let store = Store::open(&paths.database()).unwrap();
        store
            .create_conversation("chat", "console", "local", "chat", "synthetic-policy")
            .unwrap();
        let event = store
            .append_inbound(&InboundEvent {
                conversation: "chat".into(),
                native_id: "input-1".into(),
                author: "synthetic-owner".into(),
                body: "Compare the two synthetic notes in the background.".into(),
                ..Default::default()
            })
            .unwrap();
        let origin = TaskOrigin {
            requester: "synthetic-owner".into(),
            conversation: "chat".into(),
            input_seq: event.seq,
        };
        Self {
            _root: root,
            paths,
            store,
            origin,
        }
    }
    fn tasks(&self) -> TaskStore {
        TaskStore::open(&self.paths, "synthetic-owner").unwrap()
    }
    fn create(&self, tasks: &TaskStore) -> Task {
        tasks
            .create(&self.origin, &spec(), &["worker".into()])
            .unwrap()
    }
    fn sql(&self) -> Connection {
        Connection::open(self.paths.database()).unwrap()
    }
    fn result_event(&self, task: &Task) -> i64 {
        self.store
            .append_inbound(&InboundEvent {
                conversation: "chat".into(),
                native_id: task.result_key(),
                author: "worker".into(),
                kind: "task_result".into(),
                body: "Synthetic task result is available.".into(),
                ..Default::default()
            })
            .unwrap()
            .seq
    }
}
fn spec() -> TaskSpec {
    TaskSpec {
        agent: "worker".into(),
        brief: "Compare synthetic notes A and B. 中文备注。".into(),
        done_when: "A short recommendation names its source notes.".into(),
        idempotency_key: "comparison".into(),
        model: None,
        effort: None,
    }
}
fn completed() -> AttemptOutcome {
    AttemptOutcome::Completed {
        result: "Synthetic result: choose A, based on notes A and B. 完成。".into(),
    }
}
fn snapshot(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn walk(root: &Path, current: &Path, result: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(current).unwrap() {
            let path = entry.unwrap().path();
            let metadata = fs::symlink_metadata(&path).unwrap();
            if metadata.is_dir() {
                walk(root, &path, result);
            } else {
                result.insert(
                    path.strip_prefix(root).unwrap().into(),
                    if metadata.file_type().is_symlink() {
                        fs::read_link(&path)
                            .unwrap()
                            .to_string_lossy()
                            .as_bytes()
                            .to_vec()
                    } else {
                        fs::read(path).unwrap()
                    },
                );
            }
        }
    }
    let mut result = BTreeMap::new();
    walk(root, root, &mut result);
    result
}

#[test]
fn admission_is_durable_idempotent_and_adds_only_one_ordinary_table() {
    let fixture = Fixture::new();
    let tasks = fixture.tasks();
    let task = fixture.create(&tasks);
    assert_eq!(task.state, TaskState::Queued);
    let files = snapshot(&fixture.paths.runtime.join("tasks"));
    assert_eq!(fixture.create(&tasks), task);
    assert_eq!(snapshot(&fixture.paths.runtime.join("tasks")), files);
    let mut changed = spec();
    changed.brief.push_str(" Changed payload.");
    assert!(tasks
        .create(&fixture.origin, &changed, &["worker".into()])
        .is_err());
    assert_eq!(snapshot(&fixture.paths.runtime.join("tasks")), files);
    drop(tasks);
    let reopened = fixture.tasks();
    assert_eq!(reopened.get(&task.id).unwrap(), task);
    assert_eq!(reopened.list(Some("chat"), 100).unwrap(), [task.clone()]);
    assert!(
        fs::read_to_string(reopened.directory(&task.id).unwrap().join("TASK.md"))
            .unwrap()
            .contains("中文备注")
    );
    let connection = fixture.sql();
    let mut statement = connection.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'events_fts%' AND name NOT LIKE 'sqlite_%' ORDER BY name").unwrap();
    let names = statement
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        names,
        ["conversations", "events", "outbox", "sessions", "tasks"]
    );
    assert_eq!(
        fixture.store.transcript("chat", 0, 100).unwrap().len(),
        1,
        "task store appended a conversation event"
    );
}

#[test]
fn a_task_keeps_its_chosen_model_and_effort_and_a_table_from_before_them_gains_the_columns() {
    let fixture = Fixture::new();
    let tasks = fixture.tasks();
    let plain = fixture.create(&tasks);
    assert_eq!((&plain.spec.model, &plain.spec.effort), (&None, &None));
    // A spec with no choice serializes as it did before there was one, so
    // the digests of earlier tasks, receipts and journal records still hold.
    assert_eq!(
        serde_json::to_string(&plain.spec).unwrap(),
        r#"{"agent":"worker","brief":"Compare synthetic notes A and B. 中文备注。","done_when":"A short recommendation names its source notes.","idempotency_key":"comparison"}"#
    );
    drop(tasks);
    // The table as a build from before the choice left it.
    fixture
        .sql()
        .execute_batch("ALTER TABLE tasks DROP COLUMN model; ALTER TABLE tasks DROP COLUMN effort;")
        .unwrap();
    let tasks = fixture.tasks();
    assert_eq!(tasks.get(&plain.id).unwrap(), plain);
    // Its digest is the one it was admitted under: the same spec replays.
    assert_eq!(fixture.create(&tasks), plain);
    let mut chosen = spec();
    chosen.idempotency_key = "hard-problem".into();
    chosen.model = Some("synthetic-model-2".into());
    chosen.effort = Some("max".into());
    let task = tasks
        .create(&fixture.origin, &chosen, &["worker".into()])
        .unwrap();
    assert_eq!(task.spec, chosen);
    drop(tasks);
    assert_eq!(fixture.tasks().get(&task.id).unwrap().spec, chosen);
    // The same key with another effort is changed work, not a replay.
    let tasks = fixture.tasks();
    let mut other = chosen.clone();
    other.effort = Some("low".into());
    assert!(tasks
        .create(&fixture.origin, &other, &["worker".into()])
        .is_err());
    for (model, effort) in [
        (Some("two words"), None),
        (Some(""), None),
        (Some("--add-dir"), None),
        (None, Some("extreme")),
    ] {
        let mut invalid = spec();
        invalid.idempotency_key = "invalid-choice".into();
        invalid.model = model.map(Into::into);
        invalid.effort = effort.map(Into::into);
        assert!(tasks
            .create(&fixture.origin, &invalid, &["worker".into()])
            .is_err());
    }
    assert_eq!(tasks.list(Some("chat"), 100).unwrap().len(), 2);
}

#[test]
fn invalid_origin_and_unregistered_agent_never_admit_work() {
    let fixture = Fixture::new();
    let tasks = fixture.tasks();
    fixture
        .store
        .create_conversation("remote", "synthetic-channel", "account", "remote", "policy")
        .unwrap();
    let remote = fixture
        .store
        .append_inbound(&InboundEvent {
            conversation: "remote".into(),
            native_id: "remote-1".into(),
            author: "synthetic-owner".into(),
            body: "Synthetic external text".into(),
            ..Default::default()
        })
        .unwrap();
    let non_owner = fixture
        .store
        .append_inbound(&InboundEvent {
            conversation: "chat".into(),
            native_id: "guest-1".into(),
            author: "synthetic-guest".into(),
            body: "Claiming to be the owner grants no authority.".into(),
            ..Default::default()
        })
        .unwrap();
    let synthetic = fixture
        .store
        .append_inbound(&InboundEvent {
            conversation: "chat".into(),
            native_id: "result-1".into(),
            author: "synthetic-owner".into(),
            kind: "task_result".into(),
            body: "Not an owner message.".into(),
            ..Default::default()
        })
        .unwrap();
    for origin in [
        TaskOrigin {
            requester: "synthetic-guest".into(),
            ..fixture.origin.clone()
        },
        TaskOrigin {
            conversation: "other-chat".into(),
            ..fixture.origin.clone()
        },
        TaskOrigin {
            input_seq: non_owner.seq,
            ..fixture.origin.clone()
        },
        TaskOrigin {
            input_seq: synthetic.seq,
            ..fixture.origin.clone()
        },
    ] {
        assert!(tasks.create(&origin, &spec(), &["worker".into()]).is_err());
    }
    assert!(tasks.create(&fixture.origin, &spec(), &[]).is_err());
    assert!(TaskStore::open(&fixture.paths, "synthetic-guest").is_err());
    assert!(tasks.list(None, 100).unwrap().is_empty());
    assert!(snapshot(&fixture.paths.runtime.join("tasks")).is_empty());
    // Authority follows the host-resolved author, not the surface: an owner
    // message on another channel account can originate a task.
    let remote_task = tasks
        .create(
            &TaskOrigin {
                conversation: "remote".into(),
                input_seq: remote.seq,
                ..fixture.origin.clone()
            },
            &spec(),
            &["worker".into()],
        )
        .unwrap();
    assert_eq!(remote_task.origin.conversation, "remote");
}

#[test]
fn independent_connections_compete_for_one_claim_and_reopen_never_reclaims_it() {
    let fixture = Fixture::new();
    let tasks = fixture.tasks();
    let task = fixture.create(&tasks);
    let barrier = Arc::new(Barrier::new(3));
    let handles: Vec<_> = (0..2)
        .map(|index| {
            let paths = fixture.paths.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let store = TaskStore::open(&paths, "synthetic-owner").unwrap();
                barrier.wait();
                store.claim_next(&format!("worker-{index}")).unwrap()
            })
        })
        .collect();
    barrier.wait();
    let claims: Vec<_> = handles
        .into_iter()
        .filter_map(|handle| handle.join().unwrap())
        .collect();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].generation, 1);
    assert_eq!(tasks.get(&task.id).unwrap().state, TaskState::Running);
    drop(tasks);
    assert!(fixture.tasks().claim_next("replacement").unwrap().is_none());
}

#[test]
fn completed_receipt_and_artifact_survive_reopen_without_a_second_completion() {
    let fixture = Fixture::new();
    let tasks = fixture.tasks();
    let task = fixture.create(&tasks);
    let claim = tasks.claim_next("worker-1").unwrap().unwrap();
    let observations = json!({"activity":"read","telemetry_complete":true,"source_events":[fixture.origin.input_seq]});
    let completed_task = tasks
        .finish(&claim, completed(), observations.clone())
        .unwrap();
    assert_eq!(completed_task.state, TaskState::Completed);
    let receipt = tasks.receipt(&task.id).unwrap().unwrap();
    assert_eq!(receipt.origin, fixture.origin);
    assert_eq!(receipt.attempt, claim.attempt);
    let directory = tasks.directory(&task.id).unwrap();
    assert!(fs::read_to_string(directory.join("artifacts/result.md"))
        .unwrap()
        .contains("完成"));
    let before = snapshot(&directory);
    assert_eq!(
        tasks.finish(&claim, completed(), observations).unwrap(),
        completed_task
    );
    assert_eq!(snapshot(&directory), before);
    assert!(tasks
        .finish(
            &claim,
            AttemptOutcome::Completed {
                result: "Different result".into()
            },
            json!({})
        )
        .is_err());
    assert_eq!(snapshot(&directory), before);
    drop(tasks);
    let reopened = fixture.tasks();
    assert_eq!(reopened.reconcile(&task.id).unwrap(), completed_task);
    assert!(reopened.claim_next("worker-2").unwrap().is_none());
    assert_eq!(
        reopened.pending_presentations("chat", 10).unwrap(),
        [completed_task]
    );
}

#[test]
fn stopped_queued_task_never_runs_and_active_stop_does_not_claim_termination() {
    let fixture = Fixture::new();
    let tasks = fixture.tasks();
    let task = fixture.create(&tasks);
    let stopped = tasks.request_stop(&task.id).unwrap();
    assert_eq!(stopped.state, TaskState::Stopped);
    assert!(tasks.claim_next("worker").unwrap().is_none());
    assert!(
        tasks.pending_presentations("chat", 10).unwrap().is_empty(),
        "a task stopped before any attempt has nothing to present"
    );
    assert!(tasks.observe_released_attempt(&task.id).unwrap().is_none());
    let mut second = spec();
    second.idempotency_key = "second".into();
    let active = tasks
        .create(&fixture.origin, &second, &["worker".into()])
        .unwrap();
    let claim = tasks.claim_next("worker").unwrap().unwrap();
    let requested = tasks.request_stop(&active.id).unwrap();
    assert_eq!(requested.state, TaskState::Running);
    assert!(requested.stop_requested);
    assert!(tasks.receipt(&active.id).unwrap().is_none());
    assert!(tasks
        .pending_presentations("chat", 10)
        .unwrap()
        .iter()
        .all(|task| task.id != active.id));
    let interrupted = tasks
        .mark_interrupted(
            &claim,
            "Host lost process ownership; no termination was observed.",
        )
        .unwrap();
    assert_eq!(interrupted.state, TaskState::Interrupted);
    assert!(tasks.request_stop(&active.id).unwrap().state == TaskState::Interrupted);
    assert!(tasks
        .resume(
            &active.id,
            ProcessStoppedEvidence {
                generation: claim.generation,
                diagnostic: "Process reaped.".into()
            }
        )
        .is_err());
    let ended = tasks
        .finish(&claim, completed(), json!({"process_group_reaped":true}))
        .unwrap();
    assert_eq!(
        ended.state,
        TaskState::Stopped,
        "late output after stop must not complete the task"
    );
    let receipt = tasks.receipt(&active.id).unwrap().unwrap();
    assert!(receipt.stop_requested);
    assert!(
        matches!(receipt.outcome, AttemptOutcome::Completed { .. }),
        "observed outcome was hidden"
    );
    assert!(!tasks
        .directory(&active.id)
        .unwrap()
        .join("artifacts/result.md")
        .exists());
}

#[test]
fn interrupted_work_needs_explicit_termination_evidence_and_fences_late_results() {
    let fixture = Fixture::new();
    let tasks = fixture.tasks();
    let task = fixture.create(&tasks);
    let claimed = tasks.claim_next("worker-old").unwrap().unwrap();
    let old = claimed.attempt.clone();
    let interrupted = tasks
        .mark_interrupted(
            &old,
            "Worker host disappeared; process ownership is unknown.",
        )
        .unwrap();
    assert_eq!(interrupted.state, TaskState::Interrupted);
    assert!(tasks.receipt(&task.id).unwrap().is_none());
    assert!(fixture
        .tasks()
        .claim_next("automatic-restart")
        .unwrap()
        .is_none());
    assert!(
        tasks
            .resume(
                &task.id,
                ProcessStoppedEvidence {
                    generation: old.generation,
                    diagnostic: "Claimed while its process still holds the lock.".into()
                }
            )
            .is_err(),
        "a held attempt lock refuses resumption regardless of supplied evidence"
    );
    drop(claimed);
    assert!(tasks
        .resume(
            &task.id,
            ProcessStoppedEvidence {
                generation: 99,
                diagnostic: "Unrelated process stopped.".into()
            }
        )
        .is_err());
    assert!(tasks
        .resume(
            &task.id,
            ProcessStoppedEvidence {
                generation: old.generation,
                diagnostic: "".into()
            }
        )
        .is_err());
    let queued = tasks
        .resume(
            &task.id,
            ProcessStoppedEvidence {
                generation: old.generation,
                diagnostic: "Synthetic host verified and reaped the owned process group.".into(),
            },
        )
        .unwrap();
    assert_eq!(queued.state, TaskState::Queued);
    assert!(queued.generation > old.generation);
    let before = snapshot(&tasks.directory(&task.id).unwrap());
    assert!(tasks.finish(&old, completed(), json!({})).is_err());
    assert_eq!(snapshot(&tasks.directory(&task.id).unwrap()), before);
    let current = tasks.claim_next("worker-new").unwrap().unwrap();
    assert!(current.generation > old.generation);
    assert_eq!(
        tasks
            .finish(&current, completed(), json!({}))
            .unwrap()
            .state,
        TaskState::Completed
    );
}

#[test]
fn immutable_receipt_repairs_lost_terminal_database_update_without_reexecution() {
    let fixture = Fixture::new();
    let tasks = fixture.tasks();
    let task = fixture.create(&tasks);
    let claim = tasks.claim_next("worker").unwrap().unwrap();
    fixture.sql().execute_batch("CREATE TRIGGER interrupt_completion BEFORE UPDATE OF state ON tasks WHEN NEW.state='completed' BEGIN SELECT RAISE(ABORT,'synthetic crash before terminal update'); END;").unwrap();
    assert!(tasks
        .finish(&claim, completed(), json!({"activity":"read"}))
        .is_err());
    assert_eq!(tasks.get(&task.id).unwrap().state, TaskState::Running);
    let receipt = tasks.receipt(&task.id).unwrap().unwrap();
    assert_eq!(receipt.outcome, completed());
    assert!(tasks.claim_next("unsafe-retry").unwrap().is_none());
    fixture
        .sql()
        .execute_batch("DROP TRIGGER interrupt_completion")
        .unwrap();
    drop(tasks);
    let reopened = fixture.tasks();
    let recovered = reopened.reconcile(&task.id).unwrap();
    assert_eq!(recovered.state, TaskState::Completed);
    assert_eq!(reopened.receipt(&task.id).unwrap().unwrap(), receipt);
    assert!(reopened.claim_next("replacement").unwrap().is_none());
    assert_eq!(recovered.generation, claim.generation);
}

#[test]
fn receipt_wins_over_interruption_or_stop_after_lost_terminal_update() {
    for stop in [false, true] {
        let fixture = Fixture::new();
        let tasks = fixture.tasks();
        let task = fixture.create(&tasks);
        let claim = tasks.claim_next("worker").unwrap().unwrap();
        fixture.sql().execute_batch("CREATE TRIGGER interrupt_completion BEFORE UPDATE OF state ON tasks WHEN NEW.state='completed' BEGIN SELECT RAISE(ABORT,'synthetic lost update'); END;").unwrap();
        assert!(tasks.finish(&claim, completed(), json!({})).is_err());
        fixture
            .sql()
            .execute_batch("DROP TRIGGER interrupt_completion")
            .unwrap();
        let recovered = if stop {
            tasks.request_stop(&task.id)
        } else {
            tasks.mark_interrupted(&claim, "Synthetic host lost ownership.")
        }
        .unwrap();
        assert_eq!(recovered.state, TaskState::Completed);
        assert!(
            !recovered.stop_requested,
            "a later stop rewrote completed evidence"
        );
    }
}

#[test]
fn presentation_requires_matching_result_and_delivery_receipt_and_never_reclaims_started_work() {
    let fixture = Fixture::new();
    let tasks = fixture.tasks();
    let task = fixture.create(&tasks);
    let claim = tasks.claim_next("worker").unwrap().unwrap();
    let terminal = tasks.finish(&claim, completed(), json!({})).unwrap();
    assert!(tasks
        .begin_presentation(&task.id, fixture.origin.input_seq)
        .is_err());
    let event_seq = fixture.result_event(&terminal);
    assert!(tasks.begin_presentation(&task.id, event_seq).unwrap());
    assert!(!fixture
        .tasks()
        .begin_presentation(&task.id, event_seq)
        .unwrap());
    assert!(tasks.pending_presentations("chat", 100).unwrap().is_empty());
    let wrong_reply = fixture
        .store
        .enqueue_reply(
            "chat",
            "unrelated",
            "Unrelated reply",
            vec![],
            Some(fixture.origin.input_seq.to_string()),
        )
        .unwrap();
    fixture.store.claim_send(wrong_reply.id).unwrap();
    fixture
        .store
        .finish_send(
            wrong_reply.id,
            DeliveryObservation::Sent {
                native_id: "wrong".into(),
            },
        )
        .unwrap();
    assert!(tasks
        .finish_presentation(&task.id, event_seq, Some(wrong_reply.event.seq))
        .is_err());
    let reply = fixture
        .store
        .enqueue_reply(
            "chat",
            "task-presentation",
            "Synthetic integrated task result",
            vec![],
            Some(event_seq.to_string()),
        )
        .unwrap();
    assert!(tasks
        .finish_presentation(&task.id, event_seq, Some(reply.event.seq))
        .is_err());
    assert!(tasks
        .finish_presentation(&task.id, event_seq, None)
        .is_err());
    fixture.store.claim_send(reply.id).unwrap();
    fixture
        .store
        .finish_send(
            reply.id,
            DeliveryObservation::Unknown {
                evidence: "Synthetic interrupted terminal write".into(),
            },
        )
        .unwrap();
    assert!(tasks
        .finish_presentation(&task.id, event_seq, Some(reply.event.seq))
        .is_err());
    fixture
        .store
        .mark_verified(
            reply.id,
            DeliveryObservation::Sent {
                native_id: "confirmed-task-reply".into(),
            },
        )
        .unwrap();
    let presented = tasks
        .finish_presentation(&task.id, event_seq, Some(reply.event.seq))
        .unwrap();
    assert_eq!(presented.presentation_state.as_deref(), Some("completed"));
    assert_eq!(
        tasks
            .finish_presentation(&task.id, event_seq, Some(reply.event.seq))
            .unwrap(),
        presented
    );
    assert_eq!(tasks.reconcile(&task.id).unwrap(), presented);
    assert!(tasks
        .finish_presentation(&task.id, event_seq, None)
        .is_err());
}

#[test]
fn silent_presentation_is_explicit_and_task_store_never_appends_messages() {
    let fixture = Fixture::new();
    let tasks = fixture.tasks();
    let task = fixture.create(&tasks);
    let claim = tasks.claim_next("worker").unwrap().unwrap();
    let terminal = tasks
        .finish(
            &claim,
            AttemptOutcome::Failed {
                diagnostic: "Synthetic unavailable provider; no model request was made.".into(),
            },
            json!({"activity":"none"}),
        )
        .unwrap();
    assert_eq!(fixture.store.transcript("chat", 0, 100).unwrap().len(), 1);
    let seq = fixture.result_event(&terminal);
    assert!(tasks.begin_presentation(&task.id, seq).unwrap());
    assert_eq!(
        tasks
            .finish_presentation(&task.id, seq, None)
            .unwrap()
            .presentation_state
            .as_deref(),
        Some("completed")
    );
    assert_eq!(fixture.store.transcript("chat", 0, 100).unwrap().len(), 2);
}

#[cfg(unix)]
#[test]
fn task_storage_rejects_directory_and_file_links_without_touching_targets() {
    use std::os::unix::fs::{symlink, PermissionsExt};
    for target in ["root", "evidence", "brief", "log", "receipt"] {
        let fixture = Fixture::new();
        let tasks = fixture.tasks();
        let foreign = fixture
            .paths
            .runtime
            .parent()
            .unwrap()
            .join("foreign-source");
        fs::create_dir_all(foreign.join(".git")).unwrap();
        fs::write(foreign.join("keep.txt"), "Synthetic unrelated source").unwrap();
        let task = fixture.create(&tasks);
        let directory = tasks.directory(&task.id).unwrap();
        let claim = tasks.claim_next("worker").unwrap().unwrap();
        let path = match target {
            "root" => fixture.paths.runtime.join("tasks"),
            "evidence" => directory.join("EVIDENCE"),
            "brief" => directory.join("TASK.md"),
            "log" => directory.join("LOG.md"),
            _ => directory.join("EVIDENCE/attempt-1.json"),
        };
        if path.is_dir() {
            fs::rename(&path, path.with_extension("saved")).unwrap();
        } else if path.exists() {
            fs::remove_file(&path).unwrap();
        }
        let link_target = if matches!(target, "root" | "evidence") {
            foreign.clone()
        } else {
            foreign.join("keep.txt")
        };
        symlink(&link_target, &path).unwrap();
        let before = snapshot(&foreign);
        assert!(
            tasks.finish(&claim, completed(), json!({})).is_err(),
            "accepted {target} link"
        );
        assert_eq!(snapshot(&foreign), before);
        assert_eq!(
            fs::metadata(foreign.join("keep.txt"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o644
        );
    }
}

#[cfg(unix)]
#[test]
fn evidence_files_are_private_and_reject_hardlinks_or_public_permissions() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    for hardlink in [false, true] {
        let fixture = Fixture::new();
        let tasks = fixture.tasks();
        let task = fixture.create(&tasks);
        let claim = tasks.claim_next("worker").unwrap().unwrap();
        tasks.finish(&claim, completed(), json!({})).unwrap();
        let directory = tasks.directory(&task.id).unwrap();
        for path in [
            directory.join("TASK.md"),
            directory.join("LOG.md"),
            directory.join("EVIDENCE/attempt-1.json"),
            directory.join("artifacts/result.md"),
        ] {
            let metadata = fs::metadata(path).unwrap();
            assert_eq!(metadata.mode() & 0o777, 0o600);
            assert_eq!(metadata.nlink(), 1);
        }
        let receipt = directory.join("EVIDENCE/attempt-1.json");
        if hardlink {
            fs::hard_link(&receipt, directory.join("copied-receipt.json")).unwrap();
        } else {
            fs::set_permissions(&receipt, fs::Permissions::from_mode(0o644)).unwrap();
        }
        let before = snapshot(&directory);
        assert!(tasks.reconcile(&task.id).is_err());
        assert_eq!(snapshot(&directory), before);
    }
}

#[test]
fn future_database_version_is_rejected_before_schema_or_directory_mutation() {
    let fixture = Fixture::new();
    fixture
        .sql()
        .execute_batch("PRAGMA user_version=4")
        .unwrap();
    assert!(TaskStore::open(&fixture.paths, "synthetic-owner").is_err());
    assert!(!fixture.paths.runtime.join("tasks").exists());
    let sql = fixture.sql();
    let version: i64 = sql
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, 4);
    let tasks_exist: bool = sql
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='tasks')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!tasks_exist);
}

#[test]
fn already_open_task_store_loses_authority_when_configured_owner_changes() {
    let fixture = Fixture::new();
    let tasks = fixture.tasks();
    let task = fixture.create(&tasks);
    let mut config = fixture.paths.load().unwrap();
    config.owner = "replacement-owner".into();
    fs::write(
        fixture.paths.config_file(),
        toml::to_string(&config).unwrap(),
    )
    .unwrap();
    let before = snapshot(&fixture.paths.runtime.join("tasks"));
    assert!(tasks
        .create(&fixture.origin, &spec(), &["worker".into()])
        .is_err());
    assert!(tasks.claim_next("old-owner-worker").is_err());
    assert!(tasks.request_stop(&task.id).is_err());
    assert!(tasks.get(&task.id).is_err());
    assert!(tasks.list(None, 100).is_err());
    assert_eq!(snapshot(&fixture.paths.runtime.join("tasks")), before);
    let state: String = fixture
        .sql()
        .query_row("SELECT state FROM tasks WHERE id=?1", [&task.id], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(state, "queued");
}

#[test]
fn long_append_only_log_does_not_block_receipt_recovery() {
    let fixture = Fixture::new();
    let tasks = fixture.tasks();
    let task = fixture.create(&tasks);
    let claim = tasks.claim_next("worker").unwrap().unwrap();
    let log = tasks.directory(&task.id).unwrap().join("LOG.md");
    let old_log = "Synthetic historical observation.\n".repeat(40_000);
    fs::write(&log, &old_log).unwrap();
    assert!(old_log.len() > 1024 * 1024);
    tasks.mark_interrupted(&claim, &"x".repeat(4096)).unwrap();
    fixture.sql().execute_batch("CREATE TRIGGER interrupt_completion BEFORE UPDATE OF state ON tasks WHEN NEW.state='completed' BEGIN SELECT RAISE(ABORT,'synthetic lost terminal update'); END;").unwrap();
    assert!(tasks.finish(&claim, completed(), json!({})).is_err());
    assert!(tasks.receipt(&task.id).unwrap().is_some());
    fixture
        .sql()
        .execute_batch("DROP TRIGGER interrupt_completion")
        .unwrap();
    assert_eq!(
        tasks.reconcile(&task.id).unwrap().state,
        TaskState::Completed
    );
    assert!(fs::read_to_string(log).unwrap().starts_with(&old_log));
}

#[test]
fn reopening_existing_task_storage_does_not_need_the_sqlite_writer_lock() {
    let fixture = Fixture::new();
    let tasks = fixture.tasks();
    let task = fixture.create(&tasks);
    let writer = fixture.sql();
    writer.execute_batch("BEGIN IMMEDIATE").unwrap();
    let started = std::time::Instant::now();
    let reopened = fixture.tasks();
    assert_eq!(reopened.get(&task.id).unwrap(), task);
    assert!(started.elapsed() < std::time::Duration::from_secs(1));
    writer.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn released_attempt_lock_is_the_only_proof_that_a_claimant_died() {
    let fixture = Fixture::new();
    let tasks = fixture.tasks();
    let task = fixture.create(&tasks);
    let claim = tasks.claim_next("worker").unwrap().unwrap();
    let observer = fixture.tasks();
    assert!(observer
        .observe_released_attempt(&task.id)
        .unwrap()
        .is_none());
    assert_eq!(
        observer.reconcile(&task.id).unwrap().state,
        TaskState::Running,
        "a held lock means the claimant is alive"
    );
    assert!(observer.claim_next("second-worker").unwrap().is_none());
    drop(claim);
    let evidence = observer
        .observe_released_attempt(&task.id)
        .unwrap()
        .unwrap();
    assert_eq!(evidence.generation, 1);
    let interrupted = observer.reconcile(&task.id).unwrap();
    assert_eq!(interrupted.state, TaskState::Interrupted);
    assert!(interrupted.diagnostic.unwrap().contains("attempt lock"));
    assert!(
        observer.claim_next("automatic").unwrap().is_none(),
        "interrupted work is never reclaimed without explicit resume"
    );
    let queued = observer.resume(&task.id, evidence).unwrap();
    assert_eq!((queued.state, queued.generation), (TaskState::Queued, 2));
    let again = tasks.claim_next("worker-2").unwrap().unwrap();
    assert_eq!(
        (again.task_id.as_str(), again.generation),
        (task.id.as_str(), 3),
        "resume and claim each advance the generation"
    );

    let mut second = spec();
    second.idempotency_key = "second".into();
    let stopped_task = tasks
        .create(&fixture.origin, &second, &["worker".into()])
        .unwrap();
    let claimed = tasks.claim_next("worker-3").unwrap().unwrap();
    assert_eq!(claimed.task_id, stopped_task.id);
    tasks.request_stop(&stopped_task.id).unwrap();
    assert_eq!(
        observer.reconcile(&stopped_task.id).unwrap().state,
        TaskState::Running
    );
    drop(claimed);
    let stopped = observer.reconcile(&stopped_task.id).unwrap();
    assert_eq!(
        (stopped.state, stopped.stop_requested),
        (TaskState::Stopped, true)
    );
    assert!(tasks.receipt(&stopped_task.id).unwrap().is_none());
    let proof = observer
        .observe_released_attempt(&stopped_task.id)
        .unwrap()
        .unwrap();
    assert!(observer.resume(&stopped_task.id, proof).is_err());
    let presented: Vec<_> = tasks
        .pending_presentations("chat", 10)
        .unwrap()
        .into_iter()
        .map(|task| task.id)
        .collect();
    assert_eq!(presented, vec![stopped_task.id.clone()]);
}

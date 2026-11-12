//! Durable, owner-local task admission and worker outcomes.
//! This module never launches a process or appends a conversation event. A claim
//! is not a lease timeout: the claiming process holds a kernel file lock for the
//! whole attempt, and only that lock being free proves the process is gone.
//! Interrupted work still needs explicit resumption. Workers must call `finish`
//! only after reaping their process group. All model/provider observations
//! remain private task evidence.
use crate::{
    config::{private_directory, reject_directory_links, Paths},
    store::OWNER_INPUT_KINDS,
};
use anyhow::{ensure, Context, Result};
use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension, Row, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const COLUMNS: &str = "id,requester,conversation,input_seq,agent,brief,done_when,idempotency_key,state,generation,worker,stop_requested,diagnostic,result_event_seq,presentation_state,reply_event_seq,created_at,updated_at,model,effort";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskOrigin {
    pub requester: String,
    pub conversation: String,
    pub input_seq: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskSpec {
    pub agent: String,
    pub brief: String,
    pub done_when: String,
    pub idempotency_key: String,
    /// The model for this task alone; the agent's own when absent. Left out
    /// of the serialized spec when absent, so earlier tasks keep their digests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// How hard the model thinks on this task alone; the agent's own when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Queued,
    Running,
    Completed,
    Failed,
    Stopped,
    Interrupted,
}
impl TaskState {
    fn parse(value: &str) -> rusqlite::Result<Self> {
        match value {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "stopped" => Ok(Self::Stopped),
            "interrupted" => Ok(Self::Interrupted),
            _ => Err(rusqlite::Error::InvalidQuery),
        }
    }
    fn terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Stopped)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    pub origin: TaskOrigin,
    pub spec: TaskSpec,
    pub state: TaskState,
    pub generation: i64,
    pub worker: Option<String>,
    pub stop_requested: bool,
    pub diagnostic: Option<String>,
    pub result_event_seq: Option<i64>,
    /// None, "started", or "completed". Started presentation is never reclaimed.
    pub presentation_state: Option<String>,
    pub reply_event_seq: Option<i64>,
    pub created_at: String,
    pub updated_at: String,
}
impl Task {
    pub fn result_key(&self) -> String {
        format!("task:{}:{}:result", self.id, self.generation)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskAttempt {
    pub task_id: String,
    pub generation: i64,
    pub worker: String,
}

/// A claimed attempt together with its held lock. Keep it alive for the whole
/// attempt: the kernel releases the lock when the claiming process ends, and a
/// reconciler that can then take the lock has proof that the worker is gone.
#[derive(Debug)]
pub struct ClaimedAttempt {
    pub attempt: TaskAttempt,
    _lock: File,
}
impl std::ops::Deref for ClaimedAttempt {
    type Target = TaskAttempt;
    fn deref(&self) -> &TaskAttempt {
        &self.attempt
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum AttemptOutcome {
    Completed { result: String },
    Failed { diagnostic: String },
    Stopped { diagnostic: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptReceipt {
    pub attempt: TaskAttempt,
    pub origin: TaskOrigin,
    pub spec_digest: String,
    pub finished_at: String,
    pub stop_requested: bool,
    pub outcome: AttemptOutcome,
    pub observations: Value,
}

/// Host-observed termination evidence, never accepted from model tool arguments.
/// The caller must verify process ownership and termination; a timestamp, missing
/// heartbeat or expired grant alone is not sufficient evidence. A released
/// attempt lock is sufficient: see `TaskStore::observe_released_attempt`.
#[derive(Clone, Debug)]
pub struct ProcessStoppedEvidence {
    pub generation: i64,
    pub diagnostic: String,
}

pub struct TaskStore {
    connection: Connection,
    paths: Paths,
    directory: PathBuf,
    owner: String,
}

impl TaskStore {
    pub fn open(paths: &Paths, owner: &str) -> Result<Self> {
        ensure!(
            paths.load()?.owner == owner,
            "task owner does not match configuration"
        );
        reject_directory_links(&paths.runtime)?;
        regular_file(&paths.database(), false)?;
        // The state store owns schema migration; a task command may run first.
        crate::store::Store::open(&paths.database())?;
        let directory = paths.runtime.join("tasks");
        let connection = Connection::open(paths.database())?;
        connection.busy_timeout(Duration::from_secs(5))?;
        let version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        ensure!(
            version == crate::store::SCHEMA_VERSION,
            "unsupported task database version"
        );
        connection.execute_batch("PRAGMA foreign_keys=ON; PRAGMA synchronous=FULL;")?;
        let journal: String = connection.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
        ensure!(
            journal.eq_ignore_ascii_case("wal"),
            "task database must use WAL"
        );
        private_directory(&directory)?;
        let exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='tasks')",
            [],
            |row| row.get(0),
        )?;
        if !exists {
            connection.query_row("SELECT COUNT(*) FROM conversations", [], |_| Ok(()))?;
            connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS tasks (
            id TEXT PRIMARY KEY, requester TEXT NOT NULL,
            conversation TEXT NOT NULL REFERENCES conversations(id),
            input_seq INTEGER NOT NULL REFERENCES events(seq),
            agent TEXT NOT NULL, brief TEXT NOT NULL, done_when TEXT NOT NULL,
            idempotency_key TEXT NOT NULL, payload_digest TEXT NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('queued','running','completed','failed','stopped','interrupted')),
            generation INTEGER NOT NULL DEFAULT 0 CHECK(generation>=0), worker TEXT,
            stop_requested INTEGER NOT NULL DEFAULT 0 CHECK(stop_requested IN (0,1)), diagnostic TEXT,
            result_event_seq INTEGER REFERENCES events(seq),
            presentation_state TEXT CHECK(presentation_state IN ('started','completed')),
            reply_event_seq INTEGER REFERENCES events(seq),
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL, model TEXT, effort TEXT,
            UNIQUE(requester,conversation,input_seq,idempotency_key));
            CREATE INDEX IF NOT EXISTS tasks_queue ON tasks(state,created_at,id);",
        )?;
        }
        // A table from before a task could choose its model and effort. The
        // columns are added in place: a build from before them names its
        // columns, so it still reads the table if an upgrade is put back.
        let chooses = || {
            connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_table_info('tasks') WHERE name='model')",
                [],
                |row| row.get::<_, bool>(0),
            )
        };
        if !chooses()? {
            // Serve and work open the store at once: look again under the
            // write lock, so only one of them adds the columns.
            let tx = Transaction::new_unchecked(&connection, TransactionBehavior::Immediate)?;
            if !chooses()? {
                tx.execute_batch(
                    "ALTER TABLE tasks ADD COLUMN model TEXT;
                    ALTER TABLE tasks ADD COLUMN effort TEXT;",
                )?;
            }
            tx.commit()?;
        }
        Ok(Self {
            connection,
            paths: paths.clone(),
            directory,
            owner: owner.into(),
        })
    }

    /// `eligible_agents` is the host's registered/filtered list, not model input.
    pub fn create(
        &self,
        origin: &TaskOrigin,
        spec: &TaskSpec,
        eligible_agents: &[String],
    ) -> Result<Task> {
        self.validate_paths()?;
        bounded(&spec.agent, 128)?;
        bounded(&spec.idempotency_key, 256)?;
        bounded(&spec.brief, 64 * 1024)?;
        bounded(&spec.done_when, 8 * 1024)?;
        ensure!(
            spec.model.as_deref().is_none_or(crate::config::valid_model)
                && spec
                    .effort
                    .as_deref()
                    .is_none_or(|effort| crate::config::EFFORTS.contains(&effort)),
            "task model or effort is not valid"
        );
        ensure!(
            eligible_agents.contains(&spec.agent),
            "task agent is not eligible"
        );
        let tx = self.transaction()?;
        self.validate_origin(&tx, origin)?;
        let id = digest(&(origin, &spec.idempotency_key))?;
        let payload = digest(&(origin, spec))?;
        let previous: Option<String> = tx
            .query_row(
                "SELECT payload_digest FROM tasks WHERE id=?1",
                [&id],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(previous) = previous {
            ensure!(
                previous == payload,
                "task idempotency key was reused with changed payload"
            );
        } else {
            let now = now();
            tx.execute("INSERT INTO tasks(id,requester,conversation,input_seq,agent,brief,done_when,idempotency_key,payload_digest,state,created_at,updated_at,model,effort) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,'queued',?10,?10,?11,?12)", params![id,origin.requester,origin.conversation,origin.input_seq,spec.agent,spec.brief,spec.done_when,spec.idempotency_key,payload,now,spec.model,spec.effort])?;
        }
        let task = self.load(&tx, &id)?;
        self.board(&task)?;
        tx.commit()?;
        Ok(task)
    }

    pub fn get(&self, id: &str) -> Result<Task> {
        self.validate_paths()?;
        self.load(&self.connection, id)
    }

    pub fn list(&self, conversation: Option<&str>, limit: usize) -> Result<Vec<Task>> {
        ensure!(
            (1..=100).contains(&limit),
            "task page limit must be 1..=100"
        );
        self.validate_paths()?;
        let mut statement = self.connection.prepare(&format!("SELECT {COLUMNS} FROM tasks WHERE requester=?1 AND (?2 IS NULL OR conversation=?2) ORDER BY created_at DESC,id LIMIT ?3"))?;
        let tasks = statement
            .query_map(params![self.owner, conversation, limit], task_row)?
            .collect::<rusqlite::Result<_>>()?;
        Ok(tasks)
    }

    pub fn pending_presentations(&self, conversation: &str, limit: usize) -> Result<Vec<Task>> {
        ensure!(
            (1..=100).contains(&limit),
            "task page limit must be 1..=100"
        );
        self.validate_paths()?;
        // A task stopped before any attempt ran has no outcome to present; the
        // stop itself was already answered in the conversation that requested it.
        let mut statement = self.connection.prepare(&format!("SELECT {COLUMNS} FROM tasks WHERE requester=?1 AND conversation=?2 AND state IN ('completed','failed','stopped') AND NOT (state='stopped' AND generation=0) AND result_event_seq IS NULL ORDER BY created_at,id LIMIT ?3"))?;
        let tasks = statement
            .query_map(params![self.owner, conversation, limit], task_row)?
            .collect::<rusqlite::Result<_>>()?;
        Ok(tasks)
    }

    pub fn claim_next(&self, worker: &str) -> Result<Option<ClaimedAttempt>> {
        bounded(worker, 256)?;
        self.validate_paths()?;
        let tx = self.transaction()?;
        let id: Option<String> = tx.query_row("SELECT id FROM tasks WHERE requester=?1 AND state='queued' AND stop_requested=0 ORDER BY created_at,id LIMIT 1", [&self.owner], |row| row.get(0)).optional()?;
        let Some(id) = id else {
            return Ok(None);
        };
        let task = self.load(&tx, &id)?;
        self.validate_origin(&tx, &task.origin)?;
        // The lock is taken before the row says running, so any later reader
        // that finds the row running and the lock free knows the claimant died.
        let lock = self
            .try_attempt_lock(&task)?
            .context("queued task attempt lock is held by another process")?;
        tx.execute("UPDATE tasks SET state='running',generation=generation+1,worker=?2,updated_at=?3 WHERE id=?1 AND state='queued'", params![id,worker,now()])?;
        let task = self.load(&tx, &id)?;
        self.board(&task)?;
        tx.commit()?;
        Ok(Some(ClaimedAttempt {
            attempt: attempt(&task)?,
            _lock: lock,
        }))
    }

    /// Host proof that no process holds the current attempt: the kernel released
    /// its lock. Returns None while a claimant is alive or nothing was claimed.
    pub fn observe_released_attempt(&self, id: &str) -> Result<Option<ProcessStoppedEvidence>> {
        let task = self.get(id)?;
        if task.generation == 0 || task.worker.is_none() {
            return Ok(None);
        }
        Ok(self.try_attempt_lock(&task)?.map(|_lock| ProcessStoppedEvidence {
            generation: task.generation,
            diagnostic: format!(
                "The attempt lock for generation {} was free; the worker process that claimed it no longer exists.",
                task.generation
            ),
        }))
    }

    /// A stop request for an active/uncertain process does not prove termination.
    pub fn request_stop(&self, id: &str) -> Result<Task> {
        self.validate_paths()?;
        let tx = self.transaction()?;
        let mut task = self.load(&tx, id)?;
        if let Some(receipt) = self.read_receipt(&task)? {
            self.apply_receipt(&tx, &task, &receipt)?;
            task = self.load(&tx, id)?;
        }
        if !task.state.terminal() && !task.stop_requested {
            let state = if task.state == TaskState::Queued {
                "stopped"
            } else if task.state == TaskState::Running {
                "running"
            } else {
                "interrupted"
            };
            tx.execute(
                "UPDATE tasks SET stop_requested=1,state=?2,updated_at=?3 WHERE id=?1",
                params![id, state, now()],
            )?;
        }
        let task = self.load(&tx, id)?;
        self.board(&task)?;
        tx.commit()?;
        Ok(task)
    }

    /// Persist immutable evidence before changing SQLite. If that transaction is
    /// lost, `reconcile` finishes it using the receipt without rerunning a model.
    /// The host must have observed process-group termination before calling this.
    pub fn finish(
        &self,
        claim: &TaskAttempt,
        outcome: AttemptOutcome,
        observations: Value,
    ) -> Result<Task> {
        match &outcome {
            AttemptOutcome::Completed { result } => bounded(result, 256 * 1024)?,
            AttemptOutcome::Failed { diagnostic } | AttemptOutcome::Stopped { diagnostic } => {
                bounded(diagnostic, 4096)?
            }
        }
        ensure!(
            observations.is_object() && serde_json::to_vec(&observations)?.len() <= 128 * 1024,
            "task observations exceed bounds"
        );
        self.validate_paths()?;
        let tx = self.transaction()?;
        let task = self.load(&tx, &claim.task_id)?;
        validate_attempt(&task, claim)?;
        let receipt = if let Some(receipt) = self.read_receipt(&task)? {
            ensure!(
                receipt.outcome == outcome && receipt.observations == observations,
                "attempt already has a different immutable receipt"
            );
            receipt
        } else {
            ensure!(
                matches!(task.state, TaskState::Running | TaskState::Interrupted),
                "task is not active"
            );
            let receipt = AttemptReceipt {
                attempt: claim.clone(),
                origin: task.origin.clone(),
                spec_digest: digest(&task.spec)?,
                finished_at: now(),
                stop_requested: task.stop_requested,
                outcome,
                observations,
            };
            immutable(
                &self.receipt_path(&task),
                &serde_json::to_vec_pretty(&receipt)?,
            )?;
            receipt
        };
        self.apply_receipt(&tx, &task, &receipt)?;
        let task = self.load(&tx, &claim.task_id)?;
        self.board(&task)?;
        tx.commit()?;
        Ok(task)
    }

    /// Applies a persisted receipt, or records a dead claimant. A running row
    /// whose attempt lock can be taken has lost its process; it becomes stopped
    /// when a stop was requested, otherwise interrupted. Nothing is rerun.
    pub fn reconcile(&self, id: &str) -> Result<Task> {
        self.validate_paths()?;
        let tx = self.transaction()?;
        let task = self.load(&tx, id)?;
        if let Some(receipt) = self.read_receipt(&task)? {
            self.apply_receipt(&tx, &task, &receipt)?;
        } else if task.state == TaskState::Running {
            if let Some(_lock) = self.try_attempt_lock(&task)? {
                let (state, diagnostic) = if task.stop_requested {
                    ("stopped", "Worker process released its attempt lock without a receipt after a stop request; no process remains for this attempt.")
                } else {
                    ("interrupted", "Worker process released its attempt lock without a receipt; inspect execution evidence before resuming.")
                };
                tx.execute("UPDATE tasks SET state=?2,diagnostic=?3,updated_at=?4 WHERE id=?1 AND generation=?5", params![task.id,state,diagnostic,now(),task.generation])?;
            }
        }
        let task = self.load(&tx, id)?;
        self.board(&task)?;
        tx.commit()?;
        Ok(task)
    }

    pub fn receipt(&self, id: &str) -> Result<Option<AttemptReceipt>> {
        self.read_receipt(&self.get(id)?)
    }

    pub fn mark_interrupted(&self, claim: &TaskAttempt, diagnostic: &str) -> Result<Task> {
        bounded(diagnostic, 4096)?;
        self.validate_paths()?;
        let tx = self.transaction()?;
        let task = self.load(&tx, &claim.task_id)?;
        validate_attempt(&task, claim)?;
        if let Some(receipt) = self.read_receipt(&task)? {
            self.apply_receipt(&tx, &task, &receipt)?;
        } else {
            ensure!(
                matches!(task.state, TaskState::Running | TaskState::Interrupted),
                "task is not active"
            );
            tx.execute(
                "UPDATE tasks SET state='interrupted',diagnostic=?2,updated_at=?3 WHERE id=?1",
                params![task.id, diagnostic, now()],
            )?;
        }
        let task = self.load(&tx, &task.id)?;
        self.board(&task)?;
        tx.commit()?;
        Ok(task)
    }

    /// Explicit host recovery only. This does not infer liveness or reap a PID.
    pub fn resume(&self, id: &str, evidence: ProcessStoppedEvidence) -> Result<Task> {
        bounded(&evidence.diagnostic, 4096)?;
        self.validate_paths()?;
        let tx = self.transaction()?;
        let mut task = self.load(&tx, id)?;
        ensure!(
            task.generation == evidence.generation,
            "termination evidence is for another generation"
        );
        if let Some(receipt) = self.read_receipt(&task)? {
            self.apply_receipt(&tx, &task, &receipt)?;
            task = self.load(&tx, id)?;
        }
        ensure!(
            matches!(task.state, TaskState::Interrupted | TaskState::Failed),
            "only interrupted or failed tasks can resume"
        );
        ensure!(
            !task.stop_requested && task.presentation_state.as_deref() != Some("started"),
            "task is stopped or has an unresolved presentation"
        );
        // Caller-supplied evidence never overrides a lock that is still held.
        let _lock = self
            .try_attempt_lock(&task)?
            .context("attempt lock is still held; a claimant process may be alive")?;
        let evidence_path = self
            .task_path(&task)
            .join("EVIDENCE")
            .join(format!("resume-{}.json", task.generation));
        immutable(
            &evidence_path,
            &serde_json::to_vec_pretty(
                &json!({"generation":evidence.generation,"termination":evidence.diagnostic}),
            )?,
        )?;
        tx.execute("UPDATE tasks SET state='queued',generation=generation+1,worker=NULL,diagnostic=NULL,result_event_seq=NULL,presentation_state=NULL,reply_event_seq=NULL,updated_at=?2 WHERE id=?1", params![id,now()])?;
        let task = self.load(&tx, id)?;
        self.board(&task)?;
        tx.commit()?;
        Ok(task)
    }

    /// Caller holds the conversation lease and has appended the unique canonical
    /// task_result event. A crashed started presentation is inspectable, not retried.
    pub fn begin_presentation(&self, id: &str, event_seq: i64) -> Result<bool> {
        self.validate_paths()?;
        let tx = self.transaction()?;
        let task = self.load(&tx, id)?;
        ensure!(
            task.state.terminal(),
            "task has no terminal result to present"
        );
        let valid: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM events WHERE seq=?1 AND conversation=?2 AND direction='in' AND kind='task_result' AND native_id=?3)", params![event_seq,task.origin.conversation,task.result_key()], |row| row.get(0))?;
        ensure!(valid, "presentation event does not match this task");
        if let Some(previous) = task.result_event_seq {
            ensure!(
                previous == event_seq,
                "task is bound to another result event"
            );
            return Ok(false);
        }
        tx.execute("UPDATE tasks SET result_event_seq=?2,presentation_state='started',updated_at=?3 WHERE id=?1 AND result_event_seq IS NULL", params![id,event_seq,now()])?;
        tx.commit()?;
        Ok(true)
    }

    pub fn finish_presentation(
        &self,
        id: &str,
        event_seq: i64,
        reply_seq: Option<i64>,
    ) -> Result<Task> {
        self.validate_paths()?;
        let tx = self.transaction()?;
        let task = self.load(&tx, id)?;
        ensure!(
            task.result_event_seq == Some(event_seq),
            "presentation was not claimed for this event"
        );
        if task.presentation_state.as_deref() == Some("completed") {
            ensure!(
                task.reply_event_seq == reply_seq,
                "presentation already has another outcome"
            );
            return Ok(task);
        }
        if let Some(reply) = reply_seq {
            let valid: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM events e JOIN outbox o ON o.event_seq=e.seq WHERE e.seq=?1 AND e.conversation=?2 AND e.direction='out' AND e.reply_to=?3 AND e.seq>?4 AND o.state='sent')", params![reply,task.origin.conversation,event_seq.to_string(),event_seq], |row| row.get(0))?;
            ensure!(
                valid,
                "presentation reply has no delivery receipt in this conversation"
            );
        } else {
            let exists: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM events WHERE conversation=?1 AND direction='out' AND reply_to=?2)", params![task.origin.conversation,event_seq.to_string()], |row| row.get(0))?;
            ensure!(
                !exists,
                "silent presentation cannot conceal an outbound intent"
            );
        }
        tx.execute("UPDATE tasks SET presentation_state='completed',reply_event_seq=?2,updated_at=?3 WHERE id=?1", params![id,reply_seq,now()])?;
        let task = self.load(&tx, id)?;
        tx.commit()?;
        Ok(task)
    }

    pub fn directory(&self, id: &str) -> Result<PathBuf> {
        Ok(self.task_path(&self.get(id)?))
    }

    fn transaction(&self) -> Result<Transaction<'_>> {
        Ok(Transaction::new_unchecked(
            &self.connection,
            TransactionBehavior::Immediate,
        )?)
    }

    fn load(&self, connection: &Connection, id: &str) -> Result<Task> {
        bounded(id, 64)?;
        ensure!(
            id.len() == 64 && id.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "invalid task identifier"
        );
        Ok(connection.query_row(
            &format!("SELECT {COLUMNS} FROM tasks WHERE id=?1 AND requester=?2"),
            params![id, self.owner],
            task_row,
        )?)
    }

    fn validate_origin(&self, connection: &Connection, origin: &TaskOrigin) -> Result<()> {
        ensure!(
            origin.requester == self.owner && origin.input_seq > 0,
            "task requester is not the local owner"
        );
        let kind: Option<String> = connection.query_row("SELECT e.kind FROM events e JOIN conversations c ON c.id=e.conversation WHERE e.seq=?1 AND e.conversation=?2 AND e.author=?3 AND e.direction='in'", params![origin.input_seq,origin.conversation,origin.requester], |row| row.get(0)).optional()?;
        ensure!(
            kind.is_some_and(|kind| OWNER_INPUT_KINDS.contains(&kind.as_str())),
            "task origin is not an authenticated owner input"
        );
        Ok(())
    }

    fn validate_paths(&self) -> Result<()> {
        ensure!(
            self.paths.load()?.owner == self.owner,
            "configured task owner changed"
        );
        regular_file(&self.paths.database(), false)?;
        reject_directory_links(&self.directory)?;
        ensure!(self.directory.is_dir(), "task directory is missing");
        Ok(())
    }

    fn task_path(&self, task: &Task) -> PathBuf {
        self.directory.join(&task.id)
    }
    fn receipt_path(&self, task: &Task) -> PathBuf {
        self.task_path(task)
            .join("EVIDENCE")
            .join(format!("attempt-{}.json", task.generation))
    }

    /// Exclusive, non-blocking lock on the task's attempt file. The descriptor
    /// is close-on-exec, so adapter children never inherit the proof.
    fn try_attempt_lock(&self, task: &Task) -> Result<Option<File>> {
        let directory = self.task_path(task);
        private_directory(&directory)?;
        let path = directory.join("attempt.lock");
        let file = private_options()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        regular_file(&path, true)?;
        match file.try_lock() {
            Ok(()) => Ok(Some(file)),
            Err(std::fs::TryLockError::WouldBlock) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn read_receipt(&self, task: &Task) -> Result<Option<AttemptReceipt>> {
        let path = self.receipt_path(task);
        reject_directory_links(path.parent().unwrap())?;
        if !path.try_exists()? {
            // try_exists follows links; a dangling link is still an invalid record.
            ensure!(
                fs::symlink_metadata(&path).is_err(),
                "invalid task receipt link"
            );
            return Ok(None);
        }
        let bytes = private_read(&path, 1024 * 1024)?;
        let receipt: AttemptReceipt = serde_json::from_slice(&bytes)?;
        ensure!(
            receipt.attempt == attempt(task)?
                && receipt.origin == task.origin
                && receipt.spec_digest == digest(&task.spec)?,
            "task receipt binding changed"
        );
        Ok(Some(receipt))
    }

    fn apply_receipt(
        &self,
        tx: &Transaction<'_>,
        task: &Task,
        receipt: &AttemptReceipt,
    ) -> Result<()> {
        let state = if receipt.stop_requested {
            "stopped"
        } else {
            match receipt.outcome {
                AttemptOutcome::Completed { .. } => "completed",
                AttemptOutcome::Failed { .. } => "failed",
                AttemptOutcome::Stopped { .. } => "stopped",
            }
        };
        let diagnostic = match &receipt.outcome {
            AttemptOutcome::Failed { diagnostic } | AttemptOutcome::Stopped { diagnostic } => {
                Some(diagnostic.as_str())
            }
            _ => None,
        };
        if let AttemptOutcome::Completed { result } = &receipt.outcome {
            let artifacts = self.task_path(task).join("artifacts");
            private_directory(&artifacts)?;
            immutable(
                &artifacts.join(format!("attempt-{}.md", task.generation)),
                result.as_bytes(),
            )?;
            if state == "completed" {
                immutable(&artifacts.join("result.md"), result.as_bytes())?;
            }
        }
        if task.state.terminal() {
            ensure!(
                task.state == TaskState::parse(state)?
                    && task.stop_requested == receipt.stop_requested,
                "terminal task disagrees with receipt"
            );
            return Ok(());
        }
        tx.execute("UPDATE tasks SET state=?2,stop_requested=?3,diagnostic=?4,updated_at=?5 WHERE id=?1 AND generation=?6", params![task.id,state,receipt.stop_requested,diagnostic,receipt.finished_at,task.generation])?;
        Ok(())
    }

    fn board(&self, task: &Task) -> Result<()> {
        let directory = self.task_path(task);
        for path in [
            &directory,
            &directory.join("EVIDENCE"),
            &directory.join("artifacts"),
        ] {
            private_directory(path)?;
        }
        let brief = format!("# Task\n\nRequester: {}\nConversation: {}\nSource event: {}\nAgent: {}\n\n## Brief\n\n{}\n\n## Done when\n\n{}\n", task.origin.requester,task.origin.conversation,task.origin.input_seq,task.spec.agent,task.spec.brief,task.spec.done_when);
        immutable(&directory.join("TASK.md"), brief.as_bytes())?;
        let log = directory.join("LOG.md");
        let line = format!(
            "{}\n",
            json!({"at":task.updated_at,"state":task.state,"generation":task.generation,"stop_requested":task.stop_requested,"diagnostic":task.diagnostic})
        );
        let existed = log.try_exists()?;
        let previous = if existed {
            regular_file(&log, true)?;
            let mut file = private_options().read(true).open(&log)?;
            file.seek(SeekFrom::End(-(file.metadata()?.len().min(32768) as i64)))?;
            let mut tail = Vec::new();
            file.take(32768).read_to_end(&mut tail)?;
            tail
        } else {
            Vec::new()
        };
        if previous.ends_with(line.as_bytes()) {
            return Ok(());
        }
        let mut options = private_options();
        let mut file = options.append(true).create(true).open(&log)?;
        file.write_all(line.as_bytes())?;
        file.sync_all()?;
        if !existed {
            File::open(directory)?.sync_all()?;
        }
        Ok(())
    }
}

fn task_row(row: &Row<'_>) -> rusqlite::Result<Task> {
    Ok(Task {
        id: row.get(0)?,
        origin: TaskOrigin {
            requester: row.get(1)?,
            conversation: row.get(2)?,
            input_seq: row.get(3)?,
        },
        spec: TaskSpec {
            agent: row.get(4)?,
            brief: row.get(5)?,
            done_when: row.get(6)?,
            idempotency_key: row.get(7)?,
            model: row.get(18)?,
            effort: row.get(19)?,
        },
        state: TaskState::parse(&row.get::<_, String>(8)?)?,
        generation: row.get(9)?,
        worker: row.get(10)?,
        stop_requested: row.get(11)?,
        diagnostic: row.get(12)?,
        result_event_seq: row.get(13)?,
        presentation_state: row.get(14)?,
        reply_event_seq: row.get(15)?,
        created_at: row.get(16)?,
        updated_at: row.get(17)?,
    })
}
fn attempt(task: &Task) -> Result<TaskAttempt> {
    Ok(TaskAttempt {
        task_id: task.id.clone(),
        generation: task.generation,
        worker: task.worker.clone().context("task has no worker attempt")?,
    })
}
fn validate_attempt(task: &Task, claim: &TaskAttempt) -> Result<()> {
    ensure!(attempt(task)? == *claim, "stale or foreign task attempt");
    Ok(())
}
fn now() -> String {
    Utc::now().to_rfc3339()
}
fn bounded(value: &str, maximum: usize) -> Result<()> {
    ensure!(
        !value.trim().is_empty() && value.len() <= maximum && !value.contains('\0'),
        "task text is empty or exceeds bounds"
    );
    Ok(())
}
fn digest(value: &impl Serialize) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(value)?)))
}
fn regular_file(path: &Path, private: bool) -> Result<()> {
    reject_directory_links(path.parent().context("file needs parent")?)?;
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "expected a regular task file"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        ensure!(
            metadata.nlink() == 1 && metadata.uid() == unsafe { libc::geteuid() },
            "task file has foreign ownership or multiple links"
        );
        ensure!(
            !private || metadata.permissions().mode() & 0o077 == 0,
            "task file is not private"
        );
    }
    Ok(())
}
fn private_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    options
}
fn private_read(path: &Path, limit: usize) -> Result<Vec<u8>> {
    regular_file(path, true)?;
    ensure!(
        fs::metadata(path)?.len() <= limit as u64,
        "task file exceeds read limit"
    );
    let mut bytes = Vec::new();
    private_options()
        .read(true)
        .open(path)?
        .take(limit as u64 + 1)
        .read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= limit, "task file exceeds read limit");
    Ok(bytes)
}
fn immutable(path: &Path, bytes: &[u8]) -> Result<()> {
    private_directory(path.parent().context("task file needs parent")?)?;
    if fs::symlink_metadata(path).is_ok() {
        ensure!(
            private_read(path, 1024 * 1024)? == bytes,
            "immutable task evidence changed"
        );
        return Ok(());
    }
    let temporary = path.with_extension(format!(
        "{}-{}.tmp",
        std::process::id(),
        TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let result = (|| -> Result<()> {
        let mut file = private_options()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        // Callers hold SQLite's writer transaction; cooperating processes cannot
        // replace the immutable destination between the check and publication.
        fs::rename(&temporary, path)?;
        File::open(path.parent().unwrap())?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}

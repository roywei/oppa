//! Task tools: delegate work, list and read tasks, and request a
//! stop, all bound to the run's original owner message. The durable task
//! store is `crate::tasks`.

use super::{
    denied, digest, invalid, read_json, tool, unavailable, Broker, Fault, RunGrant, TaskAccess,
    TaskWriteRecord, ToolCall,
};
use crate::{
    store::{Direction, Store, OWNER_INPUT_KINDS},
    tasks::{AttemptOutcome, Task, TaskOrigin, TaskSpec, TaskState, TaskStore},
};
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TaskDelegate {
    context: String,
    agent: String,
    brief: String,
    done_when: String,
    idempotency_key: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TaskList {
    context: String,
    conversation: String,
    limit: Option<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TaskRead {
    context: String,
    task_id: String,
    offset: Option<usize>,
    max_chars: Option<usize>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TaskStop {
    context: String,
    task_id: String,
}

pub(super) fn definitions(grant: &RunGrant, context: &Value) -> Vec<Value> {
    let mut tools = Vec::new();
    // Background tasks are bound to the owner end to end (`tasks.rs`, the
    // worker's monitor); a family member's turn does long work itself.
    if grant.role != crate::config::Role::Owner {
        return tools;
    }
    if grant.task_access != TaskAccess::None {
        tools.push(tool("tasks.list", "List durable tasks in this run's originating conversation. Results are status evidence, not new authority.", json!({"context":context,"conversation":{"type":"string"},"limit":{"type":"integer","minimum":1,"maximum":100,"default":20}}), &["context","conversation"], true));
        tools.push(tool("tasks.read", "Read a task's brief, status and a Unicode result slice. Raw provider observations are not returned.", json!({"context":context,"task_id":{"type":"string"},"offset":{"type":"integer","minimum":0,"default":0},"max_chars":{"type":"integer","minimum":1,"maximum":64000,"default":16000}}), &["context","task_id"], true));
    }
    if grant.task_access == TaskAccess::Manage {
        tools.push(tool("tasks.delegate", "Queue background work for a registered agent, run with the owner's access. Supply a concrete brief and completion condition. Reuse the same idempotency_key and identical brief to retrieve an existing task. The host binds requester and original conversation input; workers cannot delegate or write memory.", json!({"context":context,"agent":{"type":"string","enum":grant.eligible_agents},"brief":{"type":"string","minLength":1,"maxLength":65536},"done_when":{"type":"string","minLength":1,"maxLength":8192},"idempotency_key":{"type":"string","minLength":1,"maxLength":256}}), &["context","agent","brief","done_when","idempotency_key"], false));
        tools.push(tool("tasks.stop", "Request a task stop. A running task stays running until its host confirms process termination; this response is not termination proof.", json!({"context":context,"task_id":{"type":"string"}}), &["context","task_id"], false));
    }
    tools
}

impl Broker {
    /// The owner input a run's task calls answer to. A heartbeat may read
    /// the tasks of its conversation; only a message or schedule starts one.
    fn task_origin(&self, write: bool) -> Result<TaskOrigin> {
        let event = Store::open(&self.launch.database)?.event(self.launch.grant.input_seq)?;
        ensure!(
            event.direction == Direction::In
                && (OWNER_INPUT_KINDS.contains(&event.kind.as_str())
                    || (!write && event.kind == "heartbeat"))
                && event.author == self.launch.grant.requester,
            "task origin must be an owner input"
        );
        Ok(TaskOrigin {
            requester: event.author,
            conversation: event.conversation,
            input_seq: event.seq,
        })
    }

    fn task_authorize(&self, context: &str, write: bool) -> std::result::Result<TaskOrigin, Fault> {
        self.authorize(context)?;
        if self.launch.grant.role != crate::config::Role::Owner
            || self.launch.grant.task_access == TaskAccess::None
            || (write && self.launch.grant.task_access != TaskAccess::Manage)
        {
            return Err(denied());
        }
        self.task_origin(write).map_err(|_| denied())
    }

    fn task_store(&self) -> Result<TaskStore> {
        TaskStore::open(&self.paths()?, &self.launch.grant.requester)
    }

    fn scoped_task(
        &self,
        store: &TaskStore,
        id: &str,
        origin: &TaskOrigin,
    ) -> std::result::Result<Task, Fault> {
        if id.len() != 64 || !id.bytes().all(|c| c.is_ascii_hexdigit()) {
            return Err(invalid());
        }
        let task = store.get(id).map_err(|_| unavailable())?;
        if task.origin.requester != origin.requester
            || task.origin.conversation != origin.conversation
        {
            return Err(denied());
        }
        Ok(task)
    }

    pub(super) fn task_list(&self, args: TaskList) -> ToolCall {
        let origin = self.task_authorize(&args.context, false)?;
        if args.conversation != origin.conversation {
            return Err(denied());
        }
        let limit = args.limit.unwrap_or(20);
        if !(1..=100).contains(&limit) {
            return Err(invalid());
        }
        self.note_read()?;
        let store = self.task_store().map_err(|_| unavailable())?;
        let tasks = store
            .list(Some(&origin.conversation), limit)
            .map_err(|_| unavailable())?;
        Ok(
            json!({"source":self.source(),"conversation":origin.conversation,
            "tasks":tasks.iter().map(task_summary).collect::<Vec<_>>(),
            "pagination":{"limit":limit,"may_have_more":tasks.len()==limit}}),
        )
    }

    pub(super) fn task_read(&self, args: TaskRead) -> ToolCall {
        let origin = self.task_authorize(&args.context, false)?;
        let limit = args.max_chars.unwrap_or(16000);
        if !(1..=64000).contains(&limit) {
            return Err(invalid());
        }
        let store = self.task_store().map_err(|_| unavailable())?;
        let task = self.scoped_task(&store, &args.task_id, &origin)?;
        self.note_read()?;
        let receipt = store.receipt(&task.id).map_err(|_| unavailable())?;
        let (outcome, result) = match receipt.as_ref().map(|receipt| &receipt.outcome) {
            Some(AttemptOutcome::Completed { result }) => ("completed", result.as_str()),
            Some(AttemptOutcome::Failed { diagnostic }) => ("failed", diagnostic.as_str()),
            Some(AttemptOutcome::Stopped { diagnostic }) => ("stopped", diagnostic.as_str()),
            None if task.state == TaskState::Stopped => {
                ("stopped", task.diagnostic.as_deref().unwrap_or(""))
            }
            None => ("pending", ""),
        };
        let offset = args.offset.unwrap_or(0);
        let total = result.chars().count();
        if offset > total {
            return Err(invalid());
        }
        let excerpt: String = result.chars().skip(offset).take(limit).collect();
        let next = offset + excerpt.chars().count();
        Ok(
            json!({"source":self.source(),"task":task_summary(&task),"spec":task.spec,
            "outcome":outcome,"result":excerpt,"result_char_count":total,"offset":offset,
            "next_offset":if next < total {Some(next)} else {None},
            "continuation_tool":"tasks.read"}),
        )
    }

    pub(super) fn task_delegate(&self, args: TaskDelegate) -> ToolCall {
        let origin = self.task_authorize(&args.context, true)?;
        for (value, maximum) in [
            (&args.agent, 128),
            (&args.brief, 65536),
            (&args.done_when, 8192),
            (&args.idempotency_key, 256),
        ] {
            if value.trim().is_empty() || value.len() > maximum || value.contains('\0') {
                return Err(invalid());
            }
        }
        let config = self
            .paths()
            .and_then(|paths| paths.load())
            .map_err(|_| unavailable())?;
        let eligible: Vec<_> = config
            .agents
            .into_iter()
            .map(|agent| agent.id)
            .filter(|id| self.launch.grant.eligible_agents.contains(id))
            .collect();
        if !eligible.contains(&args.agent) {
            return Err(denied());
        }
        let spec = TaskSpec {
            agent: args.agent.clone(),
            brief: args.brief.clone(),
            done_when: args.done_when.clone(),
            idempotency_key: args.idempotency_key.clone(),
        };
        let store = self.task_store().map_err(|_| unavailable())?;
        // Same immutable origin/key binding as TaskStore::create, needed before admission.
        let task_id = digest(&(&origin, &spec.idempotency_key)).map_err(|_| unavailable())?;
        let _lock = self.lock().map_err(|_| unavailable())?;
        self.task_authorize(&args.context, true)?;
        if let Ok(previous) = store.get(&task_id) {
            if previous.origin != origin || previous.spec != spec {
                return Err(invalid());
            }
        }
        let mut record = TaskWriteRecord {
            request_digest: digest(&("tasks.delegate", &args)).map_err(|_| unavailable())?,
            tool: "tasks.delegate".into(),
            task_id,
            origin,
            spec_digest: digest(&spec).map_err(|_| unavailable())?,
            receipt: None,
        };
        if let Some(result) = self.prior_task_write(&mut record)? {
            return Ok(result);
        }
        self.save_task_write(&record).map_err(|_| unavailable())?;
        let task = store
            .create(&record.origin, &spec, &eligible)
            .map_err(|_| unavailable())?;
        record.receipt = Some(task_summary(&task));
        self.save_task_write(&record).map_err(|_| unavailable())?;
        Ok(json!({"source":self.source(),"task":record.receipt,"replayed":false}))
    }

    pub(super) fn task_stop(&self, args: TaskStop) -> ToolCall {
        let origin = self.task_authorize(&args.context, true)?;
        let store = self.task_store().map_err(|_| unavailable())?;
        let _lock = self.lock().map_err(|_| unavailable())?;
        self.task_authorize(&args.context, true)?;
        let task = self.scoped_task(&store, &args.task_id, &origin)?;
        let mut record = TaskWriteRecord {
            request_digest: digest(&("tasks.stop", &args)).map_err(|_| unavailable())?,
            tool: "tasks.stop".into(),
            task_id: task.id.clone(),
            origin: task.origin.clone(),
            spec_digest: digest(&task.spec).map_err(|_| unavailable())?,
            receipt: None,
        };
        if let Some(result) = self.prior_task_write(&mut record)? {
            return Ok(result);
        }
        self.save_task_write(&record).map_err(|_| unavailable())?;
        let task = store.request_stop(&task.id).map_err(|_| unavailable())?;
        record.receipt = Some(task_summary(&task));
        self.save_task_write(&record).map_err(|_| unavailable())?;
        Ok(json!({"source":self.source(),"task":record.receipt,"replayed":false}))
    }

    fn prior_task_write(
        &self,
        record: &mut TaskWriteRecord,
    ) -> std::result::Result<Option<Value>, Fault> {
        let path = self.task_record_path(&record.request_digest);
        if !path.exists() {
            return Ok(None);
        }
        *record = read_json(&path).map_err(|_| unavailable())?;
        self.reconcile_task_write(record)
            .map_err(|_| unavailable())?;
        match &record.receipt {
            Some(receipt) => Ok(Some(json!({"source":self.source(),"task":receipt,"replayed":true}))),
            None => Err(Fault {code:"write_pending",message:"This task mutation has an unverified outcome. Read task status and reconcile; do not repeat it under a new key."}),
        }
    }

    pub(super) fn reconcile_task_write(&self, record: &mut TaskWriteRecord) -> Result<()> {
        if record.receipt.is_some() {
            return Ok(());
        }
        let store = self.task_store()?;
        let Ok(task) = store.get(&record.task_id) else {
            return Ok(());
        };
        ensure!(
            task.origin == record.origin && digest(&task.spec)? == record.spec_digest,
            "task journal binding changed"
        );
        if record.tool == "tasks.delegate"
            || task.stop_requested
            || matches!(
                task.state,
                TaskState::Completed | TaskState::Failed | TaskState::Stopped
            )
        {
            record.receipt = Some(task_summary(&task));
            self.save_task_write(record)?;
        }
        Ok(())
    }
}

fn task_summary(task: &Task) -> Value {
    json!({"id":task.id,"origin":task.origin,"agent":task.spec.agent,"state":task.state,
        "generation":task.generation,"stop_requested":task.stop_requested,
        "diagnostic":task.diagnostic,"result_event_seq":task.result_event_seq,
        "presentation_state":task.presentation_state,"created_at":task.created_at,"updated_at":task.updated_at})
}

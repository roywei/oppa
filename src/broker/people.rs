//! People tools: who is who, changed in conversation. The model proposes a
//! change in plain words. One that gives someone more waits for the owner's
//! answer: their own later message in the chat where they were asked, after
//! the question reached them. One that takes something away applies at once
//! in the owner's turn. The host writes config.toml and serve uses the change
//! from the next message (`people.rs`).

use super::{denied, invalid, tool, unavailable, Broker, Fault, TaskAccess, ToolCall};
use crate::{
    config::Config,
    people::{self, Answer, Change, Log, PROPOSAL_HOURS},
    store::Store,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PeopleList {
    context: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PeoplePropose {
    context: String,
    change: Change,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PeopleGroup {
    context: String,
    conversation: String,
    #[serde(default)]
    other_assistant: Option<OtherAssistant>,
    #[serde(default)]
    wait_seconds: Option<u64>,
    #[serde(default)]
    new_people: Option<NewPeople>,
}

/// Whether another person's assistant shares a group, and which of the two
/// takes questions addressed to neither.
#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum OtherAssistant {
    TakesUnaddressed,
    LeavesUnaddressed,
    None,
}

/// Whether the owner hears when someone who gets no turn writes in a group.
#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum NewPeople {
    Tell,
    Quiet,
    Default,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PeopleAnswer {
    context: String,
    proposal: i64,
    approved: bool,
}

fn cannot_apply() -> Fault {
    Fault {
        code: "cannot_apply",
        message: "That change cannot apply. A new person needs a role and a handle; the owner's entry never changes here and nobody else becomes owner; a handle is <channel>:<handle> on this instance and belongs to one person.",
    }
}

fn not_seen() -> Fault {
    Fault {
        code: "handle_not_seen",
        message: "Only a handle that has written to this assistant, or is a member of one of its groups, can be added.",
    }
}

fn not_pending() -> Fault {
    Fault {
        code: "not_pending",
        message:
            "That proposal is not waiting for an answer: it was applied, dropped, or never made.",
    }
}

fn not_asked() -> Fault {
    Fault {
        code: "not_asked_yet",
        message: "Only the owner's own message here, after your question reached them, answers a proposal. Ask in your reply and wait for their next message.",
    }
}

fn expired() -> Fault {
    Fault {
        code: "expired",
        message:
            "That proposal waited longer than a day and was dropped. Propose it again and ask.",
    }
}

/// The tools of this family a run gets: the registry for any run under the
/// owner's authority; proposals in the owner's own turns and in a read-only
/// look about a newcomer, which only ever records one; answers only in the
/// owner's own turns.
pub(super) fn definitions(reach: Reach, context: &Value) -> Vec<Value> {
    let change = json!({"type":"object","description":"action set registers someone or changes a registered person's role or handles (a field left out keeps its value); action remove takes them out.","properties":{
        "action":{"enum":["set","remove"]},
        "person":{"type":"string","pattern":"^[A-Za-z0-9_-]{1,64}$","description":"A short id such as a first name, lower case."},
        "role":{"enum":["adult","child","friend","guest","agent"],"description":"adult and child: family, whose turns have the owner's own access (history, memory, accounts and, at full access, this Mac), guarded by the owner's rules; friend and guest: known, no turns yet; agent: another person's assistant, never answered."},
        "handles":{"type":"array","maxItems":16,"items":{"type":"string","maxLength":4096},"description":"<channel>:<handle>, as members and senders appear in the conversation."}
    },"required":["action","person"],"additionalProperties":false});
    let mut tools = Vec::new();
    if reach.owner_run {
        tools.push(tool("people.list", "Who is who: each registered person's role and handles, changes waiting for the owner's answer, the latest changes, and the owner's settings per group.", json!({"context":context}), &["context"], true));
    }
    if reach.owner_turn || reach.owner_look {
        tools.push(tool("people.propose", "Propose a change to who is who, for example when the owner introduces someone, or says someone may ask you for things. A change that only takes something away (removing a person or a handle, or a lower role) applies at once. Anything else waits: ask the owner in plain words in your reply here, naming the person, their handle and what they could do; their next message here answers it, then call people.answer.", json!({"context":context,"change":change}), &["context","change"], false));
    }
    if reach.owner_turn {
        tools.push(tool("people.group", "Set how you behave in one group, as the owner asks, at once; for a direct chat, only new_people. other_assistant: takes_unaddressed when another person's assistant shares the group and you take questions addressed to neither assistant, leaves_unaddressed when that assistant takes them, none when no other assistant is there. wait_seconds: how long turns for anyone but the owner wait there for the other assistant (1 to 120). new_people: tell to hear when someone who gets no turn writes there, quiet never to, default for direct chats and groups of up to six members.", json!({"context":context,"conversation":{"type":"string","maxLength":4096},"other_assistant":{"enum":["takes_unaddressed","leaves_unaddressed","none"]},"wait_seconds":{"type":"integer","minimum":1,"maximum":120},"new_people":{"enum":["tell","quiet","default"]}}), &["context","conversation"], false));
        tools.push(tool("people.answer", "Record the owner's answer to a proposal you asked them about in this conversation: approved true applies it, false drops it. Only the owner's own message, after your question reached them, counts.", json!({"context":context,"proposal":{"type":"integer","minimum":1},"approved":{"type":"boolean"}}), &["context","proposal","approved"], false));
    }
    tools
}

/// What this run may do with the registry.
#[derive(Clone, Copy, Default)]
pub(super) struct Reach {
    /// Any run under the owner's authority: read it.
    pub owner_run: bool,
    /// The owner's own conversation turn: propose, answer, narrow at once.
    pub owner_turn: bool,
    /// A read-only look of the owner's: record a proposal, nothing more.
    pub owner_look: bool,
}

impl Broker {
    fn people_config(&self) -> std::result::Result<Config, Fault> {
        self.paths()
            .and_then(|paths| paths.load())
            .map_err(|_| unavailable())
    }

    pub(super) fn people_reach(&self) -> Reach {
        let grant = &self.launch.grant;
        let owner_run = self
            .people_config()
            .is_ok_and(|config| config.owner == grant.requester);
        let (input, native_id) = Store::open(&self.launch.database)
            .and_then(|store| store.event(grant.input_seq))
            .map(|input| (input.kind, input.native_id.unwrap_or_default()))
            .unwrap_or_default();
        // A schedule firing is the owner's standing work, not the owner
        // speaking now: it may read who is who, and change nothing. An edit
        // is the owner speaking now, as a message is.
        Reach {
            owner_run,
            owner_turn: owner_run
                && grant.task_access == TaskAccess::Manage
                && matches!(input.as_str(), "message" | "edit"),
            // Of the read-only looks, only one about a newcomer proposes: a
            // heartbeat or the digest reads who is who and changes nothing.
            owner_look: owner_run
                && grant.task_access != TaskAccess::Manage
                && input == "heartbeat"
                && native_id.starts_with("newcomer:"),
        }
    }

    pub(super) fn people_list(&self, args: PeopleList) -> ToolCall {
        self.authorize(&args.context)?;
        if !self.people_reach().owner_run {
            return Err(denied());
        }
        let config = self.people_config()?;
        let log = Log::open(&self.launch.database).map_err(|_| unavailable())?;
        let now = chrono::Utc::now();
        let waiting: Vec<people::Logged> = log
            .recent(true, 50)
            .map_err(|_| unavailable())?
            .into_iter()
            .filter(|logged| !people::lapsed(&logged.created_at, now))
            .collect();
        let decided = log.recent(false, 10).map_err(|_| unavailable())?;
        Ok(json!({
            "owner":config.owner,
            "people":config.people,
            "waiting":waiting,
            "recent":decided,
            "groups":config.groups,
        }))
    }

    pub(super) fn people_propose(&self, args: PeoplePropose) -> ToolCall {
        self.authorize(&args.context)?;
        let reach = self.people_reach();
        if !reach.owner_turn && !reach.owner_look {
            return Err(denied());
        }
        let config = self.people_config()?;
        let after = self.checked(&config, &args.change)?;
        let grant = &self.launch.grant;
        let conversation = grant.conversations.first().ok_or_else(denied)?;
        let log = Log::open(&self.launch.database).map_err(|_| unavailable())?;
        // A look changes nothing, even what the owner could take away at once.
        if reach.owner_turn && !people::widens(&config.people, &after) {
            let paths = self.paths().map_err(|_| unavailable())?;
            let written = people::apply(&paths, &args.change, false, |handle| self.seen(handle))
                .map_err(|_| cannot_apply())?;
            let id = log
                .record(conversation, grant.input_seq, &args.change, true)
                .map_err(|_| unavailable())?;
            return Ok(
                json!({"applied":true,"change":id,"person":entry(&written.people, args.change.person())}),
            );
        }
        // The same proposal made twice in one turn is one proposal.
        let pending = log.recent(true, 50).map_err(|_| unavailable())?;
        if let Some(same) = pending
            .iter()
            .find(|logged| logged.proposed_seq == grant.input_seq && logged.change == args.change)
        {
            return Ok(waiting(same.id));
        }
        let id = log
            .record(conversation, grant.input_seq, &args.change, false)
            .map_err(|_| unavailable())?;
        Ok(waiting(id))
    }

    pub(super) fn people_answer(&self, args: PeopleAnswer) -> ToolCall {
        self.authorize(&args.context)?;
        if !self.people_reach().owner_turn {
            return Err(denied());
        }
        let config = self.people_config()?;
        let grant = &self.launch.grant;
        let store = Store::open(&self.launch.database).map_err(|_| unavailable())?;
        let log = Log::open(&self.launch.database).map_err(|_| unavailable())?;
        let proposal = log
            .get(args.proposal)
            .map_err(|_| unavailable())?
            .filter(|logged| logged.state == "pending")
            .ok_or_else(not_pending)?;
        // A schedule or heartbeat speaks for the owner's standing wishes, not
        // for an answer to a question asked today.
        let answer = people::owner_answer(
            &store,
            &config.owner,
            &proposal.conversation,
            proposal.proposed_seq,
            &proposal.created_at,
            grant.input_seq,
        )
        .map_err(|_| unavailable())?;
        match answer {
            Answer::NotAsked => return Err(not_asked()),
            Answer::Lapsed => {
                log.decide(proposal.id, false, grant.input_seq)
                    .map_err(|_| unavailable())?;
                return Err(expired());
            }
            Answer::Counts => {}
        }
        if !args.approved {
            log.decide(proposal.id, false, grant.input_seq)
                .map_err(|_| unavailable())?;
            return Ok(json!({"dropped":true,"proposal":proposal.id}));
        }
        // The registry may have changed since the question: check it again,
        // and apply the change to it as it is once the file is locked.
        self.checked(&config, &proposal.change)?;
        let paths = self.paths().map_err(|_| unavailable())?;
        // Write first, then record the answer: a write that fails leaves the
        // proposal waiting, and a stop in between leaves the file changed and
        // the proposal waiting, which a repeated yes applies again unchanged.
        // Two answers never race: both are the owner's turns in the
        // proposal's one conversation, which run one at a time.
        let written = people::apply(&paths, &proposal.change, true, |handle| self.seen(handle))
            .map_err(|_| cannot_apply())?;
        log.decide(proposal.id, true, grant.input_seq)
            .map_err(|_| unavailable())?;
        Ok(
            json!({"applied":true,"proposal":proposal.id,"person":entry(&written.people, proposal.change.person())}),
        )
    }

    pub(super) fn people_group(&self, args: PeopleGroup) -> ToolCall {
        self.authorize(&args.context)?;
        if !self.people_reach().owner_turn {
            return Err(denied());
        }
        self.conversation_allowed(&args.conversation)?;
        let store = Store::open(&self.launch.database).map_err(|_| unavailable())?;
        let kind = store
            .conversation(&args.conversation)
            .map_err(|_| denied())?
            .kind;
        // A direct chat has only the one setting: whether its newcomers are told.
        let dm_only = args.other_assistant.is_none() && args.wait_seconds.is_none();
        if kind != "group" && !(kind == "dm" && dm_only) {
            return Err(invalid());
        }
        let config = self.people_config()?;
        let mut group =
            config
                .group(&args.conversation)
                .cloned()
                .unwrap_or(crate::config::SharedGroup {
                    conversation: args.conversation.clone(),
                    takes_unaddressed: None,
                    wait_seconds: crate::config::DEFAULT_GROUP_WAIT,
                    new_people: None,
                });
        if let Some(other) = args.other_assistant {
            group.takes_unaddressed = match other {
                OtherAssistant::TakesUnaddressed => Some(true),
                OtherAssistant::LeavesUnaddressed => Some(false),
                OtherAssistant::None => None,
            };
        }
        if let Some(wait) = args.wait_seconds {
            group.wait_seconds = wait;
        }
        if let Some(new_people) = args.new_people {
            group.new_people = match new_people {
                NewPeople::Tell => Some(true),
                NewPeople::Quiet => Some(false),
                NewPeople::Default => None,
            };
        }
        let paths = self.paths().map_err(|_| unavailable())?;
        let written = people::write_group(&paths, &group).map_err(|_| invalid())?;
        Ok(json!({"applied":true,"group":written.group(&args.conversation)}))
    }

    /// The registry after `change`, checked whole, with every handle it adds
    /// one the host has seen.
    fn checked(
        &self,
        config: &Config,
        change: &Change,
    ) -> std::result::Result<Vec<crate::config::Person>, Fault> {
        let after = change
            .apply_to(&config.people, &config.owner)
            .map_err(|_| cannot_apply())?;
        let mut whole = config.clone();
        whole.people = after.clone();
        whole.validate().map_err(|_| cannot_apply())?;
        if people::unseen_handle(&config.people, &after, |handle| self.seen(handle)).is_some() {
            return Err(not_seen());
        }
        if change.person().is_empty() {
            return Err(invalid());
        }
        Ok(after)
    }

    /// Whether the host has met `handle` (`<channel>:<handle>`): it wrote to
    /// the assistant unregistered, is listed in one of its groups, or belonged
    /// to someone the owner registered here before.
    fn seen(&self, handle: &str) -> bool {
        let wrote = Store::open(&self.launch.database)
            .and_then(|store| store.wrote_in(handle))
            .unwrap_or(false);
        wrote
            || Log::open(&self.launch.database).is_ok_and(|log| {
                log.seen_as_member(handle).unwrap_or(false)
                    || log.ever_registered(handle).unwrap_or(false)
            })
    }
}

fn waiting(id: i64) -> Value {
    json!({"proposal":id,"waiting":true,"expires_in_hours":PROPOSAL_HOURS,
        "next":"Ask the owner in plain words in your reply here: who, which handle, and what they could do. Call people.answer when the owner answers in a later message here."})
}

fn entry(people: &[crate::config::Person], id: &str) -> Value {
    people
        .iter()
        .find(|person| person.id == id)
        .map_or(json!({"id":id,"removed":true}), |person| json!(person))
}

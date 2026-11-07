//! The owner's registry, changed in conversation. A model proposes a change
//! and the host applies it only after the owner's yes (`broker/people.rs`);
//! taking a role, handle or ability away applies at once. A change rewrites
//! the `[[people]]` tables of config.toml, keeping everything else as the
//! owner wrote it, and is logged with the message that asked for it. Serve
//! reads the registry for each message, so a change needs no restart.

use crate::config::{handle_key, Config, Paths, Person, Role, SharedGroup};
use anyhow::{bail, ensure, Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::{path::Path, time::Duration};

/// How long a proposal waits for the owner's yes.
pub const PROPOSAL_HOURS: i64 = 24;

/// One change to the registry, as a model proposes it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "action", rename_all = "snake_case")]
pub enum Change {
    /// Register someone, or change a registered person's role or handles. A
    /// field left out keeps its current value.
    Set {
        person: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        role: Option<Role>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        handles: Option<Vec<String>>,
    },
    /// Take someone out of the registry.
    Remove { person: String },
}

impl Change {
    pub fn person(&self) -> &str {
        match self {
            Change::Set { person, .. } | Change::Remove { person } => person,
        }
    }

    /// The registry after this change, or why it cannot apply. The owner's
    /// own entry is never changed here.
    pub fn apply_to(&self, people: &[Person], owner: &str) -> Result<Vec<Person>> {
        let id = self.person();
        ensure!(
            id != owner && people.iter().all(|p| p.id != id || p.role != Role::Owner),
            "the owner's own entry is not changed in conversation"
        );
        let mut after = people.to_vec();
        let index = after.iter().position(|p| p.id == id);
        match (self, index) {
            (Change::Remove { .. }, Some(index)) => {
                after.remove(index);
            }
            (Change::Remove { .. }, None) => bail!("{id} is not registered"),
            (Change::Set { role, handles, .. }, index) => {
                ensure!(*role != Some(Role::Owner), "only the owner has role owner");
                let mut person = match index {
                    Some(index) => after[index].clone(),
                    None => Person {
                        id: id.to_owned(),
                        role: role.context("a new person needs a role")?,
                        handles: handles
                            .clone()
                            .filter(|handles| !handles.is_empty())
                            .context("a new person needs a handle")?,
                    },
                };
                if let Some(role) = role {
                    person.role = *role;
                }
                if let Some(handles) = handles {
                    person.handles = dedup(handles);
                }
                match index {
                    Some(index) => after[index] = person,
                    None => after.push(person),
                }
            }
        }
        Ok(after)
    }
}

fn dedup(items: &[String]) -> Vec<String> {
    let mut kept: Vec<String> = Vec::new();
    for item in items {
        if !kept.contains(item) {
            kept.push(item.clone());
        }
    }
    kept
}

/// How much a role lets someone do, lowest first: an agent never starts a
/// turn, guests and friends are known but get none, and a child's turn
/// keeps to the children's rules, which an adult's does not.
fn rank(role: Role) -> u8 {
    match role {
        Role::Agent => 0,
        Role::Guest => 1,
        Role::Friend => 2,
        Role::Child => 3,
        Role::Adult => 4,
        Role::Owner => 5,
    }
}

/// Whether going from `before` to `after` gives anyone more: a new person, a
/// higher role or another handle. Only such a change waits
/// for the owner's yes.
pub fn widens(before: &[Person], after: &[Person]) -> bool {
    after.iter().any(
        |person| match before.iter().find(|old| old.id == person.id) {
            None => true,
            Some(old) => {
                rank(person.role) > rank(old.role)
                    || person.handles.iter().any(|h| !old.handles.contains(h))
            }
        },
    )
}

/// A handle `after` gives someone that `before` did not, and that `seen` does
/// not know: only a handle that has met the assistant can be added.
pub fn unseen_handle<'a>(
    before: &[Person],
    after: &'a [Person],
    seen: impl Fn(&str) -> bool,
) -> Option<&'a str> {
    after.iter().find_map(|person| {
        let old = before.iter().find(|old| old.id == person.id);
        person
            .handles
            .iter()
            .find(|handle| !old.is_some_and(|old| old.handles.contains(handle)) && !seen(handle))
            .map(String::as_str)
    })
}

/// Apply `change` to the registry as it is once the configuration is locked,
/// so a change made meanwhile in another chat or by hand is kept. Every handle
/// it adds must pass `seen`. A change applied without the owner's yes
/// (`approved` false) must still only take away.
pub fn apply(
    paths: &Paths,
    change: &Change,
    approved: bool,
    seen: impl Fn(&str) -> bool,
) -> Result<Config> {
    let (config, _) = crate::config::rewrite(
        paths,
        |current, document| {
            let after = change.apply_to(&current.people, &current.owner)?;
            ensure!(
                approved || !widens(&current.people, &after),
                "this change now gives someone more, so it needs the owner's yes"
            );
            if let Some(handle) = unseen_handle(&current.people, &after, seen) {
                bail!("{handle} has not written to this assistant or been listed in its groups");
            }
            set_people(document, &after)?;
            Ok(after)
        },
        |config, after| {
            ensure!(
                config.people == *after,
                "the people tables could not be written"
            );
            Ok(())
        },
    )?;
    Ok(config)
}

fn set_people(document: &mut toml_edit::DocumentMut, people: &[Person]) -> Result<()> {
    let existing = tables(document, "people")?;
    let mut rebuilt = toml_edit::ArrayOfTables::new();
    for person in people {
        let mut table = existing
            .iter()
            .find(|table| table.get("id").and_then(|id| id.as_str()) == Some(&person.id))
            .cloned()
            .unwrap_or_default();
        let role = serde_json::to_value(person.role)?;
        let as_array = |items: &[String]| {
            toml_edit::value(
                items
                    .iter()
                    .map(String::as_str)
                    .collect::<toml_edit::Array>(),
            )
        };
        set_if_changed(&mut table, "id", toml_edit::value(person.id.as_str()));
        set_if_changed(
            &mut table,
            "role",
            toml_edit::value(role.as_str().context("role")?),
        );
        set_if_changed(&mut table, "handles", as_array(&person.handles));
        rebuilt.push(table);
    }
    put_tables(document, "people", rebuilt);
    Ok(())
}

/// Set the owner's settings for one group, keeping the rest of the file as
/// the owner wrote it; a group left with no settings is taken out.
pub fn write_group(paths: &Paths, group: &SharedGroup) -> Result<Config> {
    let empty = group.takes_unaddressed.is_none() && group.new_people.is_none();
    let (config, ()) = crate::config::rewrite(
        paths,
        |_, document| {
            let mut groups = tables(document, "groups")?;
            let index = groups.iter().position(|table| {
                table.get("conversation").and_then(|c| c.as_str()) == Some(&group.conversation)
            });
            if empty {
                if let Some(index) = index {
                    groups.remove(index);
                }
            } else {
                let mut table = index
                    .and_then(|index| groups.get(index).cloned())
                    .unwrap_or_default();
                set_if_changed(
                    &mut table,
                    "conversation",
                    toml_edit::value(group.conversation.as_str()),
                );
                for (key, value) in [
                    ("takes_unaddressed", group.takes_unaddressed),
                    ("new_people", group.new_people),
                ] {
                    match value {
                        Some(value) => set_if_changed(&mut table, key, toml_edit::value(value)),
                        None => {
                            table.remove(key);
                        }
                    }
                }
                if group.wait_seconds != crate::config::DEFAULT_GROUP_WAIT
                    || table.contains_key("wait_seconds")
                {
                    set_if_changed(
                        &mut table,
                        "wait_seconds",
                        toml_edit::value(i64::try_from(group.wait_seconds)?),
                    );
                }
                match index.and_then(|index| groups.get_mut(index)) {
                    Some(existing) => *existing = table,
                    None => groups.push(table),
                }
            }
            put_tables(document, "groups", groups);
            Ok(())
        },
        |config, ()| {
            ensure!(
                config.group(&group.conversation) == (!empty).then_some(group),
                "the group's settings could not be written"
            );
            Ok(())
        },
    )?;
    Ok(config)
}

/// The `[[name]]` tables as the owner wrote them, or none yet.
fn tables(document: &toml_edit::DocumentMut, name: &str) -> Result<toml_edit::ArrayOfTables> {
    match document.get(name) {
        None => Ok(toml_edit::ArrayOfTables::new()),
        Some(toml_edit::Item::ArrayOfTables(tables)) => Ok(tables.clone()),
        Some(_) => {
            bail!("write {name} as [[{name}]] tables so they can be changed in conversation")
        }
    }
}

fn put_tables(document: &mut toml_edit::DocumentMut, name: &str, tables: toml_edit::ArrayOfTables) {
    if tables.is_empty() {
        document.remove(name);
    } else {
        document.insert(name, toml_edit::Item::ArrayOfTables(tables));
    }
}

/// Set a key only when its value differs, so an unchanged key keeps the
/// owner's formatting and comments.
pub(crate) fn set_if_changed(table: &mut toml_edit::Table, key: &str, value: toml_edit::Item) {
    // Values compare without their spacing and comments.
    let bare = |value: &toml_edit::Value| {
        let mut value = value.clone();
        value.decor_mut().clear();
        if let toml_edit::Value::Array(array) = &mut value {
            array.fmt();
        }
        value.to_string()
    };
    let same = table
        .get(key)
        .and_then(|item| item.as_value())
        .zip(value.as_value())
        .is_some_and(|(old, new)| bare(old) == bare(new));
    if !same {
        table.insert(key, value);
    }
}

/// How an owner's message stands as the answer to a question about a
/// proposal.
#[derive(Debug, PartialEq, Eq)]
pub enum Answer {
    /// The owner's own message, where the proposal was made, after the
    /// question may have reached them.
    Counts,
    /// No answer: another person's turn, a schedule or heartbeat, another
    /// conversation, the proposing turn itself, or a message before the question.
    NotAsked,
    /// The proposal waited longer than a day.
    Lapsed,
}

/// Whether input event `input_seq` answers a proposal made in `conversation`
/// by the turn of input `proposed_seq` at `created_at`: the owner's own
/// message or edit (never a schedule or heartbeat), in that conversation, after a
/// reply of the assistant's there that may have reached them, within
/// `PROPOSAL_HOURS`. Whether the words mean yes is the model's to read. Any
/// host change that waits for the owner's yes can use it.
pub fn owner_answer(
    store: &crate::store::Store,
    owner: &str,
    conversation: &str,
    proposed_seq: i64,
    created_at: &str,
    input_seq: i64,
) -> Result<Answer> {
    let input = store.event(input_seq)?;
    if !matches!(input.kind.as_str(), "message" | "edit")
        || input.author != owner
        || input.direction != crate::store::Direction::In
        || input.conversation != conversation
        || input.seq <= proposed_seq
        || !store.assistant_wrote_between(conversation, proposed_seq, input.seq)?
    {
        return Ok(Answer::NotAsked);
    }
    Ok(if lapsed(created_at, chrono::Utc::now()) {
        Answer::Lapsed
    } else {
        Answer::Counts
    })
}

/// Whether a proposal made at `created_at` has waited too long by `now`.
pub fn lapsed(created_at: &str, now: chrono::DateTime<chrono::Utc>) -> bool {
    chrono::DateTime::parse_from_rfc3339(created_at)
        .ok()
        .is_none_or(|created| {
            now.signed_duration_since(created) > chrono::Duration::hours(PROPOSAL_HOURS)
        })
}

/// A logged change: proposed, then applied or dropped.
#[derive(Clone, Debug, Serialize)]
pub struct Logged {
    pub id: i64,
    pub conversation: String,
    pub proposed_seq: i64,
    pub change: Change,
    pub state: String,
    pub created_at: String,
    pub decided_seq: Option<i64>,
    pub decided_at: Option<String>,
}

/// The change log and the group rosters, in the instance database. Like the
/// schedule store, it creates its own tables.
pub struct Log {
    connection: Connection,
}

impl Log {
    pub fn open(database: &Path) -> Result<Self> {
        let connection = Connection::open(database).context("open state database")?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS people_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation TEXT NOT NULL,
                proposed_seq INTEGER NOT NULL,
                change TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('pending','applied','dropped')),
                created_at TEXT NOT NULL,
                decided_seq INTEGER,
                decided_at TEXT
            );
            CREATE TABLE IF NOT EXISTS group_members (
                conversation TEXT PRIMARY KEY,
                members TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );",
        )?;
        Ok(Self { connection })
    }

    /// Record a change. A pending one waits for the owner's yes; an applied
    /// one was a narrowing the owner asked for, decided by the same message.
    pub fn record(
        &self,
        conversation: &str,
        proposed_seq: i64,
        change: &Change,
        applied: bool,
    ) -> Result<i64> {
        let now = chrono::Utc::now().to_rfc3339();
        self.connection.execute(
            "INSERT INTO people_changes(conversation,proposed_seq,change,state,created_at,decided_seq,decided_at)
             VALUES(?1,?2,?3,?4,?5,?6,?7)",
            params![
                conversation,
                proposed_seq,
                serde_json::to_string(change)?,
                if applied { "applied" } else { "pending" },
                now,
                applied.then_some(proposed_seq),
                applied.then_some(&now),
            ],
        )?;
        Ok(self.connection.last_insert_rowid())
    }

    pub fn get(&self, id: i64) -> Result<Option<Logged>> {
        self.connection
            .query_row(
                "SELECT id,conversation,proposed_seq,change,state,created_at,decided_seq,decided_at
                 FROM people_changes WHERE id=?1",
                [id],
                logged,
            )
            .optional()?
            .transpose()
    }

    /// Mark a pending change applied or dropped by the message `seq`; false
    /// when it was no longer pending.
    pub fn decide(&self, id: i64, applied: bool, seq: i64) -> Result<bool> {
        Ok(self.connection.execute(
            "UPDATE people_changes SET state=?2,decided_seq=?3,decided_at=?4 WHERE id=?1 AND state='pending'",
            params![
                id,
                if applied { "applied" } else { "dropped" },
                seq,
                chrono::Utc::now().to_rfc3339()
            ],
        )? == 1)
    }

    /// The latest changes still waiting for the owner, or the latest
    /// decided, newest first.
    pub fn recent(&self, waiting: bool, limit: u32) -> Result<Vec<Logged>> {
        let mut statement = self.connection.prepare(
            "SELECT id,conversation,proposed_seq,change,state,created_at,decided_seq,decided_at
             FROM people_changes WHERE (state='pending')=?1 ORDER BY id DESC LIMIT ?2",
        )?;
        let rows = statement.query_map(params![waiting, limit], logged)?;
        rows.map(|row| row?).collect()
    }

    /// Keep the members a channel last reported for a group, as
    /// channel-qualified handles.
    pub fn remember_members(&self, conversation: &str, members: &[String]) -> Result<()> {
        self.connection.execute(
            "INSERT INTO group_members(conversation,members,updated_at) VALUES(?1,?2,?3)
             ON CONFLICT(conversation) DO UPDATE SET members=excluded.members,updated_at=excluded.updated_at",
            params![
                conversation,
                serde_json::to_string(members)?,
                chrono::Utc::now().to_rfc3339()
            ],
        )?;
        Ok(())
    }

    /// A group's members as last reported, or none when its channel never listed them.
    pub fn members(&self, conversation: &str) -> Result<Option<Vec<String>>> {
        let members: Option<String> = self
            .connection
            .query_row(
                "SELECT members FROM group_members WHERE conversation=?1",
                [conversation],
                |row| row.get(0),
            )
            .optional()?;
        members
            .map(|members| Ok(serde_json::from_str(&members)?))
            .transpose()
    }

    /// Whether a change the owner applied ever gave someone `handle`, so a
    /// person removed and added back need not write first.
    pub fn ever_registered(&self, handle: &str) -> Result<bool> {
        let mut statement = self
            .connection
            .prepare("SELECT change FROM people_changes WHERE state='applied'")?;
        let changes = statement.query_map([], |row| row.get::<_, String>(0))?;
        for change in changes {
            if let Ok(Change::Set {
                handles: Some(handles),
                ..
            }) = serde_json::from_str(&change?)
            {
                if handles
                    .iter()
                    .any(|known| handle_key(known) == handle_key(handle))
                {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Whether the host has seen `handle` (`<channel>:<handle>`) in a group's
    /// member list.
    pub fn seen_as_member(&self, handle: &str) -> Result<bool> {
        let mut statement = self
            .connection
            .prepare("SELECT members FROM group_members")?;
        let lists = statement.query_map([], |row| row.get::<_, String>(0))?;
        for list in lists {
            let members: Vec<String> = serde_json::from_str(&list?)?;
            if members
                .iter()
                .any(|member| handle_key(member) == handle_key(handle))
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

fn logged(row: &rusqlite::Row<'_>) -> rusqlite::Result<Result<Logged>> {
    let change: String = row.get(3)?;
    Ok(serde_json::from_str(&change)
        .map_err(anyhow::Error::from)
        .and_then(|change| {
            Ok(Logged {
                id: row.get(0)?,
                conversation: row.get(1)?,
                proposed_seq: row.get(2)?,
                change,
                state: row.get(4)?,
                created_at: row.get(5)?,
                decided_seq: row.get(6)?,
                decided_at: row.get(7)?,
            })
        }))
}

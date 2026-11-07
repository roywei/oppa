//! The owner's registry changed in conversation (family groups; G-3, S-3,
//! X-9): what waits for the owner's yes, what never changes in chat, and how a
//! change is written into the owner's own config.toml.

use oppa::{
    config::{Paths, Person, Role},
    people::{self, Change},
};
use std::fs;

const CONFIG: &str = r#"# Synthetic owner's instance (comment kept)
version = 1
owner = "synthetic-owner"
access = "read"

[[agents]]
id = "unused"
family = "synthetic"
command = ["/never/launch/a/provider"]

[[channels]]
id = "imessage"
account = "assistant"
command = ["/never/launch/an/adapter"]

[[connectors]]
id = "mac"
kind = "script"
command = ["/never/launch/a/connector"]

[[people]]
# the owner, written by hand
id = "synthetic-owner"
role = "owner"
handles = ["imessage:+15550100"]

[[people]]
id = "robin"   # a friend from the club
role = "friend"
handles = ["imessage:+15550103"]

[[people]]
id = "gone"
role = "guest"
handles = ["imessage:+15550109"]
"#;

fn person(id: &str, role: Role, handles: &[&str]) -> Person {
    Person {
        id: id.into(),
        role,
        handles: handles.iter().map(|h| h.to_string()).collect(),
    }
}

#[test]
fn only_a_change_that_gives_someone_more_waits_for_the_owner() {
    let owner = person("synthetic-owner", Role::Owner, &["imessage:+15550100"]);
    let sam = person("sam", Role::Adult, &["imessage:+15550101"]);
    let before = vec![owner.clone(), sam.clone()];
    let set = |role, handles: Option<&[&str]>| Change::Set {
        person: "sam".into(),
        role,
        handles: handles.map(|h| h.iter().map(|h| h.to_string()).collect()),
    };
    let widened = |change: Change| {
        let after = change.apply_to(&before, "synthetic-owner").unwrap();
        people::widens(&before, &after)
    };
    // A new person, a higher role, another handle.
    let mia = Change::Set {
        person: "mia".into(),
        role: Some(Role::Friend),
        handles: Some(vec!["imessage:+15550102".into()]),
    };
    assert!(widened(mia));
    let mut friend = before.clone();
    friend[1].role = Role::Friend;
    let to_adult = set(Some(Role::Adult), None)
        .apply_to(&friend, "synthetic-owner")
        .unwrap();
    assert!(people::widens(&friend, &to_adult));
    assert!(widened(set(
        None,
        Some(&["imessage:+15550101", "imessage:sam@example.com"])
    )));
    // Taking a person, a handle or reach away gives nobody more.
    assert!(!widened(Change::Remove {
        person: "sam".into()
    }));
    assert!(!widened(set(Some(Role::Child), None)));
    let two_handles = set(None, Some(&["imessage:+15550101", "imessage:+15550105"]))
        .apply_to(&before, "synthetic-owner")
        .unwrap();
    let one_handle = set(None, Some(&["imessage:+15550105"]))
        .apply_to(&two_handles, "synthetic-owner")
        .unwrap();
    assert!(!people::widens(&two_handles, &one_handle));
    // A field left out keeps its value.
    let only_role = set(Some(Role::Adult), None)
        .apply_to(&before, "synthetic-owner")
        .unwrap();
    assert_eq!(only_role[1].handles, sam.handles);
    // The owner's entry never changes in conversation, and nobody becomes owner.
    let owner_change = Change::Set {
        person: "synthetic-owner".into(),
        role: None,
        handles: Some(vec!["imessage:+15550199".into()]),
    };
    assert!(owner_change.apply_to(&before, "synthetic-owner").is_err());
    assert!(Change::Remove {
        person: "synthetic-owner".into()
    }
    .apply_to(&before, "synthetic-owner")
    .is_err());
    assert!(set(Some(Role::Owner), None)
        .apply_to(&before, "synthetic-owner")
        .is_err());
}

#[test]
fn a_change_rewrites_only_the_people_tables_of_the_owners_file() {
    let root = tempfile::tempdir().unwrap();
    let paths = Paths::new(
        root.path().join("runtime"),
        Some(root.path().join("private")),
    )
    .unwrap();
    paths.prepare().unwrap();
    fs::write(paths.config_file(), CONFIG).unwrap();
    let seen = |_: &str| true;
    let gone = Change::Remove {
        person: "gone".into(),
    };
    let sam = Change::Set {
        person: "sam".into(),
        role: Some(Role::Adult),
        handles: Some(vec!["imessage:+15550101".into()]),
    };
    // The owner's own file mode is kept (review of 2026-10-10).
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(paths.config_file(), fs::Permissions::from_mode(0o600)).unwrap();
    // Taking someone out needs no yes; adding someone does, even when the
    // registry changed after the turn read it.
    people::apply(&paths, &gone, false, seen).unwrap();
    let mode = fs::metadata(paths.config_file())
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600);
    let unchanged = fs::read_to_string(paths.config_file()).unwrap();
    assert!(people::apply(&paths, &sam, false, seen).is_err());
    assert_eq!(fs::read_to_string(paths.config_file()).unwrap(), unchanged);
    let written = people::apply(&paths, &sam, true, seen).unwrap();
    let ids: Vec<&str> = written.people.iter().map(|p| p.id.as_str()).collect();
    assert_eq!(ids, ["synthetic-owner", "robin", "sam"]);
    assert_eq!(paths.load().unwrap().people, written.people);
    let text = fs::read_to_string(paths.config_file()).unwrap();
    for kept in [
        "# Synthetic owner's instance (comment kept)",
        "# the owner, written by hand",
        "id = \"robin\"   # a friend from the club",
    ] {
        assert!(text.contains(kept), "lost {kept:?} from:\n{text}");
    }
    assert!(!text.contains("gone"), "{text}");
    // A handle nobody has seen is refused; the owner's file is left as it was.
    let stranger = Change::Set {
        person: "robin".into(),
        role: None,
        handles: Some(vec![
            "imessage:+15550103".into(),
            "imessage:+15550199".into(),
        ]),
    };
    assert!(people::apply(&paths, &stranger, true, |handle| handle
        != "imessage:+15550199")
    .is_err());
    assert_eq!(fs::read_to_string(paths.config_file()).unwrap(), text);
}

#[test]
fn a_groups_settings_are_written_in_place_and_an_empty_one_is_taken_out() {
    let root = tempfile::tempdir().unwrap();
    let paths = Paths::new(
        root.path().join("runtime"),
        Some(root.path().join("private")),
    )
    .unwrap();
    paths.prepare().unwrap();
    let shared = "\n[[groups]]\n# Robin's assistant takes the general questions here\nconversation = \"imessage/assistant/club\"\ntakes_unaddressed = false\nwait_seconds = 10\n";
    fs::write(paths.config_file(), format!("{CONFIG}{shared}")).unwrap();
    let group = |conversation: &str, new_people| oppa::config::SharedGroup {
        conversation: conversation.into(),
        takes_unaddressed: None,
        wait_seconds: oppa::config::DEFAULT_GROUP_WAIT,
        new_people,
    };
    // A new group's setting is added; the club's other settings stay as written.
    people::write_group(&paths, &group("imessage/assistant/school", Some(true))).unwrap();
    let mut club = paths
        .load()
        .unwrap()
        .group("imessage/assistant/club")
        .cloned()
        .unwrap();
    club.new_people = Some(false);
    people::write_group(&paths, &club).unwrap();
    let text = fs::read_to_string(paths.config_file()).unwrap();
    assert!(
        text.contains("# Robin's assistant takes the general questions here"),
        "{text}"
    );
    let groups = paths.load().unwrap().groups;
    assert_eq!(
        groups
            .iter()
            .map(|g| (
                g.conversation.as_str(),
                g.takes_unaddressed,
                g.wait_seconds,
                g.new_people
            ))
            .collect::<Vec<_>>(),
        [
            ("imessage/assistant/club", Some(false), 10, Some(false)),
            ("imessage/assistant/school", None, 20, Some(true)),
        ]
    );
    // A group left with no settings is taken out.
    people::write_group(&paths, &group("imessage/assistant/school", None)).unwrap();
    assert_eq!(paths.load().unwrap().groups.len(), 1);
}

#[test]
fn the_owner_hears_about_newcomers_in_direct_chats_and_small_listed_groups() {
    // Newcomers: only someone with no turn of their own counts, never another
    // assistant; a group counts when its channel lists up to six members, and
    // one it does not list could be any size; the owner's choice wins.
    let root = tempfile::tempdir().unwrap();
    let paths = Paths::new(
        root.path().join("runtime"),
        Some(root.path().join("private")),
    )
    .unwrap();
    paths.prepare().unwrap();
    let groups = "\n[[groups]]\nconversation = \"imessage/assistant/school\"\nnew_people = true\n\n[[groups]]\nconversation = \"imessage/assistant/club\"\nnew_people = false\n\n[[people]]\nid = \"robins-assistant\"\nrole = \"agent\"\nhandles = [\"imessage:+15550108\"]\n\n[[people]]\nid = \"sam\"\nrole = \"adult\"\nhandles = [\"imessage:+15550101\"]\n";
    fs::write(paths.config_file(), format!("{CONFIG}{groups}")).unwrap();
    let config = paths.load().unwrap();
    let told = |conversation: &str, kind: &str, members: Option<usize>, author: &str| {
        config.tells_owner_about(
            &format!("imessage/assistant/{conversation}"),
            kind,
            members,
            author,
        )
    };
    assert!(told("dm", "dm", None, "imessage:+15550199"));
    assert!(told("family", "group", Some(6), "imessage:+15550199"));
    assert!(
        told("family", "group", Some(2), "robin"),
        "a friend has no turn"
    );
    assert!(!told("family", "group", Some(7), "imessage:+15550199"));
    assert!(!told("family", "group", None, "imessage:+15550199"));
    assert!(told("school", "group", Some(40), "imessage:+15550199"));
    assert!(!told("club", "group", Some(2), "imessage:+15550199"));
    for someone in [
        "synthetic-owner",
        "assistant",
        "sam",
        "robins-assistant",
        "imessage:+15550108",
        "imessage:+15550101",
    ] {
        assert!(!told("dm", "dm", None, someone), "{someone}");
    }
}

mod answers {
    use chrono::Utc;
    use oppa::{
        broker::{Broker, BrokerLaunch, ConversationScope, RunGrant, TaskAccess, ToolResult},
        config::{Paths, Role},
        store::{InboundEvent, Store},
        vault::Vault,
    };
    use serde_json::{json, Value};
    use std::fs;
    use tempfile::TempDir;

    const CONFIG: &str = r#"version = 1
owner = "synthetic-owner"
access = "connectors"

[[agents]]
id = "unused"
family = "synthetic"
command = ["/never/launch/a/provider"]

[[channels]]
id = "fake"
account = "acct"
command = ["/never/launch/an/adapter"]

[[connectors]]
id = "mac"
kind = "script"
command = ["python3", "-c", '''
import json, sys
request = json.loads(sys.stdin.read())
schema = {'type': 'object'}
if request['method'] == 'describe':
    print(json.dumps({'protocol_version': 1, 'operations': [
        {'name': 'calendar_create_event', 'tier': 'reversible', 'description': 'Create an event.', 'input_schema': schema},
        {'name': 'send_message', 'tier': 'outward', 'description': 'Send a message.', 'input_schema': schema}]}))
else:
    print(json.dumps({'ready': True}))
''']

[[people]]
id = "synthetic-owner"
role = "owner"
handles = ["fake:+15550100"]
"#;
    const DM: &str = "fake/acct/owner-dm";
    const FAMILY: &str = "fake/acct/family";

    struct Instance {
        _root: TempDir,
        paths: Paths,
        /// One expiry for every grant, so a turn opened twice for the same
        /// input has the same grant and journal.
        expires_at: i64,
    }

    impl Instance {
        fn new() -> Self {
            let root = TempDir::new().unwrap();
            let paths = Paths::new(
                root.path().join("runtime"),
                Some(root.path().join("private")),
            )
            .unwrap();
            paths.prepare().unwrap();
            fs::write(paths.config_file(), CONFIG).unwrap();
            Vault::init(&paths.vault()).unwrap();
            let store = self_store(&paths);
            for (id, native) in [(DM, "owner-dm"), (FAMILY, "family")] {
                store
                    .create_conversation(id, "fake", "acct", native, "policy-1")
                    .unwrap();
            }
            Self {
                _root: root,
                paths,
                expires_at: Utc::now().timestamp() + 3600,
            }
        }

        fn store(&self) -> Store {
            self_store(&self.paths)
        }

        /// One message in, logged as the host would after resolving its sender.
        fn says(&self, conversation: &str, author: &str, kind: &str, body: &str) -> i64 {
            self.store()
                .append_inbound(&InboundEvent {
                    conversation: conversation.into(),
                    native_id: format!("in-{}", body.len() * 7919 + kind.len()),
                    author: author.into(),
                    body: body.into(),
                    kind: kind.into(),
                    ..Default::default()
                })
                .unwrap()
                .seq
        }

        /// The assistant's reply, as the channel observed it go out.
        fn replies(&self, conversation: &str, body: &str) {
            self.store()
                .append_observed_outbound(&InboundEvent {
                    conversation: conversation.into(),
                    native_id: format!("out-{}", body.len()),
                    author: "assistant".into(),
                    body: body.into(),
                    kind: "message".into(),
                    ..Default::default()
                })
                .unwrap();
        }

        /// A broker for a turn of `requester` on input `seq` in `conversation`.
        fn turn(&self, requester: &str, seq: i64, conversation: &str) -> (Broker, String) {
            let context = format!("context-{seq}");
            let launch = BrokerLaunch {
                private_dir: self.paths.private.clone(),
                grant: RunGrant {
                    wakes: false,
                    access: Default::default(),
                    context: context.clone(),
                    requester: requester.into(),
                    run_id: format!("run-{seq}"),
                    input_seq: seq,
                    conversations: vec![conversation.into()],
                    // The owner's turns reach every chat on the account.
                    conversation_scopes: if requester == "synthetic-owner" {
                        vec![ConversationScope {
                            channel: "fake".into(),
                            account: "acct".into(),
                            owners_account: false,
                        }]
                    } else {
                        vec![]
                    },
                    read_scopes: vec!["*".into()],
                    write_scopes: vec!["*".into()],
                    task_access: if requester == "synthetic-owner" {
                        TaskAccess::Manage
                    } else {
                        TaskAccess::None
                    },
                    eligible_agents: vec![],
                    expires_at: self.expires_at,
                    role: if requester == "synthetic-owner" {
                        Role::Owner
                    } else {
                        Role::Adult
                    },
                },
                database: self.paths.database(),
                vault: self.paths.vault(),
                journal: self.paths.runtime.join(format!("journal-{seq}")),
            };
            (Broker::open(launch).unwrap(), context)
        }

        fn people(&self) -> Vec<(String, Role, Vec<String>)> {
            self.paths
                .load()
                .unwrap()
                .people
                .into_iter()
                .map(|p| (p.id, p.role, p.handles))
                .collect()
        }
    }

    fn self_store(paths: &Paths) -> Store {
        Store::open(&paths.database()).unwrap()
    }

    fn ok(result: ToolResult) -> Value {
        assert!(!result.is_error, "{}", result.value);
        result.value
    }

    fn code(result: ToolResult) -> String {
        assert!(result.is_error, "{}", result.value);
        result.value["error"]["code"].as_str().unwrap().to_owned()
    }

    #[test]
    fn a_proposal_applies_only_after_the_owners_own_later_message_where_it_was_asked() {
        let instance = Instance::new();
        // Sam writes in the family group before anyone knows who Sam is.
        instance.says(
            FAMILY,
            "fake:+15550101",
            "message",
            "Add to the shared calendar",
        );
        let asked = instance.says(DM, "synthetic-owner", "message", "That's my wife Sam");
        let (broker, context) = instance.turn("synthetic-owner", asked, DM);
        let sam =
            json!({"action":"set","person":"sam","role":"adult","handles":["fake:+15550101"]});
        let proposal = ok(broker.call("people.propose", json!({"context":context,"change":sam})));
        assert_eq!(proposal["waiting"], true, "{proposal}");
        let id = proposal["proposal"].clone();
        // Proposing it again in the same turn is the same proposal.
        assert_eq!(
            ok(broker.call("people.propose", json!({"context":context,"change":sam})))["proposal"],
            id
        );
        // The turn that asked cannot answer for the owner.
        assert_eq!(
            code(broker.call(
                "people.answer",
                json!({"context":context,"proposal":id,"approved":true})
            )),
            "not_asked_yet"
        );
        // A message of the owner's that came before the question reached
        // them is no answer either.
        let early = instance.says(DM, "synthetic-owner", "message", "yes, before the question");
        let (broker, context) = instance.turn("synthetic-owner", early, DM);
        assert_eq!(
            code(broker.call(
                "people.answer",
                json!({"context":context,"proposal":id,"approved":true})
            )),
            "not_asked_yet"
        );
        instance.replies(
            DM,
            "Should Sam ask me for family things, and change the Family calendar?",
        );
        // Nor a yes in another chat, a schedule firing, or someone else's yes.
        let elsewhere = instance.says(FAMILY, "synthetic-owner", "message", "yes (in the group)");
        let (broker, context) = instance.turn("synthetic-owner", elsewhere, FAMILY);
        assert_eq!(
            code(broker.call(
                "people.answer",
                json!({"context":context,"proposal":id,"approved":true})
            )),
            "not_asked_yet"
        );
        let schedule = instance.says(DM, "synthetic-owner", "schedule", "standing yes at 9");
        let (broker, context) = instance.turn("synthetic-owner", schedule, DM);
        assert_eq!(
            code(broker.call(
                "people.answer",
                json!({"context":context,"proposal":id,"approved":true})
            )),
            "access_denied"
        );
        // A schedule speaks for standing work, not for the owner now: it may
        // read who is who and change nothing.
        let (broker, context) = instance.turn("synthetic-owner", schedule, DM);
        let names: Vec<String> = broker
            .definitions()
            .iter()
            .map(|tool| tool["name"].as_str().unwrap().to_owned())
            .filter(|name| name.starts_with("people."))
            .collect();
        assert_eq!(names, ["people.list"]);
        assert_eq!(
            code(broker.call(
                "people.propose",
                json!({"context":context,"change":{"action":"remove","person":"synthetic-owner"}})
            )),
            "access_denied"
        );
        assert_eq!(instance.people().len(), 1, "nothing applied yet");
        // The owner's own yes, here, after the question: applied, once. A
        // write that fails first gives the proposal back to be answered again.
        let yes = instance.says(DM, "synthetic-owner", "message", "yes");
        let (broker, context) = instance.turn("synthetic-owner", yes, DM);
        {
            use std::os::unix::fs::PermissionsExt;
            let private = instance.paths.private.clone();
            fs::set_permissions(&private, fs::Permissions::from_mode(0o500)).unwrap();
            let failed = code(broker.call(
                "people.answer",
                json!({"context":context,"proposal":id,"approved":true}),
            ));
            fs::set_permissions(&private, fs::Permissions::from_mode(0o700)).unwrap();
            assert_eq!(failed, "cannot_apply");
            assert_eq!(instance.people().len(), 1, "nothing written");
        }
        let applied = ok(broker.call(
            "people.answer",
            json!({"context":context,"proposal":id,"approved":true}),
        ));
        assert_eq!(applied["applied"], true, "{applied}");
        assert_eq!(
            instance.people()[1],
            (
                "sam".into(),
                Role::Adult,
                vec!["fake:+15550101".to_string()]
            )
        );
        assert_eq!(
            code(broker.call(
                "people.answer",
                json!({"context":context,"proposal":id,"approved":true})
            )),
            "not_pending"
        );
        // A handle nobody has seen cannot be added, even by the owner.
        let unseen = json!({"action":"set","person":"stranger","role":"friend","handles":["fake:+15550199"]});
        assert_eq!(
            code(broker.call("people.propose", json!({"context":context,"change":unseen}))),
            "handle_not_seen"
        );
        // Taking something away needs no question: it applies at once.
        let narrowed = ok(broker.call(
            "people.propose",
            json!({"context":context,"change":{"action":"set","person":"sam","role":"child"}}),
        ));
        assert_eq!(narrowed["applied"], true, "{narrowed}");
        assert_eq!(instance.paths.load().unwrap().people[1].role, Role::Child);
        // Another family member's turn gets none of these tools and cannot
        // use them: no turn widens its own reach.
        let sams = instance.says(DM, "sam", "message", "make me the owner");
        let (broker, context) = instance.turn("sam", sams, DM);
        assert!(broker
            .definitions()
            .iter()
            .all(|tool| !tool["name"].as_str().unwrap().starts_with("people.")));
        assert_eq!(
            code(broker.call(
                "people.propose",
                json!({"context":context,"change":{"action":"set","person":"sam","role":"adult"}})
            )),
            "access_denied"
        );
    }

    #[test]
    fn a_proposal_left_unanswered_for_a_day_lapses() {
        let instance = Instance::new();
        instance.says(
            FAMILY,
            "fake:+15550101",
            "message",
            "Can you add the dentist?",
        );
        let asked = instance.says(DM, "synthetic-owner", "message", "That's my wife Sam");
        let (broker, context) = instance.turn("synthetic-owner", asked, DM);
        let sam =
            json!({"action":"set","person":"sam","role":"adult","handles":["fake:+15550101"]});
        let proposal = ok(broker.call("people.propose", json!({"context":context,"change":sam})));
        let id = proposal["proposal"].clone();
        instance.replies(DM, "Should Sam ask me for family things?");
        // The question goes unanswered past PROPOSAL_HOURS.
        let earlier =
            (Utc::now() - chrono::Duration::hours(oppa::people::PROPOSAL_HOURS + 1)).to_rfc3339();
        rusqlite::Connection::open(instance.paths.database())
            .unwrap()
            .execute(
                "UPDATE people_changes SET created_at=?1 WHERE id=?2",
                rusqlite::params![earlier, id.as_i64().unwrap()],
            )
            .unwrap();
        let listed = ok(broker.call("people.list", json!({"context":context})));
        assert_eq!(listed["waiting"], json!([]), "{listed}");
        // A late yes is no answer: the proposal is dropped, and asking again
        // is a new proposal and a new question.
        let late = instance.says(DM, "synthetic-owner", "message", "yes");
        let (broker, context) = instance.turn("synthetic-owner", late, DM);
        assert_eq!(
            code(broker.call(
                "people.answer",
                json!({"context":context,"proposal":id,"approved":true})
            )),
            "expired"
        );
        assert_eq!(
            code(broker.call(
                "people.answer",
                json!({"context":context,"proposal":id,"approved":true})
            )),
            "not_pending"
        );
        assert_eq!(instance.people().len(), 1, "nothing applied");
        let again = ok(broker.call("people.propose", json!({"context":context,"change":sam})));
        assert_eq!(again["waiting"], true, "{again}");
        assert_ne!(again["proposal"], id);
    }

    #[test]
    fn the_owners_edit_after_the_question_answers_it() {
        // Edited messages: an edit is the owner speaking now, as a message is.
        let instance = Instance::new();
        instance.says(FAMILY, "fake:+15550101", "message", "Add the dentist?");
        let store = instance.store();
        let said = |body: &str| {
            store
                .append_channel_message(&InboundEvent {
                    conversation: DM.into(),
                    native_id: "owner-intro".into(),
                    author: "synthetic-owner".into(),
                    body: body.into(),
                    kind: "message".into(),
                    ..Default::default()
                })
                .unwrap()
        };
        let asked = said("That's my wife Sam");
        let (broker, context) = instance.turn("synthetic-owner", asked.seq, DM);
        let sam =
            json!({"action":"set","person":"sam","role":"adult","handles":["fake:+15550101"]});
        let id = ok(broker.call("people.propose", json!({"context":context,"change":sam})))
            ["proposal"]
            .clone();
        instance.replies(DM, "Should Sam ask me for family things?");
        let edited = said("That's my wife Sam, and yes, she may");
        assert_eq!(edited.kind, "edit");
        let (broker, context) = instance.turn("synthetic-owner", edited.seq, DM);
        let applied = ok(broker.call(
            "people.answer",
            json!({"context":context,"proposal":id,"approved":true}),
        ));
        assert_eq!(applied["applied"], true, "{applied}");
        assert_eq!(instance.people().len(), 2);
    }

    #[test]
    fn the_owner_quiets_newcomers_in_one_direct_chat() {
        // Review of 2026-10-10: without this, one persistent sender in a
        // direct chat started a look every time, up to the hourly limit.
        let instance = Instance::new();
        instance.says(FAMILY, "fake:+15550101", "message", "Hello?");
        let quiet = instance.says(
            DM,
            "synthetic-owner",
            "message",
            "Stop telling me about that number",
        );
        let (broker, context) = instance.turn("synthetic-owner", quiet, DM);
        // A direct chat has no other assistant and no wait to set.
        assert_eq!(
            code(broker.call(
                "people.group",
                json!({"context":context,"conversation":FAMILY,"new_people":"quiet","wait_seconds":5})
            )),
            "invalid_arguments"
        );
        assert!(instance.paths.load().unwrap().tells_owner_about(
            FAMILY,
            "dm",
            None,
            "fake:+15550101"
        ));
        ok(broker.call(
            "people.group",
            json!({"context":context,"conversation":FAMILY,"new_people":"quiet"}),
        ));
        let config = instance.paths.load().unwrap();
        assert!(!config.tells_owner_about(FAMILY, "dm", None, "fake:+15550101"));
        assert!(config.shared_group(FAMILY).is_none());
    }

    #[test]
    fn an_email_handle_is_seen_in_any_case() {
        // Review of 2026-10-10: people are matched by an email address in any
        // case, so the handle check is too.
        let instance = Instance::new();
        instance.says(FAMILY, "fake:Sam@example.com", "message", "Hi there");
        let asked = instance.says(DM, "synthetic-owner", "message", "That's Sam");
        let (broker, context) = instance.turn("synthetic-owner", asked, DM);
        let sam = json!({"action":"set","person":"sam","role":"adult","handles":["fake:sam@example.com"]});
        let proposal = ok(broker.call("people.propose", json!({"context":context,"change":sam})));
        assert_eq!(proposal["waiting"], true, "{proposal}");
    }

    #[test]
    fn a_waiting_proposal_stays_listed_behind_many_later_changes() {
        // Review of 2026-10-10: the list and the duplicate check read only the
        // newest twenty changes, so an older waiting one dropped out of sight.
        let instance = Instance::new();
        instance.says(FAMILY, "fake:+15550101", "message", "Anyone?");
        let asked = instance.says(DM, "synthetic-owner", "message", "That's Sam");
        let (broker, context) = instance.turn("synthetic-owner", asked, DM);
        let sam =
            json!({"action":"set","person":"sam","role":"adult","handles":["fake:+15550101"]});
        let id = ok(broker.call("people.propose", json!({"context":context,"change":sam})))
            ["proposal"]
            .clone();
        let log = oppa::people::Log::open(&instance.paths.database()).unwrap();
        for _ in 0..25 {
            let narrowing = oppa::people::Change::Remove {
                person: "someone".into(),
            };
            log.record(DM, asked, &narrowing, true).unwrap();
        }
        let listed = ok(broker.call("people.list", json!({"context":context})));
        assert_eq!(listed["waiting"][0]["id"], id, "{listed}");
        assert_eq!(
            ok(broker.call("people.propose", json!({"context":context,"change":sam})))["proposal"],
            id
        );
    }
}

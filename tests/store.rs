use oppa::store::{Attachment, DeliveryObservation, DeliveryState, Direction, InboundEvent, Store};
use tempfile::TempDir;

fn fixture() -> (TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("state.db")).unwrap();
    store
        .create_conversation("room", "web", "local", "native-room", "adult-v1")
        .unwrap();
    (dir, store)
}
fn inbound(id: &str, body: &str) -> InboundEvent {
    InboundEvent {
        conversation: "room".into(),
        native_id: id.into(),
        author: "person-a".into(),
        body: body.into(),
        kind: "message".into(),
        ..Default::default()
    }
}
fn reply(store: &Store, key: &str) -> i64 {
    store
        .enqueue_reply("room", key, "Here is the result.", vec![], None)
        .unwrap()
        .id
}

#[test]
fn native_redelivery_preserves_one_event_but_changed_payload_is_rejected() {
    let (_dir, store) = fixture();
    let mut input = inbound("native-1", "Read the link above.");
    input.attachments.push(Attachment {
        path: "attachments/example.txt".into(),
        mime_type: Some("text/plain".into()),
        name: Some("example.txt".into()),
    });
    input.spoken = true;
    input.audio_ref = Some("audio/example.opus".into());
    input.device = Some("device-a".into());
    let first = store.append_inbound(&input).unwrap();
    assert_eq!(first, store.append_inbound(&input).unwrap());
    assert!(first.spoken);
    assert_eq!(first.attachments, input.attachments);
    input.body.push_str(" Changed.");
    assert!(store.append_inbound(&input).is_err());
    assert_eq!(store.transcript("room", 0, 20).unwrap().len(), 1);
}

#[test]
fn outbound_intent_and_receipt_are_in_canonical_log_across_reopen() {
    let (dir, store) = fixture();
    let incoming = store
        .append_inbound(&inbound("native-1", "Please summarize."))
        .unwrap();
    let attachment = Attachment {
        path: "artifacts/result.pdf".into(),
        ..Default::default()
    };
    let queued = store
        .enqueue_reply(
            "room",
            "reply-1",
            "The summary.",
            vec![attachment.clone()],
            Some("native-1".into()),
        )
        .unwrap();
    assert_eq!(queued.event.direction, Direction::Out);
    assert_eq!(queued.event.delivery, Some(DeliveryState::Queued));
    assert_eq!(
        store.transcript("room", incoming.seq, 10).unwrap(),
        vec![queued.event.clone()]
    );
    assert!(store.claim_send(queued.id).unwrap());
    store
        .finish_send(
            queued.id,
            DeliveryObservation::Sent {
                native_id: "native-out-1".into(),
            },
        )
        .unwrap();
    drop(store);
    let reopened = Store::open(&dir.path().join("state.db")).unwrap();
    let messages = reopened.transcript("room", 0, 10).unwrap();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[1].delivery, Some(DeliveryState::Sent));
    assert_eq!(messages[1].native_id.as_deref(), Some("native-out-1"));
    assert_eq!(messages[1].reply_to.as_deref(), Some("native-1"));
    assert_eq!(messages[1].attachments, vec![attachment]);
    assert!(reopened.pending_outbox().unwrap().is_empty());
}

#[test]
fn idempotency_covers_conversation_body_attachments_and_reply_reference() {
    let (_dir, store) = fixture();
    let first = store
        .enqueue_reply("room", "same", "Result", vec![], None)
        .unwrap();
    assert_eq!(
        first,
        store
            .enqueue_reply("room", "same", "Result", vec![], None)
            .unwrap()
    );
    assert!(store
        .enqueue_reply("room", "same", "Different", vec![], None)
        .is_err());
    assert!(store
        .enqueue_reply(
            "room",
            "same",
            "Result",
            vec![Attachment {
                path: "extra.txt".into(),
                ..Default::default()
            }],
            None
        )
        .is_err());
    assert!(store
        .enqueue_reply("room", "same", "Result", vec![], Some("different".into()))
        .is_err());
    store
        .create_conversation("other", "web", "local", "native-other", "adult-v1")
        .unwrap();
    assert!(store
        .enqueue_reply("other", "same", "Result", vec![], None)
        .is_err());
    assert_eq!(store.transcript("room", 0, 100).unwrap().len(), 1);
    assert!(store.transcript("other", 0, 100).unwrap().is_empty());
}

#[test]
fn unknown_delivery_cannot_be_claimed_until_adapter_proves_not_sent() {
    let (_dir, store) = fixture();
    let id = reply(&store, "reply-1");
    assert!(store.claim_send(id).unwrap());
    assert!(!store.claim_send(id).unwrap());
    store
        .finish_send(
            id,
            DeliveryObservation::Unknown {
                evidence: "Transport disconnected after submit.".into(),
            },
        )
        .unwrap();
    assert!(!store.claim_send(id).unwrap());
    assert_eq!(
        store.pending_outbox().unwrap()[0].state,
        DeliveryState::Unknown
    );
    assert!(store
        .finish_send(
            id,
            DeliveryObservation::ConfirmedNotSent {
                evidence: "No active send.".into()
            }
        )
        .is_err());
    assert!(store
        .mark_verified(
            id,
            DeliveryObservation::ConfirmedNotSent {
                evidence: "".into()
            }
        )
        .is_err());
    store
        .mark_verified(
            id,
            DeliveryObservation::ConfirmedNotSent {
                evidence: "Adapter checked its durable native request ledger: request absent."
                    .into(),
            },
        )
        .unwrap();
    assert!(store.claim_send(id).unwrap());
    assert_eq!(store.outbox_entry(id).unwrap().attempts, 2);
    store
        .finish_send(
            id,
            DeliveryObservation::Sent {
                native_id: "confirmed-native".into(),
            },
        )
        .unwrap();
    assert!(!store.claim_send(id).unwrap());
}

#[test]
fn restart_marks_interrupted_sends_unknown_and_verification_can_resolve_them() {
    let (dir, store) = fixture();
    let id = reply(&store, "reply-1");
    assert!(store.claim_send(id).unwrap());
    drop(store);
    let reopened = Store::open(&dir.path().join("state.db")).unwrap();
    // An ordinary second connection is not evidence that a dispatcher died.
    assert_eq!(
        reopened.outbox_entry(id).unwrap().state,
        DeliveryState::Sending
    );
    assert_eq!(reopened.recover_interrupted_sends().unwrap(), 1);
    assert_eq!(reopened.recover_interrupted_sends().unwrap(), 0);
    assert!(!reopened.claim_send(id).unwrap());
    assert!(reopened
        .outbox_entry(id)
        .unwrap()
        .evidence
        .unwrap()
        .contains("verification"));
    reopened
        .mark_verified(
            id,
            DeliveryObservation::Sent {
                native_id: "native-found".into(),
            },
        )
        .unwrap();
    reopened
        .mark_verified(
            id,
            DeliveryObservation::Sent {
                native_id: "native-found".into(),
            },
        )
        .unwrap();
    assert!(reopened
        .mark_verified(
            id,
            DeliveryObservation::Sent {
                native_id: "contradictory-native".into()
            }
        )
        .is_err());
}

#[test]
fn independent_dispatchers_cannot_claim_the_same_attempt() {
    let (dir, store) = fixture();
    let id = reply(&store, "reply-1");
    let second = Store::open(&dir.path().join("state.db")).unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let other_barrier = barrier.clone();
    let thread = std::thread::spawn(move || {
        other_barrier.wait();
        second.claim_send(id).unwrap()
    });
    barrier.wait();
    let first_claim = store.claim_send(id).unwrap();
    let second_claim = thread.join().unwrap();
    assert_ne!(first_claim, second_claim);
    assert_eq!(store.outbox_entry(id).unwrap().attempts, 1);
}

#[test]
fn harness_cursors_are_independent_and_include_the_other_harness_reply() {
    let (_dir, store) = fixture();
    let first = store.append_inbound(&inbound("native-1", "Hello")).unwrap();
    store
        .save_session("room", "agent-a", "adult-v1", "session-a", first.seq)
        .unwrap();
    store
        .save_session("room", "agent-b", "adult-v1", "session-b", 0)
        .unwrap();
    let outgoing = store
        .enqueue_reply("room", "reply-1", "Hello back", vec![], None)
        .unwrap();
    store
        .save_session(
            "room",
            "agent-a",
            "adult-v1",
            "session-a",
            outgoing.event.seq,
        )
        .unwrap();
    let other = store
        .session("room", "agent-b", "adult-v1")
        .unwrap()
        .unwrap();
    let catch_up = store.transcript("room", other.cursor, 100).unwrap();
    assert_eq!(catch_up.len(), 2);
    assert_eq!(catch_up[1].body, "Hello back");
    assert!(store
        .save_session("room", "agent-a", "adult-v1", "session-a", first.seq)
        .is_err());
    assert!(store
        .save_session("room", "agent-a", "adult-v1", "session-a", 999)
        .is_err());
}

#[test]
fn audience_policy_change_prevents_resuming_or_updating_adult_session() {
    let (_dir, store) = fixture();
    store
        .save_session("room", "agent-a", "adult-v1", "private-session", 0)
        .unwrap();
    store
        .create_conversation("room", "web", "local", "native-room", "child-v2")
        .unwrap();
    assert!(store
        .session("room", "agent-a", "adult-v1")
        .unwrap()
        .is_none());
    assert!(store
        .session("room", "agent-a", "child-v2")
        .unwrap()
        .is_none());
    assert!(store
        .save_session("room", "agent-a", "adult-v1", "private-session", 0)
        .is_err());
    store
        .save_session("room", "restricted", "child-v2", "child-session", 0)
        .unwrap();
    assert!(store
        .create_conversation("room", "web", "other-account", "native-room", "child-v2")
        .is_err());
}

#[test]
fn search_supports_bilingual_or_terms_short_chinese_and_neighbor_context() {
    let (_dir, store) = fixture();
    store
        .append_inbound(&inbound("native-1", "Earlier context"))
        .unwrap();
    let target = store
        .append_inbound(&inbound(
            "native-2",
            "周六游泳课在下午。School pickup is at four.",
        ))
        .unwrap();
    store
        .enqueue_reply("room", "reply-1", "I have written it down.", vec![], None)
        .unwrap();
    store
        .create_conversation("other", "web", "local", "other", "adult-v1")
        .unwrap();
    let mut private = inbound("native-other", "School pickup elsewhere");
    private.conversation = "other".into();
    store.append_inbound(&private).unwrap();
    for query in [
        "swimming pickup",
        "游泳课",
        "游泳",
        "PICKUP",
        "不存在 pickup",
    ] {
        let hits = store.search("room", query, 1).unwrap();
        assert_eq!(hits.len(), 1, "query: {query}");
        assert_eq!(hits[0].event.seq, target.seq);
        assert_eq!(hits[0].context.len(), 3);
        assert_eq!(hits[0].context[0].body, "Earlier context");
        assert_eq!(hits[0].context[2].direction, Direction::Out);
    }
    assert!(store.search("room", "\" OR --", 1).unwrap().is_empty());
}

#[test]
fn fresh_bootstrap_gets_recent_turns_and_foreign_cursor_is_rejected() {
    let (_dir, store) = fixture();
    store.append_inbound(&inbound("native-1", "First")).unwrap();
    store
        .append_inbound(&inbound("native-2", "Second"))
        .unwrap();
    store.append_inbound(&inbound("native-3", "Third")).unwrap();
    assert_eq!(
        store
            .recent_transcript("room", 2)
            .unwrap()
            .iter()
            .map(|e| e.body.as_str())
            .collect::<Vec<_>>(),
        vec!["Second", "Third"]
    );
    store
        .create_conversation("other", "web", "local", "other", "adult-v1")
        .unwrap();
    let mut input = inbound("other-1", "Other");
    input.conversation = "other".into();
    let foreign = store.append_inbound(&input).unwrap();
    assert!(store
        .save_session("room", "a", "adult-v1", "handle", foreign.seq)
        .is_err());
}

#[test]
fn concurrent_enqueue_preserves_one_intent_and_its_original_destination() {
    let (dir, store) = fixture();
    let second = Store::open(&dir.path().join("state.db")).unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let other_barrier = barrier.clone();
    let thread = std::thread::spawn(move || {
        other_barrier.wait();
        second
            .enqueue_reply("room", "concurrent-key", "One result", vec![], None)
            .unwrap()
    });
    barrier.wait();
    let first = store
        .enqueue_reply("room", "concurrent-key", "One result", vec![], None)
        .unwrap();
    let other = thread.join().unwrap();
    assert_eq!(first, other);
    assert_eq!(first.destination.channel, "web");
    assert_eq!(first.destination.account, "local");
    assert_eq!(first.destination.native_id, "native-room");
    assert_eq!(store.transcript("room", 0, 100).unwrap().len(), 1);
}

#[test]
fn contradictory_native_receipt_cannot_partially_mark_another_reply_delivered() {
    let (_dir, store) = fixture();
    let first = reply(&store, "first");
    let second = reply(&store, "second");
    assert!(store.claim_send(first).unwrap());
    store
        .finish_send(
            first,
            DeliveryObservation::Sent {
                native_id: "one-native-receipt".into(),
            },
        )
        .unwrap();
    assert!(store.claim_send(second).unwrap());
    assert!(store
        .finish_send(
            second,
            DeliveryObservation::Sent {
                native_id: "one-native-receipt".into()
            }
        )
        .is_err());
    let second = store.outbox_entry(second).unwrap();
    assert_eq!(second.state, DeliveryState::Unknown);
    assert_eq!(second.event.native_id, None);
    assert_eq!(second.native_id, None);
    assert!(second.evidence.as_deref().unwrap().contains("duplicates"));
    assert!(!store.claim_send(second.id).unwrap());
    let first = store.outbox_entry(first).unwrap();
    assert_eq!(first.state, DeliveryState::Sent);
    assert_eq!(first.event.native_id.as_deref(), Some("one-native-receipt"));
    assert_eq!(first.native_id.as_deref(), Some("one-native-receipt"));
}

#[test]
fn invalid_success_receipts_leave_claimed_delivery_unknown_with_bounded_evidence() {
    let (dir, store) = fixture();
    for (index, native_id) in [" ".into(), "invalid\0receipt".into(), "x".repeat(4097)]
        .into_iter()
        .enumerate()
    {
        let id = reply(&store, &format!("invalid-{index}"));
        assert!(store.claim_send(id).unwrap());
        assert!(store
            .finish_send(id, DeliveryObservation::Sent { native_id })
            .is_err());
        let entry = store.outbox_entry(id).unwrap();
        assert_eq!(entry.state, DeliveryState::Unknown);
        assert!(entry.native_id.is_none());
        assert!(entry.event.native_id.is_none());
        assert!(entry.evidence.as_deref().unwrap().contains("invalid"));
        assert!(entry.evidence.as_deref().unwrap().len() < 128);
        assert!(!store.claim_send(id).unwrap());
    }
    drop(store);
    let reopened = Store::open(&dir.path().join("state.db")).unwrap();
    assert!(reopened
        .pending_outbox()
        .unwrap()
        .iter()
        .all(|entry| entry.state == DeliveryState::Unknown));
}

#[test]
fn opening_current_schema_reads_committed_history_while_another_writer_is_active() {
    let (dir, store) = fixture();
    store
        .append_inbound(&inbound("message", "Committed text"))
        .unwrap();
    let writer = rusqlite::Connection::open(dir.path().join("state.db")).unwrap();
    writer
        .execute_batch("BEGIN IMMEDIATE; UPDATE events SET body='Uncommitted text';")
        .unwrap();
    let reader = Store::open(&dir.path().join("state.db")).unwrap();
    assert_eq!(
        reader.transcript("room", 0, 10).unwrap()[0].body,
        "Committed text"
    );
    writer.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn version_one_conversations_migrate_as_direct_and_a_group_never_narrows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.db");
    let old = rusqlite::Connection::open(&path).unwrap();
    old.execute_batch(
        "CREATE TABLE conversations (id TEXT PRIMARY KEY, channel TEXT NOT NULL,
            account TEXT NOT NULL, native_id TEXT NOT NULL, policy_hash TEXT NOT NULL,
            UNIQUE(channel,account,native_id));
        CREATE TABLE sessions (conversation TEXT NOT NULL REFERENCES conversations(id),
            agent TEXT NOT NULL, policy_hash TEXT NOT NULL, handle TEXT NOT NULL,
            cursor INTEGER NOT NULL CHECK(cursor>=0), PRIMARY KEY(conversation,agent,policy_hash));
        CREATE TABLE events (seq INTEGER PRIMARY KEY AUTOINCREMENT,
            conversation TEXT NOT NULL REFERENCES conversations(id), created_at TEXT NOT NULL,
            direction TEXT NOT NULL CHECK(direction IN ('in','out')), author TEXT NOT NULL,
            kind TEXT NOT NULL, body TEXT NOT NULL, attachments TEXT NOT NULL, reply_to TEXT,
            native_id TEXT, spoken INTEGER NOT NULL DEFAULT 0, audio_ref TEXT, device TEXT,
            payload_digest TEXT NOT NULL, UNIQUE(conversation,direction,native_id));
        INSERT INTO conversations VALUES('old','console','local','old','p');
        INSERT INTO sessions VALUES('old','agent','p','old-handle',0);
        PRAGMA user_version=1;",
    )
    .unwrap();
    drop(old);
    let store = Store::open(&path).unwrap();
    let migrated = store.conversation("old").unwrap();
    assert_eq!((migrated.kind.as_str(), migrated.title), ("dm", None));
    let session = store.session("old", "agent", "p").unwrap().unwrap();
    assert_eq!(
        (session.started_at, session.used_at),
        (None, None),
        "a migrated session has no clock and is retired on next use"
    );
    store
        .create_conversation("club", "chat", "acct", "club", "p")
        .unwrap();
    store
        .set_conversation_shape("club", "group", Some("Synthetic Club"))
        .unwrap();
    store.set_conversation_shape("club", "dm", None).unwrap();
    drop(store);
    let store = Store::open(&path).unwrap();
    let club = store.conversation("club").unwrap();
    assert_eq!((club.kind.as_str(), club.title), ("group", None));
    assert!(store
        .set_conversation_shape("club", "thread", None)
        .is_err());
    let version: i64 = rusqlite::Connection::open(&path)
        .unwrap()
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, oppa::store::SCHEMA_VERSION);
}

#[test]
fn a_reply_counts_toward_first_contact_only_once_it_may_have_reached_the_person() {
    // Adult turns: `first_reply_to_requester` stays true until a reply to that
    // person may have reached them. A queued or refused reply never did; an
    // unknown one may have. Another person's reply does not count.
    let (_dir, store) = fixture();
    let asked = store.append_inbound(&inbound("m1", "Hello?")).unwrap();
    let other = InboundEvent {
        author: "person-b".into(),
        ..inbound("m2", "Hi.")
    };
    store.append_inbound(&other).unwrap();
    let id = reply(&store, &format!("reply:{}", asked.seq));
    assert!(!store.replied_to("person-a").unwrap(), "queued");
    assert!(store.claim_send(id).unwrap());
    store
        .finish_send(
            id,
            DeliveryObservation::ConfirmedNotSent {
                evidence: "The channel refused the send.".into(),
            },
        )
        .unwrap();
    assert!(!store.replied_to("person-a").unwrap(), "refused");
    assert!(store.claim_send(id).unwrap());
    store
        .finish_send(
            id,
            DeliveryObservation::Unknown {
                evidence: "Transport disconnected after submit.".into(),
            },
        )
        .unwrap();
    assert!(store.replied_to("person-a").unwrap(), "may have arrived");
    assert!(!store.replied_to("person-b").unwrap());
}

/// Edited iMessages: a channel message read again with other text than its
/// latest version is an edit beside the original, never a refusal, even when
/// the text goes back to an earlier version; reading the latest text again is
/// that version again.
#[test]
fn a_changed_channel_message_is_an_edit_beside_the_original() {
    let (_dir, store) = fixture();
    let read = |body: &str| {
        store
            .append_channel_message(&inbound("native-1", body))
            .unwrap()
    };
    let original = read("Meet at 5.");
    assert_eq!(read("Meet at 5."), original);
    let edit = read("Meet at 6.");
    assert_eq!(edit.kind, "edit");
    assert_eq!(edit.reply_to, Some(original.seq.to_string()));
    assert_eq!(edit.native_id, None);
    assert_eq!(read("Meet at 6."), edit);
    let back = read("Meet at 5.");
    assert_eq!((back.kind.as_str(), back.seq > edit.seq), ("edit", true));
    assert_eq!(read("Meet at 5."), back);
    let later = read("Meet at 7.");
    let again = read("Meet at 6.");
    assert_eq!((again.kind.as_str(), again.seq > later.seq), ("edit", true));
    assert_eq!(store.edited_version(&edit).unwrap(), original);
    assert_eq!(store.edited_version(&again).unwrap(), later);
    let mut other = inbound("native-1", "Meet at 8.");
    other.author = "person-b".into();
    assert!(store.append_channel_message(&other).is_err());
    assert_eq!(store.event(original.seq).unwrap(), original);
    let bodies: Vec<_> = store
        .transcript("room", 0, 20)
        .unwrap()
        .into_iter()
        .map(|event| event.body)
        .collect();
    assert_eq!(
        bodies,
        [
            "Meet at 5.",
            "Meet at 6.",
            "Meet at 5.",
            "Meet at 7.",
            "Meet at 6."
        ]
    );
}

/// Edited iMessages: an edit takes no native id, so a channel message whose
/// own id looks like a generated one is a message of its own, whether it comes
/// before or after the edit (review finding, 2026-10-10: `G#edit:1` blocked
/// G's first edit).
#[test]
fn a_message_id_shaped_like_an_edit_never_collides_with_an_edit() {
    let (_dir, store) = fixture();
    let read = |native_id: &str, body: &str| {
        store
            .append_channel_message(&inbound(native_id, body))
            .unwrap()
    };
    read("native-1", "Meet at 5.");
    let early = read("native-1#edit:1", "Shaped like an edit, first.");
    let edit = read("native-1", "Meet at 6.");
    let late = read("native-1#edit:2", "Shaped like an edit, after.");
    assert_eq!(
        [early.kind.as_str(), edit.kind.as_str(), late.kind.as_str()],
        ["message", "edit", "message"]
    );
    assert_eq!(edit.body, "Meet at 6.");
    assert_eq!(read("native-1", "Meet at 6."), edit);
    assert_eq!(read("native-1#edit:2", "Shaped like an edit, after."), late);
}

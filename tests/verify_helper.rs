//! The checks the verify helper grades live drives with, run on synthetic data.
use std::process::Command;

/// Loads `.agents/skills/verify-oppa/scripts/oppa-verify` as a module and runs `checks`,
/// with a home folder of their own.
fn run_checks(checks: &str) {
    let home = tempfile::tempdir().unwrap();
    let helper = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(".agents/skills/verify-oppa/scripts/oppa-verify");
    let script = format!(
        "import importlib.machinery, importlib.util, sys\n\
         path = sys.argv[1]\n\
         loader = importlib.machinery.SourceFileLoader('verify', path)\n\
         verify = importlib.util.module_from_spec(importlib.util.spec_from_loader('verify', loader, origin=path))\n\
         verify.__file__ = path\n\
         loader.exec_module(verify)\n{checks}"
    );
    let output = Command::new("python3")
        .arg("-c")
        .arg(script)
        .arg(helper)
        .env("HOME", home.path())
        .env_remove("CLAUDE_CONFIG_DIR")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn each_checkout_keeps_its_own_current_instance() {
    // Incident: two sessions in different worktrees shared one current-instance
    // pointer under $TMPDIR, so a launch in one redirected the other's drive.
    run_checks(
        r#"
import importlib.machinery, importlib.util, pathlib, shutil, tempfile
def load(path):
    loader = importlib.machinery.SourceFileLoader("again", str(path))
    module = importlib.util.module_from_spec(importlib.util.spec_from_loader("again", loader, origin=str(path)))
    module.__file__ = str(path)
    loader.exec_module(module)
    return module
other = pathlib.Path(tempfile.mkdtemp()) / "checkout/.agents/skills/verify-oppa/scripts/oppa-verify"
other.parent.mkdir(parents=True)
shutil.copy(verify.__file__, other)
assert load(other).CURRENT != verify.CURRENT
assert load(other).CURRENT.parent == verify.CURRENT.parent
assert load(verify.__file__).CURRENT == verify.CURRENT
"#,
    );
}
#[test]
fn the_people_drive_grades_turns_silence_and_history_from_their_own_records() {
    // Review 2026-10-05 of the people-registry drive. A continuation counts only from a successful
    // history result holding the marker since the turn began; an unregistered input fails if it had
    // any turn, silent or not; an aside is silent only on the model's explicit NO_REPLY; and the
    // turns run at the read access the preflight set, whatever the instance had before.
    run_checks(
        r#"
import json, pathlib, tempfile, types
spot = "Cove1a2b3c"
use = lambda i, name: {"timestamp": "2031-01-01T10:00:01.000Z", "type": "assistant",
                       "message": {"content": [{"type": "tool_use", "id": i, "name": name}]}}
result = lambda i, text, error=False, at="2031-01-01T10:00:02.000Z": {"timestamp": at, "type": "user",
    "message": {"content": [{"type": "tool_result", "tool_use_id": i, "is_error": error,
                             "content": [{"type": "text", "text": text}]}]}}
found = lambda transcript, since="2031-01-01T10:00:00": verify.history_found(transcript, since, spot)
hit = json.dumps({"hits": [{"seq": 9, "body": "The picnic is at " + spot}]})
assert found([use("a", "mcp__oppa__history_search"), result("a", hit)])
assert found([use("a", "mcp__oppa__history_read"), result("a", hit)])
# The right answer beside an unrelated broker read proves no retrieval.
assert not found([use("a", "mcp__oppa__memory_search"), result("a", hit)])
assert not found([use("a", "mcp__oppa__history_conversations"), result("a", "[]"),
                  use("b", "mcp__oppa__memory_read"), result("b", hit)])
assert not found([use("a", "mcp__oppa__history_search"), result("a", hit, error=True)])
assert not found([use("a", "mcp__oppa__history_search"), result("a", "{\"hits\": []}")])
assert not found([use("a", "mcp__oppa__history_search"), result("a", hit)], since="2031-01-01T10:00:03")

root = pathlib.Path(tempfile.mkdtemp())
inst = verify.Instance(root)
runs = root / "runtime" / "runs"
runs.mkdir(parents=True)
def turn(seq, state, answer, attempt=1, agent="abc", ended="2031-01-01T10:00:00Z"):
    (runs / f"turn-{seq}.json").write_text(json.dumps({"state": state}))
    (runs / f"{seq}-{agent}-{attempt}.json").write_text(json.dumps(
        {"input_seq": seq, "attempt": attempt, "ended_at": ended, "outcome": {"answer": answer},
         "broker": {"read_calls": 0}}))
assert not verify.had_turn(inst, 3)
turn(3, "completed", "NO_REPLY")
assert verify.had_turn(inst, 3) and not verify.had_turn(inst, 30)
(runs / "turn-4.json").write_text(json.dumps({"state": "started"}))
assert verify.had_turn(inst, 4)
assert verify.chose_silence(inst, 3)
turn(5, "completed", "")
turn(6, "completed", "  ")
turn(7, "failed", "NO_REPLY")
turn(8, "completed", "Sure, candles too.")
assert not any(verify.chose_silence(inst, seq) for seq in (5, 6, 7, 8, 30))
turn(9, "completed", "Sure.", attempt=2, ended="2031-01-01T10:00:01Z")
turn(9, "completed", "NO_REPLY", attempt=1, agent="fed", ended="2031-01-01T10:00:02Z")
assert verify.chose_silence(inst, 9) and [r["attempt"] for r in verify.attempts(inst, 9)] == [2, 1]
turn(11, "completed", "NO_REPLY", attempt=2, ended="2031-01-01T10:00:01Z")
turn(11, "completed", "Sure.", attempt=1, agent="fed", ended="2031-01-01T10:00:02Z")
assert not verify.chose_silence(inst, 11)

# The drive's turns run at read access even when the instance had more.
(root / "private").mkdir()
config = root / "private" / "config.toml"
original = 'version = 1\nowner = "owner"\naccess = "connectors"\nbrowser = false\n'
config.write_text(original)
inst.state["agent"] = "claude"
verify.preflight = lambda inst, feature: verify.set_access(inst, "read")
seen = []
class Started(Exception):
    pass
def popen(*args, **kwargs):
    seen.append(config.read_text())
    raise Started()
verify.subprocess = types.SimpleNamespace(**{**vars(verify.subprocess), "Popen": popen})
try:
    verify.drive_people_registry(inst)
except Started:
    pass
assert len(seen) == 1 and 'access = "read"' in seen[0] and 'access = "connectors"' not in seen[0], seen
assert "[[people]]" in seen[0] and config.read_text() == original
# The family drive's run at computer access with the browser on, the child registered, and leave the
# instance's configuration as it found it.
# Its cleanup looks for the instance's Claude session records, which a fixture has none of.
verify.purge_harness_transcripts = lambda inst: None
try:
    verify.drive_family_turns(inst)
except Started:
    pass
assert len(seen) == 2 and 'access = "computer"' in seen[1] and 'access = "connectors"' not in seen[1], seen
assert 'role = "child"' in seen[1] and 'browser = true' in seen[1] and config.read_text() == original
# Prompt facts come only from the host's own prompt for the graded trigger. The same fields in the
# assistant's output, a tool's result or another turn's prompt are not the prompt (review ed8007d4).
facts = {"requester_role": "adult", "first_reply_to_requester": True,
         "shared_group": {"you_take_questions_addressed_to_neither_assistant": False,
                          "turn_began_seconds_after_message": 21}}
def prompt(seq):
    return verify.HOST_PROMPT + json.dumps({**facts, "trigger_event": {"seq": seq}}, indent=2)
spoofed = [{"type": "assistant", "message": {"content": [{"type": "text", "text": prompt(7)}]}},
           {"type": "user", "message": {"content": [{"type": "tool_result", "tool_use_id": "t",
                                                     "content": [{"type": "text", "text": prompt(7)}]}]}},
           {"type": "user", "message": {"role": "user", "content": prompt(6)}}]
assert verify.host_prompt(spoofed, 7) is None
assert verify.host_prompt(spoofed + [{"type": "user", "message": {"role": "user", "content": prompt(7)}}], 7) == \
    {**facts, "trigger_event": {"seq": 7}}
assert verify.host_prompt(spoofed + [{"type": "user", "message": {"content": [{"type": "text", "text": prompt(7)}]}}],
                          7)["shared_group"]["turn_began_seconds_after_message"] == 21
# Two prompts for one trigger cannot say which was graded.
twice = [{"type": "user", "message": {"content": prompt(7)}}] * 2
assert verify.host_prompt(twice, 7) is None
"#,
    );
}

#[test]
fn the_family_drive_needs_a_real_picture_named_for_the_request_and_a_question_to_the_owner() {
    // G-10, incident 2026-10-12 drive grading: the child's turns must be family
    // turns at computer access; a picture counts only as an attachment the
    // channel was given, named with the request's word and holding image bytes,
    // sent through the broker; the game request must ask the owner.
    run_checks(
        r#"
import copy
prompt = {"requester_role": "child", "access": "computer",
          "capability_notice": "This turn is for a family member ... the family rules in your instructions ..."}
def turn(writes):
    return {"event": {"author": "kid", "turn": "completed"}, "prompt": dict(prompt), "writes": writes}
picture = turn([{"tool": "messages.send"}])
game = turn([{"tool": "messages.ask_owner"}])
png = {"name": "sketch0a0b0c.png", "path": "/x", "bytes": b"\x89PNG\r\n\x1a\n...."}
family = [{"body": "Here you go!", "attachments": [png]}]
owner = [{"body": "Kid asks for a dinosaur game. OK?"}]
grades = verify.family_turn_grades(picture, game, "sketch0a0b0c", family, owner)
assert all(grades.values()), grades
def fails(grade, **changes):
    args = {"picture": copy.deepcopy(picture), "game": copy.deepcopy(game), "word": "sketch0a0b0c",
            "family_sends": copy.deepcopy(family), "owner_sends": copy.deepcopy(owner)}
    args.update(changes)
    assert verify.family_turn_grades(**args)[grade] == 0, (grade, changes)
restricted = turn([{"tool": "messages.send"}]); restricted["prompt"]["access"] = "read"
fails("child_turns_are_family_turns", picture=restricted)
failed = turn([]); failed["event"]["turn"] = "failed"
fails("child_turns_are_family_turns", game=failed)
fails("picture_sent_to_the_group", family_sends=[{"body": "I can't make pictures.", "attachments": []}])
fails("picture_sent_to_the_group", family_sends=[{"attachments": [{**png, "name": "other.png"}]}])
fails("picture_sent_to_the_group", family_sends=[{"attachments": [{**png, "bytes": b"<svg"}]}])
fails("picture_sent_to_the_group", family_sends=[{"attachments": [{**png, "bytes": None}]}])
fails("picture_sent_through_the_broker", picture=turn([]))
fails("game_asked_the_owner", game=turn([]))
fails("game_asked_the_owner", owner_sends=[])
"#,
    );
}

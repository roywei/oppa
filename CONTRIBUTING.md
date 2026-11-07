# Contributing

[AGENTS.md](AGENTS.md) holds the working rules
for people and coding agents alike.

## Verification

Each meaningful boundary uses a synthetic fixture plus a real acceptance exercise
where a device, provider or external effect is involved. The public CI never uses
private accounts. Deterministic checks cannot establish conversational discretion,
real model latency, phone delivery, sign-in or reviewer judgment.

A model check proves retrieval, not luck. Put a random marker where only the tool
path can reach it, grade the tool evidence as well as the answer, and fail a
correct answer that skipped retrieval. Privacy checks plant a private marker and
fail if it appears in the reply. Each check prints grades, counts, timing and the
requested model settings, never prompts, answers or markers. A failed run is kept
next to any later passing run, and a check is not retried blindly. A preflight
first proves the harness can reach the broker from inside its sandbox, so a
permission fault is not scored as a model failure.

Judgment cases cover both directions: a closing "thanks" after a delivered result
may end in a reaction or silence; "thanks, also move it to Friday" is new work;
distress or a complaint after a failure gets a real answer; an explicit request
for silence gets none; empty output is a failure, not silence.

Production dependencies are part of the change. A pinned harness version, a model
name and an account's access move independently of the source. Before a release
is called ready, a canary runs through the installed build with the real login
and the configured models, without reading live conversations or sending.

Before an implementation batch is integrated: format changed Rust; cargo test
--offline; git diff --check; privacy and complexity audit; motivating replay. Record
failures and limits in CHANGELOG, with recovery instructions next to the command
they concern.

Every feature gets a separate review pass before merge/release, against the
principles in AGENTS.md. Any capable
model or harness may review, whichever model or harness wrote the change:
1. Which requirement and principle does this serve?
2. Does host code infer meaning, hide needed context, or write conversational prose?
3. What does it delete, or why is this first construction within the budget?
4. Which actual replay or interaction covers it?
5. What would cause removal, and how will we detect that?
6. Are the process, delivery, permission and recovery dependencies verified?
7. What is the net line change in core and edges, what did it delete, and what
   in the largest or fastest-growing code could be removed or simplified? Which
   measure moves?
8. Does each new or changed test carry signal, and is any test flaky or redundant?

## Release discipline

An incident becomes a replay case first, then an instruction or tool change, and
host code only with evidence that judgment cannot supply. A change to the
conversation path first passes the private replay cases; after an owner-approved
install it may run in shadow on the owner's instance, without sending, before it
is enabled. No same-day release chains: a release that needs a fix
within a day is rolled back first, then fixed. A definition of done does not
defer the check that matters; if the change is visible on a phone, the phone
check is part of it. Operations come first: restart, drain, permissions and quota
detection are solid before behavior builds on them.

Unattended coding runs work in an isolated worktree, never commit to the main
checkout, and record the executor's exit status, the diff and the test gate
separately, so a green report cannot hide a red test.

One owner runs each service. A service manager and a restart script that both
start the same process race, and the superseded build may keep running. On macOS
the holder of OS permissions (calendar, messages, automation) is a stable signed
identity; rebuilt ad hoc binaries lose grants, and a background service does not
inherit grants given to a terminal. Without a signing identity, `oppa-launch` is
meant to be that holder: launchd starts it, releases never rebuild it, and it
runs `oppa` as a child. A service that reads configuration only at
start says so and is restarted on change. A stopped service cannot report its own
outage, so total failure needs a separate notifier or an operator watch.

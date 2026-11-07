<picture>
  <source srcset="site/assets/oppa-lockup-dark.svg" media="(prefers-color-scheme: dark)">
  <img src="site/assets/oppa-lockup-light.svg" alt="OPPA" width="280">
</picture>

# OPPA

OPPA (Open Personal Assistant) is a self-hosted chief of staff: one conversation,
portable memory, tools shared by capable agents. It is under active
construction.

It runs on your Mac with your Claude Code or Codex subscription. You talk to it
in web chat or by iMessage, and so can the people you register, each with their
own role. Your own turns are your harness as you set it up: every account, MCP
server and plugin you connected there, your browser, and, with full access,
this Mac's shell and files. It keeps your memory as Markdown in a private Git
repository, keeps public knowledge in a separate wiki, runs reminders, briefings
and background tasks, looks in during the day once something is connected, and
improves its own skills overnight.

It starts with almost nothing bundled and grows by building for you. Ask for
something new, such as your lights or your car, and it writes the script and a
skill, reading any key from the 1Password vault you share with it, and reuses it
after. It can propose changes to its own code on a branch for you to review.

## Set up

Give this README or the repository URL to Claude Code or Codex and ask:

> Set up OPPA on this Mac. Follow the setup-oppa skill in the repository.

The coding agent checks the prerequisites, builds, configures and installs OPPA.
First setup uses that same agent as the driver: Claude Code chooses Claude;
Codex chooses Codex. You can request another driver. Repairs and upgrades keep
the existing choice.

You choose a name, voice and channel, approve macOS permissions, and send one
message. Once OPPA answers on your phone or dashboard, continue talking to OPPA.
It learns your language and household, offers memory import, and introduces
optional abilities one at a time. The setup agent leaves a note with your choices
and remaining steps so you do not need to start over.

It assumes a Mac with Rust 1.89+, Git, the Xcode command line tools and a
signed-in Codex or Claude Code CLI subscription. OPPA uses the CLI's existing
sign-in. It creates no API keys, copies no credentials and connects no
household accounts.

For a Codex-only Mac, no Claude installation or API key is needed. Check
`codex --version` and `codex login status`; if needed, install the CLI with
`brew install --cask codex`, then run `codex login` and choose **Sign in with
ChatGPT**. OPPA uses that subscription's model access and usage limits.
[OpenAI's sign-in guide](https://learn.chatgpt.com/docs/auth) explains the difference
between subscription and API billing. Either driver uses its own default model
unless you request one.

For manual setup, set `OPPA_DRIVER` to `claude` or `codex` for the subscription
you want OPPA to use. The setup agent sets this from its own identity:

```sh
cargo build --release --locked
./target/release/oppa init --agent "$OPPA_DRIVER" --web --imessage +15555550100
./target/release/oppa chat --message "hello"    # proves the harness sign-in
# edit ~/.oppa-private/config.toml: connectors, below
./target/release/oppa service install           # copies the build, writes the launch agents
# grant Full Disk Access to ~/.oppa/bin/oppa-launch
~/.oppa/bin/oppa service check                  # macOS asks for Calendars etc. as the service
~/.oppa/bin/oppa service start
~/.oppa/bin/oppa service status
```

Once it answers, add more channels by asking in chat ("set up WeChat", "I want
to text you on iMessage"); the assistant walks you through it with
`oppa channels add` and a running serve starts the channel with no restart.

## Configuration

`oppa init` asks where you will talk to the assistant and writes those channels
and your `[[people]]` entry: `--web` for web chat on this Mac (port 8765, or
`--web <port>`), `--imessage` with your phone numbers and email addresses for
iMessage on the assistant's own Apple ID. Run without either at a terminal, it
asks; either flag answers both questions, so `--imessage` alone adds no web chat;
a script that names neither gets the console alone. An email address matches
your messages in any case.

To answer on another messaging surface, add a `[[channels]]` entry to the private
`config.toml` naming an adapter command and the sender handles that are yours,
then run `./target/release/oppa serve`. The bundled adapters in
`resources/adapters` show the contract.

The bundled iMessage adapter wraps `imsg rpc` (imsg 0.15 or later) on a Mac whose
Messages app is signed in to the assistant's account. `init --imessage` writes its
channel with the bridge at `<runtime>/channels/imsg-bridge/imsg`; put imsg there
(keeping its helper files beside it) before starting `serve`. The entry below is
for an instance made without `--imessage`. The process running
`oppa serve` needs Full Disk Access to read the Messages database; under launchd
the grant is meant to go to `oppa-launch` ([The service](#the-service)).
`serve` installs the adapter from its own build into `<runtime>/adapters` on every start,
so point the command there rather than at a source checkout:

```toml
[[channels]]
id = "imessage"
account = "assistant"
command = ["/usr/bin/python3", "/path/to/runtime/adapters/imessage.py",
           "--state", "/path/to/runtime/channels/imessage", "--imsg", "/path/to/imsg"]
timeout_seconds = 45
account_holder = "assistant"   # "owner" attaches your own account: read, never answered

# Who each sender is. A handle is "<channel id>:<sender handle>". Your entry
# uses your owner id and role "owner"; only your messages, adults' and
# children's get a turn, each at your own access (see Current boundaries).
# You can also say who someone is in chat; the assistant asks, and your yes
# writes these tables. Serve reads them for each message: no restart.
[[people]]
id = "owner"
role = "owner"
handles = ["imessage:+15555550100"]

[[people]]               # roles: adult, child, friend, guest, agent
id = "sam"
role = "adult"
handles = ["imessage:+15555550101"]

# Your settings for one group, also set in chat. Where another person's
# assistant (role "agent") shares it: whether this assistant takes questions
# addressed to neither, and how long turns for anyone but you wait to see what
# the other one said. new_people: whether you hear when someone who gets no
# turn writes there (unset: where the channel lists up to six members).
[[groups]]
conversation = "imessage/assistant/<chat id>"   # from `oppa conversations`
takes_unaddressed = true
wait_seconds = 20         # 1 to 120
new_people = false

[sessions]
idle_minutes = 360        # start fresh after six idle hours; 0 disables
daily_rotation = "04:00"  # and once a day at this local time; "" disables
```

The bundled web adapter serves a chat page and the owner's dashboard on this
Mac; `serve` installs it with the others. Add:

```toml
[[channels]]
id = "web"
account = "local"
command = ["/usr/bin/python3", "/path/to/runtime/adapters/web.py",
           "--state", "/path/to/runtime/channels/web",
           "--home", "/path/to/runtime", "--private-dir", "/path/to/private",
           "--port", "8765"]
```

and add `"web:owner"` to the `handles` of your owner entry in `[[people]]`.
The page runs the same `oppa` that `serve` runs; `--oppa <path>` names another.

Restart `serve`, then open the address in `<runtime>/channels/web/login-url` in
a browser on the Mac; it sets a cookie for that browser. The page answers only
on 127.0.0.1 and its own address and refuses writes from other sites. Its tabs
hold chat, scheduled jobs, access, personality, rules, people and health. What
the assistant could improve about itself it tells you in its morning message.
Delete `<runtime>/channels/web/token` to sign every browser out.

The iMessage adapter starts at the current end of the Messages database on first
run and never answers older history. Its watch cursor lives in `--state`; after a restart it
replays a short overlap, which the host logs once. Do not run it on an account
another assistant service is already answering.

To let the assistant use the Mac's calendars (including Google accounts added
to the Calendar app), reminders and contacts, add the bundled Mac connector and
check its access:

```toml
[[connectors]]
id = "mac"
kind = "script"
command = ["/usr/bin/python3", "/path/to/runtime/adapters/apple.py"]
timeout_seconds = 60   # the first call compiles the adapter once
```

```sh
./target/release/oppa connectors test          # access as this terminal, with a plain fix
./target/release/oppa connectors test --prompt # at the Mac: lets macOS ask for access
```

It reads at every `access` level. At `connectors` or `computer` it can also
create, change and delete events, reminders and contacts; OPPA journals each
change and never repeats one whose outcome it lost. The Allow that macOS asks
for covers reading and writing. The service needs its own Calendars, Reminders
and Contacts permission; a terminal's permission does not carry over to a
background service.

Restart an already-running `oppa serve` after editing `config.toml` so it loads the new connector list.

What the assistant may do with the accounts connected in the harness depends
on `access`, a top-level line in `config.toml` that `oppa settings access
<level>` (or the dashboard) sets for you:

```toml
access = "connectors"   # or "computer", or "read"
```

- `connectors` (the default): the harness as you set it up, with every
  account, MCP server, plugin and setting you gave it, read and write, plus web
  search and your browser. Sends, deletes and other changes happen without a
  confirmation step. No shell or files.
- `computer` (also `full`): the same, plus every tool and file your login can
  reach. The service holds Full Disk Access, so this includes Mail, Messages and
  every other file. The assistant builds skills for new abilities here.
- `read`: none of your harness setup; on Claude, only the Gmail, Google
  Calendar and Drive read tools, so nothing can be sent or changed.

The browser (Claude in Chrome, Codex browser use) is on unless `config.toml`
says `browser = false`. Each `[[agents]]` entry may set `model` and `effort`
(`low`, `medium`, `high`, `xhigh`, `max`; medium by default); hard work goes to
a subagent at high effort either way.

Background tasks run with the same access setting as your turns, so
at `computer` a delegated task can write and run a program; each agent's
`task_timeout_seconds` (default 3600) bounds one. Result presentations read only.
A connector that still needs sign-in cannot be used. `oppa settings` shows what
is configured.

Keys and passwords come from a 1Password vault you share with the assistant
(`oppa init --vault OPPA`, with the service account token in the Keychain under
`oppa-1password`). New abilities, such as your lights or your car, need no
setup: ask, and the assistant writes the script and a skill in
`~/.oppa-private/skills`, reading any key from the vault, and reuses it after.

OPPA's memory is the source of truth. The assistant offers in chat to take in
what Claude Code and Codex already remember about you; after your yes it does so
in the background, then every day at the time `[memory_sync] at` sets.
`oppa memory sync` runs one batch at the console.

Each night, half an hour before that sync, the assistant reviews the day's
conversations and keeps what they taught as skills and memory, and may start a
`self/<topic>` branch with a fix to its own code (at `computer`, with `source`
set to this checkout); it never merges or installs one. It also sets the next
day's heartbeat pace, 30 to 480 minutes, from how much your channels needed you.
It sends nothing at night: the day's first heartbeat tells you what it improved
and what waits on you. `oppa memory review` runs the review at the console.

Memory is what is personal: you, your family and the people around you, your
home, health and holdings. What you read and research (articles, filings,
papers, and theses built on them) goes in a knowledge wiki of its own at
`~/.oppa-private/knowledge`: a Git repository of Markdown pages with its own
AGENTS.md. `oppa init` at full access starts one, or links the one you already
keep (`--knowledge PATH`; `oppa knowledge --link PATH` on an existing
instance). Ask the assistant to digest something and it is kept there, each
claim cited with its source.

How the assistant sounds and behaves, and its rules for groups, friends, what
may be shared with whom and which actions are pre-approved, are two notes in the
vault that reach every session: `assistant/persona.md` and
`assistant/rules.md`. Edit them in the dashboard, with `oppa memory put`, or
directly.

For standing work, just ask: "remind me at 7 tonight", "post the briefing in the
family group every weekday at 7". The assistant keeps a schedule in the
instance, and `serve` fires it at its local time into its conversation as your
turn. From a terminal, `oppa schedules add --conversation <id> --at 07:00
[--date 2026-10-04 | --days mon,wed] --prompt "..."` adds one, `oppa conversations` lists the
conversation ids it can name, `oppa schedules` shows each one's next slot and
how its last one went, `oppa schedules cancel <id>` stops one and `oppa
schedules run <id>` fires a console schedule now. Older `[[schedules]]` entries
in config.toml must move to `oppa schedules add`; a configuration that still has
them does not load. At `connectors` or `computer`, the assistant can also say
something now in another chat it is in ("tell the family group dinner's at 7").

Once anything is connected, such as your mail, the heartbeat runs in your direct
chat with the assistant: by default twice a day, at 08:30 and 16:30, until the
nightly review sets a pace. With
nothing connected there is none. To change it, add it to config.toml and
restart serve:

```toml
[heartbeat]
every_minutes = 60        # 5 to 1440, counted from waking_from; default 480; the nightly pace overrides it
waking_from = "08:00"     # local time of the day's first heartbeat; default 08:30
waking_until = "21:00"    # none starts at or after this; default 21:00
conversation = "imessage/assistant/<chat id>"   # optional: your direct chat, from `oppa conversations`
digest = "Sun 18:00"      # optional: a weekly digest, in waking hours
# off = true              # no heartbeat, and no wakes
```

On that cadence, only in waking hours, the assistant looks at what is new since
its last heartbeat and usually says nothing. Between heartbeats it may set
itself a wake for a closer look at a known time (a restock at ten, a delivery
due this afternoon), at most 12 in a day; ask what it is keeping an eye on to
see them. When something needs you, such as a
bill due in a note, it sends one suggestion that says what prompted it and asks.
A heartbeat only reads, whatever your access setting; your reply is a turn of
its own. Decline a suggestion and it is noted and not raised again; ask it to
stop suggesting about a kind of thing ("no more bill reminders") and that is
noted too, in `assistant/declined.md`, which every heartbeat is shown. With
`digest` set, once a week at that time you get one message on what the week's
heartbeats suggested and what came of each, or nothing when none spoke or
failed. Each heartbeat, wake and digest costs a model turn, most of them silent. `oppa doctor` says whether
the chat you named is one the heartbeat may speak in: your direct chat, seen
there with a message from you and none from anyone else.

Initialization asks for the harness when run interactively. It creates new private
runtime and configuration directories next to each other, outside the checkout:
`~/.oppa` and `~/.oppa-private`. Existing directories are preserved;
choose a different `--home` to try a separate instance. A private Git repository
holds configuration; its ignored `vault/` is a separate memory repository. No
remote is added. Transcript SQLite, traces and logs stay in runtime, outside Git.

To use disposable custom paths, add `--home /tmp/example-runtime --private-dir
/tmp/example-private` to each command. The private directory is sensitive even
when it currently contains only synthetic examples. Real values never belong
in this source tree.

```sh
./target/release/oppa history --conversation console
./target/release/oppa history --query 'meeting 时间'
./target/release/oppa outbox
./target/release/oppa memory revision
./target/release/oppa memory search 'preferences'
```

`memory put` and `memory delete` take an explicit current revision, a source and
a reason. `memory import` reads the current revision itself and brings a folder
of notes in as one commit, refused if the vault changes between its plan and its
commit; run it with `--dry-run` first, and stop `oppa serve` and `oppa work`
before a large folder, since memory tools wait while it writes. Markdown is also
editable directly. Managed commits refuse conflicting owner edits and never
silently stage unrelated changes. `memory delete` is logical file deletion.
`oppa memory forget '<text>'` lists where a text is kept, with a digest;
`--confirm <digest>` then removes it from notes and their Git branches, message
bodies, conversation titles and the native sessions OPPA keeps for them, and
lists where it is still found (run records, task boards, reflogs and others),
which it never changes. In chat, ask the assistant to forget something; it
shows the same lists and waits for your go-ahead.

## Background work

Run `oppa work` in a second terminal using the same instance paths. It executes
queued tasks, with your access, independently of `oppa chat`. In chat, ask for a longer
synthesis and continue the conversation; the model can delegate through the shared
broker. Keeping interactive chat open allows it to present completed work while
waiting for your next message. `chat --message` performs one turn and exits.

```sh
./target/release/oppa work
./target/release/oppa tasks list
./target/release/oppa tasks read TASK_ID
./target/release/oppa tasks stop TASK_ID
./target/release/oppa tasks present --conversation console
```

`work --once` claims at most one queued task. A queued task waits until a worker is
running. Registered agents come from the private configuration; workers use fresh
sessions and read-only history/memory tools. This is trusted-owner execution, not
a child sandbox. Tasks have a private `TASK.md`, `LOG.md`, `EVIDENCE/` and result
artifact. Workers do not send replies directly: the conversation model integrates
the recorded result once. Presenting a result has read-only permissions.

A stop request for running work remains pending until the owned process group is
confirmed stopped. Do not kill a runner to simulate a completed cancellation.
A crashed or interrupted task is not automatically reclaimed. The runner holds a
kernel lock on the task's `attempt.lock` for the whole attempt; that lock being
free is the only accepted proof that the runner is gone. `tasks reconcile TASK_ID`
applies an existing receipt without repeating work, and records a running task
whose lock is free as interrupted (or stopped, if a stop was requested). A missing
receipt is not proof that no work happened; inspect the board and execution
evidence. `tasks resume TASK_ID` queues a new attempt for an interrupted or failed
task only when that lock is free. Uncertain or failed presentation stays
inspectable and is never automatically redelivered.

## The service

`oppa service` runs the instance as two macOS launch agents, `ai.oppa.serve` and
`ai.oppa.work`, which start at login and restart on exit. Installing, starting
and granting need the owner present. No agent or scheduled job may do them on
its own.

| Command | Effect |
| --- | --- |
| `service install` | Copies this build to `<runtime>/bin/oppa`, installs and signs `oppa-launch` the first time only, and writes both plists. Starts nothing. |
| `service start` | Loads both agents, restarting any that run, so an edited plist takes effect. |
| `service check` | Runs `oppa connectors test --prompt` as a one-shot job through the launcher, so macOS asks for, and reports, the service's own access. |
| `service status` | Shows the installed build, the launcher's signature, and each agent's state, pid and log tail. |
| `service stop` | Stops both agents until the next login. |
| `service uninstall` | Stops and removes both agents. Binaries, logs and records stay. |
| `service upgrade` | Installs this build (or `--from <build>`) in place of the service's: first a canary turn by it on a copy of the instance, then the agents stop, it takes `oppa`'s place with the previous build kept as `<runtime>/bin/oppa.previous`, and both agents must start and keep running for five seconds, or the previous build goes back. A release that moves the database to a newer version needs `--migrate`; once it has moved the live database it is not rolled back, since the previous build would refuse it. |

The first install is `cargo build --release --locked`,
`./target/release/oppa service install`, then `oppa service start`. A release
after that is `cargo build --release --locked`, then
`./target/release/oppa service upgrade`, from a terminal: an upgrade stops the
service, so a run inside it may not start one, and it leaves the service running
even if it was stopped. The canary turn runs on a copy of the private folder at
read access with no connector (its channels stay, since people's handles name
them, and `oppa chat` never starts one), in a fresh conversation, with an empty
database shaped like the live one, so none of the owner's conversations, native
sessions or accounts reach it; the copy is removed afterwards, and the harness
keeps its own record of the turn, as for any turn. An upgrade keeps the agents'
plists: a release that changes how they are written, or a harness CLI that
moved, needs `service uninstall` then `service install`.
`OPPA_SERVICE_LABEL=ai.oppa.trial` gives every `oppa service` command another
label base for a trial instance, given with `--home` or `OPPA_HOME`, so its
agents never meet the owner's: install, start and upgrade refuse an instance
that another label base's agents already serve.

The plists run `oppa-launch serve` and `oppa-launch work`. `oppa` is signed ad
hoc and every release replaces it. A privacy grant (Full Disk Access, Calendars,
Reminders, Contacts, Automation) given to an ad hoc signed program is pinned to
that build, so grants go to the launcher instead, which releases never replace.
The launcher runs the `oppa` beside its real path as a child. It forwards
SIGTERM, SIGINT and SIGHUP, and kills the child 25 seconds after the first
signal (`OPPA_LAUNCH_GRACE_SECONDS`), inside the agents' 30-second
`ExitTimeOut`. `--replace-launcher` installs a new launcher, and macOS then
needs every grant again.

This rests on a premise, not yet confirmed with real grants on a running
service: macOS attributes a launch agent's privacy access to the program launchd
starts, and its children use it. `service check` passing after the grants,
followed by a reply from the phone that needs the calendar, confirms it. The
same reply after the next release, with no new grant, confirms that grants
survive releases.

**Security trade-off.** Grants given to the launcher cover whatever file sits at
`<runtime>/bin/oppa` when an agent starts, including code that malware running
as you put there, and everything that code starts: the adapters, `imsg` and the
harness CLIs. The same holds for every directory on the agents' PATH, which the
first `service install` takes from the installing shell (system directories
first) and later installs keep. Keep `<runtime>/bin` and those directories
writable only by you. To roll back, run
`oppa service uninstall` and remove `oppa-launch` from Full Disk Access.

## Current boundaries and recovery

Sender identity on a channel is adapter evidence mapped to a person through the
`[[people]]` handles, read for each message; messages from you and from people
you register with role `adult` or `child` get a turn, and everyone else is
logged. An agent's message never starts a turn. You can register someone in
chat: the model proposes, and only a later message of yours where it asked,
after the question reached you and within a day, applies the change; taking
something away applies at once. When someone who gets no
turn writes in a direct chat or a group listed with up to six members, the assistant tells you in
your direct chat, and may propose letting them ask directly. Each turn names its
requester and their role, and whether the assistant has replied to them before,
so it can say whose assistant it is the first time. A family member's turn, an
adult's or a child's, runs at your own access:
history in every conversation, your direct chats included, the memory vault,
your connected accounts and, at `computer`, this Mac. Instructions guard it, not
the host: the manual's family rules (never move money; opening the home,
building something new or acting as you waits for your yes; a child's projects
stay on the home network) and your rules note, and it asks you in your direct
chat for what they leave to you. Register as family only people you would trust
with your access. In a conversation you share,
its session still holds what your earlier turns there read. The prompt names the owner and each registered person
who has written in the conversation, with their role, and in a group every
member the channel lists, by handle. In a group you list under
`[[groups]]`, each turn says whether this assistant takes questions addressed to
neither assistant, and turns for anyone but you start only after the group's
wait, so the model reads what the other assistant said meanwhile; the model, not
the host, decides whom a question was addressed to. A serve stopped during a
wait leaves that message logged without a turn until the channel delivers it
again after the restart (iMessage replays the 20 messages
before its saved cursor); then it runs once, late. There is no audience policy yet. A configuration that still has `owner_handles` is
refused: move each handle to your `[[people]]` entry as `"<channel id>:<handle>"`. Conversation harnesses
receive the instance's history, memory, task, schedule and message tools and the
configured connectors; at `connectors` or `computer` access they also reach the
harness's own accounts, and at `computer` its shell and files, as described
under Configuration. Harness adapters are trusted code and inherit local authentication.
No guest or remote-agent execution is exposed. Native harnesses keep their
own private session caches outside these instance directories; the current vault
CLI does not erase those caches.

`doctor` checks local storage and launcher availability; authentication is proved
by an actual reply. A missing CLI/login requires repairing that harness. It does
not try alternate account access. Inspect private run records for bounded failure
categories. A quota/auth/unavailable failure with complete telemetry can continue
through the next configured harness after reads or confirmed broker commits. The
next model receives prior receipts. Pending writes, unmediated tool activity and
incomplete telemetry stop automatic continuation for reconciliation. A transport write with uncertain delivery stays unknown;
inspection does not resend it. Do not delete state to clear an uncertain action.

Fresh sessions
receive up to 64 recent events within a 96 KiB event budget, plus profiles and
explicit retrieval pointers. Large bodies and older messages remain in canonical
storage and can be retrieved through tools. A turn's grant names its own
conversation and the channel account it belongs to; the model discovers other
conversations in that account with a paged tool instead of receiving a list.

## Develop

```sh
cargo fmt --check
cargo test --offline --locked
python3 scripts/audit.py
git diff --check
```

Public fixtures use invented data. The audit is a
heuristic supplement to review; it cannot prove a tree contains no secrets.

Licensed under Apache-2.0.

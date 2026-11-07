---
name: setup-oppa
description: Set up OPPA on this Mac for its owner, from a fresh clone to a reply on their phone. The agent builds, configures and installs the service; the owner names the assistant, picks how it sounds and looks (Codex draws four faces to choose from), answers a few questions, grants macOS access with a couple of clicks and sends one text; the assistant itself finishes getting to know them in chat. Use for "set up OPPA", "install OPPA", onboarding, moving to a new Mac, repairing a service that stopped answering, upgrading an instance from before the soul note, or turning on iMessage tapbacks and the typing bubble.
---

# Set up OPPA

You do the work; the owner decides and clicks. Everything a command can do, you
do. Ask the owner only for what no command can supply: a name, a personality
and a look for the assistant, which accounts to connect, their own phone number, macOS
permission clicks, and one text from their phone. Batch the questions into one
round. The owner may never have used a terminal: say what you are doing in
plain words, and never ask them to type a command.

Setup ends at the first reply on their chosen channel (phone, dashboard or
terminal). Write the onboarding note before that handoff, with choices already
made and any optional setup still pending, so OPPA does not ask them again.
Everything else, such as which
language to reply in, the household, what other assistants know about them and
the abilities they have not tried, the assistant takes up itself in chat, one
thing at a time, from the onboarding note you write in step 5.

The owner is present for the whole run, which is what installing a service and
changing macOS permissions require. Never grant or widen a permission yourself,
never edit System Settings databases, and never pass `--replace-launcher` unless
the owner asks (it discards every grant).

Use the release build: `B=./target/release/oppa` until it is installed, then
`~/.oppa/bin/oppa`. The instance lives in `~/.oppa` (runtime) and
`~/.oppa-private` (config and memory). On macOS these names match `~/.OPPA`.
Nothing private goes into this repository.

## 1. Look before touching

```sh
sw_vers -productVersion; uname -m
ls -d ~/.oppa ~/.oppa-private 2>/dev/null
~/.oppa/bin/oppa service status 2>/dev/null
```

If an instance exists, this is a repair or an upgrade: skip `init`, keep its
config and memory, and go to the first step whose check fails. An instance
from before the soul note has `assistant/persona.md`; see "Upgrading to the
soul note" below. Never delete or
re-create an instance; if the owner wants a fresh start, move the old
directories aside with a dated name and say where.

## 2. Prerequisites

Check each; install only after saying what you will install.

| Need | Check | Fix |
| --- | --- | --- |
| Rust 1.89+ | `cargo --version` | `brew install rust`, or rustup |
| System Python and command line tools | `/usr/bin/python3 --version`, `xcode-select -p` | `xcode-select --install` (the owner clicks Install) |
| One signed-in CLI | Codex: `codex --version` and `codex login status`; Claude: `claude --version` | use the owner's chosen subscription; see below |
| imsg 0.15+ (only for iMessage) | `imsg --version` | Homebrew's tap can be older. Download the latest release from github.com/openclaw/imsg into `~/.oppa/channels/imsg-bridge/` after step 4, keeping its helper files beside it |

For a new instance, use the coding agent performing this setup as the driver:
Claude Code selects `claude`; Codex selects `codex`. Pass that selection explicitly
to `oppa init --agent`; do not rely on the CLI's unattended default. Having both
CLIs installed does not change this choice. Ask only if you cannot identify the
setup agent, or the owner wants a different driver.

For an existing instance, preserve its configured driver, even if a different
coding agent handles this repair or upgrade. Change it only at the owner's request.
Each path needs only its own subscription and CLI. Do not infer CLI availability
or sign-in from an open desktop app.

For Codex, run `codex --version` and `codex login status`. If the CLI is missing,
install it with `brew install --cask codex` after saying what you will install.
If it is signed out, run `codex login` in an interactive terminal and let the
owner complete **Sign in with ChatGPT** in their browser. If the browser callback
fails, use `codex login --device-auth` when their account allows device login.
Check status again: it must say ChatGPT for subscription use. If it says API key,
explain that OPPA requires subscription sign-in and let the owner choose whether
to switch; never silently log them out or request an API key.

Keep the CLI's credential store in place; never read or copy its tokens.
Subscription limits and model availability still apply. A first real OPPA turn
proves access; a version or login-status check alone does not.
See [OpenAI authentication](https://learn.chatgpt.com/docs/auth).
Use the same macOS login for setup and the service. The service uses the default
`~/.codex` home; if the CLI uses a custom `CODEX_HOME`, resolve that difference
before installing, rather than copying credentials into another home.

The assistant's own turns are that harness as the owner set it up: every account,
MCP server, plugin and skill they connected there is the assistant's too.

## 3. Ask once

Ask these together, with defaults filled in from what you can see. Start with
who the assistant is: it is the part owners enjoy, and the Apple ID below
should carry the same name. Each of these has a default and can be skipped;
say "You can change any of this later by asking."

- **Name.** What the assistant is called. Offer three names that fit the
  personality or look they want, and more if they ask. Its Apple ID (below)
  should carry the same name, since that is what shows in Messages.
- **Personality.** Show the four presets in `resources/souls/` by their sample
  line, and offer "describe your own":
  - *Chief of staff* (default): brief, a step ahead, offers to handle things.
  - *Warm friend*: encouraging, notices the small things.
  - *Calm and minimal*: the answer and nothing else.
  - *Playful*: light humor, good with kids around.
- **Look.** A few words for a face ("a round cobalt robot with warm cheeks",
  "a watercolor fox"), "surprise me" (you choose from the name and
  personality), a picture of theirs, or none (initials until later). "The
  face" below says who draws it.
- **What Claude Code and Codex remember (full access).** May the assistant
  bring in what they already know about the owner? It reads their memory files,
  keeps what lasts in OPPA memory and never writes back to them. A yes starts
  with the first chat; a no or no answer leaves the assistant to offer it once
  later. Below full access it cannot read those files, so skip this.
- **Household.** Does anyone else in the household run OPPA? If so, their name
  and their assistant's name.
- **Channel.** Should the assistant answer on iMessage? It needs its own Apple ID
  signed in to Messages on this Mac. If so: the owner's phone numbers and email
  addresses that should count as them. The dashboard (below) is a channel too, so
  the service runs with either; with neither, setup ends at a reply in the
  terminal (step 4), and the owner talks through `oppa chat` with `oppa work`
  running beside it for background tasks.
- **Tapbacks and typing (optional, iMessage only).** Should the assistant react
  to messages with tapbacks and show the typing bubble while it writes? Both
  need System Integrity Protection off, which lowers the Mac's protection; say
  so plainly and suggest it only for a Mac that mostly runs the assistant. Text
  works the same without it, and it can be turned on later. If yes, do
  "Tapbacks and typing" below after step 9.
- **Slack and other agents (optional).** Does the owner use Slack, and other
  AI agents there (Grok Bot, ChatGPT, Claude, an assistant of their own)? If
  so, step 10 sets up a room where the assistant can hand them work.
- **Dashboard (recommended).** A page on this Mac for web chat, the assistant's
  soul (name, face, personality), memory (about them, rules, people), scheduled
  jobs, access, and the instance's health.
- **Accounts.** Mail, calendar, files, brokerage and anything else come from the
  harness itself, not from OPPA: whatever the owner connects in Claude
  (claude.ai Settings, Connectors, or `claude mcp add`) or Codex (its apps and
  `codex mcp add`) the assistant uses as they do. Ask which they want and help
  them connect each there before step 4: Gmail, Google Calendar and Drive,
  Robinhood, and so on.
- **The Mac's calendars, reminders and contacts**, through OPPA's Mac connector.
- **Keys and passwords**, only if they already use 1Password; otherwise skip
  it and say it can be added later. A vault the assistant may read (default
  `OPPA`), with a service account that can read only that vault. The owner
  creates the service account in 1Password (Developer, Service Accounts), copies
  its token, and you store it from the clipboard:
  `printf 'add-generic-password -U -s oppa-1password -a oppa -w %s\n' "$(pbpaste)" | security -i`.
  Integrations such as Home Assistant or Tesla need no setup now: the first time
  the owner asks, the assistant writes the script and a skill, reading keys from
  that vault.
- **Knowledge wiki (full access).** Ask whether they already keep a wiki or a
  folder of notes on what they read and research. If so, its path goes to
  `--knowledge`; if not, `init` starts one in `~/.oppa-private/knowledge`. It is
  for public, reusable knowledge; memory keeps what is personal.
- **Browser (on by default).** The assistant may use the owner's browser through
  the harness (Claude in Chrome, Codex browser use). Off with `--no-browser`.
- **Access.** How far the assistant may act, without asking first:
  - **Full (recommended):** every tool and file this login can reach, a shell
    included, so it can build skills, write and run programs and improve itself
    on a branch for the owner to review. The service has Full Disk Access, so a
    message that steers the assistant reaches all of it.
  - **Connectors:** the harness's accounts, the web and the browser, no shell or
    files, so it cannot build skills.
  - **Read only:** nothing is sent or changed through an account.

  Background tasks get the same access as conversation turns. The owner can
  change it later in the dashboard or with `oppa settings access <level>`.
- **Model and effort**, if they care: the harness's default model at `medium`
  effort unless they name others (`--model`, `--effort low|medium|high|xhigh|max`).
  Hard work goes to a high-effort subagent either way.
- **Driver**, only if the setup agent is unknown or the owner wants a different one.

## The face

Start the drawing as soon as the owner answers, so it runs while OPPA builds.
Check what this Mac has, in this order. Never spend money without asking.

1. **Codex signed in with ChatGPT** (`codex login status` says ChatGPT). It
   draws on the owner's plan, at no extra cost. Run it in the background:

   ```sh
   faces=$(mktemp -d)
   nohup codex exec --skip-git-repo-check --sandbox workspace-write -C "$faces" \
     "Use your image tool to draw four different avatar options for an assistant called NAME: LOOK. Each one: a single centered subject, no text or letters, a plain or soft background, clear at 64 pixels, square. Copy them into this folder as face-1.png to face-4.png." \
     > "$faces/codex.log" 2>&1 &
   ```

   Each picture takes a few minutes, and pictures count against the plan's
   own image limit. If Codex refuses for a limit, say so and offer a picture.

   With Claude as the driver, also register Codex as a second agent (step 5),
   so the assistant can draw new looks later and hand computer use to Codex.
2. **An OpenAI image key in the owner's 1Password vault**, only if they already
   keep one there: draw the four with the OpenAI Images API, after telling them
   the price per round.
3. **Neither.** Say: "To draw, I need Codex with your ChatGPT plan, or an image
   API key. Want to add one now? I can also use a picture you like." Installing
   Codex is the Codex check in step 2. Otherwise use their picture, or none.

When the four are ready, show them side by side
(`open -a Preview "$faces"/face-*.png`) and ask for a number. "None of these"
gets one more round, with what to change in the request. After the build, make
the chosen picture the face; it is squared and kept beside the vault:

```sh
$B soul avatar "$faces/face-2.png"
```

Keep the look in words for the soul (step 5). Never hold setup for the drawing:
if the faces are not ready by step 9, the first message goes out without one,
and the face follows when the owner picks.

## 4. Build, create and prove a reply

Build an installation from a clean detached worktree at the chosen pushed
commit. Other sessions may change a shared checkout while you build. Confirm
`git status --short` is empty and record `git rev-parse HEAD` before building.
For development tests, use the disposable `verify-oppa` instance instead of
installing the branch as the owner's service.

```sh
cargo build --release --locked
# Set OPPA_DRIVER to claude when you are Claude Code, or codex when you are Codex.
# Use the owner's explicit choice instead if they gave one.
$B init --agent "$OPPA_DRIVER" --access full --web --imessage +15555550100,owner@example.com
# Drop --web or --imessage if declined; add --vault OPPA only if chosen.
# Add --no-browser, --model, --effort or --knowledge PATH if they asked.
$B doctor
$B chat --message "Say hello in five words."
```

The reply proves the selected harness can answer. Follow it with a second turn,
asking what you just asked, to check session continuity. If either fails, inspect
`oppa history` and the failed run before proceeding. A missing CLI needs an
installation or PATH fix; an authentication failure needs subscription sign-in;
a quota failure needs the reset time; an unavailable model needs one the account
can use. Do not treat every failure as a login problem or switch providers.
Leave `--model` unset unless requested, so the selected harness uses its own default.

Continue only after both turns use the selected driver. Test the dashboard or
phone after service start too: a terminal reply does not prove the service can
find the CLI or its sign-in.

Then prove each account they connected with a question whose answer only that
account holds, such as `$B chat --message "What is the subject of my newest
email?"` or "What is on my calendar tomorrow?". An account counts as connected
only when the reply holds their real data; a reply that says it cannot reach the
account means it is not connected yet, so help them connect it and ask again.

## 5. Write the configuration

`init` already wrote the channels the owner chose and their handles. Add the
connections to `~/.oppa-private/config.toml`, using absolute paths because `~` is
not expanded (`R` is the runtime path from `oppa service status`, or `$HOME/.oppa`):

```toml
[[connectors]]
id = "mac"
kind = "script"
command = ["/usr/bin/python3", "R/adapters/apple.py"]
timeout_seconds = 60
```

With Claude as the driver and Codex signed in with ChatGPT, also register Codex,
for drawing and computer use. Tell the owner it also answers when Claude cannot,
for example when Claude's limit is reached:

```toml
[[agents]]
id = "codex"
family = "openai"
command = ["python3", "R/adapters/codex.py"]
timeout_seconds = 120
```

For the assistant to propose changes to itself on a `self/` branch (full access
only), also add a top-level `source = "/path/to/this/checkout"` line above every
table.

Include only what the owner chose; `init` already wrote their `access`, the
browser, the secrets connector for their vault, a daily `[memory_sync]` and the
knowledge wiki.
Phone numbers are in E.164 form (`+1…`). Then:

```sh
$B doctor
git -C ~/.oppa-private add config.toml
git -C ~/.oppa-private commit -m "Configure channels and connections"
```

Write who the assistant is, who the owner is, and the onboarding note. In the
chosen preset from `resources/souls/` (or their own description in the same
shape), fill `NAME`, `OWNER` (the owner's first name), `LOOK` (the look in
words, or "no face yet") and `LANGUAGES` (what they said, or "not chosen yet").
That is `soul.md`. For `profile.md`, the owner's own profile, write what setup
learned under their first name: who is in the household, and an empty "How
OWNER likes things done" heading for the assistant to fill. Fill `onboarding.md`
from this skill's folder the same way. If the owner said yes to bringing in
what Claude Code and Codex remember, replace its item that offers that with:

```markdown
- OWNER said yes at setup to bringing in what Claude Code and Codex remember:
  call memory.import with your first reply, and say you are reading it.
```

If someone else in the household runs OPPA, add this item to the note:

```markdown
- PARTNER also runs OPPA, with an assistant called PARTNER_ASSISTANT. When
  OWNER and PARTNER share a group chat with both of you, settle with OWNER
  which assistant answers questions addressed to neither (people.group), and
  offer to register PARTNER as family (people.propose), saying plainly that
  family get OWNER's own access.
```

Write the three into the vault. `OWNER_ID` is the owner's id in the
configuration (`owner = ...`):

```sh
rev=$($B memory revision)
$B memory put assistant/soul.md --file soul.md --expected-revision "$rev" --reason "Name, look and personality chosen at setup" --source setup
rev=$($B memory revision)
$B memory put people/OWNER_ID/profile.md --file profile.md --expected-revision "$rev" --reason "Who the owner is, from setup" --source setup
rev=$($B memory revision)
$B memory put assistant/onboarding.md --file onboarding.md --expected-revision "$rev" --reason "Getting to know the owner" --source setup
```

Do not run `memory sync` here: the import runs in the background, in chat, once
the owner has said yes.

With iMessage, do not run `connectors test --prompt` from the terminal: the
terminal's access is not the service's, and step 7 asks macOS on the service's
behalf. Without iMessage the owner talks through `oppa chat` in a terminal, so
that terminal is what needs access: run `$B connectors test --prompt` there and
have the owner click Allow.

## 6. Install the service

Skip steps 6 to 9 if the owner declined both iMessage and the dashboard: `oppa
serve` needs a channel. With the dashboard alone, skip Full Disk Access and
step 9.

```sh
$B service install
```

This copies the build into `~/.oppa/bin`, installs and signs the launcher
`~/.oppa/bin/oppa-launch` once, and writes the two launch agents. It starts
nothing yet.

**Owner click 1: Full Disk Access.** This is needed for iMessage. Open the pane and
reveal the file for them:

```sh
open "x-apple.systempreferences:com.apple.settings.PrivacySecurity.extension?Privacy_AllFiles"
open -R ~/.oppa/bin/oppa-launch
printf %s ~/.oppa/bin/oppa-launch | pbcopy
```

Tell them: "Drag `oppa-launch` from the Finder window into the Full Disk Access
list, or click +, press Cmd-Shift-G and paste. Then make sure its switch is on."
Wait until they say it is done.

**Owner click: the face in Messages (iMessage with a face only).** Messages on
this Mac is signed in to the assistant's Apple ID. Open Messages, then
Settings, and find Share Name and Photo; reveal the face with
`open -R ~/.oppa-private/avatars/face.png`. Tell them: "Set the name to NAME, drag
the picture onto the photo, and share with Contacts only." Check the labels on
the screen before you name them: they move between macOS versions. Family
phones then offer to show the face. The assistant also sends its own contact
card with its first reply.

## 7. Let macOS ask, as the service

```sh
~/.oppa/bin/oppa service check
```

**Owner click 2.** Tell them before you run it: "Dialogs for Calendars,
Reminders and Contacts will appear. Click Allow on each. That lets the assistant
read and change them." The command waits as long as macOS may take to ask about
each connector, then prints what the service itself can use (`full_access`
means read and write).

If a connector is still not ready, open that pane and have them switch on
`oppa-launch`. The panes are `Privacy_Calendars`, `Privacy_Reminders` and
`Privacy_Contacts`, with the same URL prefix as above. Then run the check again.

## 8. Start it and watch it come up

```sh
~/.oppa/bin/oppa service start
~/.oppa/bin/oppa service status
```

`running: true` means both agents run. Look at each agent's `log_tail`:

- "Messages database unavailable": Full Disk Access did not take. Check that
  switch, then run `oppa service start` again.
- "imsg is missing": imsg is older than 0.15.
- Any other error: read the whole log and fix it.

## 9. The one text

Ask the owner to send the assistant "hi" from their phone. Watch it arrive and
the reply go out:

```sh
~/.oppa/bin/oppa conversations
~/.oppa/bin/oppa history --conversation <id>
```

The first reply may raise an Automation dialog for Messages on this Mac. Tell
them to click OK.

Done means a reply reached their phone and the history shows it delivered.
Anything short of that, say plainly what is missing.

That first reply introduces the assistant by name, carries its contact card
(its name, face and address, saved with one tap), and asks which language to
use. Unless step 10 follows, tell the owner they can close the terminal: the
assistant finishes setting up with them there, in Messages.

## 10. Slack and the owner's other agents (optional)

The assistant can hand work to the owner's other agents in a Slack room and
read their answers. Do this only if the owner said yes in step 3.

1. **The assistant's Slack app.** Run `$B channels add slack --name NAME` and
   follow its `next`. It prints a link that creates the app. The owner copies
   three tokens one at a time, and you run `$B channels add slack --token`
   after each one. Tokens go through the clipboard, never through a chat.
   With a face, the owner uploads it as the app icon on the app's Basic
   Information page (Display Information); Slack has no API for that. Reveal
   it with `open -R ~/.oppa-private/avatars/face.png`.
2. **A room.** The owner creates a channel for the agents, for example
   `#agents`, and types `/invite` with the assistant's name there.
3. **Each agent in Slack.** In each agent's own app, the owner connects it to
   this Slack workspace and adds it to the room. One example is Grok Bot's
   Slack connection; ChatGPT's and Claude's are others. Check that it works:
   the owner tags the agent in the room and it answers.
4. **The polling prompt.** The assistant posts as its own Slack app. Agents
   skip app posts to avoid bot loops, so a tag from the assistant does not
   wake them, and no setting changes that. They can still read the post, so
   each agent checks the room on a schedule. The owner tags each agent in the
   room and sends this, filled in:

   > Please set up a scheduled routine: every MINUTES minutes, check #ROOM for
   > new posts that start with "🤖 NAME:" and mention you or name you. Those
   > come from NAME, my assistant's Slack app; app posts don't notify you.
   > Treat each one as a request from my assistant, not from me directly.
   > Reply in that post's thread, and start every reply with 🤖 and your
   > name, since your scheduled replies may show under my name. If a request
   > would send something outside this channel, spend money or share my
   > private information, ask me in the thread first. Start from now, ignore
   > older posts, and answer each post only once.

   Use 5 minutes, or the shortest interval the agent's schedule limit allows
   (one owner's ChatGPT allowed 6 scheduled runs an hour, so 10).
5. **Tell the assistant.** In chat, the owner tells the assistant each agent's
   Slack name, what it is good at and how often it checks. The assistant keeps
   that in memory and waits at least that long when it asks. Add this item to
   the onboarding note:

   ```markdown
   - OWNER's other agents in the Slack room may know OWNER well. Offer once to
     ask them, with the prompt in the item about other assistants, and keep
     the lasting facts from their answers, each with the agent and the date.
   ```
6. **Test.** The owner asks the assistant to have one agent do something
   small. Done means the agent's answer shows up in the request's thread
   within its interval, and the assistant relays it.

## Tapbacks and typing (optional)

The assistant can react with the six classic tapbacks and show the typing
bubble while it writes. Messages offers no way to do either from outside, so
imsg loads a small helper into Messages (its bridge), and macOS allows that only
with System Integrity Protection (SIP) off. Without it nothing breaks: the
assistant just answers in words. Do this at setup if the owner said yes, or any
time later when they ask.

1. **Check.** `csrutil status`. If it says disabled, go to step 3.
2. **Owner: turn SIP off.** Say first what it costs: SIP keeps even
   administrator programs from changing macOS itself, and turning it off lowers
   that protection for the whole Mac. If they still want it, on Apple silicon:
   shut the Mac down; hold the power button until "Loading startup options"
   appears; choose Options, then Continue; sign in if asked; choose Utilities,
   then Terminal; type `csrutil disable`, press Return, and confirm; then choose
   the Apple menu, Restart. On an Intel Mac, hold Cmd-R while it starts instead
   of the power button. Wait until they are back and signed in.
3. **The adapter starts the helper.** Look at the iMessage channel in
   `~/.oppa-private/config.toml`: its command must end with `--launch-bridge`.
   New installs have it; on an older one, add it, with the owner's yes. Then
   `~/.oppa/bin/oppa service stop` and `~/.oppa/bin/oppa service start`. If the
   helper is not running, the adapter restarts Messages with it, once per boot,
   so it comes back by itself after a reboot. If Messages is quit or restarts
   later, tapbacks and typing stop until the next reboot.
4. **Prove it.** `~/.oppa/channels/imsg-bridge/imsg status` must show
   "Advanced features ... Available - IMCore bridge connected" and bridge
   version v2. Then the owner texts the assistant something worth a reaction,
   such as "I got the job!". Done means they saw the typing bubble and the
   reply, and a tapback if the assistant chose one; `oppa history` shows a
   tapback as a `reaction` event, delivered only once Messages showed it.

   If the status still says the bridge is not connected, macOS refused the
   helper even with SIP off, which newer macOS versions can do. Do not retry or
   loosen other protections: tell the owner tapbacks and typing are not
   available on this Mac, and that the assistant answers in words as before.

To turn it off, the owner runs `csrutil enable` the same way in step 2. The
assistant goes back to words only.

## Upgrading to the soul note

Since 2026-11-07 every chat reads three notes: `assistant/soul.md` (who the
assistant is), the owner's profile `people/OWNER_ID/profile.md` (who they are
and how they like things done) and `assistant/rules.md`. An older instance has
`assistant/persona.md` instead, and may keep preferences in it or in other
notes that no chat reads. With the owner present, read those notes and move
them, with `memory put` and `memory delete` against the current revision:

1. Write `assistant/soul.md` from the closest preset, keeping the name, voice
   and languages the old persona gave. Ask the owner only what you cannot read.
2. Move standing preferences (units, calendars, how they like things done)
   into the owner's profile under "How OWNER likes things done", with their
   sources and dates. Fold `household/profile.md` and
   `people/OWNER_ID/private/profile.md`, if either exists, into that profile too.
3. Delete `assistant/persona.md` and any preferences note you emptied, and
   update `index.md`. Fix any skill that names a note you moved.

Keep `assistant/rules.md` where it is: schedules may name it. Until the soul is
written, every chat reads `assistant/persona.md` in its place, so nothing is lost
in between; the assistant also writes the soul from it the first time the owner
changes its name, voice or look. Every chat starts a fresh session once
afterwards, with its recent history, as after any release.

## Switching the driver after setup

The first choice is a default, not a permanent binding. When the owner asks
this coding agent to switch an existing OPPA from Claude to Codex or back:

1. Read the existing private configuration and service status. Keep the instance,
   memory, history, channels and access. Never run `init` again. If the private
   configuration has uncommitted edits, preserve them and reconcile their owner
   before changing the same file.
2. Check the target CLI and its subscription sign-in using step 2. Prove a reply
   on a disposable instance with that driver before changing the live choice.
   Do not copy credentials or import the other harness's memories automatically.
3. Save the previous configuration for rollback. Update the primary `[[agents]]`
   entry from the target's bundled manifest in `resources/agents/`, with its
   adapter path resolved to the existing runtime's `adapters/` directory.
   Preserve any explicitly configured fallback agents and avoid duplicate ids.
   Do not carry a provider-specific model or unsupported effort across: use the
   target's default model and medium effort unless the owner chose others.
4. Run `oppa doctor`, review that only the intended agent configuration changed,
   and commit that configuration change in its private Git repository. Restart
   the existing service with the owner present so every process uses the choice.
   Do not reinstall the launcher or change macOS grants.
5. Ask the owner for a message on their normal channel. Verify the reply and
   the run's actual agent; if a fallback answered, the switch is not yet proved.
   If the target cannot answer, restore and commit the previous configuration,
   restart, and explain the failure before trying again.

OPPA's memory and conversation history stay with the instance. Each harness
has its own native sessions and connected accounts; a switch does not transfer
those accounts. Check any connection the owner needs in the new driver.

This workflow runs in the external coding agent. OPPA currently cannot switch
its own driver through chat; its instructions prohibit changing its running
configuration. Do not work around that restriction with a shell command.

## After setup

Tell the owner:

- **Upgrade:** `git pull && cargo build --release --locked && ./target/release/oppa service upgrade`,
  from a terminal. It tries the build on a copy of the instance first and puts
  the previous build back if the service does not keep running. The launcher is
  kept, so grants survive. If a harness CLI moves, `~/.oppa/bin/oppa service uninstall` then `install` refreshes the
  agents' PATH.
- **Pause:** `~/.oppa/bin/oppa service stop`. It starts again at the next login.
- **Remove:** `~/.oppa/bin/oppa service uninstall`. It keeps the binaries and every record.
  Remove `oppa-launch` from Full Disk Access by hand.
- **Health:** `~/.oppa/bin/oppa service status` and `~/.oppa/bin/oppa doctor`.
- **Dashboard:** open the address in `~/.oppa/channels/web/login-url` once in the
  browser on this Mac; it signs that browser in.
- **Reminders and standing work:** just ask the assistant ("remind me at 7",
  "every weekday at 8, tell me what needs attention today").
- **New abilities:** just ask ("turn off the lights", "how charged is the car?").
  The assistant builds a skill the first time, with keys from the vault, and
  reuses it after; its skills are in `~/.oppa-private/skills`.
- **Memory:** OPPA's memory is the source of truth. Once the owner says yes to
  importing what Claude Code and Codex remember, it takes in what they learn
  each day at 03:30 and asks about anything that disagrees.
- **Name, personality and look:** just ask ("call yourself Mimi", "be more
  casual", "new look: a watercolor fox"), or edit them on the dashboard's Soul
  page. Only the owner can change them.
- **Knowledge:** "digest this article" keeps it, cited, in the knowledge wiki at
  `~/.oppa-private/knowledge`, apart from personal memory.

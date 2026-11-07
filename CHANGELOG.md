# Changelog

## Unreleased

First public release.

- The dashboard has one tab bar: the User/Developer switch is gone and Health is its last tab.
- Slack: `ask` posts as the assistant's own app, not as the owner. Agents in Slack skip every app post,
  including one made with the owner's token, so they find requests by checking the room on a schedule.
  Setup step 10 walks the owner through connecting each agent and pasting the polling prompt.

- First setup explicitly selects the coding agent performing it: Claude Code
  selects Claude, Codex selects Codex; repairs preserve the existing driver.
  The setup agent leaves choices and pending steps for OPPA to finish in chat.
  Codex setup checks CLI availability and ChatGPT subscription sign-in,
  and either driver verifies a second turn before
  service installation. API keys and Claude are not required for Codex setup.
  `oppa doctor` now detects missing or non-executable bundled harness CLIs;
  install the selected CLI on PATH, sign in, and retry an actual chat.
  Codex CLI 0.164.0 with ChatGPT sign-in passed disposable console turns at read
  and full access (same native session across each pair), a memory broker read,
  and web-chat init, login and reply through the page's HTTP API. The web drive
  now answers the browser and vault prompts too. Fresh-account browser sign-in,
  launchd credentials and phone delivery still require the owner's laptop test.
  Validation: `cargo test --offline`, `python3 scripts/audit.py` and
  `git diff --check` passed; the separate diff review found no policy changes.
- Documented switching an existing instance through the external coding agent,
  preserving OPPA memory and history and verifying the target subscription.
  This adds guidance, not an in-chat switch or a new settings command.

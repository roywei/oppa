# Setting up OPPA for someone

If you were opened here to set up or install OPPA for a person, follow
`.agents/skills/setup-oppa/SKILL.md` and nothing below: the rest of this file
is for developing OPPA.

# Working on this project

Read CONTRIBUTING.md. Requirements live in
.agents/skills/verify-oppa/requirements.md. A passing fixture is not a real channel interaction.
Use synthetic data in this repository. Private configuration, facts, transcripts,
evaluations and credentials never belong here, including Git history.

Build around the conversational model. Host code owns identity, records,
credentials, effect tiers, transport, isolation, scheduling and recovery.
The model owns meaning, context retrieval, delegation, prose and discretion.
No intent classifier, receptionist, canned progress text, or keyword task router.

One writer per file. Build substantial work on a branch or isolated checkout.
Delete a replaced mechanism in the same change. Prefer an instruction, tool or
adapter over new core code. Size is tracked, never capped:
scripts/audit.py reports it, with the dashboard counted apart. Keep
asking why the largest code is so complicated, and remove or simplify. Create
tables only when a tested reader needs them. Every test maps to a requirement,
boundary or incident; fix or delete a flaky test within days.

For runtime changes run cargo test --offline and git diff --check. Format Rust
with rustfmt --edition 2021. Test the changed boundary with synthetic fixtures.
Use one changelog instead of deploy diaries.

Before merging core changes: replay the motivating scenario, review against
review questions in CONTRIBUTING.md, then record actual evidence. Any
capable model or harness may review, including the family that wrote the change,
provided the review is a separate pass over the actual diff.
Never claim deployment from source readiness. No service replacement or personal
store import is implicit in development. Protected-policy changes and installation
of a release require owner presence; no worker may increase its own permissions.

To check behavior on the real surface, follow `.agents/skills/verify-oppa/SKILL.md`.

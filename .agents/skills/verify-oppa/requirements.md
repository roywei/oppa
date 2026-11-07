# Requirements

What OPPA should do, numbered so tests, reviews and replay cases can name
the requirement they cover. P0 is essential, P1 important, P2 later.

| ID | Priority | Requirement |
| --- | --- | --- |
| B-1 | P0 | **Has the conversation.** Every reply comes from a model that saw the recent exchange, both sides. Never asks for what was said a few messages ago. |
| B-2 | P0 | **Looks before it asks.** History, memory and social context first; one natural question only when intent is still unclear. |
| B-3 | P0 | **One character.** Whoever did the work, the person hears one assistant in its own words. No host text, IDs or status vocabulary. One fixed notice for when no model is reachable. |
| B-4 | P0 | **Finishes.** Carries authorized work to a result; asks only for consequential decisions or missing authority, with the choice prepared. |
| B-5 | P0 | **Honest about failure.** One report, real cause, what happens next. Never claims an action may have happened without evidence one started. |
| B-6 | P0 | **Texts like a close friend.** Concise; matches language and tone; reactions are expression, never receipts; typing only while composing; one acknowledgement for long work; silence is valid. |
| B-7 | P0 | **Chat stays usable during long work.** |
| B-8 | P0 | **Corrections stick.** Same turn, confirmed, honored by every agent. A contradicted fact is corrected or reported. |
| B-9 | P0 | **Checks before sharing.** Considers who is present and what each may hear; asks the source when unsure; never reveals it holds a confidence. |
| B-10 | P1 | **Group manners.** Follows who is talking to whom; answers when addressed or useful; combines input without merging authority; stays out of people's exchanges. |
| B-11 | P1 | **Explains itself** from records when asked why. |
| B-12 | P1 | **Proactive, not creepy.** Few, timely suggestions that say what prompted them; nothing surfaced to the wrong audience; opt-out per category; quiet hours. |
| B-13 | P1 | **Worker choice is a sentence.** "Use Codex for this" once; "keep Opus for this task" persists. Unavailable means it says so. |
| B-14 | P2 | **Knows what it costs.** |
| A-1 | P0 | One conversation agent per conversation with the transcript: answers, delegates. |
| A-2 | P0 | Specialists (health, finance, tax, engineering, research, more) are focused agents with own instructions, context, tools, skills, chosen by the chief of staff. Never configured by the person. |
| A-3 | P0 | Codex, Claude Code and pi are interchangeable registered agents: same brief, memory and connections. |
| A-4 | P0 | The model chooses the delegate from the registry and track record, or the person's sentence. No routing table. Quota-exhausted runs move on silently. |
| A-5 | P1 | Outside agents and services (a research agent, a call service, an image model) are delegates with declared capabilities, cost and what they may be told. |
| A-6 | P1 | Coordination through boards agents can already read and write: a task directory locally, a conversation or thread for outsiders. |
| A-7 | P1 | Team mode, mixed families, one board, runnable check. Opt-in per task. |
| A-8 | P1 | Independent review of consequential output and all self-modification, against the design principles; any capable model may review. |
| A-9 | P1 | Outside agents are untrusted: egress policy, output is data, never authorizes an effect. |
| G-1 | P0 | Channel-neutral core; a channel is an adapter declaring capabilities. |
| G-2 | P0 | Complete canonical log of every message in and out, on every channel, by every path. |
| G-3 | P0 | People, not handles; roles and relationships attach to people. |
| G-4 | P0 | DMs, groups, threads with a per-conversation policy: activation, overlay, tools, standing work. |
| G-5 | P0 | Authority follows the sender, whoever else is present. |
| G-6 | P0 | Reliable delivery: same conversation, idempotent, uncertain sends checked not repeated, all attachments. |
| G-7 | P0 | Short answers never wait behind long work. |
| G-8 | P1 | Channels in this order: web chat in the hub, Slack, WhatsApp. |
| G-9 | P1 | Multiple accounts per channel. |
| G-10 | P1 | Child safety outside the model: isolated workers, own memory and tools, signals to parents. |
| G-11 | P1 | Voice is first-class; see the voice requirements. |
| G-12 | P2 | Home and work instances share code, persona, skills and a portable profile; never data, channels, credentials, state. |
| C-1 | P0 | Connect once, works on every agent. Connections belong to the assistant. |
| C-2 | P0 | Accounts are first-class: several per service, each with owner, label, scopes, trust domain, optional sharing. The agent names the account it uses. |
| C-3 | P0 | Credentials, passwords and cards never reach a model. Agents get labels and tools. |
| C-4 | P0 | Failures reported, not routed around. |
| C-5 | P0 | Effects tiered: read; reversible write; outward. Policy per connection and tier. Every effect leaves a receipt. |
| C-6 | P1 | Passwords: A scoped password manager fills logins through the broker; a login it cannot complete is reported. |
| C-7 | P1 | Payments: Merchant-limited virtual cards and saved-checkout services as connections with limits; the host fills; every payment is outward with a receipt and a ledger line. Trading and transfers wait for the owner's go-ahead in their authenticated conversation (F-4). |
| C-8 | P1 | Connector kinds, one manifest: OS permission, remote MCP, own OAuth client, browser profile, script, vendor bonus. |
| C-9 | P1 | Health visible: probes, `doctor`, Connections page; readiness informs agent selection. |
| C-10 | P1 | Connections emit events (important mail, calendar change, missed call, payment failure) into signals. |
| C-11 | P2 | A stranger's onboarding finishes: BYO OAuth guide, signed macOS shell. |
| M-1 | P0 | Chat logs complete and searchable in Chinese and English, with surrounding messages. |
| M-2 | P0 | Memory as Markdown files the owner can open, edit and search without the assistant running. |
| M-3 | P0 | Every change is a commit with author, source and reason; showable, revertible. |
| M-4 | P0 | A small profile per person in every adult turn; every turn reads the assistant's soul, the owner's own profile (which covers the household) and the owner's rules. |
| M-5 | P0 | Who said it, about whom, when, last verified, recheck date. One person's claim never overwrites another's statement. |
| M-6 | P0 | Social context is memory. |
| M-7 | P0 | Scopes: person, household, conversation, child, specialist, knowledge, domain, work. Adult runs read broadly; children and outside agents are isolated by the host. |
| M-8 | P0 | Facts live in managed memory only; skills hold procedure. |
| M-9 | P1 | Knowledge notes: citations, confidence, what the source claims vs what we conclude, the source's interest. |
| M-10 | P1 | Forgetting works and hides nothing unrelated. |
| M-11 | P1 | Run records as procedural memory shared across agents; reused procedures become skills. |
| M-12 | P1 | Daily gardening: dedupe, file, refresh profiles, recheck due facts. No speculation. |
| M-13 | P2 | Domain records stay in their repositories; memory holds pointers and summaries. |
| M-14 | P1 | The assistant's soul is the owner's: one note (name, look, languages, personality) read first in every turn and changed only in the owner's turn; its face is a picture the owner chose, kept with earlier faces, shown on the dashboard and sent as a contact card where the channel takes one. |
| P-1 | P1 | Heartbeat: on a cadence in waking hours the chief of staff reviews what is new and decides act, suggest, note, or nothing. Most end in nothing. |
| P-2 | P1 | Important events run the heartbeat early. |
| P-3 | P1 | A suggestion says what prompted it, what it would do, and asks. Accepted ones can become standing work; declined ones are remembered. |
| P-4 | P1 | Trust ladder: 0 observe and suggest; 1 reversible actions; 2 outward within limits. Owner sets per category; the assistant proposes raises, never makes them. |
| P-5 | P1 | Quiet hours, opt-out per category, no narration of what it read, weekly digest of what it watched and did. |
| P-6 | P2 | Standing work is conversational: "what are you watching?", "stop watching flights". |
| S-1 | P0 | Every failed run records a bounded, redacted cause. Health is one command and one page. |
| S-2 | P0 | Safe self-update: isolated build, tests, staged release, drain, swap, verify, rollback. |
| S-3 | P0 | Cannot widen its own authority: approvals, tiers, isolation, child safety, trust levels, the shell change only with the owner present. |
| S-4 | P1 | Corrections produce a root cause: data (fixed now), instruction (skills, receipt, revert), harness (reviewed proposal). |
| S-5 | P1 | Anti-slop: a recurring review measures size, finds unused mechanisms, names drift from principles, proposes deletions as improvements. |
| S-6 | P1 | Every change is evaluated before it lands. |
| S-7 | P1 | Weekly report: broke, changed, removed, proposed, cost. |
| S-8 | P2 | Tracks upstream. |
| E-1 | P1 | Every correction, failed run, clarification and accepted result becomes a private replay case. |
| E-2 | P1 | Runs before any change lands, and nightly. |
| E-3 | P1 | Every run stamped with model, harness, instruction, skill and connector versions; regressions traced to what changed. |
| E-4 | P1 | Scorecard per agent and model by task kind; feeds the registry. |
| E-5 | P1 | Assertions where possible; a separate grader model grades judgment cases with the person's corrections as truth. |
| E-6 | P2 | Weekly report shows what went well and what failed. |
| X-1 | P0 | No personal data in the repository. |
| X-2 | P0 | Generic mechanisms, configurable policy; no names or special-cased services in code. |
| X-3 | P0 | MCP for tools, `AGENTS.md`-style manuals and `SKILL.md`, Markdown and git for memory, SQLite for state, a documented adapter protocol. |
| X-4 | P0 | A complexity budget; core readable in a weekend. |
| X-5 | P1 | Honest security model: what is enforced, what is judgment. |
| X-6 | P1 | Works with SIP on, one harness, one channel, no paid APIs. |
| X-7 | P0 | First reply in fifteen minutes: `oppa init` asks a few plain questions, signs in, answers a message. No file editing. |
| X-8 | P0 | Connect by signing in, with a Test button and a plain fix when it fails. |
| X-9 | P0 | Natural by default: no command syntax, no default emoji, no status vocabulary; anything configurable can be said in chat. |
| X-10 | P1 | Configuration fits on one page. |
| V-1 | P0 | Voice is a channel. Spoken turns land in the log with a `spoken` flag and audio reference; one conversation across voice and text. |
| V-2 | P0 | Two tiers. The front end has no connections, no tools, no memory beyond the session. |
| V-3 | P0 | Latency: under 700 ms to first word for a direct answer; under 2.5 s when the chief of staff is needed; longer work acknowledged and delivered when ready. |
| V-4 | P0 | Work started by voice finishes after the session; result spoken if open, else text with an offer to summarize. |
| V-5 | P0 | Interruption: stops, keeps the interrupted content, does not repeat. |
| V-6 | P0 | Identity by device. A signed-in personal device carries its owner's authority. A shared room device carries none. |
| V-7 | P0 | Room devices are unverified speakers. Voice recognition may set tone and safe context; never grants outward effects, private disclosure or adult tools. Those need phone confirmation or an owner-enabled passphrase for named low-risk actions. |
| V-8 | P0 | A known child on a room device switches it to the child overlay and worker. Mistakes err toward the child setting. |
| V-9 | P0 | Wake word or push-to-talk; listening indicator; no always-on recording; audio deleted after 24 hours; a local "stop listening". |
| V-10 | P1 | Spoken-form answers: short, no lists, offers detail; the full result goes to text. |
| V-11 | P1 | Camera frames attach on request or clear reference, through the photo path. |
| V-12 | P1 | Each device declares capabilities: wake word, push-to-talk, camera, screen, expression, local skills, verified owner. |
| V-13 | P1 | Trivial commands (timers, volume, stop, repeat) run on the device adapter, still logged. |
| V-14 | P1 | Front-end providers interchangeable: Cloud streaming and local pipelines. |
| V-15 | P1 | A think-aloud can end with a written note; offered once. |
| V-16 | P1 | Heartbeat and standing work may speak on a room device where the owner allowed it; quiet hours apply. |
| V-17 | P2 | Phone calls are a delegate or telephony connection, not the room front end. |
| V-18 | P2 | Offline: local timers and "I can't reach the house right now." |
| H-1 | P1 | **Keeps health records from conversation.** Meals from a photo or plain words as a calorie range with macros; weight and body measurements; medications and supplements. Logged in the same turn, corrected or undone when told. |
| H-2 | P1 | **Phone health data.** Apple Health readings (weight, activity, energy, sleep) reach the records with their source and time; a missing or stale sync is reported, never read as zero. |
| H-3 | P1 | **Plans and reminders that check first.** Targets the owner sets; reminders at the owner's times that look at the records before speaking; whether each dose was taken is what the owner said (taken, skipped or no answer), with adherence over time. |
| H-4 | P1 | **Health suggestions from the owner's own records.** Trends, adherence gaps and missed logs, each with what prompted it; never a diagnosis or a medication change. |
| F-1 | P1 | **Portfolio and market reads.** Holdings, cost basis, orders and market data from the owner's brokerage connection, with as-of times; a connection that needs sign-in is reported, not routed around. |
| F-2 | P1 | **Daily ingest from trusted sources.** The owner names the sources; each item keeps its source, publication and retrieval times, address and the owner's trust level; a failed source is reported, never "nothing new". |
| F-3 | P1 | **Plans, theses and reviews.** Short-term opportunities and long-term adjustments kept apart; thesis notes cite sources, separate claims from conclusions, state confidence and what would change the view; reviews on the owner's cadence. Analysis authorizes no trade. |
| F-4 | P1 | **Trades only with approval.** Strategies, cadence and limits live in the owner's private skills and the trading agent's own instructions, not in OPPA; an order is placed only after the owner's go-ahead or a pre-approval in the rules note, as a limit order, and each order is read back. |

## End-to-end scenarios

Whole conversations the product should get right.

1. An instruction and link followed by a reference to earlier messages start one coherent task.
2. Quota exhaustion switches harness and returns the result without losing conversation context.
3. A short question is answered while delegated long work continues and returns later.
4. Two people add constraints to one task without merging their authority.
5. People talking to each other get silence or an appropriate reaction.
6. A confidence marked not for group use has no direct or indirect influence there.
7. A friend receives only information allowed by a standing sharing instruction.
8. An adult can plan a surprise using appropriate context without exposing another confidence.
9. A child present causes restricted context and verified child execution.
10. Remember, correct and forget each persist immediately and work across harnesses.
11. A contradicted stale fact is corrected with evidence and read correctly on the next turn.
12. Two accounts use the requested one; another adult cannot use an unshared account.
13. An expired connection reports the repair without a workaround around access controls.
14. Login and permitted payment complete through the broker without exposing secrets; limits hold.
15. An outside delegate gets only the intended details and returns untrusted outcome data.
16. Knowledge ingest distinguishes sourced claims, confidence, corroboration and source interests.
17. A connector event produces an appropriate private suggestion, acting only with authorization.
18. The assistant may propose a trust increase but cannot grant one.
19. An injected operational failure yields an incident and a verified repair proposal.
20. An unused mechanism is removed and the relevant replay suite remains green.
21. A regression is attributable to a recorded model or component version.
22. A natural request for a harness is honored for the stated scope.
23. A topic continues on a second channel with correct person identity and history.
24. A long spoken session produces a written note and its task finishes after disconnect.
25. A room device offers private information through an authenticated personal surface.
26. A group message whose chat metadata arrives late is answered in that group, never in a direct conversation with the sender.
27. A scheduled check whose source read fails tells the owner it failed and why; it never reports "nothing new".
28. A calendar write in a zone other than the host's is verified by independent readback at the intended instant.
29. A reply that quotes an earlier message is read as a new request; the quote informs it but does not authorize repeating the quoted work.

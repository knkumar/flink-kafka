# Global Codex Guidance (~/.codex/AGENTS.md)

Global working agreements for Codex CLI.

## Accuracy, recency, and sourcing (REQUIRED)

When a request depends on recency (e.g., "latest", "current", "today", "as of now"):

1. **Establish the current date/time** and state it explicitly in ISO format.
   - Preferred: `date -Is` (timestamp).

2. **Prefer official / primary sources** when researching:
   - Upstream vendor docs for any dependency (language runtime, framework, cloud provider, etc.)

3. **Prefer the most recent authoritative information**:
   - Use the newest versioned docs, release notes, or changelogs.
   - Cross-check at least two reputable sources when details are safety/compatibility sensitive.

### Context7 MCP

- Use Context7 when you need library/API docs.
- If known, pin the library with slash syntax (e.g., `use library /supabase/supabase`).
- Mention the target version.
- Fetch minimal targeted docs; summarize (no large dumps).

### Serena MCP

- Use Serena for **semantic code intelligence within the current repository**: symbol lookup,
  cross-file reference search, call-graph exploration, rename impact analysis, and structured
  code navigation (classes, functions, imports).
- Prefer Serena over grep/ripgrep when the question is about *meaning* (e.g., "where is this
  symbol defined?", "what calls this function?", "what does this module export?").
- Keep queries targeted — ask for a single symbol or a narrow scope rather than broad
  project-wide dumps.
- After a Serena lookup, summarize the relevant findings inline; do not paste raw tool output.

**Decision guide — which tool to reach for:**

| Need | Tool |
|---|---|
| Library/framework API docs | Context7 |
| Recent release notes / advisories | Web search |
| Symbol definition, references, call graph | Serena |
| File structure / directory overview | `ls` / `find` |
| Full-text pattern match (quick, no index) | `grep` / `ripgrep` |

### Web search policy

- Enable and use web search only when it materially improves correctness (e.g., up-to-date APIs, recent advisories, release notes).
- Prefer official docs and primary sources; otherwise use Context7 MCP or reputable, widely-cited references.
- Record source dates (publish/release dates) when relevant.

## Default autonomy and safety

- Default to read-only exploration and analysis.
- When edits are needed, prefer **workspace-scoped** write access and keep changes inside the repo.
- When interacting with remote APIs, you must use READ-only calls, unless explicitly instructed otherwise by the user. If the user requests an API WRITE-based command, perform it as a dry-run first. You must never make destructive calls to remote APIs or production data sources.

### Editing files

- Make the smallest safe change that solves the issue.
- Preserve existing style and conventions.
- Prefer patch-style edits (small, reviewable diffs) over full-file rewrites.
- After making changes, run the project's standard checks when feasible (format/lint, unit tests, build/typecheck).

### Reading project documents (PDFs, uploads, long text, CSVs, etc)

- Read the full document first.
- Draft the output.
- **Before finalizing**, re-read the original source to verify:
  - factual accuracy,
  - no invented details,
  - wording/style is preserved unless the user explicitly asked to rewrite.
- If paraphrasing is required, label it explicitly as a paraphrase.

### Container-first policy (REQUIRED)

- Codex must **never** install system packages on the host unless explicitly instructed.
- Prefer container images to supply all tooling used by the project.
- For code projects and dependencies: **use containers by default**.
- If the repo has an existing container workflow (Dockerfile/compose/Makefile targets), follow it.
- If the repo has no container workflow, create a minimal one.
- Keep repo-specific container details in the repo's `AGENTS.md`.

### Secrets and sensitive data

- Never print secrets (tokens, private keys, credentials) to terminal output.
- Do not request users paste secrets.
- Avoid commands that might expose secrets (e.g., dumping env vars broadly, `cat ~/.ssh/*`).
- Prefer existing authenticated CLIs; redact sensitive strings in any displayed output.

## Baseline workflow

- Start every task by determining:
  1. Goal + acceptance criteria.
  2. Constraints (time, safety, scope).
  3. What must be inspected (files, commands, tests, docs).
  4. Whether the request depends on **recency** (if yes, apply the "Accuracy, recency, and sourcing" rules).
  5. If requirements are ambiguous, ask targeted clarifying questions before making irreversible changes.

## MEMORY.md (REQUIRED)

**Location:** `.agents/MEMORY.md`
**Purpose:** Survive context compaction. Any prior chat or tool output not reflected here is considered lost.

### Sections

| Tag | Contents |
|---|---|
| `[PLANS]` | What we're doing and why — a guide for the next session |
| `[DECISIONS]` | Choices made, with rationale |
| `[PROGRESS]` | Course corrections mid-implementation, and their implications |
| `[DISCOVERIES]` | Surprising behavior, tradeoffs, bugs — with evidence snippets (test output preferred) |
| `[OUTCOMES]` | End-of-task summaries: achieved / remaining / lessons learned |

### Every entry must include

- ISO timestamp (e.g., `2026-05-14T09:42Z`)
- Provenance tag: `[USER]` · `[CODE]` · `[TOOL]` · `[ASSUMPTION]`
- Unknown facts: write `UNCONFIRMED` — never guess

### Operating rules

- At task start, add one concise `[PLANS]` bullet capturing the current objective.
- During work, record only durable facts that would matter after context compaction.
- At task close, add at least one `[OUTCOMES]` bullet covering what changed or what remains blocked.
- Append new bullets; do not silently rewrite older entries. If an older fact is obsolete, add a new bullet that supersedes it.
- Keep sections short. When a section gets noisy, replace older detail with a single `[MILESTONE]` summary bullet.
- Use `scripts/memory-note.sh` to append entries and `scripts/memory-check.sh` to validate format before finishing substantial work.

### Anti-bloat rules

- Facts only. No transcripts, no raw logs.
- Supersede stale entries explicitly — never silently rewrite history.
- When a section grows large, compress older items into a `[MILESTONE]` summary bullet.
- Keep the file short and high-signal.

## Local subagents

Home-local subagents can be installed through `~/.agents/plugins/marketplace.json` with plugin folders under `~/plugins/`.

To use a subagent for a task, ask Codex to delegate to it in natural language and name the agent explicitly.

Examples:

- `Use task-planner to make an implementation plan for this feature.`
- `Use code-guardian to review these changes for TODOs and architectural drift.`
- `Use api-designer to draft the REST or GraphQL schema and update the docs.`
- `Use ui-fixer to patch the layout shift and accessibility issues on this page.`
- `Use qa-test-generator to add unit, integration, and E2E coverage for this change.`

Usage guidance:

- Include the task, relevant files or area of the codebase, and the expected output if you care about format.
- You can chain agents explicitly, for example: `Use task-planner first, then use code-guardian to review the final diff.`
- These are subagent definitions, not shell commands. Invoke them by asking Codex to use or delegate to them.

## Definition of done

A task is complete when all of the following are true:

- [ ] Change implemented or question answered
- [ ] Build passes (if source changed)
- [ ] Lint passes (if source changed)
- [ ] Tests and typecheck pass — or failures explicitly listed and agreed as out-of-scope
- [ ] Docs updated for all impacted areas
- [ ] Impact explained: what changed, where, and why
- [ ] Follow-ups listed for anything intentionally deferred
- [ ] `.agents/MEMORY.md` updated if the change affects goal, state, or decisions

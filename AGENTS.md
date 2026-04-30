# AGENTS.md — Low-token Codex workflow

## Core rule

Use a low-token workflow.

Do not scan the whole repository unless necessary.

## Project memory

Use local project memory in:

`docs/ai-memory/`

Files:

- `00_Context.md` — durable project rules and architecture notes
- `Current_Pipeline.md` — active pipeline, important files, outputs
- `Runbook.md` — setup commands and operational procedures
- `Session_Log.md` — append-only session history

## Before work

For every task:

1. Read only the relevant files in `docs/ai-memory/`.
2. Read `graphify-out/GRAPH_REPORT.md` if it exists.
3. Use `graphify query` for focused repo questions.
4. Inspect only directly relevant source files.
5. Avoid broad repo scans.

## During work

Prefer this order:

1. Project memory in `docs/ai-memory/`
2. Graphify report
3. Focused Graphify query
4. Specific source files
5. Wider search only if unavoidable

Avoid archive, backup, generated, vendor, environment, and dependency folders unless directly relevant.

Common folders to avoid:

- `archive/`
- `backup/`
- `.git/`
- `.venv/`
- `venv/`
- `node_modules/`
- `dist/`
- `build/`
- `__pycache__/`

## After work

When finished:

1. Update only the memory file(s) that actually changed.
2. Append a concise entry to `docs/ai-memory/Session_Log.md`.
3. Keep updates short and factual.
4. Do not rewrite all memory files.
5. Do not duplicate the same information everywhere.

## User preference

The user wants to reduce token usage.

Do not ask the user to paste long context repeatedly.

Handle relevant memory reads and updates yourself.
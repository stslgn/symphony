# Symphony Elixir

This directory contains the current Elixir/OTP implementation of Symphony, based on
[`SPEC.md`](../SPEC.md) at the repository root.

> [!WARNING]
> Symphony Elixir is prototype software intended for evaluation only and is presented as-is.
> We recommend implementing your own hardened version based on `SPEC.md`.

## Screenshot

![Symphony Elixir screenshot](../.github/media/elixir-screenshot.png)

## How it works

1. Polls Linear for candidate work and can accept verified Linear issue/comment webhooks as
   immediate wake-up hints
2. Creates a workspace per issue
3. Launches Codex in [App Server mode](https://developers.openai.com/codex/app-server/) inside the
   workspace
4. Sends a workflow prompt to Codex
5. Keeps Codex working on the issue until the work is done

During app-server sessions, Symphony also serves a client-side `linear_graphql` tool so that repo
skills can make raw Linear GraphQL calls.

If a claimed issue moves to a terminal state (`Done`, `Closed`, `Cancelled`, or `Duplicate`),
Symphony stops the active agent for that issue and cleans up matching workspaces.

## How to use it

1. Make sure your codebase is set up to work well with agents: see
   [Harness engineering](https://openai.com/index/harness-engineering/).
2. Get a new personal token in Linear via Settings → Security & access → Personal API keys, and
   set it as the `LINEAR_API_KEY` environment variable.
3. Copy this directory's `WORKFLOW.md` to your repo.
4. Optionally copy the `commit`, `push`, `pull`, `land`, and `linear` skills to your repo.
   - The `linear` skill expects Symphony's `linear_graphql` app-server tool for raw Linear GraphQL
     operations such as comment editing or upload flows.
5. Customize the copied `WORKFLOW.md` file for your project.
   - To get your project's slug, right-click the project and copy its URL. The slug is part of the
     URL.
   - When creating a workflow based on this repo, note that it depends on non-standard Linear
     issue statuses: "Rework", "Human Review", and "Merging". You can customize them in
     Team Settings → Workflow in Linear.
6. Follow the instructions below to install the required runtime dependencies and start the service.

## Prerequisites

We recommend using [mise](https://mise.jdx.dev/) to manage Elixir/Erlang versions.

```bash
mise install
mise exec -- elixir --version
```

## Run

```bash
git clone https://github.com/openai/symphony
cd symphony/elixir
mise trust
mise install
mise exec -- mix setup
mise exec -- mix build
mise exec -- ./bin/symphony ./WORKFLOW.md
```

## Configuration

Pass a custom workflow file path to `./bin/symphony` when starting the service:

```bash
./bin/symphony /path/to/custom/WORKFLOW.md
```

If no path is passed, Symphony defaults to `./WORKFLOW.md`.

Optional flags:

- `--logs-root` tells Symphony to write logs under a different directory (default: `./log`)
- `--port` also starts the Phoenix observability service (default: disabled)

Symphony also writes an append-only `run-ledger.jsonl` beside the application
log. The file is kept at mode `0600` and contains only bounded run identity,
attempt, stage, workspace, terminal-reason, operator-command outcome/cursor,
dispatch-control, and resolved model fields. It never stores prompts, agent
output, credentials, catalog response details, or comment bodies. At startup,
Symphony closes unfinished attempts from the
previous runner generation before the first poll, restores unresolved waits and
dispatch pause, and resumes each operator comment cursor. An eligible issue is
then redispatched with an incremented attempt and a new run id.

Human Review and Human Clarification transitions are recorded as durable
`waiting_owner` operator waits; Deploy Ready is recorded as
`waiting_live_approval`. The same typed wait model supports secret,
infrastructure, review-cap, authentication, and explicit `operator_stopped`
pauses. Parked issues have no retry timer, are excluded from automatic pickup,
and are restored from the ledger after restart. Resuming a wait does not bypass
the normal exact Linear state eligibility check.

Run-budget stops use the same durable model with reason
`run_budget_exhausted` and exact terminal reason `turn_budget_exhausted`,
`token_budget_exhausted`, or `time_budget_exhausted`.

The `WORKFLOW.md` file uses YAML front matter for configuration, plus a Markdown body used as the
Codex session prompt. If the Markdown body contains `## Symphony Runtime Prompt`, Symphony renders
that section through the end of the file as the worker prompt and leaves earlier Markdown available
for operator documentation. Without that marker, the whole Markdown body is rendered as before.

Minimal example:

```md
---
tracker:
  kind: linear
  webhook_secret: $LINEAR_WEBHOOK_SECRET
  operator_user_ids: []
  project_slug: "..."
workspace:
  root: ~/code/workspaces
hooks:
  after_create: |
    git clone git@github.com:your-org/your-repo.git .
agent:
  max_concurrent_agents: 10
  max_turns: 20
  max_run_tokens: 250000
  max_run_seconds: 7200
codex:
  command: codex app-server
---

You are working on a Linear issue {{ issue.identifier }}.

Title: {{ issue.title }} Body: {{ issue.description }}
```

Notes:

- If a value is missing, defaults are used.
- Safer Codex defaults are used when policy fields are omitted:
  - `codex.approval_policy` defaults to `{"reject":{"sandbox_approval":true,"rules":true,"mcp_elicitations":true}}`
  - `codex.thread_sandbox` defaults to `workspace-write`
  - `codex.turn_sandbox_policy` defaults to a `workspaceWrite` policy rooted at the current issue workspace
- Supported `codex.approval_policy` values depend on the targeted Codex app-server version. In the current local Codex schema, string values include `untrusted`, `on-failure`, `on-request`, and `never`, and object-form `reject` is also supported.
- Supported `codex.thread_sandbox` values: `read-only`, `workspace-write`, `danger-full-access`.
- When `codex.turn_sandbox_policy` is set explicitly, Symphony passes the map through to Codex
  unchanged. Compatibility then depends on the targeted Codex app-server version rather than local
  Symphony validation.
- After app-server initialization, Symphony requests the authenticated `model/list` catalog on the
  same connection. It then records the model and reasoning effort returned by `thread/start` and,
  when the catalog is live, validates that pair before the first `turn/start`.
- Catalog discovery is best-effort: unavailable, malformed, or unsupported `model/list` responses
  are reported as `source=unavailable` and do not prevent older app-server versions from running.
  A successfully fetched live catalog is authoritative for the current session.
- Model observability is intentionally limited to model identifiers, supported reasoning efforts,
  default/upgrade metadata, and a bounded failure code. Symphony never calls app-server
  `config/read` for discovery because effective config can contain MCP credentials.
- `agent.max_turns` is a hard attempt limit on back-to-back Codex turns. If the final allowed turn
  completes while the issue is still active, Symphony parks the run instead of scheduling an
  automatic continuation. Default: `20`.
- `agent.max_run_tokens` optionally caps cumulative Codex tokens observed during one attempt.
- `agent.max_run_seconds` optionally caps wall-clock seconds for one attempt and can stop an
  in-flight turn.
- Reaching any run budget preserves the workspace and creates a durable
  `run_budget_exhausted` wait. Only an explicit `retry` or `reject` resolves it.
- If the Markdown body is blank, Symphony uses a default prompt template that includes the issue
  identifier, title, and body.
- Prompt templates may read immutable run metadata from `run.id`, `run.attempt`,
  `run.stage`, and `run.runner_generation`.
- Use `## Symphony Runtime Prompt` when `WORKFLOW.md` also contains operator-only instructions, so
  pickup/watch-loop guidance does not get sent to the worker as task instructions.
- Use `hooks.after_create` to bootstrap a fresh workspace. For a Git-backed repo, you can run
  `git clone ... .` there, along with any other setup commands you need.
- If a hook needs `mise exec` inside a freshly cloned workspace, trust the repo config and fetch
  the project dependencies in `hooks.after_create` before invoking `mise` later from other hooks.
- `tracker.api_key` reads from `LINEAR_API_KEY` when unset or when value is `$LINEAR_API_KEY`.
- `tracker.webhook_secret` accepts only an environment reference such as
  `$LINEAR_WEBHOOK_SECRET`. When omitted, it reads the canonical `LINEAR_WEBHOOK_SECRET`; without a
  resolved value the webhook endpoint fails closed.
- `tracker.operator_user_ids` is the explicit Linear actor allowlist for comment commands and
  defaults to `[]`, which disables comment commands. The API-key identity (`user.isMe`) is always
  rejected even if listed, because workers can write comments with that same credential. Use a
  separate runner/service identity for `LINEAR_API_KEY` and allowlist only human operator user IDs.
- For path values, `~` is expanded to the home directory.
- For env-backed path values, use `$VAR`. `workspace.root` resolves `$VAR` before path handling,
  while `codex.command` stays a shell command string and any `$VAR` expansion there happens in the
  launched shell.

```yaml
tracker:
  api_key: $LINEAR_API_KEY
  webhook_secret: $LINEAR_WEBHOOK_SECRET
workspace:
  root: $SYMPHONY_WORKSPACE_ROOT
hooks:
  after_create: |
    git clone --depth 1 "$SOURCE_REPO_URL" .
codex:
  command: "$CODEX_BIN --config 'model=\"gpt-5.5\"' app-server"
  dynamic_tool_allowlist:
    - linear_graphql
  mcp_tool_auto_approve_allowlist: []
  mcp_elicitation_auto_approve_allowlist: []
```

- All capability allowlists default to empty. Symphony advertises and executes
  only client-side dynamic tools in `dynamic_tool_allowlist`.
- MCP auto-approval is independent of `approval_policy`: tool approvals require
  an exact `server/tool` entry and elicitation approvals require an exact server
  entry. Missing or malformed identities are denied or declined.
- These MCP checks cover Symphony-mediated non-interactive approval responses;
  MCP servers configured directly in Codex and host/network isolation remain
  separate boundaries.
- If `WORKFLOW.md` is missing or has invalid YAML at startup, Symphony does not boot.
- If a later reload fails, Symphony keeps running with the last known good workflow and logs the
  reload error until the file is fixed.
- `server.port` or CLI `--port` enables the optional Phoenix LiveView dashboard and JSON API at
  `/`, `/api/v1/state`, `/api/v1/<issue_identifier>`, `/api/v1/refresh`, and
  `/api/v1/pause`, plus the webhook receiver at `/api/v1/webhooks/linear`.

### Linear webhook wake-up

When both the HTTP server and `tracker.webhook_secret` are configured, point Linear Issue and
Comment-create webhooks at `https://<public-host>/api/v1/webhooks/linear`. Linear requires a public
HTTPS URL; place Symphony behind an HTTPS reverse proxy rather than exposing the local observability
server directly.

The endpoint verifies the HMAC-SHA256 signature over the exact raw body, delivery UUID, event
identity, and a 60-second timestamp window. A valid Issue or Comment-create event only queues the
normal serialized poll/reconcile cycle. Symphony then re-fetches Linear and uses existing running,
claimed, parked, concurrency, command-cursor, and dispatch-revalidation guards. Duplicate or
out-of-order deliveries therefore do not directly create transitions, and fixed polling remains the
fallback for lost webhook delivery.

### Operator commands and global pause

For running or parked issues, Symphony recognizes this bounded vocabulary only when it appears at
the start of a native Linear issue comment authored by a configured `tracker.operator_user_ids`
actor:

- `$stop` durably parks an active run as `operator_stopped` and preserves its workspace.
- `$retry` resolves a matching wait that allows retry.
- `$approve`, `$approved`, or a standalone `👍` resolves a matching wait that allows approval.
- `$reject` records rejection for a matching wait and keeps the issue parked.

Free-form text, unsupported commands, actors outside the allowlist, API-key/self-authored comments,
mirrored external-thread comments, and oversized bodies are ignored. Commands are context-sensitive:
an action that is not valid for the issue's current run/wait is recorded as rejected and has no
scheduling effect. The durable ledger stores only bounded command identities, outcomes, and cursors,
never the comment body. This makes repeated delivery and restart reconciliation idempotent.

When an existing parked wait has no operator cursor during the first upgrade to this feature,
Symphony initializes the cursor at upgrade time. Historical comments are not executed retroactively;
only comments created after that migration boundary can act as operator commands.

Global dispatch control is available locally through `GET /api/v1/pause` and
`POST /api/v1/pause` with `{"paused": true}` or `{"paused": false}`. These endpoints accept only
loopback callers. Pause is durable across restart and blocks new candidate and retry dispatch while
letting in-flight runs, tracker reconciliation, and operator-command processing continue. Resume
wakes queued retries and schedules an immediate reconcile. A public reverse proxy MUST forward only
the authenticated webhook route, never the dashboard or operator API routes; a loopback proxy would
otherwise itself satisfy the endpoint's local-caller check.

## Web dashboard

The observability UI now runs on a minimal Phoenix stack:

- LiveView for the dashboard at `/`
- JSON API for operational debugging under `/api/v1/*`
- `/api/v1/state` exposes separate `running`, `retrying`, and `parked` lists;
  parked rows include the stable wait id, typed reason, allowed actions, and
  issue/run identity.
- The state payload and terminal header expose `control.dispatch_paused`.
- The same state payload exposes the effective capability allowlist names, but
  never credentials, tool arguments, prompts, or response bodies.
- Bandit as the HTTP server
- Phoenix dependency static assets for the LiveView client bootstrap

## Project Layout

- `lib/`: application code and Mix tasks
- `test/`: ExUnit coverage for runtime behavior
- `WORKFLOW.md`: in-repo workflow contract used by local runs
- `../.codex/`: repository-local Codex skills and setup helpers

## Testing

```bash
make all
```

Run the real external end-to-end test only when you want Symphony to create disposable Linear
resources and launch a real `codex app-server` session:

```bash
cd elixir
export LINEAR_API_KEY=...
make e2e
```

Optional environment variables:

- `SYMPHONY_LIVE_LINEAR_TEAM_KEY` defaults to `SYME2E`
- `SYMPHONY_LIVE_SSH_WORKER_HOSTS` uses those SSH hosts when set, as a comma-separated list

`make e2e` runs two live scenarios:
- one with a local worker
- one with SSH workers

If `SYMPHONY_LIVE_SSH_WORKER_HOSTS` is unset, the SSH scenario uses `docker compose` to start two
disposable SSH workers on `localhost:<port>`. The live test generates a temporary SSH keypair,
mounts the host `~/.codex/auth.json` into each worker, verifies that Symphony can talk to them
over real SSH, then runs the same orchestration flow against those worker addresses. This keeps
the transport representative without depending on long-lived external machines.

Set `SYMPHONY_LIVE_SSH_WORKER_HOSTS` if you want `make e2e` to target real SSH hosts instead.

The live test creates a temporary Linear project and issue, writes a temporary `WORKFLOW.md`, runs
a real agent turn, verifies the workspace side effect, requires Codex to comment on and close the
Linear issue, then marks the project completed so the run remains visible in Linear.

## FAQ

### Why Elixir?

Elixir is built on Erlang/BEAM/OTP, which is great for supervising long-running processes. It has an
active ecosystem of tools and libraries. It also supports hot code reloading without stopping
actively running subagents, which is very useful during development.

### What's the easiest way to set this up for my own codebase?

Launch `codex` in your repo, give it the URL to the Symphony repo, and ask it to set things up for
you.

## License

This project is licensed under the [Apache License 2.0](../LICENSE).

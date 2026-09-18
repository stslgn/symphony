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

If a claimed issue moves to a true terminal state (`Done`, `Closed`, `Cancelled`, or `Duplicate`),
Symphony stops the active agent for that issue and requests cleanup of its exact
recorded workspace. Human/operator wait states win over a conflicting legacy
`terminal_states` entry and never request cleanup.

An owner may set `workspace.preserve_terminal_parked_issue_ids` to exact Linear
issue UUIDs when an accepted parked issue must enter a terminal tracker state
without even a temporary quarantine rename. The runner durably records
`wait_released` with `tracker_terminal_preserved` and retains the workspace;
it does not create a cleanup request. This exception is empty by default and
does not apply to running or other parked issues.

Automatic terminal cleanup is Git-durability-gated. The workspace must have no
modified, staged, or non-ignored untracked files,
`workspace.durability_remote_url` must be set, and
`HEAD` must be reachable through a fresh fetch from that operator-controlled
remote into a new runner-owned bare verifier outside the workspace.
Worker-controlled Git config and refs are never accepted as durability
evidence. Repository-controlled executable Git features such as
`core.fsmonitor` are disabled during inspection. Production cleanup accepts only
credential-free network Git remotes and rejects mutable path and `file://`
sources. Cleanup atomically renames the
exact workspace to a recoverable quarantine, checks before and after
`before_remove`, and restores it on failure only when its exact identity still
matches and the original path remains empty. A successful proof completes lifecycle cleanup but retains the
quarantine artifact; physical deletion remains a separate operator/GC gate.
The proof runs outside the orchestrator in one supervised task with an overall
deadline. Stable preservation failures become operator-required and are not
repeated on each poll.
Each local Git proof command has the same deadline and a 64 KiB combined-output
limit. A keeper remains the exact process-group leader after the command exits;
completion, timeout, cancellation, or excess output stops and kills that
identity-anchored group. A partial private completion frame gets at most 250 ms
to finish before it is treated as excess output. A runner-owned validator survives termination of the
outer cleanup task long enough to complete teardown. It removes its temporary
bare repository only after group disappearance is confirmed, otherwise it
retains the verifier for operator recovery and fails closed.
The ledger records cleanup request, I/O start, operator-required, explicit
retry, I/O completion, and final completion separately. A restart after I/O
start stays operator-required; tracker state cannot authorize a replay. Once
I/O completion is durable, only the final completion record is retried.

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
previous runner generation before the first poll, restores unresolved waits,
durable queued resumes, and dispatch pause, and resumes each operator comment
cursor. An eligible issue is then redispatched with an incremented attempt and
a new run id. Interrupted attempts keep a durable recovery entry containing the
original worker host and canonical workspace path until the next claim.

Persisted operator waits reject invalid UTF-8, Unicode controls, empty values,
and oversized fields before append and recovery. Wait/issue/run ids are capped
at 128 bytes, issue identifiers at 96, tracker state at 128, worker host at 255,
and exact workspace path/root affinity at 4096 bytes. Exact affinity is never
truncated before resume or cleanup.

Human Review and Human Clarification transitions are recorded as durable
`waiting_owner` operator waits; Deploy Ready is recorded as
`waiting_live_approval`, and Blocked as `waiting_infrastructure`. These wait
states take precedence if a legacy workflow also lists them as terminal. The
same typed wait model supports secret, infrastructure, review-cap,
authentication, and explicit `operator_stopped` pauses. Parked issues have no
retry timer, are excluded from automatic pickup, and are restored from the
ledger after restart. Resuming a wait creates a
durable `resume_queued` entry that remains visible with its next attempt while
dispatch is paused or blocked; the next durable claim consumes it. Resume does
not bypass the normal exact Linear state eligibility check. Resume and restart
recovery use the persisted host exclusively and validate the prepared canonical
workspace path before hooks or Codex start. Missing, retired, or busy affinity
blocks dispatch visibly instead of falling back to another SSH worker.

Run-budget stops use the same durable model with reason
`run_budget_exhausted` and exact terminal reason `turn_budget_exhausted`,
`token_budget_exhausted`, `token_telemetry_integrity_failed`, or
`time_budget_exhausted`.

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
  durability_remote_url: git@github.com:your-org/your-repo.git
  preserve_terminal_parked_issue_ids: []
hooks:
  after_create: |
    git clone git@github.com:your-org/your-repo.git .
agent:
  max_concurrent_agents: 10
  max_turns: 20
  max_run_tokens: 250000
  max_run_uncached_input_tokens: 100000
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
- `agent.max_run_tokens` optionally caps cumulative Codex tokens observed during one attempt. An
  explicit cumulative total is accepted; when it is absent, Symphony derives a checked total only
  when both cumulative input and output counters are present. One-sided or malformed telemetry does
  not claim enforceable usage. Exact zero resets start a new telemetry epoch whose later growth is
  added to the prior bounded lifetime. Duplicate values add nothing; malformed counters, checked
  overflow, and ambiguous non-zero decreases permanently fail the attempt's telemetry integrity.
  With a configured token limit, that integrity failure creates a typed durable park instead of
  admitting more work with unknown usage.
- `agent.max_run_uncached_input_tokens` independently caps cumulative uncached
  input (`input_tokens - cached_input_tokens`) across the same checked reset
  epochs. Cached-input telemetry is optional while this guard is disabled and
  appears as unavailable in status. Once the guard is enabled, a cumulative
  usage event without a valid cached-input counter fails telemetry integrity
  closed; reaching the limit parks with
  `uncached_input_budget_exhausted`. The existing total-token limit remains an
  independent coarse ceiling.
- `agent.max_run_seconds` optionally caps wall-clock seconds for one attempt and can stop an
  in-flight turn.
- Reaching any run budget preserves the workspace and creates a durable
  `run_budget_exhausted` wait. Only an explicit `retry` or `reject` resolves it.
- In explicit `full_prompt_compat` mode, a blank Markdown body uses a default
  prompt template. Managed mode rejects a blank body because the required
  runtime heading is absent.
- Prompt templates may read immutable run metadata from `run.id`, `run.attempt`,
  `run.stage`, and `run.runner_generation`.
- For an `Agent Ready` candidate, the Orchestrator owns the pre-model
  `Agent Ready` to `Agent Running` mutation. It records the admission I/O
  intent, performs the tracker mutation, reads the exact issue back, verifies
  the target state and a state-independent canonical issue snapshot, and only
  then starts the agent task. Tracker I/O failures and read-back conflicts
  create typed durable `tracker_admission_failed` or
  `tracker_admission_conflict` waits; neither path starts Codex.
- Managed prompt templates receive only the sanitized evidence fields under
  `run.admission`: `id`, source/target states, snapshot schema/byte count/hash,
  and tracker-authority hash. No credential or raw authority value crosses
  this boundary. The worker inherits that completed admission and must not
  repeat the initial mutation/read-back.
- An admission interrupted by a runner restart retains claim ownership and is
  exposed under the status snapshot's `admitting` collection. If the exact
  issue is already in the target state, Symphony completes the same admission
  without another mutation. If it is still in the exact source state, Symphony
  retries the mutation once under the same admission ID and reads it back. Any
  other state, snapshot, or tracker-authority value creates a conflict wait.
  Tracker read/mutation failure creates a failure wait. Neither recovery path
  can dispatch a duplicate run or start Codex before durable completion.
  The persisted authority hash binds the stable tracker contract rather than a
  process PID/epoch; live I/O still requires the current generation, while an
  exact contract can reconcile across a full process restart.
- Managed workflows must use an exact `## Symphony Runtime Prompt` line so
  pickup/watch-loop guidance does not get sent to the worker as task
  instructions.
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
  The allowlist and raw tracker kind, endpoint, API-key selector, webhook-secret selector, and project
  slug are pinned to the runner generation at startup. Every observed change advances a monotonic
  authority generation. Operator-ID-only drift disables comment commands; tracker identity/scope
  drift blocks every tracker request and discards in-flight poll results until restart, even if the
  file is later restored.
  Admitted polls, in-flight worker state checks, and worker-facing `linear_graphql` calls use the
  immutable startup authority snapshot for the adapter, credential, endpoint, and project. Routing
  assignee and active/terminal state sets are frozen per poll or worker session, so safe reloads
  affect future work without changing an admitted decision. Every worker-side tracker call rechecks
  the monotonic authority generation through a synchronous workflow refresh before every GraphQL
  request, including viewer lookup, pagination, batching, reads, and mutations. The public tracker
  facade, Linear client, and worker entrypoints require the admitted poll/session context; none mint
  a replacement generation when it is missing. Missing or stale context fails before workspace
  preparation, app-server port startup, or network I/O, and the network client never re-reads those
  fields from hot-reloaded config.
- A managed launcher sets `SYMPHONY_EXPECTED_WORKFLOW_SHA256` and
  `SYMPHONY_EXPECTED_RUNTIME_SHA256` to the lowercase SHA-256 values of the exact workflow bytes and
  generation-specific runtime image it admitted, and identifies that image with
  `SYMPHONY_RUNTIME_IMAGE_PATH`. The launcher also pins
  `SYMPHONY_EXPECTED_EXECUTION_SHA256`, a deterministic fingerprint of the loaded BEAM identities
  for Symphony and the recursive graph of bundled application dependencies. `mix build` writes a
  mode-0600 `bin/symphony.runtime-identity` manifest that binds the completed escript image digest
  to that execution fingerprint. Manifest generation invokes the exact escript probe through an
  explicit `env -i` allowlist, so inherited escript-emulator, Erlang-root, BEAM-loader, or
  version-specific VM selectors cannot fabricate build evidence. The side-effect-free identity
  probe reports both its own image
  SHA-256 and its loaded-code fingerprint; the launcher accepts it only when the snapshot digest,
  build manifest, and live probe all agree. Before application startup, Symphony computes the
  execution fingerprint from loaded code and refuses an image/path A-B-A substitution even if the
  pathname bytes are restored.
  A managed launcher may also call
  `bin/symphony --managed-workflow-identity <workflow-path> <project-slug>` in a clean environment.
  This side-effect-free probe validates the fixed Linear endpoint, environment-backed credential
  selectors, explicit project scope, non-ambient assignee routing, and UUID-shaped operator IDs,
  then prints one operator ID per line. Invalid managed authority exits with status 65 before the
  application starts.
  It also compares the workflow value with its single initial
  snapshot and measures the named image bytes. With `SYMPHONY_MANAGED_PROJECT`,
  `SYMPHONY_STARTUP_ATTESTATION_PATH` and
  `SYMPHONY_RUNTIME_READINESS_PATH` are also mandatory. Immediately after `WorkflowStore`
  verification and before Orchestrator or HTTP starts, Symphony atomically writes a mode-0600
  protocol-3 startup-admission attestation. Orchestrator receives the exact immutable settings and
  authority generations held by that live admission child; it cannot replace them with a later
  workflow reload during startup. A separate mode-0600 protocol-3 runtime-readiness attestation is
  written only after Orchestrator, HTTP, and status children initialize. Both attestations contain
  the OS PID, locale-independent process start time, verified workflow digest, runtime-image digest,
  and loaded-code execution digest. Missing or unwritable evidence fails closed. Under the
  `:rest_for_one` supervisor,
  `WorkflowStore` and the admission child precede both task supervisors and all side-effectful
  children. Losing either authority boundary therefore terminates old agent, cleanup, and polling
  tasks before a replacement Orchestrator starts, invalidates final readiness, and re-attests the
  replacement generation. Ordinary in-process hot reload remains supported.
- For path values, `~` is expanded to the home directory.
- For env-backed path values, use `$VAR`. `workspace.root` resolves `$VAR` before path handling,
  while `codex.command` stays a shell command string and any `$VAR` expansion there happens in the
  launched shell.

```yaml
workflow:
  runtime_prompt_mode: managed
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
  required_dynamic_tools:
    - linear_graphql
  mcp_tool_auto_approve_allowlist: []
  mcp_elicitation_auto_approve_allowlist: []
```

- All capability allowlists default to empty. Symphony advertises and executes
  only client-side dynamic tools in `dynamic_tool_allowlist`.
- `required_dynamic_tools` declares worker obligations. Missing entries in the
  effective dynamic-tool allowlist block dispatch before `run_claimed`; the
  same preflight runs before the Codex process starts.
- MCP auto-approval is independent of `approval_policy`: tool approvals require
  an exact `server/tool` entry and elicitation approvals require an exact server
  entry. Tool approval is correlated to a prior structured `mcpToolCall`
  lifecycle event by thread, turn, and item id; display prose is never an
  authorization input. Missing or malformed identities are denied or declined.
- These MCP checks cover Symphony-mediated non-interactive approval responses;
  MCP servers configured directly in Codex and host/network isolation remain
  separate boundaries.
- If `WORKFLOW.md` is missing or has invalid YAML at startup, Symphony does not boot.
- Managed workflows require an exact `## Symphony Runtime Prompt` heading and
  send only the final such section to workers. A missing heading fails closed.
  Full-body prompt fallback exists only as the explicit
  `workflow.runtime_prompt_mode: full_prompt_compat` compatibility mode.
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
normal poll/reconcile cycle. Tracker reads run in one supervised, monitored task while the
orchestrator remains responsive to status, budgets, worker messages, and operator controls. Wake-ups
during that task coalesce behind one dirty latch and cause exactly one follow-up poll. Task
references/generations reject stale results; crash and timeout recovery use bounded backoff. The
poll worker is owned by a supervised registry-backed guard. If the orchestrator dies abnormally,
the guard terminates that worker and retains exclusive poll admission until termination is confirmed,
so a restarted owner cannot overlap an orphaned tracker request. The webhook wake-up call also has a
bounded timeout and returns an unavailable response without crashing the request process. Symphony
re-fetches Linear and uses existing running, claimed, parked,
concurrency, command-cursor, and dispatch-revalidation guards. Duplicate or out-of-order deliveries
therefore do not directly create transitions, and fixed polling remains the fallback for lost
webhook delivery.

### Operator commands and global pause

For running, parked, or queued-retry issues (including retries restored after restart),
Symphony recognizes this bounded vocabulary only when it appears at
the start of a native Linear issue comment authored by a configured `tracker.operator_user_ids`
actor:

- `$stop` durably parks an active run or queued retry as `operator_stopped` and preserves its
  workspace, including when the tracker issue is terminal. Recovered retries are polled for
  commands even when dispatch is paused. The exact run, attempt, and workspace affinity must
  match; conflicting live/recovered entries or a competing queued resume fail closed. Only a
  successful `retry_parked` ledger append retires the retry and recovery entries and cancels any
  live retry timer. A failed append retains the queue and workspace; no worker or cleanup is started.
- `$retry` resolves a matching wait that allows retry, or explicitly authorizes
  one new attempt for an operator-required workspace cleanup.
- `$approve`, `$approved`, or a standalone `👍` resolves a matching wait that allows approval.
- `$reject` records rejection for a matching wait and keeps the issue parked.

Free-form text, unsupported commands, actors outside the allowlist, API-key/self-authored comments,
mirrored external-thread comments, and oversized bodies are ignored. Commands are context-sensitive:
an action that is not valid for the issue's current run/wait is recorded as rejected and has no
scheduling effect. The durable ledger stores only bounded command identities, outcomes, and cursors,
never the comment body. This makes repeated delivery and restart reconciliation idempotent.
If the configured operator authority changes after startup, comment command reconciliation fails
closed until the runner restarts with the new generation. Poll completion rechecks the generation
before applying fetched comments.

When an existing parked wait or recovered retry has no operator cursor during the first upgrade to this feature,
Symphony initializes the cursor at upgrade time. Historical comments are not executed retroactively;
only comments created after that migration boundary can act as operator commands.

### Offline cursor adoption planning

`SymphonyElixir.OperatorCursorMigration.prepare/2` is a read-only library helper,
not a CLI, startup hook, or live migration writer. It accepts a ledger path and an
expected SHA-256, last `runner_generation`, exact set of recovered `issue_ids`, and
UTC ISO-8601 `boundary` strictly later than their existing cursors. Missing cursors,
unfinished runs, mismatched identities, and malformed ledgers fail closed.
The caller must independently establish a stopped generation and choose the
boundary after all historical comments to be excluded; no Linear access occurs.

The plan binds the original byte prefix, file identity, queues, waits, outcomes,
cursors, and intended `operator_cursor_initialized` events. These contain no
fabricated comment ID or owner command. `remaining/2` returns only the unappended
portion of an exact batch, including after a whole-event partial application;
a completed batch returns an empty list. Unexpected appends, changed prefix,
truncated records, replacement files and mutated plans are rejected without repair.
The plan digest detects accidental drift; it is not an authorization signature.

`SymphonyElixir.OperatorCursorApply.request/2` builds a read-only request from the
plan and exact workflow path. Its digest also binds workflow bytes and filesystem
identities. `apply/2` accepts that request and the separately owner-approved request
digest. The caller is responsible for recording real approval; a digest alone is
not authentication. This is an offline library API, not a startup hook or HTTP route.

The macOS adapter accepts only the managed `logs/log/run-ledger.jsonl` layout,
private owned state directories and a single-link private ledger. It creates the
same `start-controller.lock` directory used by managed start, with private owner
metadata binding PID, process start and request digest. Existing locks (even dead
owners) are never stolen. Fresh process snapshots, PID-file checks, workflow and
ledger checks run before appends. Both lock and owner inodes are pinned. Events
use `RunLedger.append/2`; exact-prefix validation runs after application. Success
removes only the adapter's validated owner file and empty lock directory.

Any error after lock acquisition, append failure, or task/process crash retains
the lock and ledger for explicit recovery. Complete-event partial batches can be
resumed after separately authorized lock recovery. Partial JSON records are never
truncated, repaired or reset. Repetition after success is a no-op, but still requires
the stopped-project guard. No workspace, command, image or attestation is removed.

This is a cooperative single-user managed-controller protocol: all starts and
ledger writers must obey the controller lock. It cannot fence malicious same-user
or privileged writers bypassing it. Such access must be excluded by the operational
single-writer window, not assumed away by a hash check. Deployment, live application
and subsequent preflight/start remain separate owner gates. Tests mutate synthetic
fixtures only; no ad-hoc append loop is an approved alternative.

### Dispatch pause

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
- `/api/v1/state` exposes separate `running`, `retrying`, `cleanup_pending`, and
  `parked` lists;
  `retrying` also includes durable `resume_queued` and `recovery_queued` rows
  with their next attempt and host/path affinity,
  while `cleanup_pending` retains its durable claim, has no retry deadline, and
  exposes captured host/path affinity plus only `workspace_cleanup_pending`,
  `workspace_cleanup_failed`, `workspace_affinity_missing`, or
  `workspace_preservation_required`,
  and parked rows include the stable wait id, typed reason, allowed actions,
  issue/run identity, worker host, and canonical workspace path. JSON and
  LiveView share one control-safe, stably sorted parked projection with
  per-field display bounds, a 100-row/65536-byte collection cap, and exact
  total/returned/omitted truncation metadata; the internal cleanup path remains
  exact and separate.
- The state payload and terminal header expose `control.dispatch_paused`.
- The same state payload exposes the effective capability allowlist names, but
  never credentials, tool arguments, prompts, or response bodies.
- Coding-agent status is categorical: event/method names, bounded identifiers,
  counts, and sanitized error codes. Runtime state, logs, JSON, and LiveView do
  not retain or render provider payloads, agent/reasoning deltas, command
  arguments, session titles, or free-form provider errors.
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

Run the deterministic cross-feature scenario profile directly with:

```bash
make scenarios
```

The scenario harness is offline and credential-free. It drives the real
orchestrator, run ledger, run budgets, and Codex app-server client through an
in-memory tracker and bounded fake Codex process. The profile currently covers:

- duplicate wake-ups while global dispatch is paused, followed by exactly one
  eligible dispatch;
- live model discovery, a typed turn-budget park, and parked-wait restoration
  after runner restart;
- batched token telemetry crossing a configured threshold exactly once without
  completion or failure retry;
- fail-closed live model mismatch before prompt delivery and compatibility when
  model discovery is unavailable;
- capability preflight rejecting unknown client-side tools before the Codex
  app-server process launches.

These tests also run inside `make all`. The separate target exists for quick
release-gate and regression checks.

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

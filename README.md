# Symphony

Symphony turns project work into isolated, autonomous implementation runs, allowing teams to manage
work instead of supervising coding agents.

[![Symphony demo video preview](.github/media/symphony-demo-poster.jpg)](.github/media/symphony-demo.mp4)

_In this [demo video](.github/media/symphony-demo.mp4), Symphony monitors a Linear board for work and spawns agents to handle the tasks. The agents complete the tasks and provide proof of work: CI status, PR review feedback, complexity analysis, and walkthrough videos. When accepted, the agents land the PR safely. Engineers do not need to supervise Codex; they can manage the work at a higher level._

> [!WARNING]
> Symphony is a low-key engineering preview for testing in trusted environments.

Symphony can bound each autonomous run by turns, observed Codex tokens, and
wall-clock time. Reaching a configured budget parks the run for explicit
operator resolution instead of silently starting another attempt.

Automatic terminal cleanup is fail-closed. Human/operator wait states always
preserve their workspace, even when a legacy workflow also lists them as
terminal. A true terminal cleanup first proves that the Git workspace has no
modified, staged, or non-ignored untracked files and that its current commit is
reachable through a fresh fetch from the
operator-configured durability remote. Verification runs in a new runner-owned
bare repository outside the workspace. The original path is atomically renamed
to a retained quarantine artifact and is never automatically deleted; failures
restore it only when the exact quarantined directory still owns the identity
and the original path is empty, otherwise both paths remain untouched for
operator recovery. Repository-controlled executable Git features such as
`core.fsmonitor` are disabled during the proof. Production cleanup accepts only
credential-free network Git remotes; mutable path and `file://` remotes are
rejected.
Cleanup I/O runs in one supervised deadline-limited task, so a stalled Git
transport cannot block status or operator controls, and stable preservation
failures are not retried on every poll.
Local durability commands run behind a keeper that remains the exact process
group leader even when the command exits before one of its descendants.
Completion, timeout, cancellation, or more than 64 KiB of combined output stops
and kills that identity-anchored group. The runner-owned verifier outlives
cancellation of the outer cleanup task long enough to complete teardown; if
group disappearance cannot be confirmed, cleanup fails closed and retains the
verifier for operator recovery.
Cleanup authorization is also durable. The ledger records request, I/O start,
operator-required, explicit retry, I/O completion, and final completion as
separate transitions. A restart after I/O starts never infers that it is safe to
repeat the operation; only an explicit operator retry can authorize another
attempt. Once I/O completion is durable, restart retries only the final ledger
completion.

The Elixir implementation also supports durable operator commands and a global
dispatch pause so operators can stop or resume work without losing restart
reconciliation or bypassing normal eligibility checks.

At worker startup it discovers the authenticated Codex model catalog from the
live app-server, records the model and reasoning effort actually selected for
the thread, and rejects an incompatible live pair before sending the first
prompt. Catalog discovery is best-effort so older app-server versions remain
usable.

Managed workflows also declare required worker capabilities. Symphony rejects
pickup before a durable claim or process launch when an obligation such as
`linear_graphql` is absent from the effective dynamic-tool allowlist.

The worker prompt is bounded by the final exact
`## Symphony Runtime Prompt` section. Managed workflows fail validation when
that boundary is absent, so operator-only workflow text cannot broaden worker
authority.

## Running Symphony

### Requirements

Symphony works best in codebases that have adopted
[harness engineering](https://openai.com/index/harness-engineering/). Symphony is the next step --
moving from managing coding agents to managing work that needs to get done.

### Option 1. Make your own

Tell your favorite coding agent to build Symphony in a programming language of your choice:

> Implement Symphony according to the following spec:
> https://github.com/openai/symphony/blob/main/SPEC.md

### Option 2. Use our experimental reference implementation

Check out [elixir/README.md](elixir/README.md) for instructions on how to set up your environment
and run the Elixir-based Symphony implementation. You can also ask your favorite coding agent to
help with the setup:

> Set up Symphony for my repository based on
> https://github.com/openai/symphony/blob/main/elixir/README.md

---

## License

This project is licensed under the [Apache License 2.0](LICENSE).

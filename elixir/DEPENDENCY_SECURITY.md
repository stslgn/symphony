# Dependency security baseline

Reviewed on 2026-09-17 following the PR13 CI advisory output. This is a
dependency-only change: no runner installation, ledger migration or workflow
transition is authorized by these updates.

| Package | Previous lock | Reviewed minimum | EEF CVEs (2026 prefix) |
| --- | --- | --- | --- |
| bandit | 1.10.3 | 1.12.5 | 42788, 39805, 39807, 39804, 42786, 39806, 39803, 74836, 75484 |
| decimal | 2.3.0 | 3.0.0 | 32686 |
| hpax | 1.0.3 | 1.0.4 | 58226 |
| mint | 1.7.1 | 1.10.0 | 49753, 49754, 48861, 59246, 56810, 82728, 48862, 59249, 58229 |
| phoenix | 1.8.4 | 1.8.9 | 32689, 56812, 56811 |
| phoenix_live_view | 1.1.25 | 1.1.33 | 64941 |
| plug | 1.19.1 | 1.19.5 | 56814, 8468, 54892, 56813 |
| req | 0.5.17 | 0.6.1 | 49756, 49755 |

Primary advisory records are published by the Erlang Ecosystem Foundation at
`https://cna.erlef.org/osv/EEF-CVE-2026-<number>.json`; for example,
[Decimal CVE-2026-32686](https://cna.erlef.org/osv/EEF-CVE-2026-32686.json).
The table covers all 30 distinct advisories observed in that CI run, not a
claim that future versions or all application configurations are risk-free.

Explicit constraints for Decimal, HPAX, Mint and Plug protect transitive floors
without overriding parent dependency requirements. Ecto 3.13.6 adds support for
Decimal 3; Ecto stays on 3.13.x. Req stays on 0.6.x, LiveView on 1.1.x and Plug
on 1.19.x to avoid unrelated release-line changes. Bandit requires a compatible
Thousand Island update. Solid 1.2.x also requires Decimal 2, so Solid moves to
1.3.4, which explicitly supports Decimal 3; Jason likewise needs 1.4.5. Phoenix
1.8.14 requires Plug Crypto 2.2.0. Unrelated transitive versions stay locked.
Prompt rendering compatibility must pass the existing tests. No dependency
code is vendored or patched.

`test/symphony_elixir/dependency_security_test.exs` protects these lockfile
minimums. It is not a vulnerability scanner: rescan the entire resolved graph
against a current advisory database before release. Hex 2.4.2 `hex.audit` only
checks retired packages and is insufficient for this purpose.

## Local validation (2026-09-17)

- OSV `v1/querybatch`: all 37 locked Hex packages checked, no findings for the
  resolved candidate. Positive control against the previous lock reproduced all
  30 EEF advisories (48 records including overlapping GHSA entries).
- The eight floor tests failed against the old lock, then passed with the update.
- macOS `make all`: build, format, specs, strict Credo and Dialyzer passed;
  569 tests, zero failures, two opt-in live tests skipped, configured coverage 100%.
- Hex retirement audit: no retired packages.

These are local results. Ubuntu CI and independent review of this dependency
patch remain release gates; previous CI/reviews do not cover the new lockfile.

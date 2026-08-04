```text
╭─ SYMPHONY STATUS
│ Agents: 1/10
│ Dispatch: running
│ Throughput: 15 tps
│ Runtime: 45m 0s
│ Tokens: in 18,000 | out 2,200 | total 20,200
│ Rate Limits: gpt-5 | primary 0/20,000 reset 95s | secondary 0/60 reset 45s | credits none
│ Project: https://linear.app/project/project/issues
│ Next refresh: n/a
├─ Running
│
│   ID       STAGE          PID      AGE / TURN   TOKENS     SESSION        EVENT                                  
│   ───────────────────────────────────────────────────────────────────────────────────────────────────────────────
│ ● MT-638   retrying       4242     20m 25s / 7      14,200 thre...567890  agent message streaming                
│
├─ Backoff queue
│
│  ↻ MT-450 attempt=4 in 1.250s error_code=worker_failure
│  ↻ MT-451 attempt=2 in 3.900s error_code=worker_failure
│  ↻ MT-452 attempt=6 in 8.100s error_code=worker_failure
│  ↻ MT-453 attempt=1 in 11.000s error_code=worker_failure
╰─
```

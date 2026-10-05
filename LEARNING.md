# Read and understand workq

Start with the running dashboard. Submit one of each example and inspect `/v1/jobs/{id}`. A completed result should contain a count and sum; the retry example should finish on attempt three, and the dead-letter example should stop at three attempts. Use those observations to anchor the source walkthrough.

## Suggested reading order

1. `internal/queue/schema.sql`: the durable state and two partial indexes. Explain why `report_results.job_id` is a primary key and why running jobs need both a token and expiry.
2. `internal/queue/store.go`, `Enqueue`: identify the client idempotency key, normalized fingerprint, transaction, and conflict case. Trace what two simultaneous callers receive.
3. `Claim`: read the CTE one clause at a time. The row lock and state update share one statement. Explain what another worker can select while a row is locked.
4. `Heartbeat`, `Complete`, `Fail`, and `RecoverExpired`: compare their predicates. Find the checks that prevent an expired or superseded owner from writing queue state.
5. `internal/worker/worker.go`: follow one job through context creation, heartbeat, handler, cancellation, and acknowledgment. Explain why shutdown leaves recovery to another worker.
6. `internal/worker/report.go`: locate the durable side effect. The database write and queue acknowledgment are separate on purpose. Explain how duplicate execution returns one saved report.
7. `internal/httpapi/api.go`: follow input limits, strict JSON decoding, optional auth, response codes, and the five-second database request timeout.
8. `internal/queue/store_integration_test.go`: each test tells a failure story. Reproduce one and explain the database state before and after it.

## Core questions to answer without reading the README

- Why can two worker processes safely claim different jobs?
- Is this queue strictly FIFO? What does `SKIP LOCKED` give up?
- What changes if a worker crashes before its handler starts? After its report write? After acknowledgment?
- Why is an idempotency key on submission insufficient for the report side effect?
- How does a stale lease token protect the next owner, and what does it fail to protect?
- Why are lease comparisons based on the database clock?
- What happens if a handler ignores context cancellation?
- How are retry delay and the dead-letter attempt cap related?
- Which exact assertions verify replay safety? Which assertion verifies only queue acknowledgment fencing?
- Which production concerns remain outside this implementation?

## Local experiments

Do these against your own demo stack; retain data unless you intentionally want a fresh test database.

1. Submit the same request twice with one `Idempotency-Key`. Confirm the job ID is stable. Then change an item and observe `409`.
2. Scale workers to three. Submit several 2-second jobs and inspect the status list while they are running.
3. Submit a 10-second job and stop one worker process. Its lease should eventually expire and another worker should claim it with a higher attempt count. This is a delivery replay, even though the report record is deduplicated.
4. Set an attempt cap of one and inject failure. Confirm it dead-letters rather than retrying.
5. Run the race-enabled database tests. Read the crash-before-acknowledgment test and explain why the unique report row remains one despite two handler calls.

## Validation boundaries

The tests exercise PostgreSQL-backed jobs, atomic claims, lease recovery, bounded retries, dead-letter status, idempotent submission, and durable report replay protection. These correctness scenarios do not establish throughput, public-host uptime, or general exactly-once processing. Compare each assertion with the mechanism it verifies and the limitations in the README.

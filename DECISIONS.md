# Design decisions

## PostgreSQL is the queue and durable effect store

For a small deployment, PostgreSQL already provides transactions, row locks, indexes, and persistence. Keeping submission identity and job state in one database removes a second broker to operate. This trades queue throughput and polling overhead for a compact system whose failure cases can be tested with real transactions. There is no throughput claim here.

The partial ready index covers only queued jobs, while an expiry index covers running jobs. `FOR UPDATE SKIP LOCKED` avoids consumers waiting for a selected row. It can sacrifice strict ordering because a locked earlier job is skipped; the queue provides approximate schedule order, not global FIFO. [PostgreSQL's documented locking behavior](https://www.postgresql.org/docs/17/sql-select.html)

## Two forms of idempotency solve different problems

Submission idempotency uses a unique client key and a request fingerprint. A transaction's `INSERT ... ON CONFLICT DO NOTHING` followed by a read returns one stable job, even when callers submit that key simultaneously. A mismatched fingerprint is a conflict rather than a silent replacement.

Effect idempotency uses a unique job ID in `report_results`. It handles the independent crash window after handler work but before acknowledgment. API idempotency alone cannot close that window. Both mechanisms are retained rather than calling the whole system exactly once.

## Lease tokens fence queue state writes

Each claim replaces the previous random token. All ownership-sensitive updates require that token and an unexpired lease according to the database clock. Recovery clears the token before requeueing. A resumed old worker may have already performed an idempotent effect, but it cannot finish or fail the new owner's attempt. General external systems would need their own deduplication or fencing protocol.

Workers heartbeat while the handler runs, cancel work on heartbeat failure, and recover expired leases while polling. A short network interruption may cause abandoned processing even if the database still considers the lease live; conservative cancellation is simpler than assuming ownership after an uncertain write.

## Attempts are bounded and visible

Every successful claim consumes an attempt, including crashes before the handler starts. This bounds poison jobs and repeated process death. Handler failures back off with equal jitter; expired leases become immediately visible to avoid adding a second delay after their visibility timeout. The attempt cap still bounds crash loops. Final failures remain in `dead`, with the last error and attempt count visible. Automatic replays would hide failure evidence and can be added later as an explicit operation.

## The demo handler is intentionally small

A numerical report demonstrates durable effects, retry injection, and long-running heartbeats without network access or third-party mutations. Validated payload bounds keep the handler cheap. Panic recovery converts a bad handler attempt into a retryable error, while a context deadline bounds this cooperative handler. Go cannot forcibly stop a handler goroutine that ignores its context; new handlers must preserve that contract.

## Operations stay honest about scope

The container is non-root and the local stack publishes only loopback ports. Optional shared Bearer auth allows a private hosted demo behind a TLS/rate-limiting proxy. The implementation does not include tenant isolation, per-user authorization, job retention, database failover, or exactly-once external effects. A versioned migration runner and observability history would be the next operational additions after measuring real workloads.

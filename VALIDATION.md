# Local validation

Checked October 4, 2026, at 8:03 p.m. America/New_York. These results describe a local synthetic workload, not a public deployment or throughput benchmark.

- Go 1.27.1: `go vet ./...` passed.
- `go test -race -count=1 ./...` passed with `TEST_DATABASE_URL` pointing to PostgreSQL 17 in the local Compose stack. All 18 test functions ran: 12 unit tests and 6 real-database integration tests. The integration tests created and removed only their own random schemas.
- Database scenarios verified concurrent submissions of one idempotency key, 40 jobs claimed by 10 consumers without duplicate ownership, scheduled retry visibility, attempt caps, expired-lease recovery, stale-token rejection, one durable report record across replay after a simulated crash, and heartbeats keeping an 800 ms job alive across a 300 ms lease.
- Docker image built and the Compose deployment started successfully. API and PostgreSQL health checks passed; the worker process was running. API and worker containers use UID/GID `10001:10001`, a read-only root filesystem, and dropped capabilities.
- `python3 scripts/smoke.py` passed against `http://127.0.0.1:18380`: stable submission identity, payload conflict `409`, successful report at attempt 1, retry success at attempt 3, and dead letter at attempt 3. Successful reports returned a sum of 60.

The local deployment was checked at port 18380, with PostgreSQL exposed only on loopback port 15433. Demo smoke records are synthetic and remain visible in its dashboard. These results describe local checks; separate hosted results are recorded in [GitHub Actions](https://github.com/OlaecheaMK0/workq/actions/workflows/ci.yml).

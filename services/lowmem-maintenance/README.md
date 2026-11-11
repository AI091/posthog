# Redis maintenance service

This opt-in Go process replaces `posthog.tasks.tasks.redis_heartbeat` and
`posthog.tasks.tasks.redis_celery_queue_depth`. Both run every ten seconds in
separate goroutines. The heartbeat keeps the existing Redis key and Unix-second
value without expiry. Queue depths keep the existing Pushgateway job, metric,
and labels. Queue names come from `posthog/celery_queues.py` at image build time.
The probe reads the same base Redis lists as Python, including queues this
worker does not consume. It does not include Kombu priority lists.

Build from the repository root:

```sh
docker build -f services/lowmem-maintenance/Dockerfile -t posthog-maintenance .
```

Configure `REDIS_URL` and optionally `PROM_PUSHGATEWAY_ADDRESS`. These must point
to the same Redis database and Pushgateway as Django. The Pushgateway request
bypasses HTTP proxy environment variables, matching `posthog.metrics`.
The service opens no listening socket. It logs failures without addresses or
credentials, bounds each operation, and drains both goroutines on SIGTERM.
It requires Redis SET/LLEN access, not Django, Postgres, or ClickHouse credentials.

## Cutover and rollback

Start and validate the Go service first. Set `POSTHOG_GO_MAINTENANCE=1` on the
Python scheduler and restart it. RedBeat's static-schedule reconciliation removes
the two old schedule entries. Keep the Python task definitions so queued jobs
can drain. Temporary duplicate executions are harmless for these idempotent
writes. Other periodic tasks continue to run in Python.

For rollback, unset the flag and restart the Python scheduler, then stop Go.
A bare environment change does not update an already running beat scheduler.
The switch defaults to Python. For DEBUG installations, do not enable this
service: Python intentionally omits the queue-depth schedule in DEBUG.

## Deployment

Run the image as its own container next to the Celery worker, on a network that
reaches Redis and the Pushgateway. It needs only the two environment variables
above and no volumes. Set `POSTHOG_GO_MAINTENANCE=1` only on the Python scheduler.

## Memory limit of this slice

This removes two Celery job streams, not a Python process. The existing worker,
beat, and API workers remain necessary. The Go RSS must not be subtracted from
an entire Python worker to claim fleet savings. This slice alone adds its small
resident footprint; process-count savings require moving enough remaining work
to retire Python processes. The next useful boundary is the complete scheduler,
including task routing, expiry, cron, persisted run state, and locking.

## Verification

Run `go test -race ./...` and `go vet ./...` in this directory. Tests cover the
Redis heartbeat contract, queue observation without consumption, Pushgateway
path and labels, rejected pushes, and cancellation. The Python scheduling test
covers both flag states and DEBUG states while retaining unrelated schedules.

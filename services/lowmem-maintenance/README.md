# Redis maintenance service

## Threaded web profile (experimental)

`POSTHOG_LOW_MEMORY_WEB=1` selects a single Granian application process in
`bin/docker-server`, with eight concurrent requests per process. It defaults
off. The profile uses ASGI: Django's request-scoped thread contexts let sync
views overlap while retaining async streaming responses. A blanket WSGI switch
would buffer async response iterators before returning their contents.
Authentication, permissions, middleware, serializers and query execution stay
in Django. No metadata endpoint is implemented in Go.

Build `Dockerfile.scheduler` as described below and use that image for web as
well as worker, retaining each service's normal command. The image includes
both default-off switches. Pass the desired environment variables into the
containers explicitly, for example in a Compose environment override.

The profile sets `GRANIAN_INTERFACE=asgi`, defaults `GRANIAN_WORKERS` to `1`
and defaults `GRANIAN_BACKPRESSURE` to `8`. HTTP/1 keep-alive defaults off so idle
proxy connections cannot occupy the limited admission slots and block new requests.
`GRANIAN_HTTP1_KEEP_ALIVE` remains available as an explicit override.
Explicit worker and backpressure values take precedence.
Remove any existing `GRANIAN_WORKERS` override to use the single-process default.
`GRANIAN_BLOCKING_THREADS` controls WSGI, not the
ASGI request threads; it is not needed for this profile. Each concurrent
request can hold its own database connection until request completion.

Set `POSTHOG_LOW_MEMORY_WEB=1` only on web. Worker consumer concurrency remains
independently controlled by `WEB_CONCURRENCY`. To roll back, unset
`POSTHOG_LOW_MEMORY_WEB`, restore the previous `GRANIAN_*` overrides and restart
web. For example, restore `GRANIAN_WORKERS=3` if that was the previous web
configuration. Rolling back web does not change worker scheduling or Redis
state. To roll back the worker too, follow the housekeeping rollback below.

## Complete scheduler (experimental)

`POSTHOG_GO_SCHEDULER=1` selects the Go scheduler in `bin/docker-worker-beat`.
It defaults off. Build the matching Python/Go image from the repository root:

```sh
docker build -f services/lowmem-maintenance/Dockerfile.scheduler -t posthog-go-scheduler .
```

Use this image for the worker with its usual `--with-scheduler` command. A
short-lived Python initialization imports Django, rebuilds the effective beat
schedule (failing on initialization errors), and captures Celery's serialized
messages and routing. Python exits before Go starts scheduling. The existing
Python workers execute every periodic task unless native housekeeping is also enabled.
Do not run the standalone maintenance service alongside scheduler mode.

Go supports positive intervals and Celery cron schedules in UTC, including
Celery's AND semantics for day-of-week and day-of-month. It preserves arguments,
task defaults, queue routing, Redis priority lists, time limits and relative
task expiry. Each dispatch gets new Celery task/correlation identifiers and a
new expiry timestamp. Missed intervals coalesce into one dispatch, like beat.

The scheduler shares RedBeat's lock and static entry definitions, metadata and
schedule index. Dispatch is one Redis Lua operation that checks the lease and
previous metadata, pushes the Kombu message, and records the new last-run time.
This fences expired leaders and prevents duplicate publication if a successful
Redis response is lost. Redis durability and Celery worker acknowledgement
settings continue to determine recovery after broker/worker failures.

Stop the existing scheduler before cutover. If standalone maintenance was enabled,
unset `POSTHOG_GO_MAINTENANCE` and stop that service first; the entrypoint rejects
enabling both modes. A second Go scheduler waits for
the shared lease; it does not publish while another owner holds it. Removed
static entries are reconciled on startup. Configuration changes require a
restart, as with the Python static schedule. To roll back, unset
`POSTHOG_GO_SCHEDULER` and restart the worker; RedBeat resumes the shared run
history. Never delete Redis scheduling state during cutover or rollback.

Unsupported settings fail closed: non-UTC scheduling, relative interval
rounding, cron starting deadlines, non-JSON serializers, non-v2 task protocols,
custom broker transport/TLS options, sent-event publication, separate broker/scheduler Redis databases,
and dynamic RedBeat entries. Keep Python beat for those configurations.
The generated schedule is a local runtime artifact and can contain configured
task arguments; with native housekeeping it also contains connection credentials.
The exporter creates it with mode 0600. Protect it like the worker configuration.

### Schedule and message parity

Run inside the matching PostHog Python environment with its dependencies and
an isolated Redis broker. The corpus compares all effective periodic entries
with actual RedBeat publication through Kombu's Redis transport. It also adds
a synthetic cron AND case with arguments, keyword arguments and priority.

```sh
export CELERY_PARITY_FILE="${TMPDIR:?set TMPDIR to a scratch directory}/parity.json"
POSTHOG_PARITY=1 python -m unittest discover -s services/lowmem-maintenance -p test_export_schedule.py
cd services/lowmem-maintenance
go test -race ./...
go vet ./...
```

The Go test reads `CELERY_PARITY_FILE` and diffs queue names, envelope fields,
arguments, routing and next execution times at leap-day, month and year
boundaries. Generated IDs, publisher identity and reply queue are excluded;
expiry timestamps are covered separately. Without the corpus, this integration
test explicitly skips. Unit tests exercise restart continuity, lease fencing,
ambiguous dispatch retries, expiry and removed/dynamic schedule handling.

### Scheduler memory measurement

On a hobby stack at base `8213ee3814c736bf649153864597bca537e4cfdf`, five
alternating runs per scheduler kept eight Celery children and three API workers.
Each measured window submitted 2,400 events, 12 asynchronous insight queries and
three replays. Both modes ingested exactly 12,000 measured events, completed all
60 queries, and returned all 15 replays, without HTTP or task failures.

| Metric | Python beat | Go scheduler |
| --- | ---: | ---: |
| Worker-container Python processes | 10 | 9 |
| Worker-container total processes | 12 | 12 |
| Worker-container PSS, MiB | 6,051 | 5,004 |
| Worker-container working set, MiB | 6,048 | 5,013 |
| Scheduler RSS, MiB | 1,184 | 10.1 |
| Whole-stack working set, MiB | 12,732 | 11,807 |

Values are medians of five run medians. Worker PSS run medians ranged from
6,010–6,104 MiB with beat and 4,999–5,034 MiB with Go. Cgroup v2 memory and
`/proc` RSS/PSS were collected the same way: twelve snapshots at five-second
cadence, excluding the first two after warmup. Worker swap was zero in both
modes. Whole-stack measurements include the other containers and their cache
variation. These are short synthetic-load windows, not a long-duration soak.

The measured worker PSS saving is 1,047 MiB, explained by retiring the resident
Python scheduler (about 1,045 MiB PSS) and adding the small Go scheduler. Python
consumer concurrency and API workers were retained. The fixture used the same
configuration and data shape for both modes, including composite person indexes
on `(team_id, uuid)` and `(team_id, id)` required by the ingestion upsert.

## Native housekeeping (experimental)

Set both `POSTHOG_GO_SCHEDULER=1` and `POSTHOG_GO_HOUSEKEEPING=1` on the
worker using the scheduler image. Both switches default off. Go then runs:

- Redis heartbeat and Celery queue-depth observations.
- PostgreSQL cache-hit and plugin query-timing metrics.
- ClickHouse query-progress observations every two seconds.

The remaining schedules still publish their original Celery messages. The SQL
is extracted from the Python maintenance functions at startup, and credentials,
queue names, metric configuration and Redis key constants come from Django.
Progress polling only reads the system process/query logs; analytics query
execution remains in Python. Rotating ClickHouse credentials are read on every
request with the same static-password fallback and JWT expiry leeway.

Go merges progress into the existing per-query Redis dictionaries, preserves
unrecognized fields, and refreshes the existing status and heartbeat TTLs.
Progress writes are fenced by the scheduler lease. An already queued Python
poll-chain job drains without rescheduling when both switches are enabled.
Metrics run separately from scheduling and progress polling, with one bounded
operation per metric kind. The legacy SQL and its compatibility limitations
are retained, including best-effort handling of unsupported PostgreSQL statistics.

To return housekeeping to Python, unset `POSTHOG_GO_HOUSEKEEPING` and restart
the worker. Its watchdog restarts the Python poll chain; the shared singleton
timestamp prevents an overlapping old chain. To restore beat too, unset
`POSTHOG_GO_SCHEDULER`. Do not erase Redis state. Configure Python consumer
concurrency separately with the existing `WEB_CONCURRENCY` setting; neither
switch changes the upstream concurrency default.

### Housekeeping contract tests

Use isolated Redis (database 15 is used by the contract producer) and PostgreSQL
instances. The PostgreSQL test creates only connection-local temporary tables.
It runs the original SQL over fixture rows and compares real UDP and HTTP
metric output with Python. The progress test compares Python-generated values,
merging, integer conversion and TTLs; it also covers credential rotation and
loss of leadership during an in-flight HTTP request.

```sh
export HOUSEKEEPING_PARITY_FILE="${TMPDIR:?set TMPDIR to a scratch directory}/housekeeping.json"
python -m unittest discover -s services/lowmem-maintenance -p test_housekeeping.py
export LOWMEM_TEST_POSTGRES_URL="postgresql://localhost/isolated_test_database"
cd services/lowmem-maintenance
go test -race ./...
go vet ./...
```

Keep `CELERY_PARITY_FILE` set too to run the complete schedule/message corpus.
Run `posthog/tasks/test/test_scheduled.py` and
`posthog/tasks/test/test_poll_query_performance.py` in the matching Python
test environment. The flag matrix proves that unsetting either switch keeps
the Python poll-chain implementation available.

### Worker concurrency measurement

Five alternating hobby-stack runs compared upstream beat with eight Celery
children against Go scheduling/housekeeping with one child. Both used three
API workers, identical images for the other services and the fixture described
above. Each mode ingested exactly 12,000 measured events, completed 60 distinct
asynchronous insights and returned 15 captured replays. Stored replay blobs
from both final capture phases were also retrieved and checked for their
captured content. No measured request, task or native-progress failures occurred.

| Metric | Beat, eight children | Go, one child |
| --- | ---: | ---: |
| Worker-container Python processes | 10 | 2 |
| Worker-container total processes | 12 | 5 |
| Worker-container PSS, MiB | 6,051 | 1,230 |
| Worker-container working set, MiB | 6,030 | 1,188 |
| Worker-container swap, MiB | 0 | 0 |
| Scheduler RSS, MiB | 1,185 | 15.0 |
| Whole-stack working set, MiB | 9,927 | 5,127 |
| Whole-stack working set plus swap, MiB | 13,184 | 8,366 |
| Insight pickup time, median / p95 / max, ms | 49 / 62 / 65 | 49 / 68 / 308 |

Memory values are medians of five run medians, using the same cgroup and
per-process sampling method as the scheduler comparison. Each run waits for
the Python consumers and scheduler before a 30-second warmup and 60-second
measurement. Worker PSS run medians ranged from 6,049–6,057 MiB before and
1,224–1,251 MiB after. Other containers held about 3.2 GiB in swap in both
modes, which is why the whole-stack swap-inclusive figure is included.

The measured worker PSS reduction is 4,821 MiB. This combines retirement of
beat with an explicit concurrency change; native housekeeping does not
automatically remove consumers. Python received 370 tasks in the baseline
windows and 178 in the final windows. The latter contained none of the six
replaced housekeeping task streams. The baseline included 149 progress polls,
31 heartbeats, 31 queue probes, 16 progress watchdogs and two cache-hit jobs.
Returning to baseline between runs restarted the Python poll chain.
Business cron counts vary with the wall-clock phase; task totals are not a
throughput comparison.

One child is the smallest positive concurrency and handled this fixture; it
is not a capacity recommendation for larger projects, long exports or cohort
jobs. Those still execute in Python and need their own sizing. API workers
remain at three. Neither concurrency reduction nor API changes are enabled by
default, and these short synthetic windows do not replace a longer soak.

## Standalone Redis maintenance

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
to retire Python processes. The scheduler mode above retires beat. Reducing
Python consumer or API concurrency needs separate load verification.

## Verification

Run `go test -race ./...` and `go vet ./...` in this directory. Tests cover the
Redis heartbeat contract, queue observation without consumption, Pushgateway
path and labels, rejected pushes, and cancellation. The Python scheduling test
covers both flag states and DEBUG states while retaining unrelated schedules.

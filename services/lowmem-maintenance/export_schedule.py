import os
import ast
import sys
import copy
import json
import inspect
from typing import Any
from urllib.parse import quote, urlencode

from unittest.mock import patch

import django
from django.conf import settings

from celery import Celery
from celery.beat import Scheduler
from celery.schedules import crontab, schedule
from kombu import Connection, Producer
from kombu.transport.memory import Channel
from kombu.utils.json import dumps
from redbeat.decoder import RedBeatJSONEncoder
from redbeat.schedulers import RedBeatSchedulerEntry, ensure_conf


def export_schedule(app: Celery) -> dict[str, Any]:
    config = ensure_conf(app)
    if config.redis_url != app.conf.broker_url or app.conf.broker_write_url != app.conf.broker_url:
        raise ValueError("scheduler and broker must use the same Redis database")
    if app.conf.broker_use_ssl or app.conf.task_send_sent_event:
        raise ValueError("custom TLS options and sent-event publication are not supported")
    if app.conf.task_protocol != 2 or app.conf.task_serializer != "json":
        raise ValueError("Go scheduler requires Celery protocol 2 and JSON")
    if app.conf.timezone not in (None, "UTC") or not app.conf.enable_utc:
        raise ValueError("Go scheduler currently requires UTC")
    if app.conf.beat_cron_starting_deadline is not None:
        raise ValueError("cron starting deadline is not supported")
    transport = app.conf.broker_transport_options
    if transport:
        raise ValueError("custom broker transport options are not supported")
    if not config.lock_key:
        raise ValueError("a RedBeat lock is required")

    scheduler = Scheduler(app=app, lazy=True)
    definitions = copy.deepcopy(app.conf.beat_schedule)
    scheduler.install_default_entries(definitions)
    entries = []
    with Connection("memory://") as connection:
        producer = Producer(connection)
        for name, definition in sorted(definitions.items()):
            entry = RedBeatSchedulerEntry(name=name, app=app, **definition)
            timing: dict[str, Any]
            if isinstance(entry.schedule, crontab):
                timing = {
                    "kind": "cron",
                    **{
                        field: sorted(getattr(entry.schedule, field))
                        for field in ("minute", "hour", "day_of_week", "day_of_month", "month_of_year")
                    },
                }
            elif type(entry.schedule) is schedule and not entry.schedule.relative:
                timing = {"kind": "interval", "seconds": entry.schedule.run_every.total_seconds()}
            else:
                raise ValueError(f"unsupported schedule: {name}")
            with patch.object(Channel, "_put") as publish:
                scheduler.apply_async(entry, producer=producer, advance=False)
            if publish.call_count != 1:
                raise ValueError(f"expected one message: {name}")
            queue, message = publish.call_args.args
            message = json.loads(dumps(message))
            priority = max(0, min(int(message["properties"].get("priority", 0)), 9))
            priority = max(step for step in (0, 3, 6, 9) if step <= priority)
            if priority:
                queue += f"\x06\x16{priority}"
            task = app.tasks.get(entry.task)
            expires = entry.options.get("expires", getattr(task, "expires", None))
            if expires is not None and not isinstance(expires, (int, float)):
                raise ValueError(f"expiry must be a number of seconds: {name}")
            if entry.options.get("countdown") or entry.options.get("eta"):
                raise ValueError(f"delayed publication is not supported: {name}")
            if message["headers"].get("expires") is not None and expires is None:
                raise ValueError(f"could not resolve task expiry: {name}")
            if message["headers"].get("eta") is not None:
                raise ValueError(f"delayed task defaults are not supported: {name}")
            entries.append(
                {
                    "name": name,
                    "enabled": entry.enabled,
                    "schedule": timing,
                    "queue": queue,
                    "message": message,
                    "expires": expires,
                    "definition": json.dumps(
                        {
                            "name": name,
                            "task": entry.task,
                            "args": entry.args,
                            "kwargs": entry.kwargs,
                            "options": entry.options,
                            "schedule": entry.schedule,
                            "enabled": entry.enabled,
                        },
                        cls=RedBeatJSONEncoder,
                    ),
                }
            )
    housekeeping = (
        export_housekeeping()
        if os.getenv("POSTHOG_GO_SCHEDULER") == "1" and os.getenv("POSTHOG_GO_HOUSEKEEPING") == "1"
        else None
    )
    return {
        "version": 2 if housekeeping else 1,
        "prefix": config.key_prefix,
        "lock_key": config.lock_key,
        "lock_seconds": config.lock_timeout or 45,
        "entries": entries,
        "housekeeping": housekeeping,
    }


def export_housekeeping() -> dict[str, Any]:
    from posthog.celery_queues import CeleryQueue  # noqa: PLC0415 - Django must be initialized first
    from posthog.clickhouse.client.connection import ClickHouseUser, get_clickhouse_creds  # noqa: PLC0415
    from posthog.clickhouse.client.execute_async import QueryStatusManager  # noqa: PLC0415
    from posthog.tasks.poll_query_performance import get_query_results  # noqa: PLC0415
    from posthog.tasks.tasks import (  # noqa: PLC0415
        Polling,
        pg_plugin_server_query_timing,
        pg_table_cache_hit_rate,
        redis_celery_queue_depth,
        redis_heartbeat,
        start_poll_query_performance,
    )

    credentials = get_clickhouse_creds(ClickHouseUser.OPS)
    database = settings.DATABASES["default"]
    if database["ENGINE"] != "django.db.backends.postgresql":
        raise ValueError("Go housekeeping requires the standard PostgreSQL backend")
    options = database.get("OPTIONS", {})
    supported = {
        "sslmode",
        "sslcert",
        "sslkey",
        "sslrootcert",
        "sslcrl",
        "application_name",
        "connect_timeout",
        "options",
    }
    if any(key not in supported and value is not None and value is not False for key, value in options.items()):
        raise ValueError("unsupported PostgreSQL connection options")
    parameters = {
        "host": database["HOST"],
        "port": database["PORT"] or "5432",
        "user": database["USER"],
        "password": database["PASSWORD"],
        **{key: value for key, value in options.items() if key in supported and value is not None},
    }
    postgres_url = "postgresql:///" + quote(database["NAME"], safe="") + "?" + urlencode(parameters)
    handlers = {
        redis_heartbeat.name: "heartbeat",
        redis_celery_queue_depth.name: "queue_depth",
        pg_table_cache_hit_rate.name: "pg_cache",
        pg_plugin_server_query_timing.name: "pg_timing",
        start_poll_query_performance.name: "query_watchdog",
    }
    return {
        "handlers": handlers,
        "queues": [queue.value for queue in CeleryQueue],
        "gateway": settings.PROM_PUSHGATEWAY_ADDRESS or "",
        "postgres_url": postgres_url,
        "pg_cache_sql": extract_maintenance_sql(pg_table_cache_hit_rate.run),
        "pg_timing_sql": extract_maintenance_sql(pg_plugin_server_query_timing.run),
        "statsd_host": settings.STATSD_HOST or "",
        "statsd_port": int(settings.STATSD_PORT),
        "statsd_prefix": settings.STATSD_PREFIX,
        "statsd_separator": settings.STATSD_SEPARATOR,
        "clickhouse": {
            "url": settings.CLICKHOUSE_HTTP_URL,
            "database": settings.CLICKHOUSE_DATABASE,
            "user": credentials.user,
            "password": credentials.password,
            "password_file": credentials.password_file or "",
            "ca": settings.CLICKHOUSE_CA or "",
            "verify": settings.CLICKHOUSE_VERIFY,
            "cluster": settings.CLICKHOUSE_CLUSTER,
            "sql": extract_maintenance_sql(get_query_results).replace("%(cluster)s", "{cluster:String}"),
        },
        "progress_prefix": QueryStatusManager.KEY_PREFIX_ASYNC_RESULTS,
        "progress_ttl": QueryStatusManager.STATUS_TTL_SECONDS,
        "heartbeat_ttl": QueryStatusManager.HEARTBEAT_TTL_SECONDS,
        "poll_seconds": Polling.TIME_BETWEEN_RUNS_SECONDS,
        "singleton_key": Polling._SINGLETON_REDIS_KEY,
    }


def extract_maintenance_sql(function: Any) -> str:
    tree = ast.parse(inspect.getsource(function))
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "SYSTEM_PROCESSES_SQL" for target in node.targets
        ):
            return ast.literal_eval(node.value)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "execute":
            return ast.literal_eval(node.args[0])
    raise ValueError("maintenance SQL must remain a literal")


def main() -> None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "posthog.settings")
    # Capture all schedules, including the two maintenance tasks. Go scheduler mode
    # publishes those tasks; the standalone maintenance loops must not run alongside it.
    os.environ["POSTHOG_GO_MAINTENANCE"] = "0"
    django.setup()
    from posthog.celery import app  # noqa: PLC0415 - Django must be initialized first
    from posthog.tasks.scheduled import setup_periodic_tasks  # noqa: PLC0415

    app.loader.import_default_modules()
    app.finalize()
    # The signal handler logs and swallows initialization errors. Rebuild explicitly
    # so an incomplete schedule cannot replace beat.
    app.conf.beat_schedule = {}
    setup_periodic_tasks(app)
    exported = export_schedule(app)
    descriptor = os.open(sys.argv[1], os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.fchmod(descriptor, 0o600)
    with os.fdopen(descriptor, "w") as output:
        json.dump(exported, output)


if __name__ == "__main__":
    main()

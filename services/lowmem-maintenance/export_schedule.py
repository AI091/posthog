import os
import sys
import copy
import json
from typing import Any

from unittest.mock import patch

import django

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
    return {
        "version": 1,
        "prefix": config.key_prefix,
        "lock_key": config.lock_key,
        "lock_seconds": config.lock_timeout or 45,
        "entries": entries,
    }


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
    with open(sys.argv[1], "w") as output:
        json.dump(exported, output)


if __name__ == "__main__":
    main()

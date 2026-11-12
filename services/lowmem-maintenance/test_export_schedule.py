import os
import copy
import json
import importlib
from datetime import UTC, datetime
from pathlib import Path

import unittest
from unittest.mock import patch

import django

from celery import Celery, Task
from celery.schedules import crontab
from kombu import Producer
from kombu.transport.redis import Channel
from kombu.utils.json import dumps
from redbeat.schedulers import RedBeatScheduler, RedBeatSchedulerEntry

export_schedule = importlib.import_module("services.lowmem-maintenance.export_schedule").export_schedule


class SyntheticTask(Task):
    name = "synthetic.default_expiry"
    queue = "parity"
    expires = 45

    def run(self, value: int) -> int:
        return value


class TestExportSchedule(unittest.TestCase):
    def setUp(self) -> None:
        self.app = Celery("parity", broker=os.environ.get("REDIS_URL", "redis://localhost/"))
        self.app.conf.update(timezone="UTC", enable_utc=True, task_serializer="json")

    def test_rejects_unsupported_settings(self) -> None:
        for settings in (
            {"timezone": "Europe/London"},
            {"task_serializer": "pickle"},
            {"broker_transport_options": {"global_keyprefix": "other:"}},
            {"beat_cron_starting_deadline": 30},
        ):
            with self.subTest(settings=settings):
                app = Celery("unsupported", broker="redis://localhost/")
                app.conf.update(settings)
                with self.assertRaises(ValueError):
                    export_schedule(app)

    def test_celery_parity_corpus(self) -> None:
        path = os.environ.get("CELERY_PARITY_FILE")
        if not path:
            self.skipTest("set CELERY_PARITY_FILE to generate the Go parity corpus using Redis")
        app = self.app
        if os.environ.get("POSTHOG_PARITY") == "1":
            os.environ.setdefault("DJANGO_SETTINGS_MODULE", "posthog.settings")
            os.environ["POSTHOG_GO_MAINTENANCE"] = "0"
            django.setup()
            from posthog.celery import app  # noqa: PLC0415 - Django must be initialized first
            from posthog.tasks.scheduled import setup_periodic_tasks  # noqa: PLC0415

            app.loader.import_default_modules()
            app.finalize()
            app.conf.beat_schedule = {}
            setup_periodic_tasks(app)
        app.conf.beat_schedule["parity cron AND"] = {
            "task": "synthetic.housekeeping",
            "schedule": crontab(minute="7,37", hour="1,16", day_of_week="mon", day_of_month="1-7"),
            "args": [3],
            "kwargs": {"dry_run": True},
            "options": {"queue": "parity", "priority": 7, "expires": 90},
        }
        app.register_task(SyntheticTask())
        app.conf.beat_schedule["parity task defaults"] = {
            "task": SyntheticTask.name,
            "schedule": 7.5,
            "args": [3],
        }
        exported = export_schedule(app)
        scheduler = RedBeatScheduler(app=app, lazy=True)
        definitions = copy.deepcopy(app.conf.beat_schedule)
        scheduler.install_default_entries(definitions)
        cases = []
        dates = [
            datetime(2024, 2, 28, 23, 59, 59, tzinfo=UTC),
            datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC),
            datetime(2026, 11, 1, 1, 7, 29, tzinfo=UTC),
            datetime(2026, 12, 31, 23, 59, 59, tzinfo=UTC),
        ]
        with app.connection_for_write() as connection:
            producer = Producer(connection)
            for name, definition in sorted(definitions.items()):
                entry = RedBeatSchedulerEntry(name=name, app=app, **definition)
                for last in dates:
                    entry.schedule.nowfun = lambda last=last: last
                    next_run = last + entry.schedule.remaining_estimate(last)
                    with patch.object(Channel, "_put") as publish:
                        scheduler.apply_async(entry, producer=producer, advance=False)
                    self.assertEqual(publish.call_count, 1, name)
                    queue, message = publish.call_args.args
                    priority = connection.default_channel._get_message_priority(message, reverse=False)
                    queue = connection.default_channel._q_for_pri(queue, priority)
                    cases.append(
                        {
                            "name": name,
                            "last": last.isoformat(),
                            "next": next_run.isoformat(),
                            "queue": queue,
                            "message": json.loads(dumps(message)),
                        }
                    )
        self.assertEqual({entry["name"] for entry in exported["entries"]}, set(definitions))
        Path(path).write_text(json.dumps({"config": exported, "cases": cases}))


if __name__ == "__main__":
    unittest.main()

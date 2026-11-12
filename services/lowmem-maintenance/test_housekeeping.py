import os
import json
import math
import importlib
from types import SimpleNamespace

import unittest
from unittest.mock import MagicMock, patch

import django
from django.test import override_settings

from redis import Redis
from statshog.client.udp import StatsClient


class TestHousekeepingParity(unittest.TestCase):
    def test_generate_python_contract(self) -> None:
        output = os.environ.get("HOUSEKEEPING_PARITY_FILE")
        if not output:
            self.skipTest("set HOUSEKEEPING_PARITY_FILE and REDIS_URL to generate the contract")
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "posthog.settings")
        django.setup()
        tasks = importlib.import_module("posthog.tasks.tasks")
        polling = importlib.import_module("posthog.tasks.poll_query_performance")
        manager = importlib.import_module("posthog.clickhouse.client.execute_async").QueryStatusManager

        exporter = importlib.import_module("services.lowmem-maintenance.export_schedule")
        client = Redis.from_url(os.environ["REDIS_URL"], db=15)
        first = "550e8400-e29b-41d4-a716-446655440000"
        second = "00008400-e29b-41d4-a716-446655440000"
        rows = [
            (f"7_{first}_b", 222, 111, 333, 4.301284, 3321332424, "child-a"),
            ("not_an_initial_query_id", 0, 0, 0, float("nan"), 0, "ignored"),
            (f"0007_{first}_a", 1, 2**64 - 1, 99, 1.9, 9, "child-b"),
            (f"7_{first}_b", 444, 555, 444, 7.9, 33, "child-a"),
            (f"9_{second.upper()}_suffix", 10, 20, float("nan"), -1.5, 6, "child-c"),
        ]
        old = {
            "child-old": {
                "initial_query_id": f"7_{first}",
                "query_id": "child-old",
                "bytes_read": 7,
                "rows_read": 8,
                "estimated_rows_total": 9,
                "time_elapsed": 2,
                "active_cpu_time": 3,
                "future_field": "preserve",
            }
        }
        key = f"query_async:7:{first}:status"
        for team, query in ((7, first), (9, second.upper())):
            client.delete(f"query_async:{team}:{query}:status", f"query_async:{team}:{query}:heartbeat")
        client.set(key, json.dumps(old), ex=30)
        with patch("posthog.redis.get_client", return_value=client):
            with patch.object(polling, "sync_execute", return_value=rows):
                polling.poll_query_performance()
        expected = []
        for team, query in ((7, first), (9, second.upper())):
            base = f"query_async:{team}:{query}"
            expected.append(
                {
                    "key": base + ":status",
                    "value": json.loads(client.get(base + ":status")),
                    "ttl": client.ttl(base + ":status"),
                    "heartbeat": client.get(base + ":heartbeat").decode(),
                    "heartbeat_ttl": client.ttl(base + ":heartbeat"),
                }
            )
            client.delete(base + ":status", base + ":heartbeat")
        client.close()
        http_rows = []
        for row in rows:
            http_rows.append(
                dict(
                    zip(
                        (
                            "initial_query_id",
                            "rows_read",
                            "bytes_read",
                            "estimated_rows_total",
                            "time_elapsed",
                            "active_cpu_time",
                            "query_id",
                        ),
                        ["nan" if isinstance(value, float) and math.isnan(value) else value for value in row],
                    )
                )
            )
        metrics = []
        for kind, values in (
            ("pg_cache", [("events", 97.5), ('quoted"\\table', 100.0)]),
            ("pg_cache_null", [("events", 97.5), ("unused", None)]),
            ("pg_timing", [("fetch", 10.0, 5.0, 1.0, 9.0, 2.0, 2, 3), (None, 4.0, 4.0, 0.0, 4.0, 0.0, 1, 1)]),
        ):
            task = tasks.pg_plugin_server_query_timing if kind == "pg_timing" else tasks.pg_table_cache_hit_rate
            cursor = MagicMock()
            cursor.fetchall.return_value = values
            cursor.description = [
                SimpleNamespace(name=name)
                for name in (
                    "query_type",
                    "total_time",
                    "avg_time",
                    "min_time",
                    "max_time",
                    "stddev_time",
                    "calls",
                    "rows_read_or_affected",
                )
            ]
            stats = StatsClient(prefix="fixture", telegraf=True, separator="_")
            with override_settings(PROM_PUSHGATEWAY_ADDRESS="http://gateway.invalid"):
                with patch.object(tasks, "connection") as connection:
                    connection.cursor.return_value.__enter__.return_value = cursor
                    with patch("statshog.defaults.django.statsd", stats), patch.object(stats, "_send") as send:
                        with patch("posthog.metrics.push_to_gateway") as push:
                            task.run()
            from prometheus_client import generate_latest  # noqa: PLC0415

            metrics.append(
                {
                    "kind": kind,
                    "rows": values,
                    "sql": cursor.execute.call_args.args[0],
                    "statsd": [call.args[0] for call in send.call_args_list],
                    "prometheus": generate_latest(push.call_args.kwargs["registry"]).decode() if push.called else "",
                }
            )
            stats.close()
        corpus = {
            "rows": http_rows,
            "old_key": key,
            "old": old,
            "expected": expected,
            "metrics": metrics,
            "progress_prefix": manager.KEY_PREFIX_ASYNC_RESULTS,
            "progress_ttl": manager.STATUS_TTL_SECONDS,
            "heartbeat_ttl": manager.HEARTBEAT_TTL_SECONDS,
            "singleton_key": tasks.Polling._SINGLETON_REDIS_KEY,
            "sql": exporter.extract_maintenance_sql(polling.get_query_results).replace(
                "%(cluster)s", "{cluster:String}"
            ),
        }
        descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w") as file:
            json.dump(corpus, file)


if __name__ == "__main__":
    unittest.main()

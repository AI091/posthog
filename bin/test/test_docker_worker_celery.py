import os
import json
import signal
import selectors
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory

import unittest

from parameterized import parameterized

LAUNCHER = Path(__file__).resolve().parents[1] / "docker-worker-celery"
QUEUES = LAUNCHER.with_name("celery-queues.env")


class TestDockerWorkerCelery(unittest.TestCase):
    @parameterized.expand(
        [
            ("default", {}, 1, None, False),
            ("isolated", {"POSTHOG_LOW_MEMORY_WORKER": "1"}, 2, "3", False),
            ("scheduler", {"POSTHOG_LOW_MEMORY_WORKER": "1"}, 2, "3", True),
            ("consumer_exit", {"POSTHOG_LOW_MEMORY_WORKER": "1"}, 2, "3", True),
            (
                "overrides",
                {
                    "POSTHOG_LOW_MEMORY_WORKER": "1",
                    "WEB_CONCURRENCY": "4",
                    "CELERY_QUERY_CONCURRENCY": "2",
                    "CELERY_WORKER_PREFETCH_MULTIPLIER": "3",
                },
                2,
                "4",
                False,
            ),
        ]
    )
    def test_consumers_and_shutdown(
        self, name: str, overrides: dict[str, str], count: int, concurrency: str | None, scheduler: bool
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "bin").mkdir()
            (root / "bin/celery-queues.env").write_text(QUEUES.read_text())
            (root / "bin/migrate-check").write_text("#!/bin/sh\nexit 0\n")
            (root / "bin/migrate-check").chmod(0o755)
            (root / "bin/docker-worker-beat").write_text(
                '#!/bin/sh\nprintf "%s\\n" "$$" >> "$TMPDIR/schedulers"\nexec sleep 60\n'
            )
            (root / "bin/docker-worker-beat").chmod(0o755)
            (root / "celery").write_text(
                """#!/usr/bin/env python
import json, os, signal, sys
print(json.dumps({"pid": os.getpid(), "args": sys.argv[1:],
    "queues": os.environ["CELERY_WORKER_QUEUES"],
    "metrics": os.environ["PROMETHEUS_MULTIPROC_DIR"]}), flush=True)
signal.pause()
"""
            )
            (root / "celery").chmod(0o755)
            env = {
                key: value
                for key, value in os.environ.items()
                if not key.startswith(
                    ("CELERY_", "POSTHOG_LOW_MEMORY_WORKER", "PROMETHEUS_MULTIPROC_DIR", "WEB_CONCURRENCY")
                )
            }
            env.update(PATH=f"{root}:{env['PATH']}", TMPDIR=directory, **overrides)
            process = subprocess.Popen(
                ["bash", str(LAUNCHER), *(["--with-scheduler"] if scheduler else [])],
                cwd=root,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
            )
            workers = []
            try:
                assert process.stdout is not None
                with selectors.DefaultSelector() as selector:
                    selector.register(process.stdout, selectors.EVENT_READ)
                    buffer = b""
                    while len(workers) < count:
                        self.assertTrue(selector.select(timeout=5), "isolated query consumer did not start")
                        chunk = os.read(process.stdout.fileno(), 65536)
                        self.assertTrue(chunk, "launcher exited before both consumers started")
                        buffer += chunk
                        while b"\n" in buffer:
                            line, buffer = buffer.split(b"\n", 1)
                            if line.startswith(b"{"):
                                workers.append(json.loads(line))
                expected_queues = set(QUEUES.read_text().split("CELERY_WORKER_QUEUES=")[1].strip().split(","))
                self.assertEqual(
                    {queue for worker in workers for queue in worker["queues"].split(",")}, expected_queues
                )
                if count == 2:
                    foreground = next(worker for worker in workers if worker["queues"] == "analytics_queries")
                    background = next(worker for worker in workers if worker is not foreground)
                    self.assertNotIn("analytics_queries", background["queues"].split(","))
                    self.assertNotEqual(foreground["metrics"], background["metrics"])
                    self.assertNotEqual(
                        foreground["args"][foreground["args"].index("-n") + 1],
                        background["args"][background["args"].index("-n") + 1],
                    )
                for worker in workers:
                    args = worker["args"]
                    if concurrency is None:
                        self.assertNotIn("--concurrency", args)
                        self.assertNotIn("--prefetch-multiplier", args)
                    else:
                        expected_concurrency = (
                            overrides.get("CELERY_QUERY_CONCURRENCY", "1")
                            if worker["queues"] == "analytics_queries"
                            else concurrency
                        )
                        self.assertEqual(args[args.index("--concurrency") + 1], expected_concurrency)
                        self.assertEqual(
                            args[args.index("--prefetch-multiplier") + 1],
                            overrides.get("CELERY_WORKER_PREFETCH_MULTIPLIER", "1"),
                        )
                if name == "consumer_exit":
                    os.kill(workers[0]["pid"], signal.SIGTERM)
                else:
                    process.terminate()
                process.communicate(timeout=5)
                self.assertEqual(process.returncode, 143)
                if scheduler:
                    scheduler_pids = (root / "schedulers").read_text().splitlines()
                    self.assertEqual(len(scheduler_pids), 1)
                    with self.assertRaises(ProcessLookupError):
                        os.kill(int(scheduler_pids[0]), 0)
                for worker in workers:
                    with self.assertRaises(ProcessLookupError):
                        os.kill(worker["pid"], 0)
            finally:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.communicate()

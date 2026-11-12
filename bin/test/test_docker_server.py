import os
import json
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory

import unittest

from parameterized import parameterized

LAUNCHER = Path(__file__).resolve().parents[1] / "docker-server"


class TestDockerServer(unittest.TestCase):
    @parameterized.expand(
        [
            ({}, {"interface": "asgi", "workers": "4", "backpressure": None, "target": "posthog.asgi:application"}),
            (
                {"GRANIAN_INTERFACE": "wsgi", "GRANIAN_WORKERS": "3", "GRANIAN_BLOCKING_THREADS": "8"},
                {"interface": "wsgi", "workers": "3", "backpressure": None, "target": "posthog.wsgi:application"},
            ),
            (
                {"POSTHOG_LOW_MEMORY_WEB": "1"},
                {"interface": "asgi", "workers": "1", "backpressure": "8", "target": "posthog.asgi:application"},
            ),
            (
                {
                    "POSTHOG_LOW_MEMORY_WEB": "1",
                    "GRANIAN_INTERFACE": "wsgi",
                    "GRANIAN_WORKERS": "2",
                    "GRANIAN_BACKPRESSURE": "16",
                },
                {"interface": "asgi", "workers": "2", "backpressure": "16", "target": "posthog.asgi:application"},
            ),
        ]
    )
    def test_serving_mode(self, overrides: dict[str, str], expected: dict[str, str | None]) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "bin").mkdir()
            executables = {
                "bin/migrate-check": "#!/bin/sh\nexit 0\n",
                "bin/granian_metrics.py": "",
                "setpriv": '#!/bin/sh\nshift 3\nexec "$@"\n',
                "granian": """#!/usr/bin/env python
import json
import os
import sys
print(json.dumps({
    "interface": os.getenv("GRANIAN_INTERFACE"),
    "workers": os.getenv("GRANIAN_WORKERS"),
    "backpressure": os.getenv("GRANIAN_BACKPRESSURE"),
    "target": sys.argv[-1],
}))
""",
            }
            for name, content in executables.items():
                path = root / name
                path.write_text(content)
                path.chmod(0o755)
            env = {
                key: value
                for key, value in os.environ.items()
                if not key.startswith(("GRANIAN_", "POSTHOG_LOW_MEMORY_WEB", "POSTHOG_GRANIAN_SHARED_SOCKET"))
            }
            env.update(PATH=f"{root}:{env['PATH']}", TMPDIR=directory, **overrides)
            result = subprocess.run(
                ["bash", str(LAUNCHER)], cwd=root, env=env, capture_output=True, text=True, check=True
            )
            self.assertEqual(json.loads(result.stdout), expected)

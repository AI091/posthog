import sys
import json
import runpy
from pathlib import Path

root = Path(__file__).resolve().parents[2]
queues = runpy.run_path(str(root / "posthog/celery_queues.py"))["CeleryQueue"]
sys.stdout.write(json.dumps([queue.value for queue in queues]) + "\n")

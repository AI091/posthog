import os

from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase, TestCase

from parameterized import parameterized

from posthog.celery import app
from posthog.tasks.scheduled import instance_spread_minute, setup_periodic_tasks
from posthog.tasks.team_llm_gateway_quota import reconcile_llm_gateway_quota_projection


class TestScheduledTasks(TestCase):
    def test_scheduled_tasks(self) -> None:
        """
        `setup_periodic_tasks` may fail silently. This test ensures that it doesn't.
        """
        try:
            setup_periodic_tasks(app)
        except Exception as exc:
            assert exc is None, exc


class TestGoMaintenanceScheduling(SimpleTestCase):
    @parameterized.expand([(False, False), (True, False), (False, True), (True, True)])
    def test_maintenance_has_one_owner(self, enabled: bool, debug: bool) -> None:
        sender = MagicMock()
        with patch.dict(os.environ, {"POSTHOG_GO_MAINTENANCE": "1" if enabled else "0"}):
            with self.settings(DEBUG=debug):
                setup_periodic_tasks(sender)
        names = [call.kwargs.get("name") for call in sender.add_periodic_task.call_args_list]
        assert ("10 sec heartbeat" in names) == (not enabled)
        assert ("10 sec queue probe" in names) == (not enabled and not debug)
        assert "query performance heartbeat" in names


class TestPrivacyTaskScheduling(SimpleTestCase):
    @parameterized.expand([("disabled", "", 0), ("enabled", "test-table", 1)])
    def test_privacy_schedule_requires_configured_worker(self, _name: str, table: str, expected_count: int) -> None:
        sender = MagicMock()
        with self.settings(AI_RESEARCH_REPLAY_KEY_TABLE=table):
            setup_periodic_tasks(sender)
        calls = [
            call
            for call in sender.add_periodic_task.call_args_list
            if call.kwargs.get("name") == "process-ai-training-privacy"
        ]
        self.assertEqual(len(calls), expected_count)
        if calls:
            self.assertEqual(calls[0].args[1].type.queue, "ai_research_privacy")


class TestLLMGatewayQuotaReconcileScheduling(SimpleTestCase):
    def test_the_reconcile_runs_every_15_minutes(self) -> None:
        sender = MagicMock()
        setup_periodic_tasks(sender)
        calls = [
            call
            for call in sender.add_periodic_task.call_args_list
            if call.kwargs.get("name") == "llm-gateway quota projection reconcile"
        ]
        self.assertEqual(len(calls), 1)
        schedule, signature = calls[0].args
        self.assertEqual(schedule.minute, {7, 22, 37, 52})
        self.assertEqual(signature.task, reconcile_llm_gateway_quota_projection.name)


class TestInstanceSpreadMinute(SimpleTestCase):
    def test_one_installation_keeps_its_minute_in_every_process(self) -> None:
        # A literal, because the minute has to survive a beat restart. A hash that
        # is only stable inside one process, such as the built-in hash(), passes an
        # equality check between two calls and still moves the schedule on restart.
        with self.settings(SITE_URL="https://an-installation.example.com"):
            assert instance_spread_minute("send license usage", 40) == "18"

    def test_installations_do_not_all_get_the_same_minute(self) -> None:
        minutes = set()
        for site_url in ("site-one", "site-two", "site-three", "site-four"):
            with self.settings(SITE_URL=site_url):
                minutes.add(instance_spread_minute("send license usage", 40))
        assert len(minutes) > 1

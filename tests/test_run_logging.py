"""External-behavior tests for proactive structured run logging."""

from __future__ import annotations

import contextlib
import io
import json
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from hermes_constants import reset_hermes_home_override, set_hermes_home_override
from tests.test_goal_tracking import SEED_GOAL_ID, load_plugin_package, seed_tracking_fixture


REPO_ROOT = Path(__file__).resolve().parents[1]
PLUGIN_DIR = REPO_ROOT / "plugins" / "proactive"
LOG_LOGGER = logging.getLogger("proactive")
UTC = timezone.utc
SOURCE_RE = re.compile(r"\[source:(\d+)\]")


class FakeStructuredLlm:
    """Deterministic structured LLM double; a responder may raise instead of replying."""

    def __init__(self, responders):
        self._responders = list(responders)
        self.calls = []

    def complete_structured(self, **kwargs):
        self.calls.append(kwargs)
        responder = self._responders[len(self.calls) - 1]
        payload = responder(kwargs) if callable(responder) else responder
        if isinstance(payload, BaseException):
            raise payload
        if isinstance(payload, SimpleNamespace):
            return payload
        return SimpleNamespace(
            parsed=payload,
            text=json.dumps(payload, ensure_ascii=False),
            content_type="json",
        )


def first_marker(call) -> int:
    text = "\n".join(str(block.get("text") or "") for block in call["input"])
    return int(SOURCE_RE.search(text).group(1))


def utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def unix_ts(value: datetime) -> float:
    return value.astimezone(timezone.utc).timestamp()


class RecordingContext:
    """Registration surface with the host's plugin-config getter."""

    def __init__(self, config=None):
        self.config = dict(config or {})
        self.tools = []
        self.cli_commands = []
        self.commands = []
        self.skills = []
        self.llm = None

    def get_config(self, key, default=None):
        return self.config.get(key, default)

    def register_tool(self, **kwargs):
        self.tools.append(kwargs)

    def register_cli_command(self, **kwargs):
        self.cli_commands.append(kwargs)

    def register_command(self, name, handler, description="", args_hint=""):
        self.commands.append({"name": name, "handler": handler})

    def register_skill(self, name, path, description=""):
        self.skills.append({"name": name, "path": Path(path), "description": description})


class RunLoggingTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.plugin = load_plugin_package()
        cls.log = cls.plugin.log
        cls.snapshot = cls.plugin.snapshot
        cls.storage = cls.plugin.storage

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="hermes-proactive-logging-test-")
        self.home = Path(self._tmp.name)
        self._old_home = os.environ.get("HERMES_HOME")
        os.environ["HERMES_HOME"] = str(self.home)
        self._override_token = set_hermes_home_override(str(self.home))
        self.addCleanup(self._cleanup_home)
        self.addCleanup(LOG_LOGGER.setLevel, logging.NOTSET)
        self.storage.init_db()
        seed_tracking_fixture(self.storage)

    def _cleanup_home(self):
        reset_hermes_home_override(self._override_token)
        if self._old_home is None:
            os.environ.pop("HERMES_HOME", None)
        else:
            os.environ["HERMES_HOME"] = self._old_home
        self._tmp.cleanup()

    def _capture(self, level=logging.DEBUG):
        """Capture records emitted through the plugin's namespaced logger."""
        return self.assertLogs("proactive", level=level)

    @property
    def plugin_data(self) -> Path:
        return self.home / "plugin-data" / "proactive"

    @property
    def hidden_files(self) -> Path:
        return self.plugin_data / "hidden_files"

    @property
    def db_path(self) -> Path:
        return self.plugin_data / "data.db"

    @property
    def jobs_path(self) -> Path:
        return self.home / "cron" / "jobs.json"

    def _config(self, **overrides) -> dict:
        config = {"memory_source": "builtin", "timezone": "Asia/Shanghai"}
        config.update(overrides)
        return config

    def _seed_state_db(self, rows, *, started_at: datetime) -> None:
        with contextlib.closing(sqlite3.connect(self.home / "state.db")) as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS sessions (
                    id TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    title TEXT,
                    display_name TEXT,
                    parent_session_id TEXT,
                    started_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    content TEXT,
                    timestamp REAL NOT NULL,
                    active INTEGER NOT NULL DEFAULT 1
                );
                """
            )
            conn.execute("DELETE FROM messages")
            conn.execute("DELETE FROM sessions")
            conn.execute(
                "INSERT INTO sessions (id, source, title, display_name, parent_session_id, started_at)"
                " VALUES ('session-1', 'feishu', '飞书会话', NULL, NULL, ?)",
                (unix_ts(started_at),),
            )
            conn.executemany(
                "INSERT INTO messages (session_id, role, content, timestamp, active) VALUES (?, ?, ?, ?, 1)",
                [("session-1", role, content, unix_ts(stamp)) for role, content, stamp in rows],
            )
            conn.commit()

    def _write_cron_jobs(self, jobs) -> None:
        self.jobs_path.parent.mkdir(parents=True, exist_ok=True)
        self.jobs_path.write_text(
            json.dumps({"jobs": jobs, "updated_at": utc_iso(datetime.now(timezone.utc))}, ensure_ascii=False),
            encoding="utf-8",
        )

    def _proactive_job(self, *, job_id="job-log", last_status="ok", last_delivery_error=None) -> dict:
        return {
            "id": job_id,
            "name": "proactive_notifier",
            "schedule": {"kind": "cron", "expr": "30 9,12,15,18,21 * * *"},
            "schedule_display": "30 9,12,15,18,21 * * *",
            "timezone": "Asia/Shanghai",
            "next_run_at": "2026-11-09T15:30:00+08:00",
            "last_status": last_status,
            "last_delivery_error": last_delivery_error,
            "deliver": "origin",
        }

    def _create_checkpoint(self, run_id, window_start, window_end, *, status="committed"):
        return self.storage.create_checkpoint(
            run_id,
            str(self.hidden_files / run_id),
            utc_iso(window_start),
            utc_iso(window_end),
            status=status,
            candidate_count=0,
            created_at=utc_iso(window_end),
            committed_at=utc_iso(window_end) if status == "committed" else None,
        )

    def _pin_tracking_updated_at(self, stamp: datetime) -> None:
        """Move the seeded goal behind the scan window so it is not 'changed'."""
        with contextlib.closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute(
                "UPDATE tracking_items SET updated_at = ?, momentum_status = 'on_track'",
                (utc_iso(stamp),),
            )
            conn.commit()

    def _insert_pending_sent(self, run_id, content, created_at) -> None:
        with contextlib.closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute(
                "INSERT INTO sent_log (run_id, content, delivery_status, created_at)"
                " VALUES (?, ?, 'pending', ?)",
                (run_id, content, utc_iso(created_at)),
            )
            conn.commit()

    def _insert_delivered_sent(self, run_id, *, message_id, delivered_at) -> None:
        with contextlib.closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute(
                "INSERT INTO sent_log (run_id, cron_job_id, message_id, content, delivery_status,"
                " created_at, delivered_at) VALUES (?, 'job-log', ?, '已送达内容', 'delivered', ?, ?)",
                (run_id, message_id, utc_iso(delivered_at), utc_iso(delivered_at)),
            )
            conn.commit()

    def _write_archive(self, job_id, archive_id, response, *, run_time="2026-11-09 14:05:00") -> Path:
        path = self.home / "cron" / "output" / job_id / f"{archive_id}.md"
        path.parent.mkdir(parents=True, exist_ok=True)
        if response is None:
            return path
        body = (
            "# Cron Job: proactive_notifier\n\n"
            f"**Job ID:** {job_id}\n"
            f"**Run Time:** {run_time}\n"
            f"**Response Characters:** {len(response)}\n"
            "## Response\n\n"
            f"{response}\n"
        )
        path.write_text(body, encoding="utf-8")
        return path

    def _sent_row(self, run_id) -> dict:
        with contextlib.closing(sqlite3.connect(self.db_path)) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM sent_log WHERE run_id = ?", (run_id,)).fetchone()
        self.assertIsNotNone(row)
        return dict(row)

    @staticmethod
    def _events(captured) -> list[str]:
        return [record.getMessage() for record in captured.records]

    def _event(self, captured, name) -> str:
        for message in self._events(captured):
            if message.startswith(f"event={name} "):
                return message
        self.fail(f"event {name!r} not logged; captured: {self._events(captured)}")

    def _assert_no_canary(self, captured, canary: str) -> None:
        joined = "\n".join(self._events(captured))
        self.assertNotIn(canary, joined)

    # Stage 1 snapshot boundary and fallback observability.

    def test_quiet_hours_logs_reason_and_duration_without_error(self):
        now = datetime(2026, 11, 9, 14, 0, tzinfo=UTC)  # 22:00 Beijing
        with self._capture() as captured:
            result = self.snapshot.build_snapshot(
                hermes_home=self.home, now_utc=now, config=self._config()
            )
        self.assertEqual(result.reason, "quiet_hours")
        self.assertTrue(self._event(captured, "stage1.start").startswith("event=stage1.start "))
        quiet = self._event(captured, "stage1.quiet_hours")
        self.assertIn("reason=quiet_hours", quiet)
        self.assertRegex(quiet, r"duration_ms=\d+")
        self.assertFalse([record for record in captured.records if record.levelno >= logging.ERROR])

    def test_no_op_logs_counts_and_duration(self):
        now = datetime(2026, 11, 9, 4, 0, tzinfo=UTC)  # 12:00 Beijing
        window_start = now - timedelta(hours=1)
        self._create_checkpoint("run-prev", window_start - timedelta(hours=1), window_start)
        self._pin_tracking_updated_at(window_start - timedelta(minutes=1))
        with self._capture() as captured:
            result = self.snapshot.build_snapshot(
                hermes_home=self.home, now_utc=now, force=True, config=self._config()
            )
        self.assertEqual(result.reason, "no_op")
        no_op = self._event(captured, "stage1.no_op")
        self.assertIn("reason=no_op", no_op)
        self.assertIn("new_message_count=0", no_op)
        self.assertIn("changed_goal_count=0", no_op)
        self.assertIn("failing_cron_count=0", no_op)
        self.assertRegex(no_op, r"duration_ms=\d+")
        self.assertFalse([record for record in captured.records if record.levelno >= logging.ERROR])

    def test_ready_logs_run_id_snapshot_dir_and_wake_counts(self):
        now = datetime(2026, 11, 9, 4, 0, tzinfo=UTC)
        self._seed_state_db(
            [("user", "记得下午跟进软考公告", now - timedelta(minutes=10))],
            started_at=now - timedelta(hours=2),
        )
        with self._capture() as captured:
            result = self.snapshot.build_snapshot(
                hermes_home=self.home, now_utc=now, force=True, config=self._config()
            )
        self.assertEqual(result.reason, "snapshot_ready")
        ready = self._event(captured, "stage1.ready")
        self.assertIn(f"run_id={result.run_id}", ready)
        self.assertIn(f"snapshot_dir={result.snapshot_dir}", ready)
        self.assertIn(f"source_count={result.source_count}", ready)
        self.assertIn("new_message_count=1", ready)
        self.assertIn("reason=snapshot_ready", ready)
        self.assertRegex(ready, r"duration_ms=\d+")

    def test_ready_logs_never_copy_conversation_text(self):
        now = datetime(2026, 11, 9, 4, 0, tzinfo=UTC)
        canary = "CANARY-conversation-body"
        self._seed_state_db(
            [("user", f"帮我记住这件事 {canary}", now - timedelta(minutes=10))],
            started_at=now - timedelta(hours=2),
        )
        with self._capture() as captured:
            self.snapshot.build_snapshot(
                hermes_home=self.home, now_utc=now, force=True, config=self._config()
            )
        self._assert_no_canary(captured, canary)

    def test_state_db_read_failure_is_a_warning_not_a_silent_empty_scan(self):
        now = datetime(2026, 11, 9, 4, 0, tzinfo=UTC)
        window_start = now - timedelta(hours=1)
        self._create_checkpoint("run-prev", window_start - timedelta(hours=1), window_start)
        self._pin_tracking_updated_at(window_start - timedelta(minutes=1))
        (self.home / "state.db").write_bytes(b"CANARY-not-a-database-file")
        with self._capture() as captured:
            result = self.snapshot.build_snapshot(
                hermes_home=self.home, now_utc=now, force=True, config=self._config()
            )
        self.assertEqual(result.reason, "no_op")
        degraded = self._event(captured, "state_db.unreadable")
        self.assertIn("stage=stage1", degraded)
        self.assertIn("error_type=", degraded)
        self._assert_no_canary(captured, "CANARY-not-a-database-file")

    def test_cron_jobs_parse_failure_logs_warning_with_error_type(self):
        now = datetime(2026, 11, 9, 4, 0, tzinfo=UTC)
        self.jobs_path.parent.mkdir(parents=True, exist_ok=True)
        self.jobs_path.write_text("{not-json CANARY", encoding="utf-8")
        with self._capture() as captured:
            self.snapshot.build_snapshot(
                hermes_home=self.home, now_utc=now, force=True, config=self._config()
            )
        degraded = self._event(captured, "cron_jobs.unreadable")
        self.assertIn("error_type=", degraded)
        self._assert_no_canary(captured, "CANARY")

    def test_memory_source_failure_logs_reason_without_query_text_or_body(self):
        now = datetime(2026, 11, 9, 4, 0, tzinfo=UTC)
        canary = "CANARY-openviking-keyword"
        self._seed_state_db(
            [("user", f"下午跟进入学材料 {canary}", now - timedelta(minutes=10))],
            started_at=now - timedelta(hours=2),
        )
        with self._capture() as captured:
            result = self.snapshot.build_snapshot(
                hermes_home=self.home,
                now_utc=now,
                force=True,
                config=self._config(memory_source="missing-provider"),
            )
        self.assertTrue(result.wake_agent)
        degraded = self._event(captured, "memory_source.unavailable")
        self.assertIn("stage=stage1", degraded)
        self.assertIn("source=missing-provider", degraded)
        self.assertIn("status=unavailable", degraded)
        self.assertIn("reason=provider_not_installed", degraded)
        self.assertIn("result_count=0", degraded)
        self.assertIn("duration_ms=", degraded)
        self._assert_no_canary(captured, canary)

    def test_delivery_reconciliation_logs_delivered_receipt(self):
        now = datetime(2026, 11, 9, 7, 0, tzinfo=UTC)
        t0 = now - timedelta(hours=2)
        t2 = now - timedelta(hours=1)
        self._create_checkpoint("run-log0", t0 - timedelta(hours=1), t0)
        self._create_checkpoint("run-log2", t2 - timedelta(minutes=30), t2, status="pending")
        self._insert_pending_sent("run-log2", "这条提醒应该送达。", t2)
        self._write_cron_jobs([self._proactive_job(job_id="job-log2")])
        self._write_archive("job-log2", "2026-11-09_14-00-00", "软考公告我再帮你看一眼。", run_time="2026-11-09 14:00:00")
        self._seed_state_db(
            [("user", "下午继续看公告", now - timedelta(minutes=5))],
            started_at=t0,
        )
        with self._capture() as captured:
            self.snapshot.build_snapshot(
                hermes_home=self.home, now_utc=now, force=True, config=self._config()
            )
        delivered = self._event(captured, "delivery.delivered")
        self.assertIn("run_id=run-log2", delivered)
        self.assertIn("cron_job_id=job-log2", delivered)
        self.assertIn("message_id=2026-11-09_14-00-00", delivered)
        self.assertIn("delivery_status=delivered", delivered)

    def test_delivery_failures_log_stable_safe_reason_codes(self):
        variants = (
            {
                "name": "job_missing",
                "job_id": None,
                "status": "ok",
                "delivery_error": None,
                "archive_id": None,
                "response": None,
                "run_time": "2026-11-09 14:00:00",
            },
            {
                "name": "status_not_ok",
                "job_id": "job-log2",
                "status": "error",
                "delivery_error": None,
                "archive_id": "2026-11-09_14-00-00",
                "response": "软考提醒",
                "run_time": "2026-11-09 14:00:00",
            },
            {
                "name": "delivery_error",
                "job_id": "job-log2",
                "status": "ok",
                "delivery_error": "CANARY-delivery-error",
                "archive_id": "2026-11-09_14-00-00",
                "response": "软考提醒",
                "run_time": "2026-11-09 14:00:00",
            },
            {
                "name": "archive_missing",
                "job_id": "job-log2",
                "status": "ok",
                "delivery_error": None,
                "archive_id": None,
                "response": None,
                "run_time": "2026-11-09 14:00:00",
            },
            {
                "name": "silent",
                "job_id": "job-log2",
                "status": "ok",
                "delivery_error": None,
                "archive_id": "2026-11-09_14-00-00",
                "response": "[SILENT]",
                "run_time": "2026-11-09 14:00:00",
            },
            {
                "name": "empty",
                "job_id": "job-log2",
                "status": "ok",
                "delivery_error": None,
                "archive_id": "2026-11-09_14-00-00",
                "response": "",
                "run_time": "2026-11-09 14:00:00",
            },
            {
                "name": "stale",
                "job_id": "job-log2",
                "status": "ok",
                "delivery_error": None,
                "archive_id": "2026-11-09_08-00-00",
                "response": "旧归档内容",
                "run_time": "2026-11-09 08:00:00",
            },
        )
        for variant in variants:
            name = variant["name"]
            with self.subTest(name=name):
                shutil.rmtree(self.plugin_data, ignore_errors=True)
                shutil.rmtree(self.home / "cron" / "output", ignore_errors=True)
                with contextlib.suppress(FileNotFoundError):
                    (self.home / "state.db").unlink()
                self.storage.init_db()
                now = datetime(2026, 11, 9, 7, 0, tzinfo=UTC)
                t0 = now - timedelta(hours=2)
                t2 = now - timedelta(hours=1)
                self._create_checkpoint("run-log0", t0 - timedelta(hours=1), t0)
                self._create_checkpoint("run-log2", t2 - timedelta(minutes=30), t2)
                self._insert_pending_sent("run-log2", "这条提醒没有送达。", t2)
                jobs = [] if variant["job_id"] is None else [
                    self._proactive_job(
                        job_id=variant["job_id"],
                        last_status=variant["status"],
                        last_delivery_error=variant["delivery_error"],
                    )
                ]
                self._write_cron_jobs(jobs)
                if variant["archive_id"] is not None:
                    self._write_archive(
                        "job-log2",
                        variant["archive_id"],
                        variant["response"],
                        run_time=variant["run_time"],
                    )
                self._seed_state_db(
                    [("user", "窗口期内仍需要重新评估的事项", now - timedelta(minutes=5))],
                    started_at=t0,
                )
                with self._capture() as captured:
                    self.snapshot.build_snapshot(
                        hermes_home=self.home, now_utc=now, force=True, config=self._config()
                    )
                self.assertEqual(self._sent_row("run-log2")["delivery_status"], "failed")
                failed = self._event(captured, "delivery.failed")
                self.assertIn("run_id=run-log2", failed)
                self.assertIn(f"reason={name}", failed)
                self.assertIn("delivery_status=failed", failed)
                self._assert_no_canary(captured, "CANARY")

    def test_claimed_archive_reports_claimed_reason(self):
        now = datetime(2026, 11, 9, 7, 0, tzinfo=UTC)
        t0 = now - timedelta(hours=2)
        t2 = now - timedelta(hours=1)
        self._create_checkpoint("run-log0", t0 - timedelta(hours=1), t0)
        self._create_checkpoint("run-log2", t2 - timedelta(minutes=30), t2)
        self._insert_pending_sent("run-log2", "这条提醒没有送达。", t2)
        self._insert_delivered_sent(
            "run-log0", message_id="2026-11-09_14-00-00", delivered_at=t2
        )
        self._write_cron_jobs([self._proactive_job(job_id="job-log2")])
        self._write_archive(
            "job-log2", "2026-11-09_14-00-00", "已经送达过的提醒。", run_time="2026-11-09 14:00:00"
        )
        self._seed_state_db(
            [("user", "窗口期内仍需要重新评估的事项", now - timedelta(minutes=5))],
            started_at=t0,
        )
        with self._capture() as captured:
            self.snapshot.build_snapshot(
                hermes_home=self.home, now_utc=now, force=True, config=self._config()
            )
        self.assertEqual(self._sent_row("run-log2")["delivery_status"], "failed")
        failed = self._event(captured, "delivery.failed")
        self.assertIn("reason=claimed", failed)

    def test_newer_silent_archive_is_not_masked_by_an_older_stale_archive(self):
        now = datetime(2026, 11, 9, 7, 0, tzinfo=UTC)
        t0 = now - timedelta(hours=2)
        t2 = now - timedelta(hours=1)
        self._create_checkpoint("run-log0", t0 - timedelta(hours=1), t0)
        self._create_checkpoint("run-log2", t2 - timedelta(minutes=30), t2)
        self._insert_pending_sent("run-log2", "这条提醒没有送达。", t2)
        self._write_cron_jobs([self._proactive_job(job_id="job-log2")])
        self._write_archive(
            "job-log2", "2026-11-08_09-00-00", "昨天的归档内容。", run_time="2026-11-08 09:00:00"
        )
        self._write_archive(
            "job-log2", "2026-11-09_14-00-00", "[SILENT]", run_time="2026-11-09 14:00:00"
        )
        self._seed_state_db(
            [("user", "窗口期内仍需要重新评估的事项", now - timedelta(minutes=5))],
            started_at=t0,
        )
        with self._capture() as captured:
            self.snapshot.build_snapshot(
                hermes_home=self.home, now_utc=now, force=True, config=self._config()
            )
        self.assertEqual(self._sent_row("run-log2")["delivery_status"], "failed")
        failed = self._event(captured, "delivery.failed")
        self.assertIn("reason=silent", failed)

    def test_supersede_count_and_prune_failure_are_observable(self):
        now = datetime(2026, 11, 9, 4, 0, tzinfo=UTC)
        self._create_checkpoint("run-interrupted", now - timedelta(hours=2), now - timedelta(hours=1), status="pending")
        self._seed_state_db(
            [("user", "新的待跟进事项", now - timedelta(minutes=5))],
            started_at=now - timedelta(hours=3),
        )
        stale_dir = self.hidden_files / "0-stale"
        stale_dir.mkdir(parents=True, exist_ok=True)
        old = (now - timedelta(days=30)).timestamp()
        os.utime(stale_dir, (old, old))
        with mock.patch.object(self.snapshot.shutil, "rmtree", return_value=None):
            with self._capture() as captured:
                self.snapshot.build_snapshot(
                    hermes_home=self.home,
                    now_utc=now,
                    force=True,
                    config=self._config(retention_days=1),
                )
        superseded = self._event(captured, "checkpoint.superseded")
        self.assertIn("run_id=run-interrupted", superseded)
        self.assertIn("count=1", superseded)
        prune = self._event(captured, "snapshot.prune_failed")
        self.assertIn("count=1", prune)

    # Stage 2 worker lifecycle, tracking outcomes, and checkpoint results.

    def _candidate(self, marker: int, *, title="跟进软考公告", draft=None) -> dict:
        candidate = {
            "title": title,
            "priority": "regular",
            "relevant_until": "2026-11-10T12:00:00+08:00",
            "why_it_matters": "公告可能发布。",
            "evidence_refs": [f"[source:{marker}]"],
            "proposed_action": "提醒确认。",
            "what_remains": "等待编辑门复核。",
        }
        if draft is not None:
            candidate["draft"] = draft
        return candidate

    def _stage2_now(self) -> datetime:
        return datetime(2026, 11, 9, 4, 0, tzinfo=UTC)

    def _seed_conversation(self, now: datetime, text: str = "记得下午跟进软考公告") -> None:
        self._seed_state_db(
            [("user", text, now - timedelta(minutes=10))],
            started_at=now - timedelta(hours=2),
        )

    def test_stage2_logs_worker_lifecycle_tracking_outcomes_and_checkpoint_results(self):
        now = self._stage2_now()
        self._seed_conversation(now)

        self.storage.upsert_tracking_item(
            "closed-goal", title="已办结事项", description="描述", summary="摘要"
        )
        self.storage.close_tracking_item("closed-goal", summary="已办结")

        def worker1(kwargs):
            marker = first_marker(kwargs)
            return {
                "tracking_upserts": [
                    {"goal_id": "new-goal", "title": "新目标", "description": "描述", "summary": "摘要", "momentum_status": "waiting"},
                    {"goal_id": SEED_GOAL_ID, "summary": "更新后的摘要"},
                    {"goal_id": "closed-goal", "summary": "不应被改动"},
                    {"goal_id": "broken-goal", "description": "只有描述"},
                ],
                "candidates": [self._candidate(marker, title="CANARY-candidate-title")],
            }

        def worker2(kwargs):
            marker = first_marker(kwargs)
            return {
                "candidates": [
                    self._candidate(marker, title="软考截止时间", draft="CANARY-draft-body")
                ]
            }

        llm = FakeStructuredLlm([worker1, worker2])
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            with self._capture() as captured:
                result = self.snapshot.run_tick(
                    hermes_home=self.home,
                    now_utc=now,
                    llm_client=llm,
                    force=True,
                    config=self._config(worker_timeout_seconds=45),
                )

        self.assertIn("Edition Handoff", output.getvalue())
        self.assertEqual([call["timeout"] for call in llm.calls], [45.0, 45.0])

        start = self._event(captured, "stage2.start")
        self.assertIn(f"run_id={result.run_id}", start)
        self.assertIn("timeout_seconds=45", start)
        checkpoint = self._event(captured, "checkpoint.created")
        self.assertIn(f"run_id={result.run_id}", checkpoint)
        self.assertIn(f"window_start_utc={result.window_start_utc}", checkpoint)
        self.assertIn(f"window_end_utc={result.window_end_utc}", checkpoint)
        self.assertIn("checkpoint_status=pending", checkpoint)

        starts = [m for m in self._events(captured) if m.startswith("event=worker.started ")]
        self.assertEqual(len(starts), 2)
        worker1_start = next(m for m in starts if "worker=worker1" in m)
        self.assertIn("purpose=proactive_worker1_opportunities", worker1_start)
        self.assertIn("schema=worker1_opportunities_schema", worker1_start)
        self.assertIn("timeout_seconds=45", worker1_start)
        worker2_start = next(m for m in starts if "worker=worker2" in m)
        self.assertIn("purpose=proactive_worker2_prepared", worker2_start)
        self.assertIn("schema=worker2_prepared_schema", worker2_start)

        finishes = [m for m in self._events(captured) if m.startswith("event=worker.finished ")]
        self.assertEqual(len(finishes), 2)
        for message in finishes:
            self.assertRegex(message, r"duration_ms=\d+")

        tracking = self._event(captured, "tracking.upserts")
        self.assertIn("created_count=1", tracking)
        self.assertIn("updated_count=1", tracking)
        self.assertIn("skipped_closed_count=1", tracking)
        self.assertIn("invalid_count=1", tracking)
        self.assertNotIn("closed_count=", tracking.replace("skipped_closed_count=", ""))

        candidates_event = self._event(captured, "stage2.candidates")
        self.assertIn("candidate_count=2", candidates_event)
        self.assertIn("checkpoint_status=pending", candidates_event)
        self.assertIn("opportunities_count=1", candidates_event)
        self.assertIn("prepared_count=1", candidates_event)
        self.assertRegex(candidates_event, r"source_count=\d+")

        pending = self._event(captured, "stage2.pending")
        self.assertIn("candidate_count=2", pending)
        self.assertIn("checkpoint_status=pending", pending)
        self.assertRegex(self._event(captured, "stage2.finished"), r"duration_ms=\d+")
        handoff = self._event(captured, "stage2.handoff")
        self.assertIn(f"run_id={result.run_id}", handoff)
        self.assertIn("candidate_count=2", handoff)
        self._assert_no_canary(captured, "CANARY")
        self.assertFalse([record for record in captured.records if record.levelno >= logging.ERROR])

    def test_zero_candidates_commit_the_checkpoint_without_handoff(self):
        now = self._stage2_now()
        self._seed_conversation(now)
        llm = FakeStructuredLlm(
            [{"tracking_upserts": [], "candidates": []}, {"candidates": []}]
        )
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            with self._capture() as captured:
                result = self.snapshot.run_tick(
                    hermes_home=self.home,
                    now_utc=now,
                    llm_client=llm,
                    force=True,
                    config=self._config(),
                )
        self.assertEqual(output.getvalue().splitlines()[-1], '{"wakeAgent": false}')
        committed = self._event(captured, "stage2.committed")
        self.assertIn("candidate_count=0", committed)
        self.assertIn("checkpoint_status=committed", committed)
        self.assertFalse([m for m in self._events(captured) if m.startswith("event=stage2.handoff ")])
        self.assertEqual(self.storage.get_checkpoint(result.run_id)["status"], "committed")
        self.assertFalse([record for record in captured.records if record.levelno >= logging.ERROR])

    def test_worker_timeout_is_logged_with_type_and_stack_then_reraised(self):
        now = self._stage2_now()
        self._seed_conversation(now)
        canary = "CANARY-provider-timeout"
        llm = FakeStructuredLlm([TimeoutError(f"request timed out for {canary}")])
        with self._capture() as captured:
            with self.assertRaises(TimeoutError):
                self.snapshot.run_tick(
                    hermes_home=self.home,
                    now_utc=now,
                    llm_client=llm,
                    force=True,
                    config=self._config(worker_timeout_seconds=45),
                )
        failed = self._event(captured, "worker.failed")
        self.assertIn("worker=worker1", failed)
        self.assertIn("purpose=proactive_worker1_opportunities", failed)
        self.assertIn("error_type=TimeoutError", failed)
        self.assertRegex(failed, r"error_stack=\S*workers\.py:\S+:\d+")
        self.assertRegex(failed, r"duration_ms=\d+")
        stage_failed = self._event(captured, "stage2.failed")
        self.assertIn("error_type=TimeoutError", stage_failed)
        pending = self.storage.latest_pending_checkpoint()
        self.assertIsNotNone(pending)
        self.assertEqual(pending["status"], "pending")
        self._assert_no_canary(captured, canary)

    def test_unparseable_worker_response_is_logged_at_the_parse_stage(self):
        now = self._stage2_now()
        self._seed_conversation(now)
        canary = "CANARY-raw-response"
        llm = FakeStructuredLlm([SimpleNamespace(parsed=None, text=f"not-json {canary}")])
        with self._capture() as captured:
            with self.assertRaises(self.plugin.workers.WorkerOutputError):
                self.snapshot.run_tick(
                    hermes_home=self.home,
                    now_utc=now,
                    llm_client=llm,
                    force=True,
                    config=self._config(),
                )
        failed = self._event(captured, "worker.parse_failed")
        self.assertIn("worker=worker1", failed)
        self.assertIn("error_type=WorkerOutputError", failed)
        self._assert_no_canary(captured, canary)

    def test_invalid_evidence_ref_is_logged_at_the_normalize_stage(self):
        now = self._stage2_now()
        self._seed_conversation(now)
        llm = FakeStructuredLlm(
            [
                {
                    "tracking_upserts": [],
                    "candidates": [
                        {
                            "title": "CANARY-candidate-title",
                            "priority": "regular",
                            "relevant_until": "2026-11-10T12:00:00+08:00",
                            "why_it_matters": "CANARY-why",
                            "evidence_refs": ["[source:999]"],
                            "proposed_action": "CANARY-action",
                            "what_remains": "CANARY-remains",
                        }
                    ],
                },
                {"candidates": []},
            ]
        )
        with self._capture() as captured:
            with self.assertRaises(self.plugin.workers.WorkerOutputError):
                self.snapshot.run_tick(
                    hermes_home=self.home,
                    now_utc=now,
                    llm_client=llm,
                    force=True,
                    config=self._config(),
                )
        failed = self._event(captured, "worker.normalize_failed")
        self.assertIn("worker=worker1", failed)
        self.assertIn("worker_kind=opportunities", failed)
        self.assertIn("error_type=WorkerOutputError", failed)
        self._assert_no_canary(captured, "CANARY")

    # Stage 3 editorial gate tools.

    def _editorial_candidate(self, title: str, *, draft: str | None = None) -> dict:
        return {
            "title": title,
            "priority": "regular",
            "relevant_until": utc_iso(datetime(2026, 11, 10, 4, 0, tzinfo=UTC)),
            "why_it_matters": f"{title} 需要跟进。",
            "evidence_refs": ["[source:1]"],
            "proposed_action": "确认当前状态。",
            "what_remains": "等待用户确认。",
            "draft": draft,
        }

    def _create_pending_edition(self, *, run_id="run-editorial-log", candidate_count=2, title_prefix="事项"):
        now = datetime(2026, 11, 9, 4, 0, tzinfo=UTC)
        window_start = now - timedelta(hours=1)
        snapshot_dir = self.plugin_data / "hidden_files" / run_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.snapshot._write_private_file(
            snapshot_dir / "opportunities.md", "# Opportunities\n\n- 跟进软考批次公告。\n"
        )
        self.snapshot._write_private_file(snapshot_dir / "prepared.md", "# Prepared\n\n- 准备提醒材料。\n")
        self.snapshot._write_private_file(
            snapshot_dir / "sources.md", "# Sources\n\n[source:1] 用户提到等待批次公告。\n"
        )
        checkpoint = self.storage.create_checkpoint(
            run_id,
            str(snapshot_dir),
            utc_iso(window_start),
            utc_iso(now),
            status="pending",
            candidate_count=candidate_count,
        )
        for index in range(1, candidate_count + 1):
            worker_kind = "opportunities" if index == 1 else "prepared"
            draft = None if worker_kind == "opportunities" else f"初稿 {index}"
            self.storage.add_candidate_item(
                run_id,
                index,
                worker_kind,
                self._editorial_candidate(f"{title_prefix} {index}", draft=draft),
            )
        return checkpoint

    def _seed_live_diff(self, *, window_end: datetime, text: str) -> None:
        with contextlib.closing(sqlite3.connect(self.home / "state.db")) as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS sessions (
                    id TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    title TEXT,
                    display_name TEXT,
                    parent_session_id TEXT,
                    started_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    content TEXT,
                    timestamp REAL NOT NULL,
                    active INTEGER NOT NULL DEFAULT 1
                );
                """
            )
            conn.execute("DELETE FROM messages")
            conn.execute("DELETE FROM sessions")
            conn.execute(
                "INSERT INTO sessions (id, source, title, display_name, parent_session_id, started_at)"
                " VALUES ('session-1', 'feishu', '飞书会话', NULL, NULL, ?)",
                (unix_ts(window_end - timedelta(hours=1)),),
            )
            conn.execute(
                "INSERT INTO messages (session_id, role, content, timestamp, active) VALUES (?, 'user', ?, ?, 1)",
                ("session-1", text, unix_ts(window_end + timedelta(minutes=5))),
            )
            conn.commit()

    def test_proactivity_read_logs_edition_correlation_without_bodies(self):
        checkpoint = self._create_pending_edition(title_prefix="CANARY-candidate")
        self._seed_live_diff(
            window_end=datetime.fromisoformat(checkpoint["window_end_utc"].replace("Z", "+00:00")),
            text="CANARY-live-diff-text",
        )
        with self._capture() as captured:
            payload = json.loads(self.plugin.tools.handle_proactivity_read())
        self.assertTrue(payload["ok"])
        read = self._event(captured, "editorial.read")
        self.assertIn(f"run_id={checkpoint['run_id']}", read)
        self.assertIn(f"snapshot_dir={checkpoint['snapshot_dir']}", read)
        self.assertIn("undecided_count=2", read)
        self.assertIn("live_diff_count=1", read)
        self.assertRegex(read, r"duration_ms=\d+")
        self._assert_no_canary(captured, "CANARY")

    def test_proactivity_read_failure_keeps_json_contract_and_logs_diagnostics(self):
        with self._capture() as captured:
            payload = json.loads(
                self.plugin.tools.handle_proactivity_read({"run_id": "run-unknown"})
            )
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["action"], "read")
        self.assertTrue(payload["error"])
        failed = self._event(captured, "editorial.read_failed")
        self.assertIn("run_id=run-unknown", failed)
        self.assertIn("action=read", failed)
        self.assertIn("error_type=KeyError", failed)
        self.assertRegex(failed, r"duration_ms=\d+")
        self.assertIn("unknown run_id", payload["error"])

    def test_proactivity_read_logs_missing_snapshot_files_as_warning(self):
        checkpoint = self._create_pending_edition()
        (Path(checkpoint["snapshot_dir"]) / "sources.md").unlink()
        with self._capture() as captured:
            payload = json.loads(self.plugin.tools.handle_proactivity_read())
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["sources"], "")
        degraded = self._event(captured, "editorial.file_missing")
        self.assertIn(f"run_id={checkpoint['run_id']}", degraded)
        self.assertIn("reason=missing_snapshot_file", degraded)

    def test_proactivity_select_logs_decision_counts_without_final_message(self):
        checkpoint = self._create_pending_edition()
        final_message = "CANARY-final-message"
        with self._capture() as captured:
            payload = json.loads(
                self.plugin.tools.handle_proactivity_select(
                    {
                        "run_id": checkpoint["run_id"],
                        "decisions": [
                            {"item_num": 1, "decision": "send", "reason": "changed", "note": "CANARY-note"},
                            {"item_num": 2, "decision": "skip", "reason": "already_handled"},
                        ],
                        "final_message": final_message,
                        "close_goal_ids": [SEED_GOAL_ID],
                    }
                )
            )
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["action"], "deliver")
        self.assertEqual(payload["final_message"], final_message)
        selected = self._event(captured, "editorial.select")
        self.assertIn(f"run_id={checkpoint['run_id']}", selected)
        self.assertIn("action=deliver", selected)
        self.assertIn("send_count=1", selected)
        self.assertIn("skip_count=1", selected)
        self.assertIn("close_count=1", selected)
        self.assertIn("checkpoint_status=committed", selected)
        self.assertIn("pending_receipt=true", selected)
        self.assertRegex(selected, r"duration_ms=\d+")
        self._assert_no_canary(captured, "CANARY")

    def test_proactivity_select_silent_edition_logs_no_pending_receipt(self):
        checkpoint = self._create_pending_edition()
        with self._capture() as captured:
            payload = json.loads(
                self.plugin.tools.handle_proactivity_select(
                    {
                        "run_id": checkpoint["run_id"],
                        "decisions": [
                            {"item_num": 1, "decision": "skip", "reason": "moot"},
                            {"item_num": 2, "decision": "skip", "reason": "moot"},
                        ],
                    }
                )
            )
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["action"], "silent")
        self.assertEqual(payload["reply_token"], "[SILENT]")
        selected = self._event(captured, "editorial.select")
        self.assertIn("action=silent", selected)
        self.assertIn("send_count=0", selected)
        self.assertIn("skip_count=2", selected)
        self.assertIn("pending_receipt=false", selected)

    def test_proactivity_select_failure_keeps_json_contract_and_omits_notes(self):
        checkpoint = self._create_pending_edition()
        with self._capture() as captured:
            payload = json.loads(
                self.plugin.tools.handle_proactivity_select(
                    {
                        "run_id": checkpoint["run_id"],
                        "decisions": [
                            {"item_num": 1, "decision": "send", "reason": "changed", "note": "CANARY-note"},
                        ],
                        "final_message": "CANARY-final-message",
                    }
                )
            )
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["action"], "select")
        self.assertIn("missing decisions", payload["error"])
        failed = self._event(captured, "editorial.select_failed")
        self.assertIn(f"run_id={checkpoint['run_id']}", failed)
        self.assertIn("action=select", failed)
        self.assertIn("error_type=ValueError", failed)
        self._assert_no_canary(captured, "CANARY")
        # The failed edition must remain undecided.
        self.assertEqual(
            self.storage.get_checkpoint(checkpoint["run_id"])["status"], "pending"
        )

    def test_tracking_manage_logs_state_transitions_without_bodies(self):
        with self._capture() as captured:
            payload = json.loads(
                self.plugin.tools.handle_tracking_manage(
                    {"action": "close", "goal_id": SEED_GOAL_ID, "summary": "CANARY-summary"}
                )
            )
        self.assertTrue(payload["ok"])
        managed = self._event(captured, "tracking.manage")
        self.assertIn("action=close", managed)
        self.assertIn(f"goal_id={SEED_GOAL_ID}", managed)
        self.assertIn("status=closed", managed)
        self._assert_no_canary(captured, "CANARY")

    def test_tracking_manage_failure_logs_action_and_error_type(self):
        with self._capture() as captured:
            payload = json.loads(
                self.plugin.tools.handle_tracking_manage({"action": "close", "goal_id": "missing-CANARY"})
            )
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["action"], "close")
        failed = self._event(captured, "tracking.manage_failed")
        self.assertIn("action=close", failed)
        self.assertIn("error_type=KeyError", failed)
        self._assert_no_canary(captured, "CANARY")

    # Standalone Cron bridge logging.

    def _install_bridge(self) -> Path:
        return self.plugin.cli._install_bridge(self.home)

    def _write_plugin_settings(self, settings: dict) -> None:
        import hermes_yaml as yaml

        (self.home / "config.yaml").write_text(
            yaml.safe_dump({"plugins": {"entries": {"proactive": {"settings": settings}}}}),
            encoding="utf-8",
        )

    @staticmethod
    def _quiet_active_window() -> str:
        """Return a one-minute window that cannot contain the current minute."""
        beijing = timezone(timedelta(hours=8))
        local = datetime.now(beijing)
        now_minute = local.hour * 60 + local.minute
        start = (now_minute + 5) % (24 * 60)
        if start >= 23 * 60:
            start = (now_minute - 5) % (24 * 60)
        return f"{start // 60:02d}:{start % 60:02d}-{(start + 1) // 60:02d}:{(start + 1) % 60:02d}"

    def _run_bridge(self, bridge: Path, *, env_overrides=None) -> subprocess.CompletedProcess:
        env = dict(os.environ)
        env["HERMES_HOME"] = str(self.home)
        env.update(env_overrides or {})
        return subprocess.run(
            [sys.executable, str(bridge)],
            cwd=str(REPO_ROOT),
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )

    def test_generated_bridge_initializes_host_cron_logging_before_plugin_load(self):
        bridge = self._install_bridge()
        text = bridge.read_text(encoding="utf-8")
        self.assertIn("from hermes_logging import setup_logging", text)
        self.assertIn('setup_logging(hermes_home=home, mode="cron")', text)
        self.assertIn("run_tick(", text)

    def test_bridge_logs_success_to_host_files_and_keeps_stdout_protocol(self):
        bridge = self._install_bridge()
        self._write_plugin_settings({"active_window": self._quiet_active_window()})
        result = self._run_bridge(bridge)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, '{"wakeAgent": false}\n')
        log_dir = self.home / "logs"
        agent_log = (log_dir / "agent.log").read_text(encoding="utf-8")
        for event in (
            "event=bridge.start",
            "event=bridge.plugin_selected",
            "event=bridge.finished",
            "event=stage1.start",
            "event=stage1.quiet_hours",
        ):
            self.assertIn(event, agent_log)
        self.assertIn(f"hermes_home={self.home}", agent_log)
        self.assertIn(f"plugin_dir={PLUGIN_DIR.resolve()}", agent_log)
        self.assertRegex(agent_log, r"event=bridge.finished .*duration_ms=\d+")
        self.assertFalse((log_dir / "proactive.log").exists())

    def test_bridge_logs_plugin_load_failure_without_touching_stdout_or_exit_code(self):
        bridge = self._install_bridge()
        broken = self.home / "broken-plugin"
        broken.mkdir()
        (broken / "__init__.py").write_text(
            'raise RuntimeError("CANARY-plugin-load")\n', encoding="utf-8"
        )
        result = self._run_bridge(
            bridge, env_overrides={"PROACTIVE_PLUGIN_DIR": str(broken)}
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        agent_log = (self.home / "logs" / "agent.log").read_text(encoding="utf-8")
        self.assertIn("event=bridge.start", agent_log)
        self.assertIn("event=bridge.failed", agent_log)
        self.assertIn("phase=plugin_load", agent_log)
        self.assertIn("error_type=RuntimeError", agent_log)
        self.assertNotIn("CANARY-plugin-load", agent_log)

    def test_bridge_distinguishes_pipeline_failure_and_logs_it(self):
        bridge = self._install_bridge()
        failing_plugin = self.home / "failing-plugin"
        shutil.copytree(
            PLUGIN_DIR, failing_plugin, ignore=shutil.ignore_patterns("__pycache__")
        )
        with open(failing_plugin / "snapshot.py", "a", encoding="utf-8") as handle:
            handle.write(
                "\n\ndef run_tick(*args, **kwargs):\n"
                '    raise RuntimeError("CANARY-pipeline-failure")\n'
            )
        result = self._run_bridge(
            bridge, env_overrides={"PROACTIVE_PLUGIN_DIR": str(failing_plugin)}
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        agent_log = (self.home / "logs" / "agent.log").read_text(encoding="utf-8")
        self.assertIn("event=bridge.plugin_selected", agent_log)
        self.assertIn("event=bridge.failed", agent_log)
        self.assertIn("phase=pipeline", agent_log)
        self.assertIn("error_type=RuntimeError", agent_log)
        self.assertNotIn("CANARY-pipeline-failure", agent_log)
        self.assertNotIn("event=bridge.finished", agent_log)

    # Event helper, field whitelist, and level configuration.

    def test_event_line_is_single_line_key_value_with_stable_field_order(self):
        logger = self.log.logger("snapshot")
        with self._capture() as captured:
            self.log.info(
                logger,
                "stage1.ready",
                run_id="run-abc",
                snapshot_dir="/home/x/hidden_files/1-a1b2c3",
                source_count=7,
                reason="snapshot_ready",
            )
        self.assertEqual(len(captured.records), 1)
        message = captured.records[0].getMessage()
        self.assertNotIn("\n", message)
        self.assertTrue(message.startswith("event=stage1.ready "))
        self.assertRegex(
            message,
            re.compile(r"^event=stage1\.ready run_id=run-abc snapshot_dir=/home/x/hidden_files/1-a1b2c3 reason=snapshot_ready source_count=7$"),
        )

    def test_event_helper_drops_unknown_fields_and_caps_oversized_values(self):
        logger = self.log.logger("snapshot")
        canary = "CANARY-body-text"
        with self._capture() as captured:
            self.log.warning(
                logger,
                "stage1.file_unreadable",
                reason="missing_file",
                run_id="r" * 400,
                conversation_body=canary,
            )
        message = captured.records[0].getMessage()
        self.assertNotIn(canary, message)
        self.assertNotIn("conversation_body", message)
        run_id_value = re.search(r"run_id=(\S+)", message).group(1)
        self.assertLessEqual(len(run_id_value), self.log.FIELD_LIMITS["run_id"] + 3)

    def test_error_event_records_type_and_frame_based_stack_without_exception_text(self):
        logger = self.log.logger("workers")
        canary = "sk-CANARY-SECRET-TOKEN"

        def explode():
            raise RuntimeError(f"provider rejected key {canary}")

        try:
            explode()
        except RuntimeError as exc:
            with self._capture() as captured:
                self.log.error_event(logger, "worker.failed", exc, worker="worker1", stage="stage2")
        message = captured.records[0].getMessage()
        self.assertIn("event=worker.failed", message)
        self.assertIn("worker=worker1", message)
        self.assertIn("error_type=RuntimeError", message)
        self.assertRegex(message, r"error_stack=\S+\.py:explode:\d+")
        self.assertNotIn(canary, message)

    def test_level_configuration_accepts_known_names_and_falls_back_to_info(self):
        for value, expected in (
            ("debug", "DEBUG"),
            ("WARNING", "WARNING"),
            ("error", "ERROR"),
            ("info", "INFO"),
            ("verbose", "INFO"),
            (None, "INFO"),
            (12, "INFO"),
        ):
            with self.subTest(value=value):
                self.assertEqual(self.log.configure_level(value), expected)
                self.assertEqual(LOG_LOGGER.level, getattr(logging, expected))

    def test_level_configuration_does_not_touch_root_or_host_loggers(self):
        root = logging.getLogger()
        original_root = root.level
        other = logging.getLogger("agent.test")
        original_other = other.level
        try:
            self.log.configure_level("ERROR")
            self.assertEqual(root.level, original_root)
            self.assertEqual(other.level, original_other)
            self.assertFalse(root.handlers and root.handlers[-1].name == "proactive")
        finally:
            root.setLevel(original_root)
            other.setLevel(original_other)

    def test_snapshot_config_normalizes_log_level(self):
        self.assertEqual(self.snapshot._build_config(self.home, {"log_level": "debug"})["log_level"], "DEBUG")
        self.assertEqual(self.snapshot._build_config(self.home, {"log_level": "noisy"})["log_level"], "INFO")
        self.assertEqual(self.snapshot._build_config(self.home, None)["log_level"], "INFO")

    def test_plugin_manifest_declares_log_level_default(self):
        import hermes_yaml as yaml

        manifest = yaml.safe_load((PLUGIN_DIR / "plugin.yaml").read_text(encoding="utf-8"))
        entry = manifest["config_schema"]["log_level"]
        self.assertEqual(entry["default"], "INFO")

    def test_register_applies_configured_level(self):
        ctx = RecordingContext({"log_level": "WARNING"})
        self.plugin.register(ctx)
        self.assertEqual(LOG_LOGGER.level, logging.WARNING)


if __name__ == "__main__":
    unittest.main()

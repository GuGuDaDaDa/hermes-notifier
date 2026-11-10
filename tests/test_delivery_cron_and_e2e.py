"""Behavioral tests for delivery reconciliation, cron setup, and the full pipeline."""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import re
import shutil
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from hermes_constants import reset_hermes_home_override, set_hermes_home_override
from tests.test_goal_tracking import SEED_GOAL_ID, load_plugin_package, seed_tracking_fixture


BEIJING_TZ = timezone(timedelta(hours=8))
SOURCE_RE = re.compile(r"\[source:(\d+)\]")


def utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def unix_ts(value: datetime) -> float:
    return value.astimezone(timezone.utc).timestamp()


def parse_aware(value: str) -> datetime:
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


class StructuredLlmRecorder:
    """Deterministic structured-LLM double for both research workers."""

    def __init__(self, responders):
        self._responders = list(responders)
        self.calls = []

    def complete_structured(self, **kwargs):
        self.calls.append(kwargs)
        index = len(self.calls) - 1
        responder = self._responders[index] if index < len(self._responders) else {"candidates": []}
        payload = responder(kwargs) if callable(responder) else responder
        return SimpleNamespace(
            parsed=payload,
            text=json.dumps(payload, ensure_ascii=False),
            content_type="json",
        )


def _input_text(call) -> str:
    parts = []
    for block in call.get("input") or []:
        if isinstance(block, dict):
            parts.append(str(block.get("text") or ""))
        else:
            parts.append(str(getattr(block, "text", "")))
    return "\n".join(parts)


class DeliveryCronAndEndToEndTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.plugin = load_plugin_package()
        cls.storage = cls.plugin.storage
        cls.snapshot = cls.plugin.snapshot
        cls.tools = cls.plugin.tools
        cls.cli = cls.plugin.cli

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="hermes-proactive-e2e-test-")
        self.home = Path(self._tmp.name)
        self._old_home = os.environ.get("HERMES_HOME")
        os.environ["HERMES_HOME"] = str(self.home)
        self._override_token = set_hermes_home_override(str(self.home))
        self.addCleanup(self._cleanup_home)
        self.storage.init_db()
        seed_tracking_fixture(self.storage)

    def _cleanup_home(self):
        reset_hermes_home_override(self._override_token)
        if self._old_home is None:
            os.environ.pop("HERMES_HOME", None)
        else:
            os.environ["HERMES_HOME"] = self._old_home
        self._tmp.cleanup()

    @property
    def plugin_data(self) -> Path:
        return self.home / "plugin-data" / "proactive"

    @property
    def db_path(self) -> Path:
        return self.plugin_data / "data.db"

    @property
    def jobs_path(self) -> Path:
        return self.home / "cron" / "jobs.json"

    def _write_host_config(self, values: dict) -> None:
        (self.home / "config.yaml").write_text(json.dumps(values), encoding="utf-8")

    def _connect(self):
        return contextlib.closing(sqlite3.connect(self.db_path))

    def _config(self) -> dict:
        return {"memory_source": "builtin", "timezone": "Asia/Shanghai"}

    def _clear_runtime_state(self) -> None:
        shutil.rmtree(self.plugin_data, ignore_errors=True)
        shutil.rmtree(self.home / "cron" / "output", ignore_errors=True)
        for name in ("state.db", "state.db-wal", "state.db-shm"):
            with contextlib.suppress(FileNotFoundError):
                (self.home / name).unlink()
        self.storage.init_db()
        seed_tracking_fixture(self.storage)

    def _create_checkpoint(
        self,
        run_id: str,
        window_start: datetime,
        window_end: datetime,
        *,
        status: str = "committed",
        committed_at: datetime | None = None,
    ) -> dict:
        committed_at = committed_at or window_end
        return self.storage.create_checkpoint(
            run_id,
            str(self.plugin_data / "hidden_files" / run_id),
            utc_iso(window_start),
            utc_iso(window_end),
            status=status,
            candidate_count=0,
            created_at=utc_iso(window_end),
            committed_at=utc_iso(committed_at) if status == "committed" else None,
        )

    def _insert_pending_sent(self, run_id: str, content: str, created_at: datetime) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO sent_log (
                    run_id, cron_job_id, message_id, content,
                    delivery_status, created_at, delivered_at
                ) VALUES (?, NULL, NULL, ?, 'pending', ?, NULL)
                """,
                (run_id, content, utc_iso(created_at)),
            )
            conn.commit()

    def _sent_row(self, run_id: str) -> dict:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM sent_log WHERE run_id = ?", (run_id,)
            ).fetchone()
        self.assertIsNotNone(row)
        return dict(row)

    def _write_cron_jobs(self, jobs: list[dict]) -> None:
        self.jobs_path.parent.mkdir(parents=True, exist_ok=True)
        self.jobs_path.write_text(
            json.dumps({"jobs": jobs, "updated_at": utc_iso(datetime.now(timezone.utc))}, ensure_ascii=False),
            encoding="utf-8",
        )

    def _read_cron_jobs(self) -> list[dict]:
        payload = json.loads(self.jobs_path.read_text(encoding="utf-8"))
        return payload["jobs"] if isinstance(payload, dict) else payload

    def _proactive_job(
        self,
        *,
        job_id: str = "cronproactive",
        last_status: str = "ok",
        last_delivery_error: str | None = None,
    ) -> dict:
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
            "origin": {
                "platform": "feishu",
                "chat_id": "oc-primary",
                "chat_name": "飞书主会话",
                "thread_id": None,
                "user_id": "ou-primary",
            },
        }

    def _write_archive(
        self,
        job_id: str,
        archive_id: str,
        response: str | None,
        *,
        run_time: str = "2026-11-09 12:05:00",
    ) -> Path:
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

    def _set_archive_mtime(self, path: Path, stamp: datetime) -> None:
        timestamp = stamp.timestamp()
        os.utime(path, (timestamp, timestamp))

    def _insert_delivered_sent(
        self,
        run_id: str,
        *,
        message_id: str,
        content: str,
        delivered_at: datetime,
    ) -> None:
        self._create_checkpoint(run_id, delivered_at - timedelta(minutes=5), delivered_at)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO sent_log (
                    run_id, cron_job_id, message_id, content,
                    delivery_status, created_at, delivered_at
                ) VALUES (?, 'job-prior', ?, ?, 'delivered', ?, ?)
                """,
                (run_id, message_id, content, utc_iso(delivered_at), utc_iso(delivered_at)),
            )
            conn.commit()

    def _seed_state_db(
        self,
        messages: list[tuple[str, str, datetime]],
        *,
        started_at: datetime,
    ) -> None:
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
                    active INTEGER NOT NULL DEFAULT 1,
                    reasoning TEXT,
                    reasoning_content TEXT
                );
                """
            )
            conn.execute("DELETE FROM messages")
            conn.execute("DELETE FROM sessions")
            conn.execute(
                """
                INSERT INTO sessions (
                    id, source, title, display_name, parent_session_id, started_at
                ) VALUES ('s-feishu', 'feishu', '飞书主会话', NULL, NULL, ?)
                """,
                (unix_ts(started_at),),
            )
            conn.executemany(
                """
                INSERT INTO messages (
                    session_id, role, content, timestamp, active, reasoning, reasoning_content
                ) VALUES ('s-feishu', ?, ?, ?, 1, NULL, NULL)
                """,
                [(role, content, unix_ts(stamp)) for role, content, stamp in messages],
            )
            conn.commit()

    def test_confirmed_delivery_is_promoted_and_rendered_in_sent_snapshot(self):
        now = datetime(2026, 11, 9, 4, 0, tzinfo=timezone.utc)
        window_start = now - timedelta(hours=1)
        self._create_checkpoint("run-r1", window_start, now)
        self._insert_pending_sent("run-r1", "软考批次公告还没出，我晚点再看。", now)
        self._write_cron_jobs([self._proactive_job(job_id="job-r1")])
        self._write_archive("job-r1", "2026-11-09_12-05-00", "软考批次公告还没出，我晚点再看。")
        self._seed_state_db(
            [("user", "下午继续看软考公告", now + timedelta(minutes=10))],
            started_at=window_start,
        )

        result = self.snapshot.build_snapshot(
            hermes_home=self.home,
            now_utc=now + timedelta(hours=1),
            force=True,
            config=self._config(),
        )

        self.assertTrue(result.wake_agent)
        row = self._sent_row("run-r1")
        self.assertEqual(row["delivery_status"], "delivered")
        self.assertEqual(row["cron_job_id"], "job-r1")
        self.assertEqual(row["message_id"], "2026-11-09_12-05-00")
        self.assertTrue(row["delivered_at"].endswith("Z"))
        checkpoint = self.storage.get_checkpoint("run-r1")
        self.assertEqual(checkpoint["status"], "committed")

        sent_md = Path(result.snapshot_dir, "sent.md").read_text(encoding="utf-8")
        self.assertIn("软考批次公告还没出，我晚点再看。", sent_md)
        self.assertIn("Run id: run-r1", sent_md)
        self.assertIn("Message id: 2026-11-09_12-05-00", sent_md)

    def test_reconciliation_rejects_archive_older_than_receipt(self):
        now = datetime(2026, 11, 9, 4, 0, tzinfo=timezone.utc)
        window_start = now - timedelta(hours=1)
        self._create_checkpoint("run-stale", window_start, now)
        self._insert_pending_sent("run-stale", "这条本轮提醒尚未送达。", now)
        self._write_cron_jobs([self._proactive_job(job_id="job-stale")])
        archive = self._write_archive(
            "job-stale",
            "2026-11-09_11-00-00",
            "上一轮运行的旧回复。",
        )
        self._set_archive_mtime(archive, now - timedelta(hours=2))
        self._seed_state_db(
            [("user", "触发新一轮检查", now + timedelta(minutes=10))],
            started_at=window_start,
        )

        result = self.snapshot.build_snapshot(
            hermes_home=self.home,
            now_utc=now + timedelta(hours=1),
            force=True,
            config=self._config(),
        )

        self.assertTrue(result.wake_agent)
        row = self._sent_row("run-stale")
        self.assertEqual(row["delivery_status"], "failed")
        self.assertIsNone(row["message_id"])
        self.assertEqual(self.storage.get_checkpoint("run-stale")["status"], "superseded")

    def test_reconciliation_does_not_reuse_archive_claimed_by_delivered_receipt(self):
        now = datetime(2026, 11, 9, 4, 0, tzinfo=timezone.utc)
        window_start = now - timedelta(hours=1)
        self._insert_delivered_sent(
            "run-prior",
            message_id="2026-11-09_12-05-00",
            content="上一轮已经确认送达。",
            delivered_at=now - timedelta(hours=2),
        )
        self._create_checkpoint("run-current", window_start, now)
        self._insert_pending_sent("run-current", "本轮提醒尚未送达。", now)
        self._write_cron_jobs([self._proactive_job(job_id="job-reuse")])
        archive = self._write_archive(
            "job-reuse",
            "2026-11-09_12-05-00",
            "上一轮已经确认送达。",
        )
        self._set_archive_mtime(archive, now)
        self._seed_state_db(
            [("user", "触发新一轮检查", now + timedelta(minutes=10))],
            started_at=window_start,
        )

        result = self.snapshot.build_snapshot(
            hermes_home=self.home,
            now_utc=now + timedelta(hours=1),
            force=True,
            config=self._config(),
        )

        self.assertTrue(result.wake_agent)
        row = self._sent_row("run-current")
        self.assertEqual(row["delivery_status"], "failed")
        self.assertIsNone(row["message_id"])

    def test_reconciliation_prefers_eligible_non_silent_archive(self):
        now = datetime(2026, 11, 9, 4, 0, tzinfo=timezone.utc)
        window_start = now - timedelta(hours=1)
        self._create_checkpoint("run-prefer", window_start, now)
        self._insert_pending_sent("run-prefer", "这轮提醒已经送达。", now)
        self._write_cron_jobs([self._proactive_job(job_id="job-prefer")])
        delivered_archive = self._write_archive(
            "job-prefer",
            "2026-11-09_12-05-00",
            "这轮提醒已经送达。",
            run_time="2026-11-09 12:05:00",
        )
        self._set_archive_mtime(delivered_archive, now)
        silent_archive = self._write_archive(
            "job-prefer",
            "2026-11-09_12-10-00",
            "[SILENT]",
            run_time="2026-11-09 12:10:00",
        )
        self._set_archive_mtime(silent_archive, now + timedelta(minutes=5))
        self._seed_state_db(
            [("user", "触发新一轮检查", now + timedelta(minutes=10))],
            started_at=window_start,
        )

        result = self.snapshot.build_snapshot(
            hermes_home=self.home,
            now_utc=now + timedelta(hours=1),
            force=True,
            config=self._config(),
        )

        self.assertTrue(result.wake_agent)
        row = self._sent_row("run-prefer")
        self.assertEqual(row["delivery_status"], "delivered")
        self.assertEqual(row["message_id"], "2026-11-09_12-05-00")

    def test_failed_delivery_rolls_back_checkpoint_and_rescans_from_previous_watermark(self):
        variants = (
            ("status", "error", None, "这条提醒不会送达。"),
            ("delivery_error", "ok", "feishu delivery failed", "这条提醒没有送达。"),
            ("silent", "ok", None, "[SILENT]"),
            ("empty", "ok", None, ""),
            ("missing", "ok", None, None),
        )
        for name, status, delivery_error, response in variants:
            with self.subTest(name=name):
                self._clear_runtime_state()
                now = datetime(2026, 11, 9, 7, 0, tzinfo=timezone.utc)
                t0 = now - timedelta(hours=2)
                t2 = now - timedelta(hours=1)
                self._create_checkpoint("run-r0", t0 - timedelta(hours=1), t0)
                self._create_checkpoint("run-r2", t2 - timedelta(minutes=30), t2)
                self._insert_pending_sent("run-r2", "这条提醒没有送达。", t2)
                self._write_cron_jobs([
                    self._proactive_job(
                        job_id="job-r2",
                        last_status=status,
                        last_delivery_error=delivery_error,
                    )
                ])
                self._write_archive("job-r2", "2026-11-09_14-00-00", response)
                self._seed_state_db(
                    [("user", "窗口期内仍需要重新评估的事项", now - timedelta(minutes=10))],
                    started_at=t0,
                )

                result = self.snapshot.build_snapshot(
                    hermes_home=self.home,
                    now_utc=now,
                    force=True,
                    config=self._config(),
                )

                row = self._sent_row("run-r2")
                self.assertEqual(row["delivery_status"], "failed")
                self.assertEqual(self.storage.get_checkpoint("run-r2")["status"], "superseded")
                self.assertEqual(parse_aware(result.window_start_utc), t0)
                self.assertTrue(result.wake_agent)

    def test_setup_cron_installs_bridge_and_is_idempotent_across_cli_and_slash(self):
        self._write_host_config(
            {
                "timezone": "Asia/Shanghai",
                "plugins": {
                    "entries": {
                        "proactive": {"settings": {"cron_schedule": "15 8,13,18 * * *"}}
                    }
                },
            }
        )
        self._write_cron_jobs([
            {
                "id": "existing-feishu",
                "name": "Existing Feishu DM",
                "deliver": "origin",
                "origin": {
                    "platform": "feishu",
                    "chat_id": "oc-existing",
                    "chat_name": "飞书私聊",
                    "thread_id": None,
                    "user_id": "ou-existing",
                },
            }
        ])
        parser = argparse.ArgumentParser()
        self.cli.setup_cli(parser)

        slash_output = self.cli.handle_slash("setup-cron")
        self.assertIn("proactive_notifier", slash_output)

        bridge = self.home / "scripts" / "proactive-tick.py"
        self.assertTrue(bridge.is_file())
        self.assertEqual(bridge.stat().st_mode & 0o777, 0o755)
        bridge_text = bridge.read_text(encoding="utf-8")
        self.assertTrue(bridge_text.startswith("#!"))
        compile(bridge_text, str(bridge), "exec")
        self.assertIn("run_bridge(", bridge_text)

        jobs = self._read_cron_jobs()
        proactive = [job for job in jobs if job.get("name") == "proactive_notifier"]
        self.assertEqual(len(proactive), 1)
        job = proactive[0]
        self.assertEqual(job["schedule"]["expr"], "15 8,13,18 * * *")
        self.assertEqual(job["timezone"], "Asia/Shanghai")
        self.assertTrue(job["next_run_at"].endswith("+08:00"))
        self.assertEqual(job["script"], "proactive-tick.py")
        self.assertEqual(job["skills"], ["proactive:editorial-gate"])
        self.assertEqual(job["enabled_toolsets"], ["proactivity", "session_search", "file"])
        self.assertEqual(job["deliver"], "origin")
        self.assertEqual(job["origin"]["chat_id"], "oc-existing")
        self.assertTrue(job["attach_to_session"])
        self.assertIsNone(job.get("model"))
        self.assertIsNone(job.get("provider"))
        self.assertIsNone(job.get("reasoning_effort"))

        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = self.cli.handle_cli(parser.parse_args(["setup-cron"]))
        self.assertEqual(code, 0)
        self.assertIn("proactive_notifier", output.getvalue())
        jobs = self._read_cron_jobs()
        self.assertEqual(sum(1 for item in jobs if item.get("name") == "proactive_notifier"), 1)
        self.assertTrue((self.home / "scripts" / "proactive-tick.py").is_file())

    def test_status_and_history_show_watermark_decisions_delivery_and_next_run(self):
        self._write_host_config({"timezone": "Asia/Shanghai"})
        now = datetime(2026, 11, 9, 4, 0, tzinfo=timezone.utc)
        self._create_checkpoint("run-status", now - timedelta(hours=1), now)
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO candidate_items (
                    run_id, item_num, worker_kind, title, priority, relevant_until,
                    why_it_matters, evidence_refs, proposed_action, what_remains, draft,
                    decision, decision_reason, decision_note, decided_at
                ) VALUES (?, ?, ?, ?, 'regular', ?, '原因', '[]', '动作', '剩余', NULL, ?, ?, NULL, ?)
                """,
                [
                    ("run-status", 1, "opportunities", "发送的提醒", utc_iso(now), "send", "changed", utc_iso(now)),
                    ("run-status", 2, "prepared", "跳过的提醒", utc_iso(now), "skip", "already_handled", utc_iso(now)),
                ],
            )
            conn.execute(
                """
                INSERT INTO sent_log (
                    run_id, cron_job_id, message_id, content,
                    delivery_status, created_at, delivered_at
                ) VALUES (?, 'job-status', 'archive-status', ?, 'delivered', ?, ?)
                """,
                ("run-status", "已送达的主动提醒内容", utc_iso(now), utc_iso(now)),
            )
            conn.commit()
        self._write_cron_jobs([
            {
                "id": "job-status",
                "name": "proactive_notifier",
                "schedule": {"kind": "cron", "expr": "30 9,12,15,18,21 * * *"},
                "schedule_display": "30 9,12,15,18,21 * * *",
                "timezone": "Asia/Shanghai",
                "next_run_at": "2026-11-09T15:30:00+08:00",
                "last_status": "ok",
                "deliver": "origin",
            }
        ])

        parser = argparse.ArgumentParser()
        self.cli.setup_cli(parser)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = self.cli.handle_cli(parser.parse_args(["status"]))
        self.assertEqual(code, 0)
        status = output.getvalue()
        self.assertIn("run-status", status)
        self.assertIn("Active tracked goals: 1", status)
        self.assertIn("2026-11-09 12:00:00", status)
        self.assertIn("Asia/Shanghai", status)
        self.assertIn("Delivered", status)
        self.assertIn("2026-11-09 15:30:00", status)

        slash_status = self.cli.handle_slash("status")
        self.assertIn("run-status", slash_status)
        self.assertIn("Asia/Shanghai", slash_status)

        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = self.cli.handle_cli(parser.parse_args(["history"]))
        self.assertEqual(code, 0)
        history = output.getvalue()
        self.assertIn("run-status", history)
        self.assertIn("[item:1] send", history)
        self.assertIn("changed", history)
        self.assertIn("[item:2] skip", history)
        self.assertIn("already_handled", history)
        self.assertIn("已送达的主动提醒内容", history)
        self.assertIn("2026-11-09 12:00:00", history)

        slash_history = self.cli.handle_slash("history")
        self.assertIn("run-status", slash_history)
        self.assertIn("已送达的主动提醒内容", slash_history)

    def test_full_pipeline_round_trip_reconciles_delivery_without_memory_writes(self):
        now = datetime(2026, 11, 9, 4, 0, tzinfo=timezone.utc)
        t0 = now - timedelta(hours=1)
        self._create_checkpoint("run-before", t0 - timedelta(hours=1), t0)
        self._seed_state_db(
            [
                ("user", "明天记得跟进软考报名材料", now - timedelta(minutes=20)),
                ("assistant", "我记下了，会在合适的时候提醒你。", now - timedelta(minutes=19)),
            ],
            started_at=t0,
        )
        self.storage.upsert_tracking_item(
            "handled-goal",
            title="提交软考报名材料",
            description="用户表示已经缴费并提交材料。",
            summary="等待用户确认提交完成。",
            momentum_status="waiting",
        )
        self._write_cron_jobs([self._proactive_job(job_id="job-e2e")])

        memory_path = self.home / "MEMORY.md"
        user_memory_path = self.home / "USER.md"
        memory_path.write_text("长期记忆不得被主动提醒污染。\n", encoding="utf-8")
        user_memory_path.write_text("用户画像不得被主动提醒污染。\n", encoding="utf-8")
        memory_before = memory_path.read_bytes()
        user_memory_before = user_memory_path.read_bytes()

        def worker1(kwargs):
            marker = int(SOURCE_RE.search(_input_text(kwargs)).group(1))
            return {
                "tracking_upserts": [],
                "candidates": [
                    {
                        "title": "跟进软考报名材料",
                        "priority": "high",
                        "relevant_until": "2026-11-10T12:00:00+08:00",
                        "why_it_matters": "报名材料可能即将截止。",
                        "evidence_refs": [f"[source:{marker}]"],
                        "proposed_action": "提醒用户确认材料状态。",
                        "what_remains": "等待编辑门复核。",
                    }
                ],
            }

        def worker2(kwargs):
            marker = int(SOURCE_RE.search(_input_text(kwargs)).group(1))
            return {
                "candidates": [
                    {
                        "title": "软考报名截止时间",
                        "priority": "regular",
                        "relevant_until": "2026-11-10T12:00:00+08:00",
                        "why_it_matters": "截止时间临近。",
                        "evidence_refs": [f"[source:{marker}]"],
                        "proposed_action": "给出一句简短提醒。",
                        "what_remains": "等待编辑门定稿。",
                        "draft": "软考报名材料记得确认一下。",
                    }
                ]
            }

        llm = StructuredLlmRecorder([worker1, worker2])
        stage_one = io.StringIO()
        with contextlib.redirect_stdout(stage_one):
            result = self.snapshot.run_tick(
                hermes_home=self.home,
                now_utc=now,
                llm_client=llm,
                force=True,
                config=self._config(),
            )
        self.assertEqual(result.candidate_count, 2)
        self.assertIn("Edition Handoff", stage_one.getvalue())
        self.assertEqual(len(llm.calls), 2)
        self.assertEqual(self.storage.get_checkpoint(result.run_id)["status"], "pending")

        self._seed_state_db(
            [
                ("user", "明天记得跟进软考报名材料", now - timedelta(minutes=20)),
                ("assistant", "我记下了，会在合适的时候提醒你。", now - timedelta(minutes=19)),
                ("user", "我刚已经把材料交了。", now + timedelta(minutes=5)),
            ],
            started_at=t0,
        )

        read_payload = json.loads(self.tools.handle_proactivity_read())
        self.assertTrue(read_payload["ok"])
        diff = [item["content"] for item in read_payload["live_conversation_diff"]]
        self.assertIn("我刚已经把材料交了。", diff)

        final_message = "软考材料你已经交了，我这边就不再反复提醒，接下来只留意准考证时间。"
        selected = json.loads(
            self.tools.handle_proactivity_select(
                {
                    "run_id": result.run_id,
                    "decisions": [
                        {"item_num": 1, "decision": "send", "reason": "changed"},
                        {"item_num": 2, "decision": "skip", "reason": "already_handled"},
                    ],
                    "final_message": final_message,
                    "close_goal_ids": ["handled-goal"],
                }
            )
        )
        self.assertTrue(selected["ok"])
        self.assertEqual(selected["action"], "deliver")
        self.assertEqual(self.storage.get_tracking_item("handled-goal")["status"], "closed")
        self.assertEqual(self._sent_row(result.run_id)["delivery_status"], "pending")

        self._write_cron_jobs([self._proactive_job(job_id="job-e2e")])
        # The receipt records the real wall clock, so the archive must carry a
        # logical timestamp at or after it; a fixed fixture date goes stale late in the day.
        archive_stamp = datetime.now(BEIJING_TZ)
        archive_id = archive_stamp.strftime("%Y-%m-%d_%H-%M-%S")
        self._write_archive(
            "job-e2e",
            archive_id,
            final_message,
            run_time=archive_stamp.strftime("%Y-%m-%d %H:%M:%S"),
        )
        later = now + timedelta(hours=3)
        self._seed_state_db(
            [
                ("user", "明天记得跟进软考报名材料", now - timedelta(minutes=20)),
                ("assistant", "我记下了，会在合适的时候提醒你。", now - timedelta(minutes=19)),
                ("user", "我刚已经把材料交了。", now + timedelta(minutes=5)),
                ("user", "收到，谢谢。", later - timedelta(minutes=10)),
            ],
            started_at=t0,
        )

        next_result = self.snapshot.build_snapshot(
            hermes_home=self.home,
            now_utc=later,
            force=True,
            config=self._config(),
        )

        self.assertTrue(next_result.wake_agent)
        receipt = self._sent_row(result.run_id)
        self.assertEqual(receipt["delivery_status"], "delivered")
        self.assertEqual(receipt["cron_job_id"], "job-e2e")
        self.assertEqual(receipt["message_id"], archive_id)
        self.assertEqual(self.storage.get_checkpoint(result.run_id)["status"], "committed")
        sent_md = Path(next_result.snapshot_dir, "sent.md").read_text(encoding="utf-8")
        self.assertIn(final_message, sent_md)
        self.assertEqual(memory_path.read_bytes(), memory_before)
        self.assertEqual(user_memory_path.read_bytes(), user_memory_before)


if __name__ == "__main__":
    unittest.main()

"""Behavioral tests for partitioned research workers and evidence compilation."""

from __future__ import annotations

import contextlib
import io
import json
import os
import re
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from hermes_constants import reset_hermes_home_override, set_hermes_home_override
from tests.test_goal_tracking import load_plugin_package


BEIJING_TZ = timezone(timedelta(hours=8))
SOURCE_RE = re.compile(r"\[source:(\d+)\]")


def utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def unix_ts(value: datetime) -> float:
    return value.astimezone(timezone.utc).timestamp()


class StructuredLlmRecorder:
    """Deterministic structured-LLM double that records every host call."""

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


def input_text(call):
    blocks = call["input"]
    parts = []
    for block in blocks:
        if isinstance(block, dict):
            parts.append(str(block.get("text") or ""))
        else:
            parts.append(str(getattr(block, "text", "")))
    return "\n".join(parts)


def markers(text):
    return [int(value) for value in SOURCE_RE.findall(text)]


def section(text: str, start_label: str, end_label: str | None = None) -> str:
    start = text.find(start_label)
    if start < 0:
        return ""
    start += len(start_label)
    if end_label is None:
        return text[start:]
    end = text.find(end_label, start)
    return text[start:] if end < 0 else text[start:end]


class ResearchWorkersAndSourcesTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.plugin = load_plugin_package()
        cls.storage = cls.plugin.storage
        cls.snapshot = cls.plugin.snapshot
        cls.workers = cls.plugin.workers

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="hermes-proactive-workers-test-")
        self.home = Path(self._tmp.name)
        self._old_home = os.environ.get("HERMES_HOME")
        os.environ["HERMES_HOME"] = str(self.home)
        self._override_token = set_hermes_home_override(str(self.home))
        self.addCleanup(self._cleanup_home)

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

    def _open_db(self):
        return contextlib.closing(sqlite3.connect(self.db_path))

    def _commit_checkpoint(self, window_start: datetime) -> str:
        self.storage.init_db()
        window = utc_iso(window_start)
        run_id = f"previous-{window_start.timestamp()}"
        with self._open_db() as conn:
            conn.execute(
                """
                INSERT INTO checkpoints (
                    run_id, snapshot_dir, window_start_utc, window_end_utc,
                    status, candidate_count, created_at, committed_at
                ) VALUES (?, ?, ?, ?, 'committed', 0, ?, ?)
                """,
                (run_id, "previous", window, window, window, window),
            )
            conn.commit()
        return window

    def _seed_state(self, now_utc: datetime, window_start: datetime):
        state_db = self.home / "state.db"
        with contextlib.closing(sqlite3.connect(state_db)) as conn:
            conn.executescript(
                """
                CREATE TABLE sessions (
                    id TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    title TEXT,
                    display_name TEXT,
                    parent_session_id TEXT,
                    started_at REAL NOT NULL
                );
                CREATE TABLE messages (
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
            conn.execute(
                """
                INSERT INTO sessions (
                    id, source, title, display_name, parent_session_id, started_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("s-feishu", "feishu", "飞书会话", None, None, unix_ts(window_start)),
            )
            rows = [
                ("user", "飞书独有对话内容XYZ，提醒我跟进软考报名。", window_start + timedelta(minutes=5)),
                ("assistant", "好的，我会留意报名材料。", window_start + timedelta(minutes=6)),
            ]
            conn.executemany(
                """
                INSERT INTO messages (
                    session_id, role, content, timestamp, active, reasoning, reasoning_content
                ) VALUES ('s-feishu', ?, ?, ?, 1, NULL, NULL)
                """,
                [(role, content, unix_ts(stamp)) for role, content, stamp in rows],
            )
            conn.commit()

    def _write_cron_jobs(self):
        cron_dir = self.home / "cron"
        cron_dir.mkdir(parents=True, exist_ok=True)
        (cron_dir / "jobs.json").write_text(
            json.dumps(
                {
                    "jobs": [
                        {
                            "id": "5de04fc729fe",
                            "name": "U2",
                            "schedule_display": "0 10 * * *",
                            "next_run_at": "2026-11-10T10:00:00+08:00",
                            "last_status": "ok",
                        },
                        {
                            "id": "broken-cron",
                            "name": "Broken",
                            "schedule_display": "0 12 * * *",
                            "next_run_at": "2026-11-10T12:00:00+08:00",
                            "last_status": "failed",
                        },
                    ],
                    "updated_at": utc_iso(datetime.now(timezone.utc)),
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

    def _seed_tracking(self, now_utc: datetime, window_start: datetime):
        self.storage.init_db()
        self.storage.upsert_tracking_item(
            "existing-goal",
            title="旧标题",
            description="旧描述",
            summary="旧摘要",
            momentum_status="waiting",
        )
        self.storage.upsert_tracking_item(
            "close-attempt-goal",
            title="可关闭尝试目标",
            description="验证关闭尝试被忽略。",
            summary="原摘要",
            momentum_status="on_track",
        )
        with self._open_db() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO tracking_items (
                    goal_id, title, description, summary, status, momentum_status,
                    source_ref, created_at, updated_at, closed_at
                ) VALUES (?, ?, ?, ?, 'closed', 'on_track', NULL, ?, ?, ?)
                """,
                (
                    "closed-goal",
                    "已关闭目标",
                    "不可被 Worker 1 修改。",
                    "已关闭摘要",
                    utc_iso(window_start - timedelta(days=1)),
                    utc_iso(window_start + timedelta(minutes=1)),
                    utc_iso(window_start + timedelta(minutes=1)),
                ),
            )
            conn.commit()

    def _seed_sent_log(self, window_start: datetime):
        run_id = "prior-delivered-run"
        with self._open_db() as conn:
            conn.execute(
                """
                INSERT INTO checkpoints (
                    run_id, snapshot_dir, window_start_utc, window_end_utc,
                    status, candidate_count, created_at, committed_at
                ) VALUES (?, 'old', ?, ?, 'committed', 0, ?, ?)
                """,
                (
                    run_id,
                    utc_iso(window_start - timedelta(days=1)),
                    utc_iso(window_start),
                    utc_iso(window_start),
                    utc_iso(window_start),
                ),
            )
            conn.execute(
                """
                INSERT INTO sent_log (
                    run_id, cron_job_id, message_id, content,
                    delivery_status, created_at, delivered_at
                ) VALUES (?, 'proactive_notifier', 'msg-1', ?, 'delivered', ?, ?)
                """,
                (
                    run_id,
                    "上次主动提醒内容SENT",
                    utc_iso(window_start - timedelta(hours=1)),
                    utc_iso(window_start - timedelta(hours=1)),
                ),
            )
            conn.commit()

    def _prepare_home(self):
        now_utc = datetime(2026, 11, 9, 4, 0, tzinfo=timezone.utc)  # 12:00 Beijing
        window_start = now_utc - timedelta(hours=1)
        previous_watermark = self._commit_checkpoint(window_start)
        self._seed_state(now_utc, window_start)
        self._write_cron_jobs()
        self._seed_tracking(now_utc, window_start)
        self._seed_sent_log(window_start)
        (self.home / "memories").mkdir(parents=True, exist_ok=True)
        (self.home / "memories" / "MEMORY.md").write_text(
            "提醒我跟进软考报名相关事项。", encoding="utf-8"
        )
        return now_utc, window_start, previous_watermark

    def _run_tick(self, llm):
        now_utc, window_start, previous_watermark = self._prepare_home()
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            result = self.snapshot.run_tick(
                hermes_home=self.home,
                now_utc=now_utc,
                llm_client=llm,
                force=True,
                config={"memory_source": "builtin"},
            )
        return result, output.getvalue(), now_utc, window_start, previous_watermark

    def _checkpoint(self, run_id: str) -> dict | None:
        with self._open_db() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM checkpoints WHERE run_id = ?", (run_id,)).fetchone()
            return dict(row) if row else None

    def _candidate_rows(self, run_id: str) -> list[dict]:
        with self._open_db() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM candidate_items WHERE run_id = ? ORDER BY item_num", (run_id,)
            ).fetchall()
            return [dict(row) for row in rows]

    def _tracking(self, goal_id: str) -> dict:
        item = self.storage.get_tracking_item(goal_id)
        self.assertIsNotNone(item)
        return item

    def _snapshot_path(self, result, filename: str) -> Path:
        return Path(result["snapshot_dir"]) / filename

    def test_worker_inputs_are_partitioned_and_calls_use_temperature_and_schemas(self):
        def worker1(kwargs):
            return {
                "tracking_upserts": [
                    {
                        "goal_id": "existing-goal",
                        "title": "更新后的标题",
                        "description": "更新后的描述",
                        "summary": "Worker1 更新摘要",
                        "momentum_status": "stalled",
                    }
                ],
                "candidates": [],
            }

        llm = StructuredLlmRecorder([worker1, {"candidates": []}])
        self._run_tick(llm)

        self.assertEqual(len(llm.calls), 2)
        first, second = llm.calls
        self.assertIs(first["json_schema"], self.workers.WORKER1_OPPORTUNITIES_SCHEMA)
        self.assertIs(second["json_schema"], self.workers.WORKER2_PREPARED_SCHEMA)
        self.assertEqual(first["temperature"], 0.1)
        self.assertEqual(second["temperature"], 0.1)
        for call in (first, second):
            self.assertNotIn("tools", call)
            self.assertTrue(call["input"])

        first_input = input_text(first)
        self.assertIn("飞书独有对话内容XYZ", first_input)
        self.assertIn("existing-goal", first_input)
        self.assertIn("5de04fc729fe", first_input)
        self.assertIn("Hard non-interference rule", first_input)
        self.assertIn("U2", first_input)
        self.assertNotIn("提醒我跟进软考报名相关事项。", first_input)
        self.assertNotIn("Runtime context", first_input)
        self.assertNotIn("上次主动提醒内容SENT", first_input)
        self.assertNotIn("Delivered proactive messages", first_input)

        second_input = input_text(second)
        self.assertIn("Worker1 更新摘要", second_input)
        self.assertIn("提醒我跟进软考报名相关事项。", second_input)
        self.assertIn("Memory source: builtin · status: ok", second_input)
        self.assertIn("Runtime context", second_input)
        self.assertIn("上次主动提醒内容SENT", second_input)
        self.assertNotIn("飞书独有对话内容XYZ", second_input)
        self.assertNotIn("## conversation.md", second_input)

    def test_worker1_upserts_insert_update_and_never_touch_closed_goals(self):
        self.storage.init_db()
        with self._open_db() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO tracking_items (
                    goal_id, title, description, summary, status, momentum_status,
                    source_ref, created_at, updated_at, closed_at
                ) VALUES (?, ?, ?, ?, 'closed', 'on_track', NULL, ?, ?, ?)
                """,
                (
                    "closed-goal",
                    "已关闭目标",
                    "不可被 Worker 1 修改。",
                    "已关闭摘要",
                    utc_iso(datetime(2026, 11, 9, 2, 0, tzinfo=timezone.utc)),
                    utc_iso(datetime(2026, 11, 9, 3, 1, tzinfo=timezone.utc)),
                    utc_iso(datetime(2026, 11, 9, 3, 1, tzinfo=timezone.utc)),
                ),
            )
            conn.commit()
        closed_before = self._tracking("closed-goal")

        def worker1(kwargs):
            return {
                "tracking_upserts": [
                    {
                        "goal_id": "new-goal",
                        "title": "新目标",
                        "description": "新描述",
                        "summary": "新摘要",
                        "momentum_status": "waiting",
                        "status": "closed",
                    },
                    {
                        "goal_id": "existing-goal",
                        "title": "更新后的标题",
                        "description": "更新后的描述",
                        "summary": "Worker1 更新摘要",
                        "momentum_status": "stalled",
                    },
                    {
                        "goal_id": "close-attempt-goal",
                        "title": "关闭尝试目标",
                        "description": "仍应保持活动。",
                        "summary": "关闭尝试后的摘要",
                        "momentum_status": "time_sensitive",
                        "status": "closed",
                        "closed_at": utc_iso(datetime.now(timezone.utc)),
                    },
                    {
                        "goal_id": "closed-goal",
                        "title": "篡改标题",
                        "description": "篡改描述",
                        "summary": "篡改摘要",
                        "momentum_status": "on_track",
                        "status": "active",
                        "closed_at": None,
                    },
                ],
                "candidates": [],
            }

        llm = StructuredLlmRecorder([worker1, {"candidates": []}])
        result, stdout, _, _, _ = self._run_tick(llm)

        new_goal = self._tracking("new-goal")
        self.assertEqual(new_goal["status"], "active")
        self.assertEqual(new_goal["title"], "新目标")
        self.assertEqual(new_goal["momentum_status"], "waiting")

        existing = self._tracking("existing-goal")
        self.assertEqual(existing["title"], "更新后的标题")
        self.assertEqual(existing["description"], "更新后的描述")
        self.assertEqual(existing["summary"], "Worker1 更新摘要")
        self.assertEqual(existing["momentum_status"], "stalled")
        self.assertEqual(existing["status"], "active")

        close_attempt = self._tracking("close-attempt-goal")
        self.assertEqual(close_attempt["status"], "active")
        self.assertIsNone(close_attempt["closed_at"])
        self.assertEqual(close_attempt["summary"], "关闭尝试后的摘要")

        self.assertEqual(self._tracking("closed-goal"), closed_before)

        second_input = input_text(llm.calls[1])
        self.assertIn("新目标", second_input)
        self.assertIn("Worker1 更新摘要", second_input)
        self.assertNotIn("已关闭目标", second_input)
        self.assertEqual(self._candidate_rows(result["run_id"]), [])
        self.assertEqual(stdout.splitlines()[-1], '{"wakeAgent": false}')

    def test_candidates_are_numbered_persisted_and_rendered_with_sources(self):
        def worker1(kwargs):
            text = input_text(kwargs)
            conversation_markers = markers(section(text, "## conversation.md", "## tracking.md"))
            tracking_markers = markers(section(text, "## tracking.md", "## scheduled-crons.md"))
            self.assertTrue(conversation_markers)
            self.assertTrue(tracking_markers)
            return {
                "tracking_upserts": [],
                "candidates": [
                    {
                        "title": "跟进软考报名材料",
                        "priority": "high",
                        "relevant_until": "2026-11-10T12:00:00+08:00",
                        "why_it_matters": "用户提到需要报名材料。",
                        "evidence_refs": [
                            f"[source:{conversation_markers[0]}]",
                            f"[source:{tracking_markers[0]}]",
                        ],
                        "proposed_action": "询问材料是否齐全。",
                        "what_remains": "确认报名材料准备进度。",
                    }
                ],
            }

        def worker2(kwargs):
            text = input_text(kwargs)
            memory_markers = markers(section(text, "## memory.md", "## context.md"))
            sent_markers = markers(section(text, "## sent.md"))
            self.assertTrue(memory_markers)
            self.assertTrue(sent_markers)
            return {
                "candidates": [
                    {
                        "title": "软考公告时间窗口",
                        "priority": "regular",
                        "relevant_until": "2026-11-11T12:00:00+08:00",
                        "why_it_matters": "记忆和投递记录显示需要关注公告。",
                        "evidence_refs": [
                            f"[source:{memory_markers[0]}]",
                            f"[source:{sent_markers[0]}]",
                        ],
                        "proposed_action": "提醒查看公告。",
                        "what_remains": "等待官方公告。",
                        "draft": "软考公告可以留意一下，别错过时间窗口。",
                    }
                ]
            }

        llm = StructuredLlmRecorder([worker1, worker2])
        result, _, _, _, _ = self._run_tick(llm)

        rows = self._candidate_rows(result["run_id"])
        self.assertEqual([row["item_num"] for row in rows], [1, 2])
        self.assertEqual([row["worker_kind"] for row in rows], ["opportunities", "prepared"])
        self.assertEqual(rows[0]["title"], "跟进软考报名材料")
        self.assertEqual(rows[1]["title"], "软考公告时间窗口")
        self.assertEqual(rows[1]["draft"], "软考公告可以留意一下，别错过时间窗口。")
        first_refs = json.loads(rows[0]["evidence_refs"])
        self.assertEqual(len(first_refs), 2)
        self.assertTrue(all(SOURCE_RE.fullmatch(ref) for ref in first_refs))

        opportunities = self._snapshot_path(result, "opportunities.md")
        prepared = self._snapshot_path(result, "prepared.md")
        sources = self._snapshot_path(result, "sources.md")
        for path in (opportunities, prepared, sources):
            self.assertTrue(path.is_file(), path.name)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600, path.name)

        opportunities_text = opportunities.read_text(encoding="utf-8")
        prepared_text = prepared.read_text(encoding="utf-8")
        sources_text = sources.read_text(encoding="utf-8")

        self.assertIn("[item:1] 跟进软考报名材料", opportunities_text)
        self.assertNotIn("[item:2]", opportunities_text)
        self.assertIn("[item:2] 软考公告时间窗口", prepared_text)
        self.assertIn("Draft:", prepared_text)
        self.assertIn("软考公告可以留意一下", prepared_text)
        self.assertNotIn("[item:1]", prepared_text)

        cited = set()
        for row in rows:
            for ref in json.loads(row["evidence_refs"]):
                cited.add(int(SOURCE_RE.fullmatch(ref).group(1)))
        source_map = result["source_map"]
        for number in cited:
            self.assertIn(f"## [source:{number}]", sources_text)
            self.assertIn(source_map[number]["text"], sources_text)

        uncited = set(source_map) - cited - {
            number for number, entry in source_map.items() if entry["kind"] == "sent"
        }
        self.assertTrue(uncited)
        for number in uncited:
            self.assertNotIn(f"## [source:{number}]", sources_text)

        self.assertIn("7-day sent summary", sources_text)
        self.assertIn("上次主动提醒内容SENT", sources_text)
        self.assertIn("Delivered proactive messages in the last 7 days: 1", sources_text)

    def test_k_zero_commits_checkpoint_and_prints_wake_false(self):
        llm = StructuredLlmRecorder([{"tracking_upserts": [], "candidates": []}, {"candidates": []}])
        result, stdout, _, _, _ = self._run_tick(llm)

        checkpoint = self._checkpoint(result["run_id"])
        self.assertIsNotNone(checkpoint)
        self.assertEqual(checkpoint["status"], "committed")
        self.assertEqual(checkpoint["candidate_count"], 0)
        self.assertIsNotNone(checkpoint["committed_at"])
        self.assertEqual(self.storage.latest_committed_watermark(), result["window_end_utc"])
        self.assertEqual(stdout.splitlines()[-1], '{"wakeAgent": false}')

    def test_k_positive_keeps_checkpoint_pending_and_prints_edition_handoff(self):
        def worker1(kwargs):
            text = input_text(kwargs)
            marker = markers(text)[0]
            return {
                "tracking_upserts": [],
                "candidates": [
                    {
                        "title": "需要主 Agent 复核的候选",
                        "priority": "high",
                        "relevant_until": "2026-11-10T12:00:00+08:00",
                        "why_it_matters": "候选需要编辑门判断。",
                        "evidence_refs": [f"[source:{marker}]"],
                        "proposed_action": "阅读候选并决定是否发送。",
                        "what_remains": "等待编辑门决策。",
                    }
                ],
            }

        llm = StructuredLlmRecorder([worker1, {"candidates": []}])
        result, stdout, _, _, previous_watermark = self._run_tick(llm)

        checkpoint = self._checkpoint(result["run_id"])
        self.assertIsNotNone(checkpoint)
        self.assertEqual(checkpoint["status"], "pending")
        self.assertEqual(checkpoint["candidate_count"], 1)
        self.assertIsNone(checkpoint["committed_at"])
        self.assertEqual(self.storage.latest_committed_watermark(), previous_watermark)

        lines = stdout.splitlines()
        self.assertNotEqual(lines[-1], '{"wakeAgent": false}')
        self.assertIn(result["run_id"], stdout)
        self.assertIn("Edition Handoff", stdout)
        self.assertIn("Candidates: 1", stdout)
        self.assertIn("[item:1] 需要主 Agent 复核的候选", stdout)
        self.assertIn("proactivity_read", stdout)


if __name__ == "__main__":
    unittest.main()

"""Behavioral tests for the deterministic snapshot builder and wake gates."""

from __future__ import annotations

import argparse
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
from unittest import mock

from hermes_constants import reset_hermes_home_override, set_hermes_home_override
from tests.test_goal_tracking import load_plugin_package


REPO_ROOT = Path(__file__).resolve().parents[1]
PLUGIN_DIR = REPO_ROOT / "plugins" / "proactive"
BEIJING_TZ = timezone(timedelta(hours=8))
SNAPSHOT_FILES = (
    "conversation.md",
    "tracking.md",
    "sent.md",
    "scheduled-crons.md",
    "memory.md",
    "context.md",
)


def utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def unix_ts(value: datetime) -> float:
    return value.astimezone(timezone.utc).timestamp()


class NoLlmClient:
    def __init__(self):
        self.calls = []

    def complete_structured(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        raise AssertionError("Stage 1 must not invoke an LLM")


class SnapshotAndWakeGateTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.plugin = load_plugin_package()
        cls.storage = cls.plugin.storage
        cls.cli = cls.plugin.cli
        cls.snapshot = cls.plugin.snapshot

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="hermes-proactive-snapshot-test-")
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
    def hidden_files(self) -> Path:
        return self.plugin_data / "hidden_files"

    @property
    def state_db(self) -> Path:
        return self.home / "state.db"

    @property
    def db_path(self) -> Path:
        return self.plugin_data / "data.db"

    def snapshot_dirs(self) -> list[Path]:
        if not self.hidden_files.exists():
            return []
        return sorted(path for path in self.hidden_files.iterdir() if path.is_dir())

    def _open_proactive_db(self):
        return contextlib.closing(sqlite3.connect(self.db_path))

    def _commit_checkpoint(self, window_start: datetime, window_end: datetime | None = None):
        self.storage.init_db()
        window_end = window_end or window_start
        with self._open_proactive_db() as conn:
            conn.execute(
                """
                INSERT INTO checkpoints (
                    run_id, snapshot_dir, window_start_utc, window_end_utc,
                    status, candidate_count, created_at, committed_at
                ) VALUES (?, ?, ?, ?, 'committed', 0, ?, ?)
                """,
                (
                    f"checkpoint-{window_start.timestamp()}",
                    str(self.plugin_data / "hidden_files" / "previous"),
                    utc_iso(window_start),
                    utc_iso(window_end),
                    utc_iso(window_end),
                    utc_iso(window_end),
                ),
            )
            conn.commit()

    def _seed_state_db(self, sessions, messages):
        with contextlib.closing(sqlite3.connect(self.state_db)) as conn:
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
            conn.executemany(
                """
                INSERT INTO sessions (
                    id, source, title, display_name, parent_session_id, started_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                sessions,
            )
            conn.executemany(
                """
                INSERT INTO messages (
                    session_id, role, content, timestamp, active, reasoning, reasoning_content
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                messages,
            )
            conn.commit()

    @staticmethod
    def _message(session_id, role, content, timestamp, *, active=1, reasoning=None):
        return (
            session_id,
            role,
            content,
            unix_ts(timestamp),
            active,
            reasoning,
            reasoning,
        )

    def _write_cron_jobs(self, jobs):
        cron_dir = self.home / "cron"
        cron_dir.mkdir(parents=True, exist_ok=True)
        (cron_dir / "jobs.json").write_text(
            json.dumps({"jobs": jobs, "updated_at": utc_iso(datetime.now(timezone.utc))}, ensure_ascii=False),
            encoding="utf-8",
        )

    def _snapshot_config(self) -> dict:
        return {"memory_source": "builtin", "timezone": "Asia/Shanghai"}

    def test_first_run_initializes_isolated_preferences_with_required_rules(self):
        now_utc = datetime(2026, 11, 9, 14, 0, tzinfo=timezone.utc)  # 22:00 local
        real_preferences = Path.home() / "PROACTIVE_PREFERENCES.md"
        real_before = real_preferences.read_bytes() if real_preferences.exists() else None

        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.snapshot.run_tick(
                hermes_home=self.home,
                now_utc=now_utc,
                snapshot_only=True,
                config=self._snapshot_config(),
            )

        self.assertEqual(output.getvalue().splitlines()[-1], '{"wakeAgent": false}')
        preferences = self.home / "PROACTIVE_PREFERENCES.md"
        self.assertTrue(preferences.is_file())
        content = preferences.read_text(encoding="utf-8")
        self.assertIn("09:00-21:30", content)
        self.assertIn("Asia/Shanghai", content)
        self.assertIn("UTC+08:00", content)
        self.assertIn("scheduled-crons.md", content)
        # The template must stay user-neutral: no local job ids, names, or persona.
        for local_value in (
            "5de04fc729fe",
            "332c60e27345",
            "858767f4b4ed",
            "1f363645398b",
            "77d26248750d",
            "b5450d7ca64d",
            "4605e65e5053",
            "b6a8024ee728",
            "5f4d9f41a2b7",
            "SakuraFrp",
            "Vaultwarden",
            "DailyNote",
            "Aier",
        ):
            self.assertNotIn(local_value, content)
        if real_preferences.exists():
            self.assertEqual(real_preferences.read_bytes(), real_before)
        else:
            self.assertFalse(real_preferences.exists())

    def test_quiet_hour_gate_short_circuits_without_snapshot_pruning_or_llm(self):
        now_utc = datetime(2026, 11, 9, 14, 0, tzinfo=timezone.utc)  # 22:00 Beijing
        window_start = now_utc - timedelta(hours=1)
        self._commit_checkpoint(window_start)
        self._seed_state_db(
            [("s-feishu", "feishu", "飞书会话", "飞书会话", None, unix_ts(window_start))],
            [self._message("s-feishu", "user", "这条消息不应触发快照", window_start + timedelta(minutes=5))],
        )
        stale = self.hidden_files / "1700000000000-abcdef"
        stale.mkdir(parents=True)
        (stale / "conversation.md").write_text("stale", encoding="utf-8")
        llm = NoLlmClient()

        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            result = self.snapshot.run_tick(
                hermes_home=self.home,
                now_utc=now_utc,
                llm_client=llm,
                snapshot_only=True,
                config=self._snapshot_config(),
            )

        self.assertEqual(output.getvalue().splitlines()[-1], '{"wakeAgent": false}')
        self.assertFalse(result["wake_agent"])
        self.assertIsNone(result["snapshot_dir"])
        self.assertEqual(self.snapshot_dirs(), [stale])
        self.assertEqual(llm.calls, [])

    def test_force_bypasses_quiet_hour_gate(self):
        now_utc = datetime(2026, 11, 9, 14, 0, tzinfo=timezone.utc)  # 22:00 Beijing
        window_start = now_utc - timedelta(hours=1)
        self._commit_checkpoint(window_start)
        self._seed_state_db(
            [("s-desktop", "desktop", "桌面会话", None, None, unix_ts(window_start))],
            [self._message("s-desktop", "user", "强制生成快照", window_start + timedelta(minutes=5))],
        )

        result = self.snapshot.build_snapshot(
            hermes_home=self.home,
            now_utc=now_utc,
            force=True,
            config=self._snapshot_config(),
        )

        self.assertTrue(result["wake_agent"])
        self.assertIsNotNone(result["snapshot_dir"])
        self.assertTrue(Path(result["snapshot_dir"]).is_dir())

    def test_pruning_removes_only_expired_snapshot_directories_and_keeps_sqlite_rows(self):
        now_utc = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)
        window_start = now_utc - timedelta(hours=1)
        self._commit_checkpoint(window_start)
        self._seed_state_db(
            [("s-cli", "cli", "CLI", None, None, unix_ts(window_start))],
            [self._message("s-cli", "user", "新增事项", window_start + timedelta(minutes=1))],
        )
        self.hidden_files.mkdir(parents=True, exist_ok=True)
        expired = self.hidden_files / "1600000000000-oldold"
        retained = self.hidden_files / "1700000000000-keepme"
        expired.mkdir()
        retained.mkdir()
        old_time = (now_utc - timedelta(days=15)).timestamp()
        retained_time = (now_utc - timedelta(days=13)).timestamp()
        os.utime(expired, (old_time, old_time))
        os.utime(retained, (retained_time, retained_time))
        with self._open_proactive_db() as conn:
            before_items = conn.execute("SELECT COUNT(*) FROM tracking_items").fetchone()[0]
            before_checkpoints = conn.execute("SELECT COUNT(*) FROM checkpoints").fetchone()[0]

        self.snapshot.build_snapshot(
            hermes_home=self.home,
            now_utc=now_utc,
            force=True,
            config=self._snapshot_config(),
        )

        self.assertFalse(expired.exists())
        self.assertTrue(retained.exists())
        with self._open_proactive_db() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM tracking_items").fetchone()[0], before_items)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM checkpoints").fetchone()[0], before_checkpoints)

    def test_snapshot_bundle_permissions_and_unified_source_numbering(self):
        now_utc = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)
        window_start = now_utc - timedelta(hours=1)
        self._commit_checkpoint(window_start)
        self._seed_state_db(
            [("s-feishu", "feishu", "飞书会话", None, None, unix_ts(window_start))],
            [
                self._message("s-feishu", "user", "我准备报名软考", window_start + timedelta(minutes=10)),
                self._message("s-feishu", "assistant", "可以先看公告", window_start + timedelta(minutes=11)),
            ],
        )
        with self._open_proactive_db() as conn:
            run_id = "sent-source-run"
            conn.execute(
                """
                INSERT INTO checkpoints (
                    run_id, snapshot_dir, window_start_utc, window_end_utc,
                    status, candidate_count, created_at, committed_at
                ) VALUES (?, ?, ?, ?, 'committed', 0, ?, ?)
                """,
                (run_id, "old", utc_iso(window_start), utc_iso(window_start), utc_iso(window_start), utc_iso(window_start)),
            )
            conn.execute(
                """
                INSERT INTO sent_log (
                    run_id, cron_job_id, message_id, content, delivery_status, created_at, delivered_at
                ) VALUES (?, ?, ?, ?, 'delivered', ?, ?)
                """,
                (run_id, "proactive_notifier", "m1", "上次提醒内容", utc_iso(now_utc - timedelta(days=1)), utc_iso(now_utc - timedelta(days=1))),
            )
            conn.commit()
        self._write_cron_jobs(
            [{"id": "5f4d9f41a2b7", "name": "DailyNote", "schedule_display": "every 47m", "next_run_at": "2026-11-09T14:00:00+08:00", "last_status": "ok"}]
        )

        result = self.snapshot.build_snapshot(
            hermes_home=self.home,
            now_utc=now_utc,
            force=True,
            config=self._snapshot_config(),
        )

        snapshot_dir = Path(result["snapshot_dir"])
        self.assertRegex(snapshot_dir.name, r"^\d{13}-[0-9a-f]{6}$")
        self.assertEqual(snapshot_dir.stat().st_mode & 0o777, 0o700)
        self.assertEqual(set(result["files"]), set(SNAPSHOT_FILES))
        for filename in SNAPSHOT_FILES:
            path = snapshot_dir / filename
            self.assertTrue(path.is_file())
            self.assertEqual(path.stat().st_mode & 0o777, 0o600, filename)

        numbers = []
        for filename in SNAPSHOT_FILES:
            text = (snapshot_dir / filename).read_text(encoding="utf-8")
            numbers.extend(int(value) for value in re.findall(r"\[source:(\d+)\]", text))
        self.assertGreater(len(numbers), 0)
        self.assertEqual(numbers, list(range(1, len(numbers) + 1)))
        self.assertEqual(sorted(result["source_map"]), list(range(1, len(numbers) + 1)))
        self.assertEqual(result["source_count"], len(numbers))
        self.assertIn(1, result["source_map"])
        self.assertIn("origin", result["source_map"][1])

    def test_conversation_extraction_filters_surfaces_and_noise(self):
        now_utc = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)
        window_start = now_utc - timedelta(hours=1)
        self._commit_checkpoint(window_start)
        sessions = [
            ("s-feishu", "feishu", "飞书会话", "飞书主会话", None, unix_ts(window_start)),
            ("s-desktop", "desktop", "桌面会话", None, None, unix_ts(window_start)),
            ("s-cli", "cli", "CLI 会话", None, None, unix_ts(window_start)),
            ("s-tui", "tui", "TUI 会话", None, None, unix_ts(window_start)),
            ("s-local", "local", "本地会话", None, None, unix_ts(window_start)),
            ("s-cron", "cron", "Cron", None, None, unix_ts(window_start)),
            ("s-subagent", "subagent", "Subagent", None, None, unix_ts(window_start)),
            ("s-subagent-child", "subagent", "Subagent 子会话", None, "s-desktop", unix_ts(window_start)),
            ("s-child", "desktop", "子会话", None, "s-desktop", unix_ts(window_start)),
        ]
        messages = [
            self._message("s-feishu", "user", "飞书新消息", window_start + timedelta(minutes=10)),
            self._message("s-feishu", "assistant", "飞书助手回复", window_start + timedelta(minutes=11)),
            self._message("s-desktop", "user", "[Cron proactive] 镜像消息", window_start + timedelta(minutes=12)),
            self._message("s-desktop", "user", "[Webhook delivery: abc] 镜像消息", window_start + timedelta(minutes=13)),
            self._message("s-desktop", "user", "[SILENT]", window_start + timedelta(minutes=14)),
            self._message("s-desktop", "assistant", " NO_REPLY ", window_start + timedelta(minutes=15)),
            self._message("s-desktop", "tool", "工具结果", window_start + timedelta(minutes=16)),
            self._message("s-desktop", "user", "", window_start + timedelta(minutes=17)),
            self._message("s-desktop", "user", "保留桌面消息", window_start + timedelta(minutes=18), reasoning="REASONING_MUST_NOT_APPEAR"),
            self._message("s-cli", "user", "CLI 新消息", window_start + timedelta(minutes=19)),
            self._message("s-tui", "user", "TUI 新消息", window_start + timedelta(minutes=20)),
            self._message("s-local", "user", "本地较早消息", window_start - timedelta(minutes=30)),
            self._message("s-cron", "user", "Cron 不应出现", window_start + timedelta(minutes=21)),
            self._message("s-subagent", "user", "Subagent 不应出现", window_start + timedelta(minutes=22)),
            self._message("s-subagent-child", "user", "Subagent 子会话不应出现", window_start + timedelta(minutes=22, seconds=30)),
            self._message("s-child", "user", "人类子会话消息", window_start + timedelta(minutes=23)),
        ]
        self._seed_state_db(sessions, messages)

        result = self.snapshot.build_snapshot(
            hermes_home=self.home,
            now_utc=now_utc,
            force=True,
            config=self._snapshot_config(),
        )
        conversation = (Path(result["snapshot_dir"]) / "conversation.md").read_text(encoding="utf-8")

        self.assertIn("# New", conversation)
        self.assertIn("# Earlier", conversation)
        for expected in (
            "飞书新消息",
            "飞书助手回复",
            "保留桌面消息",
            "CLI 新消息",
            "TUI 新消息",
            "本地较早消息",
            "人类子会话消息",
        ):
            self.assertIn(expected, conversation)
        for excluded in (
            "[Cron proactive]",
            "[Webhook delivery:",
            "[SILENT]",
            "NO_REPLY",
            "工具结果",
            "Cron 不应出现",
            "Subagent 不应出现",
            "Subagent 子会话不应出现",
            "REASONING_MUST_NOT_APPEAR",
        ):
            self.assertNotIn(excluded, conversation)
        new_section, earlier_section = conversation.split("# Earlier", 1)
        self.assertIn("本地较早消息", earlier_section)
        self.assertNotIn("本地较早消息", new_section)

    def test_conversation_groups_new_and_earlier_by_surface_and_session_title(self):
        now_utc = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)
        window_start = now_utc - timedelta(hours=1)
        self._commit_checkpoint(window_start)
        self._seed_state_db(
            [
                ("s-feishu", "feishu", "飞书会话", None, None, unix_ts(window_start)),
                ("s-desktop", "desktop", "桌面会话", None, None, unix_ts(window_start)),
                ("s-local", "local", "本地会话", None, None, unix_ts(window_start)),
            ],
            [
                self._message("s-feishu", "user", "飞书新消息", window_start + timedelta(minutes=5)),
                self._message("s-desktop", "user", "桌面新消息", window_start + timedelta(minutes=6)),
                self._message("s-feishu", "assistant", "飞书后续回复", window_start + timedelta(minutes=7)),
                self._message("s-local", "user", "本地较早上下文", window_start - timedelta(minutes=10)),
            ],
        )

        result = self.snapshot.build_snapshot(
            hermes_home=self.home,
            now_utc=now_utc,
            force=True,
            config=self._snapshot_config(),
        )
        conversation = (Path(result["snapshot_dir"]) / "conversation.md").read_text(encoding="utf-8")
        new_section, earlier_section = conversation.split("# Earlier ·", 1)

        self.assertIn("# New · feishu (飞书会话)", new_section)
        self.assertIn("# New · desktop (桌面会话)", new_section)
        self.assertIn("# Earlier · local (本地会话)", f"# Earlier ·{earlier_section}")
        self.assertLess(new_section.index("# New · feishu (飞书会话)"), new_section.index("飞书新消息"))
        self.assertLess(new_section.index("# New · desktop (桌面会话)"), new_section.index("桌面新消息"))
        self.assertLess(new_section.index("飞书新消息"), new_section.index("# New · desktop (桌面会话)"))
        self.assertLess(new_section.index("飞书后续回复"), new_section.index("# New · desktop (桌面会话)"))

    def test_conversation_code_folding_and_message_truncation(self):
        now_utc = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)
        window_start = now_utc - timedelta(hours=1)
        self._commit_checkpoint(window_start)
        code = "```python\n" + "\n".join(f"line_{index}" for index in range(15)) + "\n```"
        long_message = "H" * 800 + "MIDDLE" + "Z" * 800
        self._seed_state_db(
            [("s-feishu", "feishu", "飞书", None, None, unix_ts(window_start))],
            [
                self._message("s-feishu", "user", code, window_start + timedelta(minutes=5)),
                self._message("s-feishu", "user", long_message, window_start + timedelta(minutes=6)),
            ],
        )

        result = self.snapshot.build_snapshot(
            hermes_home=self.home,
            now_utc=now_utc,
            force=True,
            config=self._snapshot_config(),
        )
        conversation = (Path(result["snapshot_dir"]) / "conversation.md").read_text(encoding="utf-8")

        self.assertIn("[代码块已省略，共 15 行]", conversation)
        self.assertNotIn("line_14", conversation)
        self.assertIn("H" * 700, conversation)
        self.assertIn("Z" * 700, conversation)
        self.assertNotIn("H" * 701, conversation)
        self.assertNotIn("Z" * 701, conversation)
        self.assertIn("...", conversation)

    def test_earlier_section_uses_newest_first_selection_with_chronological_rendering(self):
        now_utc = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)
        window_start = now_utc - timedelta(hours=1)
        self._commit_checkpoint(window_start)
        messages = []
        for index in range(25):
            stamp = window_start - timedelta(minutes=50 - index)
            messages.append(
                self._message("s-local", "user", f"M{index:02d}:" + "x" * 990, stamp)
            )
        messages.append(
            self._message("s-local", "user", "触发快照的新消息", window_start + timedelta(minutes=1))
        )
        self._seed_state_db(
            [("s-local", "local", "本地", None, None, unix_ts(window_start - timedelta(hours=2)))],
            messages,
        )

        result = self.snapshot.build_snapshot(
            hermes_home=self.home,
            now_utc=now_utc,
            force=True,
            config={"memory_source": "builtin", "max_earlier_chars": 20000},
        )
        conversation = (Path(result["snapshot_dir"]) / "conversation.md").read_text(encoding="utf-8")
        earlier = conversation.split("# Earlier", 1)[1]

        self.assertIn("M24:", earlier)
        self.assertNotIn("M00:", earlier)
        included = re.findall(r"M(\d{2}):", earlier)
        self.assertGreater(len(included), 0)
        self.assertLess(len(included), 25)
        self.assertEqual(included, sorted(included))
        self.assertEqual(included[-1], "24")
        self.assertLessEqual(len(earlier), 20000 + 200)

    def test_tracking_rendering_uses_checkpoint_changed_flag_and_active_only(self):
        now_utc = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)
        window_start = now_utc - timedelta(hours=1)
        self._commit_checkpoint(window_start)
        self._seed_state_db(
            [("s-cli", "cli", "CLI", None, None, unix_ts(window_start))],
            [self._message("s-cli", "user", "让快照生成", window_start + timedelta(minutes=1))],
        )
        with self._open_proactive_db() as conn:
            conn.execute(
                """
                INSERT INTO tracking_items (
                    goal_id, title, description, summary, status, momentum_status,
                    source_ref, created_at, updated_at, closed_at
                ) VALUES (?, ?, ?, ?, 'active', 'waiting', NULL, ?, ?, NULL)
                """,
                (
                    "changed-goal",
                    "已变化目标",
                    "描述 A",
                    "摘要 A",
                    utc_iso(window_start - timedelta(days=1)),
                    utc_iso(window_start + timedelta(minutes=5)),
                ),
            )
            conn.execute(
                """
                INSERT INTO tracking_items (
                    goal_id, title, description, summary, status, momentum_status,
                    source_ref, created_at, updated_at, closed_at
                ) VALUES (?, ?, ?, ?, 'active', 'on_track', NULL, ?, ?, NULL)
                """,
                (
                    "unchanged-goal",
                    "未变化目标",
                    "描述 B",
                    "摘要 B",
                    utc_iso(window_start - timedelta(days=1)),
                    utc_iso(window_start - timedelta(minutes=5)),
                ),
            )
            conn.execute(
                """
                INSERT INTO tracking_items (
                    goal_id, title, description, summary, status, momentum_status,
                    source_ref, created_at, updated_at, closed_at
                ) VALUES (?, ?, ?, ?, 'closed', 'on_track', NULL, ?, ?, ?)
                """,
                (
                    "closed-goal",
                    "已关闭目标",
                    "描述 C",
                    "摘要 C",
                    utc_iso(window_start - timedelta(days=1)),
                    utc_iso(window_start + timedelta(minutes=6)),
                    utc_iso(window_start + timedelta(minutes=6)),
                ),
            )
            conn.commit()

        result = self.snapshot.build_snapshot(
            hermes_home=self.home,
            now_utc=now_utc,
            force=True,
            config=self._snapshot_config(),
        )
        tracking = (Path(result["snapshot_dir"]) / "tracking.md").read_text(encoding="utf-8")

        self.assertIn("Goal id: changed-goal", tracking)
        self.assertIn("Changed since checkpoint: true", tracking)
        self.assertIn("Goal id: unchanged-goal", tracking)
        self.assertRegex(
            tracking,
            r"Goal id: unchanged-goal(?:.|\n)*?Changed since checkpoint: false",
        )
        self.assertNotIn("closed-goal", tracking)
        for label in ("Description", "Momentum status", "Status", "Summary", "Updated at"):
            self.assertIn(label, tracking)

    def test_sent_ledger_lists_only_recent_delivered_messages_with_warning(self):
        now_utc = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)
        window_start = now_utc - timedelta(hours=1)
        self._commit_checkpoint(window_start)
        self._seed_state_db(
            [("s-cli", "cli", "CLI", None, None, unix_ts(window_start))],
            [self._message("s-cli", "user", "触发快照", window_start + timedelta(minutes=1))],
        )
        with self._open_proactive_db() as conn:
            for run_id, age_days, status, content in (
                ("recent-delivered", 1, "delivered", "七天内的主动提醒"),
                ("old-delivered", 8, "delivered", "过期提醒不应出现"),
                ("recent-pending", 1, "pending", "未投递提醒不应出现"),
            ):
                conn.execute(
                    """
                    INSERT INTO checkpoints (
                        run_id, snapshot_dir, window_start_utc, window_end_utc,
                        status, candidate_count, created_at, committed_at
                    ) VALUES (?, ?, ?, ?, 'committed', 0, ?, ?)
                    """,
                    (
                        run_id,
                        "old",
                        utc_iso(now_utc - timedelta(days=age_days + 1)),
                        utc_iso(now_utc - timedelta(days=age_days)),
                        utc_iso(now_utc - timedelta(days=age_days)),
                        utc_iso(now_utc - timedelta(days=age_days)),
                    ),
                )
                stamp = utc_iso(now_utc - timedelta(days=age_days))
                conn.execute(
                    """
                    INSERT INTO sent_log (
                        run_id, cron_job_id, message_id, content, delivery_status, created_at, delivered_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (run_id, "proactive_notifier", f"msg-{run_id}", content, status, stamp, stamp if status == "delivered" else None),
                )
            conn.commit()

        result = self.snapshot.build_snapshot(
            hermes_home=self.home,
            now_utc=now_utc,
            force=True,
            config=self._snapshot_config(),
        )
        sent = (Path(result["snapshot_dir"]) / "sent.md").read_text(encoding="utf-8")

        self.assertIn("Ordinary chat replies are not listed", sent)
        self.assertIn("七天内的主动提醒", sent)
        self.assertNotIn("过期提醒不应出现", sent)
        self.assertNotIn("未投递提醒不应出现", sent)

    def test_scheduled_crons_render_both_timezones_and_runtime_managed_markers(self):
        now_utc = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)
        window_start = now_utc - timedelta(hours=1)
        self._commit_checkpoint(window_start)
        self._seed_state_db(
            [("s-cli", "cli", "CLI", None, None, unix_ts(window_start))],
            [self._message("s-cli", "user", "触发快照", window_start + timedelta(minutes=1))],
        )
        self._write_cron_jobs(
            [
                {
                    "id": "5f4d9f41a2b7",
                    "name": "DailyNote",
                    "schedule_display": "every 47m",
                    "next_run_at": "2026-11-09T14:00:00+08:00",
                    "last_status": "ok",
                },
                {
                    "id": "proactive_notifier",
                    "name": "proactive_notifier",
                    "schedule_display": "30 9,12,15,18,21 * * *",
                    "next_run_at": "2026-11-09T15:30:00+08:00",
                    "last_status": "failed",
                },
            ]
        )

        result = self.snapshot.build_snapshot(
            hermes_home=self.home,
            now_utc=now_utc,
            force=True,
            config=self._snapshot_config(),
        )
        snapshot_dir = Path(result["snapshot_dir"])
        scheduled = (snapshot_dir / "scheduled-crons.md").read_text(encoding="utf-8")
        context = (snapshot_dir / "context.md").read_text(encoding="utf-8")

        self.assertIn("5f4d9f41a2b7", scheduled)
        self.assertIn("proactive_notifier", scheduled)
        self.assertIn("2026-11-09T06:00:00Z", scheduled)
        self.assertIn("2026-11-09 14:00:00", scheduled)
        self.assertIn("Asia/Shanghai", scheduled)
        self.assertEqual(
            scheduled.count("Managed by the runtime; cron tools cannot change it."),
            1,
        )
        self.assertIn("2026-11-09T12:00:00Z", context)
        self.assertIn("2026-11-09 20:00:00", context)
        self.assertIn("proactive_notifier", context)
        self.assertIn("failed", context)
        self.assertIn("09:00-21:30", context)
        self.assertIn("Asia/Shanghai", context)
        self.assertIn("Tool capabilities", context)
        self.assertIn("MCP servers", context)
        self.assertIn("Memory source", context)

    def test_memory_keyword_matches_builtin_entries_and_marks_unavailable_source(self):
        now_utc = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)
        window_start = now_utc - timedelta(hours=1)
        self._commit_checkpoint(window_start)
        self._seed_state_db(
            [("s-feishu", "feishu", "飞书", None, None, unix_ts(window_start))],
            [self._message("s-feishu", "user", "软考报名材料", window_start + timedelta(minutes=5))],
        )
        (self.home / "memories").mkdir(parents=True, exist_ok=True)
        (self.home / "memories" / "USER.md").write_text(
            "用户正在关注广东软考报名材料批次公告", encoding="utf-8"
        )

        result = self.snapshot.build_snapshot(
            hermes_home=self.home,
            now_utc=now_utc,
            force=True,
            config={"memory_source": "builtin"},
        )

        memory = (Path(result["snapshot_dir"]) / "memory.md").read_text(encoding="utf-8")
        self.assertIn("<!-- proactive-memory source=builtin status=ok -->", memory)
        self.assertIn("用户正在关注广东软考报名材料批次公告", memory)
        self.assertIn("target=user", memory)
        self.assertIn("[source:", memory)

        fallback = self.snapshot.build_snapshot(
            hermes_home=self.home,
            now_utc=now_utc + timedelta(seconds=2),
            force=True,
            config={"memory_source": "missing-provider"},
        )

        fallback_memory = (Path(fallback["snapshot_dir"]) / "memory.md").read_text(encoding="utf-8")
        self.assertIn("source=missing-provider status=unavailable", fallback_memory)
        self.assertIn("provider_not_installed", fallback_memory)
        self.assertNotIn("用户正在关注广东软考报名材料批次公告", fallback_memory)

    def test_no_op_short_circuit_skips_snapshot_and_llm(self):
        now_utc = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)
        window_start = now_utc - timedelta(hours=1)
        self._commit_checkpoint(window_start)
        with self._open_proactive_db() as conn:
            conn.execute(
                "UPDATE tracking_items SET momentum_status = 'on_track', updated_at = ?",
                (utc_iso(window_start - timedelta(minutes=1)),),
            )
            conn.commit()
        self._write_cron_jobs(
            [{"id": "5de04fc729fe", "name": "U2", "schedule_display": "0 10 * * *", "last_status": "ok"}]
        )
        llm = NoLlmClient()

        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            result = self.snapshot.run_tick(
                hermes_home=self.home,
                now_utc=now_utc,
                llm_client=llm,
                snapshot_only=True,
                config=self._snapshot_config(),
            )

        self.assertEqual(output.getvalue().splitlines()[-1], '{"wakeAgent": false}')
        self.assertFalse(result["wake_agent"])
        self.assertIsNone(result["snapshot_dir"])
        self.assertEqual(self.snapshot_dirs(), [])
        self.assertEqual(llm.calls, [])

    def test_cli_and_slash_tick_snapshot_only_both_generate_and_report(self):
        now_utc = datetime.now(timezone.utc)
        window_start = now_utc - timedelta(hours=1)
        self._commit_checkpoint(window_start)
        self._seed_state_db(
            [("s-cli", "cli", "CLI", None, None, unix_ts(window_start))],
            [self._message("s-cli", "user", "命令行触发快照", now_utc - timedelta(minutes=5))],
        )
        self._write_cron_jobs([])
        (self.home / "config.yaml").write_text(
            "plugins:\n  entries:\n    proactive:\n      settings:\n"
            "        memory_source: builtin\n",
            encoding="utf-8",
        )

        parser = argparse.ArgumentParser()
        self.cli.setup_cli(parser)
        output = io.StringIO()
        with mock.patch.dict(os.environ, {"HERMES_HOME": str(self.home)}):
            with contextlib.redirect_stdout(output):
                code = self.cli.handle_cli(parser.parse_args(["tick", "--snapshot-only", "--force"]))
        self.assertEqual(code, 0)
        cli_output = output.getvalue()
        self.assertIn("Snapshot:", cli_output)
        self.assertIn("Sources:", cli_output)
        self.assertTrue(any(self.hidden_files.iterdir()))

        slash = self.cli.handle_slash("tick --snapshot-only --force")
        self.assertIn("Snapshot:", slash)
        self.assertIn("Sources:", slash)


if __name__ == "__main__":
    unittest.main()

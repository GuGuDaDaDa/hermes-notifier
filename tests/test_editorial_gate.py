"""Behavioral tests for the Stage 3 editorial gate tools and bundled skill."""

from __future__ import annotations

import contextlib
import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from hermes_constants import reset_hermes_home_override, set_hermes_home_override
from tests.test_goal_tracking import SEED_GOAL_ID, load_plugin_package, seed_tracking_fixture


REPO_ROOT = Path(__file__).resolve().parents[1]
PLUGIN_DIR = REPO_ROOT / "plugins" / "proactive"
SKILL_PATH = PLUGIN_DIR / "skills" / "editorial-gate" / "SKILL.md"


def utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def unix_ts(value: datetime) -> float:
    return value.astimezone(timezone.utc).timestamp()


def parse_utc(value: str) -> datetime:
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


class RecordingContext:
    plugin_config = {}

    def __init__(self):
        self.tools = []
        self.cli_commands = []
        self.commands = []
        self.skills = []

    def register_tool(self, **kwargs):
        self.tools.append(kwargs)

    def register_cli_command(self, **kwargs):
        self.cli_commands.append(kwargs)

    def register_command(self, name, handler, description="", args_hint=""):
        self.commands.append(
            {
                "name": name,
                "handler": handler,
                "description": description,
                "args_hint": args_hint,
            }
        )

    def register_skill(self, name, path, description=""):
        self.skills.append({"name": name, "path": Path(path), "description": description})


class EditorialGateTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.plugin = load_plugin_package()
        cls.storage = cls.plugin.storage
        cls.snapshot = cls.plugin.snapshot
        cls.tools = cls.plugin.tools

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="hermes-proactive-editorial-test-")
        self.home = Path(self._tmp.name)
        self._old_home = os.environ.get("HERMES_HOME")
        os.environ["HERMES_HOME"] = str(self.home)
        self._override_token = set_hermes_home_override(str(self.home))
        self.addCleanup(self._cleanup_home)
        self.now = datetime(2026, 11, 9, 4, 0, tzinfo=timezone.utc)
        self.window_start = self.now - timedelta(hours=1)
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

    def _connect(self):
        return contextlib.closing(sqlite3.connect(self.db_path))

    def _candidate(self, title: str, *, draft: str | None = None) -> dict:
        return {
            "title": title,
            "priority": "regular",
            "relevant_until": utc_iso(self.now + timedelta(days=1)),
            "why_it_matters": f"{title} 需要跟进。",
            "evidence_refs": ["[source:1]"],
            "proposed_action": "确认当前状态。",
            "what_remains": "等待用户确认。",
            "draft": draft,
        }

    def _create_pending_edition(
        self,
        *,
        run_id: str = "run-editorial-test",
        candidate_count: int = 2,
        window_end: datetime | None = None,
    ) -> dict:
        window_end = window_end or self.now
        snapshot_dir = self.plugin_data / "hidden_files" / run_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.snapshot._write_private_file(
            snapshot_dir / "opportunities.md",
            "# Opportunities\n\n- 跟进软考批次公告。\n",
        )
        self.snapshot._write_private_file(
            snapshot_dir / "prepared.md",
            "# Prepared\n\n- 准备提醒材料。\n",
        )
        self.snapshot._write_private_file(
            snapshot_dir / "sources.md",
            "# Sources\n\n[source:1] 用户提到等待批次公告。\n",
        )
        checkpoint = self.storage.create_checkpoint(
            run_id,
            str(snapshot_dir),
            utc_iso(self.window_start),
            utc_iso(window_end),
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
                self._candidate(f"事项 {index}", draft=draft),
            )
        return checkpoint

    def _write_preferences(self, content: str) -> Path:
        path = self.home / "PROACTIVE_PREFERENCES.md"
        path.write_text(content, encoding="utf-8")
        return path

    def _seed_state_db(self) -> None:
        with contextlib.closing(sqlite3.connect(self.home / "state.db")) as conn:
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
                    source_label TEXT,
                    active INTEGER NOT NULL DEFAULT 1
                );
                """
            )
            conn.executemany(
                """
                INSERT INTO sessions (
                    id, source, title, display_name, parent_session_id, started_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    ("s-human", "feishu", "飞书主会话", None, None, unix_ts(self.window_start)),
                    ("s-cron", "cron", "定时任务", None, None, unix_ts(self.window_start)),
                    ("s-subagent", "subagent", "子代理", None, None, unix_ts(self.window_start)),
                ],
            )
            rows = [
                ("s-human", "user", "快照之前的老消息", self.now - timedelta(minutes=5), None, 1),
                ("s-human", "user", "我刚已经交了材料", self.now + timedelta(minutes=2), None, 1),
                ("s-human", "assistant", "收到，我不再提醒这件事。", self.now + timedelta(minutes=3), None, 1),
                ("s-human", "tool", "tool output should not leak", self.now + timedelta(minutes=4), None, 1),
                ("s-human", "assistant", "[SILENT]", self.now + timedelta(minutes=5), None, 1),
                ("s-human", "assistant", "[Cron abc123] 镜像消息", self.now + timedelta(minutes=6), None, 1),
                ("s-human", "user", "cron-labeled noise", self.now + timedelta(minutes=7), "cron", 1),
                ("s-cron", "user", "cron session noise", self.now + timedelta(minutes=8), None, 1),
                ("s-subagent", "assistant", "subagent noise", self.now + timedelta(minutes=9), None, 1),
            ]
            conn.executemany(
                """
                INSERT INTO messages (
                    session_id, role, content, timestamp, source_label, active
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                [(session, role, content, unix_ts(ts), label, active) for session, role, content, ts, label, active in rows],
            )
            conn.commit()

    def _candidate_decisions(self, run_id: str) -> list[tuple]:
        with self._connect() as conn:
            return conn.execute(
                "SELECT item_num, decision, decision_reason, decided_at FROM candidate_items "
                "WHERE run_id = ? ORDER BY item_num",
                (run_id,),
            ).fetchall()

    def _sent_rows(self) -> list[tuple]:
        with self._connect() as conn:
            return conn.execute(
                "SELECT run_id, content, delivery_status FROM sent_log ORDER BY id"
            ).fetchall()

    def test_proactivity_read_returns_pending_edition_preferences_and_live_diff(self):
        checkpoint = self._create_pending_edition()
        self._write_preferences("# Proactive Preferences\n\n- 保持克制。\n")
        self._seed_state_db()

        payload = json.loads(self.tools.handle_proactivity_read())
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["run_id"], checkpoint["run_id"])
        self.assertIn("跟进软考批次公告", payload["opportunities"])
        self.assertIn("准备提醒材料", payload["prepared"])
        self.assertIn("source:1", payload["sources"])
        self.assertIn("保持克制", payload["preferences"])
        self.assertEqual([item["item_num"] for item in payload["candidate_items"]], [1, 2])
        self.assertTrue(all(item["decision"] is None for item in payload["candidate_items"]))

        diff_contents = [item["content"] for item in payload["live_conversation_diff"]]
        self.assertIn("我刚已经交了材料", diff_contents)
        self.assertIn("收到，我不再提醒这件事。", diff_contents)
        for noise in (
            "快照之前的老消息",
            "tool output should not leak",
            "[SILENT]",
            "[Cron abc123] 镜像消息",
            "cron-labeled noise",
            "cron session noise",
            "subagent noise",
        ):
            self.assertNotIn(noise, diff_contents)

        explicit = json.loads(self.tools.handle_proactivity_read({"run_id": checkpoint["run_id"]}))
        self.assertTrue(explicit["ok"])
        self.assertEqual(explicit["run_id"], checkpoint["run_id"])

    def test_proactivity_read_reports_missing_checkpoint_as_json_error(self):
        payload = json.loads(self.tools.handle_proactivity_read({"run_id": "missing-run"}))
        self.assertFalse(payload["ok"])
        self.assertIn("missing-run", payload["error"])

    def test_proactivity_select_rejects_invalid_or_incomplete_decisions_without_mutation(self):
        checkpoint = self._create_pending_edition()
        run_id = checkpoint["run_id"]
        valid = [
            {"item_num": 1, "decision": "skip", "reason": "moot"},
            {"item_num": 2, "decision": "skip", "reason": "not_useful_now"},
        ]
        cases = [
            ([valid[0]], None, "missing"),
            ([valid[0], {"item_num": 99, "decision": "skip", "reason": "moot"}], None, "unknown"),
            ([valid[0], valid[0], valid[1]], None, "duplicate"),
            ([valid[0], {"item_num": 2, "decision": "skip", "reason": "because"}], None, "reason"),
            ([{"item_num": 1, "decision": "send", "reason": "changed"}, valid[1]], None, "final_message"),
            ([{"item_num": 1, "decision": "send", "reason": "changed"}, valid[1]], "   ", "final_message"),
            ([{"item_num": 1, "decision": "maybe", "reason": "changed"}, valid[1]], "hello", "decision"),
        ]

        for decisions, final_message, expected_error in cases:
            with self.subTest(expected_error=expected_error):
                args = {"run_id": run_id, "decisions": decisions}
                if final_message is not None:
                    args["final_message"] = final_message
                payload = json.loads(self.tools.handle_proactivity_select(args))
                self.assertFalse(payload["ok"])
                self.assertIn(expected_error, payload["error"].lower())

                current = self.storage.get_checkpoint(run_id)
                self.assertEqual(current["status"], "pending")
                self.assertIsNone(current["committed_at"])
                self.assertEqual([row[1] for row in self._candidate_decisions(run_id)], [None, None])
                self.assertEqual(self._sent_rows(), [])

    def test_skip_all_commits_checkpoint_without_sent_log_and_returns_silent(self):
        checkpoint = self._create_pending_edition()
        run_id = checkpoint["run_id"]

        payload = json.loads(
            self.tools.handle_proactivity_select(
                {
                    "run_id": run_id,
                    "decisions": [
                        {"item_num": 1, "decision": "skip", "reason": "already_handled"},
                        {"item_num": 2, "decision": "skip", "reason": "preferences", "note": "已有 Cron"},
                    ],
                }
            )
        )
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["action"], "silent")
        self.assertEqual(payload["reply_token"], "[SILENT]")

        committed = self.storage.get_checkpoint(run_id)
        self.assertEqual(committed["status"], "committed")
        self.assertIsNotNone(committed["committed_at"])
        self.assertEqual(self.storage.latest_committed_watermark(), committed["window_end_utc"])
        self.assertEqual(self._sent_rows(), [])
        decisions = self._candidate_decisions(run_id)
        self.assertEqual([row[1] for row in decisions], ["skip", "skip"])
        self.assertEqual([row[2] for row in decisions], ["already_handled", "preferences"])
        self.assertTrue(all(row[3] for row in decisions))

    def test_send_commits_checkpoint_and_queues_pending_delivery(self):
        checkpoint = self._create_pending_edition()
        run_id = checkpoint["run_id"]
        final_message = "软考批次公告还没出，我下午再帮你确认一次。"

        payload = json.loads(
            self.tools.handle_proactivity_select(
                {
                    "run_id": run_id,
                    "decisions": [
                        {"item_num": 1, "decision": "send", "reason": "time_sensitive"},
                        {"item_num": 2, "decision": "skip", "reason": "unconfirmed"},
                    ],
                    "final_message": final_message,
                }
            )
        )
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["action"], "deliver")
        self.assertEqual(payload["final_message"], final_message)

        committed = self.storage.get_checkpoint(run_id)
        self.assertEqual(committed["status"], "committed")
        self.assertIsNotNone(committed["committed_at"])
        sent = self._sent_rows()
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0][0], run_id)
        self.assertEqual(sent[0][1], final_message)
        self.assertEqual(sent[0][2], "pending")

    def test_close_goal_ids_closes_goals_with_closure_and_advanced_timestamps(self):
        checkpoint = self._create_pending_edition(candidate_count=1)
        before = self.storage.get_tracking_item(SEED_GOAL_ID)
        self.storage.upsert_tracking_item(
            "resolved-goal",
            title="已办结事项",
            description="用户已经确认完成。",
            summary="等待关闭。",
        )

        payload = json.loads(
            self.tools.handle_proactivity_select(
                {
                    "run_id": checkpoint["run_id"],
                    "decisions": [
                        {"item_num": 1, "decision": "skip", "reason": "already_handled"}
                    ],
                    "close_goal_ids": [SEED_GOAL_ID, "resolved-goal"],
                }
            )
        )
        self.assertTrue(payload["ok"])

        for goal_id in (SEED_GOAL_ID, "resolved-goal"):
            item = self.storage.get_tracking_item(goal_id)
            self.assertEqual(item["status"], "closed")
            self.assertIsNotNone(item["closed_at"])
            self.assertTrue(item["closed_at"].endswith("Z"))
        seed_after = self.storage.get_tracking_item(SEED_GOAL_ID)
        self.assertGreater(parse_utc(seed_after["updated_at"]), parse_utc(before["updated_at"]))
        self.assertGreaterEqual(parse_utc(seed_after["closed_at"]), parse_utc(seed_after["updated_at"]))

    def test_unknown_close_goal_rolls_back_decisions_and_commit(self):
        checkpoint = self._create_pending_edition(candidate_count=1)
        payload = json.loads(
            self.tools.handle_proactivity_select(
                {
                    "run_id": checkpoint["run_id"],
                    "decisions": [
                        {"item_num": 1, "decision": "skip", "reason": "moot"}
                    ],
                    "close_goal_ids": ["does-not-exist"],
                }
            )
        )
        self.assertFalse(payload["ok"])
        self.assertEqual(self.storage.get_checkpoint(checkpoint["run_id"])["status"], "pending")
        self.assertEqual([row[1] for row in self._candidate_decisions(checkpoint["run_id"])], [None])

    def test_new_run_supersedes_stale_pending_and_rescans_from_committed_watermark(self):
        committed_end = self.now - timedelta(minutes=30)
        committed_run = "run-committed"
        self.storage.create_checkpoint(
            committed_run,
            str(self.plugin_data / "hidden_files" / committed_run),
            utc_iso(committed_end - timedelta(hours=1)),
            utc_iso(committed_end),
            status="committed",
            candidate_count=0,
            committed_at=utc_iso(committed_end),
        )
        stale = self._create_pending_edition(
            run_id="run-stale-pending",
            candidate_count=1,
            window_end=self.now - timedelta(minutes=10),
        )
        self._seed_state_db()

        later = self.now + timedelta(minutes=20)
        result = self.snapshot.build_snapshot(
            hermes_home=self.home,
            now_utc=later,
            force=True,
            config={"memory_source": "builtin"},
        )

        self.assertTrue(result.wake_agent)
        self.assertEqual(parse_utc(result.window_start_utc), committed_end)
        self.assertEqual(self.storage.get_checkpoint(stale["run_id"])["status"], "superseded")
        conversation = Path(result.snapshot_dir, "conversation.md").read_text(encoding="utf-8")
        self.assertIn("我刚已经交了材料", conversation)

    def test_register_exposes_stage3_tools_and_bundled_editorial_skill(self):
        ctx = RecordingContext()
        self.plugin.register(ctx)

        self.assertEqual(
            [tool["name"] for tool in ctx.tools],
            ["proactivity_read", "proactivity_select", "tracking_manage"],
        )
        self.assertTrue(all(tool["toolset"] == "proactivity" for tool in ctx.tools))
        self.assertIs(ctx.tools[0]["handler"], self.tools.handle_proactivity_read)
        self.assertIs(ctx.tools[1]["handler"], self.tools.handle_proactivity_select)
        self.assertEqual(ctx.tools[0]["schema"]["name"], "proactivity_read")
        self.assertEqual(ctx.tools[1]["schema"]["name"], "proactivity_select")

        self.assertEqual(len(ctx.skills), 1)
        skill = ctx.skills[0]
        self.assertEqual(skill["name"], "editorial-gate")
        self.assertTrue(skill["description"])
        self.assertEqual(skill["path"], SKILL_PATH)
        self.assertTrue(SKILL_PATH.is_file())

        text = SKILL_PATH.read_text(encoding="utf-8")
        self.assertTrue(text.startswith("---\nname: editorial-gate\n"))
        for required in (
            "proactivity_read",
            "proactivity_select",
            "live_conversation_diff",
            "preferences text",
            "final_message",
            "close_goal_ids",
            "[SILENT]",
            "profile persona",
            "time_sensitive",
            "already_handled",
        ):
            self.assertIn(required, text)


if __name__ == "__main__":
    unittest.main()

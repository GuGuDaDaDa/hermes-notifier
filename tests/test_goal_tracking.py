"""Behavioral tests for the proactive plugin's durable goal-tracking slice."""

from __future__ import annotations

import argparse
import contextlib
import importlib.util
import io
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

from hermes_constants import reset_hermes_home_override, set_hermes_home_override


REPO_ROOT = Path(__file__).resolve().parents[1]
PLUGIN_DIR = REPO_ROOT / "plugins" / "proactive"
PLUGIN_MODULE = "proactive_plugin_under_test"
SEED_GOAL_ID = "ruankao-2026h2-batch"
FIXTURE_GOAL = {
    "title": "广东 2026 下半年软考中级软件设计师批次安排公告",
    "description": (
        "监控广东省 2026 年下半年计算机技术与软件专业技术资格（水平）考试"
        "（中级：软件设计师 / Guangdong 2026 H2 Software Designer intermediate exam）"
        "官方批次安排、准考证打印及考试时间通知公告。"
    ),
    "summary": "等待官方发布 2026 下半年广东软考软件设计师批次安排公告。",
}


def seed_tracking_fixture(storage) -> None:
    """Insert the test-only tracked goal now that plugin init no longer seeds one."""
    storage.upsert_tracking_item(SEED_GOAL_ID, **FIXTURE_GOAL)


def load_plugin_package():
    """Load the repository's plugin package without relying on the host's plugins namespace."""
    if PLUGIN_MODULE in sys.modules:
        return sys.modules[PLUGIN_MODULE]
    init_file = PLUGIN_DIR / "__init__.py"
    spec = importlib.util.spec_from_file_location(
        PLUGIN_MODULE,
        init_file,
        submodule_search_locations=[str(PLUGIN_DIR)],
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load plugin package from {PLUGIN_DIR}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[PLUGIN_MODULE] = module
    spec.loader.exec_module(module)
    return module


def parse_utc(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


class RecordingContext:
    """Minimal registration surface used by ``register(ctx)``."""

    plugin_config = {}

    def __init__(self, llm=None):
        self.llm = llm
        self.tools = []
        self.cli_commands = []
        self.commands = []
        self.skills = []

    def register_tool(self, **kwargs):
        self.tools.append(kwargs)

    def register_cli_command(self, **kwargs):
        self.cli_commands.append(kwargs)

    def register_command(self, name, handler, description="", args_hint=""):
        self.commands.append({"name": name, "handler": handler, "description": description, "args_hint": args_hint})

    def register_skill(self, name, path, description=""):
        self.skills.append({"name": name, "path": path, "description": description})


class GoalTrackingTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.plugin = load_plugin_package()
        cls.storage = cls.plugin.storage
        cls.tools = cls.plugin.tools
        cls.cli = cls.plugin.cli

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="hermes-proactive-test-")
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
    def db_path(self) -> Path:
        return self.home / "plugin-data" / "proactive" / "data.db"

    def _write_host_config(self, values: dict) -> None:
        (self.home / "config.yaml").write_text(json.dumps(values), encoding="utf-8")

    def _connect(self):
        return contextlib.closing(sqlite3.connect(self.db_path))

    def test_manifest_declares_plugin_contract(self):
        import hermes_yaml as yaml

        manifest = yaml.safe_load((PLUGIN_DIR / "plugin.yaml").read_text(encoding="utf-8"))
        self.assertEqual(manifest["name"], "proactive")
        self.assertEqual(manifest["version"], "0.1.0")
        self.assertEqual(
            manifest["description"],
            "Muse-style three-stage proactive notification pipeline for Hermes.",
        )
        self.assertEqual(manifest["author"], "Aier")
        self.assertEqual(manifest["license"], "MIT")
        self.assertEqual(manifest["manifest_version"], 2)
        self.assertEqual(manifest["api_version"], 1)
        self.assertEqual(manifest["kind"], "standalone")
        self.assertEqual(
            manifest["provides_tools"],
            ["proactivity_read", "proactivity_select", "tracking_manage"],
        )
        self.assertEqual(manifest["provides_hooks"], [])

        schema = manifest["config_schema"]
        self.assertEqual(schema["active_window"]["type"], "string")
        self.assertEqual(schema["active_window"]["default"], "09:00-21:30")
        self.assertEqual(schema["timezone"]["default"], "auto")
        self.assertEqual(schema["preferences_file"]["default"], "PROACTIVE_PREFERENCES.md")
        self.assertEqual(schema["delivery_platform"]["default"], "auto")
        self.assertEqual(schema["cron_schedule"]["default"], "30 9,12,15,18,21 * * *")
        self.assertEqual(schema["retention_days"]["type"], "integer")
        self.assertEqual(schema["retention_days"]["default"], 14)
        self.assertEqual(schema["max_message_chars"]["default"], 1500)
        self.assertEqual(schema["max_earlier_chars"]["default"], 20000)
        self.assertEqual(schema["memory_source"]["type"], "string")
        self.assertEqual(schema["memory_source"]["default"], "auto")
        self.assertEqual(schema["memory_limit"]["type"], "integer")
        self.assertEqual(schema["memory_limit"]["default"], 5)
        self.assertEqual(schema["memory_timeout_seconds"]["default"], 3)
        self.assertEqual(schema["memory_tool"]["default"], "")

    def test_register_initializes_wal_database_and_registers_all_surfaces(self):
        ctx = RecordingContext()
        self.plugin.register(ctx)

        self.assertTrue(self.db_path.is_file())
        with self._connect() as conn:
            journal_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
            tables = {
                row[0]
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
            }
        self.assertEqual(journal_mode.lower(), "wal")
        self.assertTrue({"checkpoints", "tracking_items", "candidate_items", "sent_log"} <= tables)
        self.assertEqual(self.storage.list_tracking_items(), [])

        self.assertEqual(
            [tool["name"] for tool in ctx.tools],
            ["proactivity_read", "proactivity_select", "tracking_manage"],
        )
        self.assertTrue(all(tool["toolset"] == "proactivity" for tool in ctx.tools))
        self.assertIs(ctx.tools[0]["handler"], self.tools.handle_proactivity_read)
        self.assertIs(ctx.tools[1]["handler"], self.tools.handle_proactivity_select)
        self.assertIs(ctx.tools[2]["handler"], self.tools.handle_tracking_manage)
        self.assertEqual(ctx.tools[0]["schema"]["name"], "proactivity_read")
        self.assertEqual(ctx.tools[1]["schema"]["name"], "proactivity_select")
        self.assertEqual(ctx.tools[2]["schema"]["name"], "tracking_manage")

        self.assertEqual([entry["name"] for entry in ctx.cli_commands], ["proactive"])
        self.assertIs(ctx.cli_commands[0]["setup_fn"], self.cli.setup_cli)
        self.assertIs(ctx.cli_commands[0]["handler_fn"], self.cli.handle_cli)
        self.assertEqual([entry["name"] for entry in ctx.commands], ["proactive"])
        self.assertIs(ctx.commands[0]["handler"], self.cli.handle_slash)

    def test_register_forwards_host_llm_and_explicit_clients_win(self):
        host_llm = object()
        explicit_llm = object()
        ctx = RecordingContext(llm=host_llm)
        self.addCleanup(self.plugin.workers.set_host_llm, None)

        self.plugin.register(ctx)

        self.assertIs(self.plugin.workers._resolve_llm(None), host_llm)
        self.assertIs(self.plugin.workers._resolve_llm(explicit_llm), explicit_llm)

        self.plugin.workers.set_host_llm(None)
        with mock.patch("agent.plugin_llm.PluginLlm") as plugin_llm:
            self.plugin.workers._resolve_llm(None)
        plugin_llm.assert_called_once_with(plugin_id="proactive")

    def test_schema_has_exact_columns_and_check_constraints(self):
        self.storage.init_db()
        seed_tracking_fixture(self.storage)
        expected_columns = {
            "checkpoints": [
                "run_id", "snapshot_dir", "window_start_utc", "window_end_utc", "status",
                "candidate_count", "created_at", "committed_at",
            ],
            "tracking_items": [
                "goal_id", "title", "description", "summary", "status", "momentum_status",
                "source_ref", "created_at", "updated_at", "closed_at",
            ],
            "candidate_items": [
                "id", "run_id", "item_num", "worker_kind", "title", "priority", "relevant_until",
                "why_it_matters", "evidence_refs", "proposed_action", "what_remains", "draft",
                "decision", "decision_reason", "decision_note", "decided_at",
            ],
            "sent_log": [
                "id", "run_id", "cron_job_id", "message_id", "content", "delivery_status",
                "created_at", "delivered_at",
            ],
        }
        with self._connect() as conn:
            for table, columns in expected_columns.items():
                actual = [row[1] for row in conn.execute(f"PRAGMA table_info({table})")]
                self.assertEqual(actual, columns, table)
            definitions = {
                row[0]: " ".join((row[1] or "").split())
                for row in conn.execute(
                    "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
                )
            }

        self.assertIn("CHECK (status IN ('active', 'closed'))", definitions["tracking_items"])
        self.assertIn("CHECK (status IN ('pending', 'committed', 'superseded', 'failed'))", definitions["checkpoints"])
        self.assertIn("CHECK (worker_kind IN ('opportunities', 'prepared'))", definitions["candidate_items"])
        self.assertIn("CHECK (priority IN ('regular', 'high'))", definitions["candidate_items"])
        self.assertIn("CHECK (decision IN ('send', 'skip'))", definitions["candidate_items"])
        self.assertIn(
            "CHECK (decision_reason IN ('time_sensitive', 'changed', 'already_handled', 'moot', "
            "'unconfirmed', 'preferences', 'not_useful_now'))",
            definitions["candidate_items"],
        )
        self.assertIn(
            "CHECK (delivery_status IN ('pending', 'delivered', 'failed'))",
            definitions["sent_log"],
        )

        with self._connect() as conn:
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute(
                    "UPDATE tracking_items SET status = 'paused' WHERE goal_id = ?",
                    (SEED_GOAL_ID,),
                )

    def test_init_db_is_idempotent_and_never_overwrites_updates(self):
        self.storage.init_db()
        seed_tracking_fixture(self.storage)
        changed = self.storage.upsert_tracking_item(
            SEED_GOAL_ID,
            summary="官方批次安排仍未发布，继续等待。",
            momentum_status="waiting",
        )
        self.assertEqual(changed["summary"], "官方批次安排仍未发布，继续等待。")

        self.storage.init_db()
        reloaded = self.storage.get_tracking_item(SEED_GOAL_ID)
        self.assertEqual(reloaded["summary"], "官方批次安排仍未发布，继续等待。")
        self.assertEqual(reloaded["momentum_status"], "waiting")

    def test_tracking_manage_tool_lifecycle_returns_valid_json(self):
        self.storage.init_db()
        seed_tracking_fixture(self.storage)

        listed = json.loads(self.tools.handle_tracking_manage(action="list"))
        self.assertTrue(listed["ok"])
        self.assertEqual(listed["action"], "list")
        self.assertEqual(listed["count"], 1)
        self.assertEqual(listed["items"][0]["goal_id"], SEED_GOAL_ID)

        created = json.loads(self.tools.handle_tracking_manage(args={
            "action": "upsert",
            "goal_id": "goal-test",
            "title": "测试目标",
            "description": "验证工具生命周期。",
            "summary": "初始状态。",
            "momentum_status": "on_track",
        }))
        self.assertTrue(created["ok"])
        self.assertEqual(created["item"]["goal_id"], "goal-test")
        created_at = created["item"]["updated_at"]

        fetched = json.loads(self.tools.handle_tracking_manage({"action": "get", "goal_id": "goal-test"}))
        self.assertEqual(fetched["item"]["title"], "测试目标")

        updated = json.loads(self.tools.handle_tracking_manage(
            action="upsert",
            goal_id="goal-test",
            summary="已经推进。",
            momentum_status="waiting",
        ))
        self.assertTrue(updated["ok"])
        self.assertEqual(updated["item"]["description"], "验证工具生命周期。")
        self.assertGreater(parse_utc(updated["item"]["updated_at"]), parse_utc(created_at))

        closed = json.loads(self.tools.handle_tracking_manage(action="close", goal_id="goal-test"))
        self.assertTrue(closed["ok"])
        self.assertEqual(closed["item"]["status"], "closed")
        self.assertIsNotNone(closed["item"]["closed_at"])
        self.assertGreaterEqual(parse_utc(closed["item"]["updated_at"]), parse_utc(updated["item"]["updated_at"]))

        reopened = json.loads(self.tools.handle_tracking_manage(
            action="reopen",
            goal_id="goal-test",
            summary="重新打开。",
            momentum_status="on_track",
        ))
        self.assertTrue(reopened["ok"])
        self.assertEqual(reopened["item"]["status"], "active")
        self.assertIsNone(reopened["item"]["closed_at"])
        self.assertEqual(reopened["item"]["summary"], "重新打开。")
        self.assertGreater(parse_utc(reopened["item"]["updated_at"]), parse_utc(closed["item"]["updated_at"]))

        missing = json.loads(self.tools.handle_tracking_manage(action="get", goal_id="missing"))
        self.assertTrue(missing["ok"])
        self.assertIsNone(missing["item"])

        bad_action = json.loads(self.tools.handle_tracking_manage(action="explode"))
        self.assertFalse(bad_action["ok"])
        self.assertIn("action", bad_action["error"])

        no_args = json.loads(self.tools.handle_tracking_manage())
        self.assertFalse(no_args["ok"])

    def test_tracking_manage_accepts_add_and_update_aliases(self):
        self.storage.init_db()

        added = json.loads(self.tools.handle_tracking_manage(
            action="add",
            goal_id="alias-goal",
            title="别名目标",
            description="通过 add 创建。",
            summary="初始状态。",
        ))
        self.assertTrue(added["ok"])
        self.assertEqual(added["action"], "add")
        self.assertEqual(added["item"]["goal_id"], "alias-goal")

        updated = json.loads(self.tools.handle_tracking_manage(
            action="update",
            goal_id="alias-goal",
            summary="通过 update 推进。",
            momentum_status="waiting",
        ))
        self.assertTrue(updated["ok"])
        self.assertEqual(updated["action"], "update")
        self.assertEqual(updated["item"]["title"], "别名目标")
        self.assertEqual(updated["item"]["summary"], "通过 update 推进。")

        action_enum = self.plugin.schemas.TRACKING_MANAGE_SCHEMA["parameters"]["properties"]["action"]["enum"]
        self.assertIn("add", action_enum)
        self.assertIn("update", action_enum)

    def test_timestamps_advance_on_every_modification(self):
        self.storage.init_db()
        item = self.storage.upsert_tracking_item(
            "strict-clock",
            title="严格时钟",
            description="验证时间戳单调递增。",
            summary="第一版。",
        )
        stamps = [parse_utc(item["updated_at"])]

        for summary in ("第二版。", "第三版。"):
            item = self.storage.upsert_tracking_item("strict-clock", summary=summary)
            stamps.append(parse_utc(item["updated_at"]))
        item = self.storage.close_tracking_item("strict-clock")
        stamps.append(parse_utc(item["updated_at"]))
        item = self.storage.reopen_tracking_item("strict-clock")
        stamps.append(parse_utc(item["updated_at"]))

        for earlier, later in zip(stamps, stamps[1:]):
            self.assertGreater(later, earlier)
        self.assertTrue(item["updated_at"].endswith("Z"))
        self.assertLessEqual(parse_utc(item["updated_at"]), datetime.now(timezone.utc) + timedelta(seconds=1))

    def test_cli_and_slash_surfaces_use_same_database_and_show_local_time(self):
        self.storage.init_db()
        seed_tracking_fixture(self.storage)
        self._write_host_config({"timezone": "Asia/Shanghai"})
        parser = argparse.ArgumentParser()
        self.cli.setup_cli(parser)

        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = self.cli.handle_cli(parser.parse_args(["track", "list"]))
        self.assertEqual(code, 0)
        rendered = output.getvalue()
        self.assertIn(SEED_GOAL_ID, rendered)
        self.assertIn("Asia/Shanghai", rendered)
        self.assertIn("UTC+08:00", rendered)

        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = self.cli.handle_cli(parser.parse_args([
                "track", "upsert", "cli-goal",
                "--title", "CLI 目标",
                "--description", "从 CLI 创建。",
                "--summary", "等待中。",
            ]))
        self.assertEqual(code, 0)

        slash = self.cli.handle_slash("track get cli-goal")
        self.assertIn("CLI 目标", slash)
        self.assertIn("Asia/Shanghai", slash)
        self.assertIn("UTC+08:00", slash)

        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = self.cli.handle_cli(parser.parse_args(["track", "close", "cli-goal"]))
        self.assertEqual(code, 0)
        self.assertEqual(self.storage.get_tracking_item("cli-goal")["status"], "closed")

        slash = self.cli.handle_slash("track reopen cli-goal")
        self.assertEqual(self.storage.get_tracking_item("cli-goal")["status"], "active")
        self.assertIn("active", slash)

    def test_cli_and_slash_track_support_add_and_update(self):
        self.storage.init_db()
        parser = argparse.ArgumentParser()
        self.cli.setup_cli(parser)

        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = self.cli.handle_cli(parser.parse_args([
                "track", "add", "cli-alias",
                "--title", "CLI 别名目标",
                "--description", "通过 CLI add 创建。",
                "--summary", "初始状态。",
            ]))
        self.assertEqual(code, 0)
        self.assertIn("CLI 别名目标", output.getvalue())
        self.assertEqual(self.storage.get_tracking_item("cli-alias")["summary"], "初始状态。")

        slash = self.cli.handle_slash(
            "track update cli-alias --summary '通过 slash update 推进。' --momentum-status waiting"
        )
        self.assertIn("通过 slash update 推进。", slash)
        item = self.storage.get_tracking_item("cli-alias")
        self.assertEqual(item["summary"], "通过 slash update 推进。")
        self.assertEqual(item["momentum_status"], "waiting")

    def test_tool_errors_do_not_escape_as_exceptions(self):
        self.storage.init_db()
        with mock.patch.object(self.storage, "list_tracking_items", side_effect=RuntimeError("database unavailable")):
            payload = json.loads(self.tools.handle_tracking_manage(action="list"))
        self.assertFalse(payload["ok"])
        self.assertIn("database unavailable", payload["error"])


if __name__ == "__main__":
    unittest.main()

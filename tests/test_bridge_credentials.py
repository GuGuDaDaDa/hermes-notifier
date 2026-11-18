"""Credential-hydration tests for the standalone Cron bridge and Stage 2 guard."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from hermes_constants import reset_hermes_home_override, set_hermes_home_override

from tests.test_goal_tracking import PLUGIN_DIR, load_plugin_package


TEST_ENV_NAME = "PROACTIVE_TEST_MODEL_KEY"
TEST_ENV_VALUE = "test-model-key-value"


def _host_loader_available() -> bool:
    """True when the host modules the bridge hydrates through are importable."""
    try:
        import hermes_cli.config  # noqa: F401
        import hermes_cli.env_loader  # noqa: F401
    except Exception:
        return False
    return True


class BridgeCredentialTestCase(unittest.TestCase):
    """The Cron script child runs without provider credentials; the bridge must rehydrate."""

    @classmethod
    def setUpClass(cls):
        cls.plugin = load_plugin_package()

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="hermes-proactive-cred-test-")
        self.home = Path(self._tmp.name)
        self._old_home = os.environ.get("HERMES_HOME")
        os.environ["HERMES_HOME"] = str(self.home)
        self._override_token = set_hermes_home_override(str(self.home))
        self.addCleanup(self._cleanup)
        self.addCleanup(self.plugin.workers.set_host_llm, None)
        self.plugin.workers.set_host_llm(None)

    def _cleanup(self):
        reset_hermes_home_override(self._override_token)
        if self._old_home is None:
            os.environ.pop("HERMES_HOME", None)
        else:
            os.environ["HERMES_HOME"] = self._old_home
        self._tmp.cleanup()

    def _write_home(self, *, declared: bool) -> None:
        """Write a host config whose main-model key is an env reference."""
        (self.home / "config.yaml").write_text(
            json.dumps(
                {
                    "model": {
                        "provider": "custom",
                        "default": "test-model",
                        "api_key": "${" + TEST_ENV_NAME + "}",
                    }
                }
            ),
            encoding="utf-8",
        )
        (self.home / ".env").write_text(
            f"{TEST_ENV_NAME}={TEST_ENV_VALUE}\n" if declared else "",
            encoding="utf-8",
        )

    def _resolved_key(self) -> str:
        from hermes_cli.config import load_config_readonly

        model = (load_config_readonly() or {}).get("model") or {}
        return str(model.get("api_key") or "")

    @unittest.skipUnless(_host_loader_available(), "host env loader is not installed")
    def test_bridge_hydrates_profile_env_before_stage1(self):
        self._write_home(declared=True)
        seen: dict[str, str] = {}

        def probe(*, hermes_home):
            seen["env"] = str(os.environ.get(TEST_ENV_NAME) or "")
            seen["key"] = self._resolved_key()

        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(TEST_ENV_NAME, None)
            with mock.patch.object(self.plugin.snapshot, "run_tick", side_effect=probe):
                code = self.plugin.bridge.run_bridge(
                    home=self.home, plugin_dir=PLUGIN_DIR, started=time.monotonic()
                )
        self.assertEqual(code, 0)
        self.assertEqual(seen["env"], TEST_ENV_VALUE)
        self.assertEqual(seen["key"], TEST_ENV_VALUE)

    @unittest.skipUnless(_host_loader_available(), "host config loader is not installed")
    def test_stage2_names_the_unresolved_key_reference(self):
        self._write_home(declared=False)
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(TEST_ENV_NAME, None)
            with self.assertRaises(RuntimeError) as caught:
                self.plugin.workers._resolve_llm(None)
        self.assertIn(TEST_ENV_NAME, str(caught.exception))

    def test_injected_host_client_bypasses_the_guard(self):
        self._write_home(declared=False)
        sentinel = object()
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(TEST_ENV_NAME, None)
            self.assertIs(self.plugin.workers._resolve_llm(sentinel), sentinel)

    @unittest.skipUnless(_host_loader_available(), "host env loader is not installed")
    def test_sanitized_cron_child_resolves_the_key(self):
        """Same spawn shape as the Cron scheduler: sanitized env, no provider credential."""
        from tools.environments.local import build_subprocess_env

        self._write_home(declared=True)
        code = (
            "import importlib.util, json, os, sys, time\n"
            f"plugin_dir = {str(PLUGIN_DIR)!r}\n"
            "spec = importlib.util.spec_from_file_location("
            "'proactive_child_probe', os.path.join(plugin_dir, '__init__.py'),\n"
            "    submodule_search_locations=[plugin_dir])\n"
            "module = importlib.util.module_from_spec(spec)\n"
            "sys.modules[spec.name] = module\n"
            "spec.loader.exec_module(module)\n"
            "def probe(*, hermes_home):\n"
            "    from hermes_cli.config import load_config_readonly\n"
            "    model = (load_config_readonly() or {}).get('model') or {}\n"
            "    print(json.dumps({'env': os.environ.get(" + repr(TEST_ENV_NAME) + "),\n"
            "                      'key': model.get('api_key')}))\n"
            "module.snapshot.run_tick = probe\n"
            f"module.bridge.run_bridge(home={str(self.home)!r}, "
            f"plugin_dir=plugin_dir, started=time.monotonic())\n"
        )
        env = build_subprocess_env(strip_launch_profile=True)
        self.assertNotIn(TEST_ENV_NAME, env)  # the credential is absent, as in production
        env["HERMES_HOME"] = str(self.home)
        env.pop(TEST_ENV_NAME, None)
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            env=env,
            timeout=120,
        )
        self.assertEqual(result.returncode, 0, result.stderr[-2000:])
        payload = json.loads([line for line in result.stdout.splitlines() if line.strip()][-1])
        self.assertEqual(payload["env"], TEST_ENV_VALUE)
        self.assertEqual(payload["key"], TEST_ENV_VALUE)


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
"""Pre-run Cron wake-gate launcher for the proactive plugin."""

from __future__ import annotations

import importlib.util
import logging
import os
import sys
import time
from pathlib import Path


LOGGER_NAME = "proactive.bridge"
MAX_FIELD_CHARS = 96
MAX_PATH_CHARS = 240
MAX_STACK_CHARS = 300
MAX_TRACE_FRAMES = 8
FALLBACK_PLUGIN_DIR = ""  # @PROACTIVE_PLUGIN_DIR@


def _setup_host_logging(home: Path) -> None:
    """Initialize host Cron file handlers before the plugin package loads.

    setup_logging is idempotent and owns levels, rotation, and retention;
    without it this child process would drop pre-load and Stage 1/2 logs.
    """
    try:
        from hermes_logging import setup_logging

        setup_logging(hermes_home=home, mode="cron")
    except Exception:
        # Observability must never alter the wake gate, stdout, or exit code.
        pass


def _configured_level(home: Path) -> str:
    try:
        import hermes_yaml as yaml

        payload = yaml.safe_load((home / "config.yaml").read_text(encoding="utf-8")) or {}
        entry = (((payload.get("plugins") or {}).get("entries") or {}).get("proactive")) or {}
        settings = entry.get("settings", entry.get("config", {})) or {}
        value = str(settings.get("log_level") or "").strip().upper()
    except Exception:
        return "INFO"
    return value if value in {"DEBUG", "INFO", "WARNING", "ERROR"} else "INFO"


def _apply_log_level(home: Path) -> None:
    """Apply plugin log level before plugin load without touching host handlers."""
    try:
        logging.getLogger("proactive").setLevel(getattr(logging, _configured_level(home)))
    except Exception:
        pass


def _clean_value(value: object, limit: int) -> str:
    try:
        text = "" if value is None else str(value)
    except Exception:
        return "-"
    text = "".join(" " if (not char.isprintable() or char in "\r\n\t") else char for char in text)
    if len(text) > limit:
        text = text[:limit] + "..."
    return text or "-"


def _stack(exc: BaseException) -> str:
    """Describe an exception by file, function, and line only."""
    try:
        frames = []
        tb = exc.__traceback__
        while tb is not None:
            frames.append(tb)
            tb = tb.tb_next
        parts = []
        for frame in frames[-MAX_TRACE_FRAMES:]:
            code = frame.tb_frame.f_code
            parts.append(f"{Path(code.co_filename).name}:{code.co_name}:{frame.tb_lineno}")
        return ",".join(parts) or "-"
    except Exception:
        return "-"


def _emit(logger: logging.Logger, level: int, event: str, **fields: object) -> None:
    limits = {
        "hermes_home": MAX_PATH_CHARS,
        "plugin_dir": MAX_PATH_CHARS,
        "error_stack": MAX_STACK_CHARS,
    }
    try:
        parts = [f"event={_clean_value(event, 64)}"]
        for key, value in fields.items():
            if value is not None:
                parts.append(f"{key}={_clean_value(value, limits.get(key, MAX_FIELD_CHARS))}")
        logger.log(level, "%s", " ".join(parts))
    except Exception:
        pass


def _load_plugin(home: Path):
    # Precedence: explicit test/operator override -> active profile install ->
    # fallback path recorded by setup-cron. Once an __init__.py exists at a
    # tier, fail loudly if its bridge entrypoint is missing instead of silently
    # executing an older copy further down the list.
    env_override = os.environ.get("PROACTIVE_PLUGIN_DIR", "").strip()
    candidates = [
        Path(env_override).expanduser() if env_override else None,
        home / "plugins" / "proactive",
        Path(FALLBACK_PLUGIN_DIR).expanduser() if FALLBACK_PLUGIN_DIR else None,
    ]
    for candidate in candidates:
        if candidate is None:
            continue
        init_file = candidate / "__init__.py"
        if not init_file.is_file():
            continue
        resolved_dir = init_file.resolve().parent
        spec = importlib.util.spec_from_file_location(
            "proactive_runtime_bridge",
            resolved_dir / "__init__.py",
            submodule_search_locations=[str(resolved_dir)],
        )
        if spec is None or spec.loader is None:
            continue
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        # Roll back partial module registration if import or entrypoint checks fail.
        try:
            spec.loader.exec_module(module)
            bridge = getattr(module, "bridge", None)
            if bridge is None or not callable(getattr(bridge, "run_bridge", None)):
                raise RuntimeError(f"proactive bridge entrypoint missing in {resolved_dir}")
        except BaseException:
            sys.modules.pop(spec.name, None)
            raise
        return bridge, resolved_dir
    raise RuntimeError("proactive plugin not found; run 'hermes proactive setup-cron' again")


def main() -> int:
    home = Path(os.environ.get("HERMES_HOME") or (Path.home() / ".hermes")).expanduser().resolve()
    _setup_host_logging(home)
    _apply_log_level(home)
    logger = logging.getLogger(LOGGER_NAME)
    started = time.monotonic()
    _emit(logger, logging.INFO, "bridge.start", stage="bridge", mode="cron", hermes_home=home)
    try:
        bridge, plugin_dir = _load_plugin(home)
    except Exception as exc:
        _emit(
            logger,
            logging.ERROR,
            "bridge.failed",
            stage="bridge",
            phase="plugin_load",
            error_type=type(exc).__name__,
            error_stack=_stack(exc),
            duration_ms=int((time.monotonic() - started) * 1000),
        )
        raise
    return int(bridge.run_bridge(home=home, plugin_dir=plugin_dir, started=started))


if __name__ == "__main__":
    raise SystemExit(main())

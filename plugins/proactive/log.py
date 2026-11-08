"""Structured run logging shared by the proactive pipeline stages.

Every business record is one human-readable ``key=value`` line carrying a stable
``event`` name, so runs can be correlated across Stage 1 → Worker 1 → Worker 2 →
Edition Handoff with grep or any log platform. Records are deliberately limited
to the whitelisted run metadata below: conversation text, memory content,
candidate bodies, credentials and raw exception text must never reach a log.
"""

from __future__ import annotations

import logging
from pathlib import Path
from time import monotonic
from typing import Any, Mapping


LOGGER_NAMESPACE = "proactive"
LEVEL_NAMES = ("DEBUG", "INFO", "WARNING", "ERROR")
DEFAULT_LEVEL = "INFO"
MAX_TRACE_FRAMES = 8

# Whitelisted fields and their caps. Insertion order is the emitted field order,
# so a given event always renders its keys in the same sequence.
FIELD_LIMITS: dict[str, int] = {
    "stage": 32,
    "run_id": 96,
    "snapshot_dir": 240,
    "worker": 24,
    "worker_kind": 24,
    "purpose": 64,
    "schema": 64,
    "reason": 48,
    "duration_ms": 16,
    "source_count": 12,
    "source": 64,
    "candidate_count": 12,
    "checkpoint_status": 16,
    "cron_job_id": 64,
    "message_id": 96,
    "error_type": 64,
    "error_stack": 300,
    "earlier_message_count": 12,
    "new_message_count": 12,
    "changed_goal_count": 12,
    "time_sensitive_goal_count": 12,
    "failing_cron_count": 12,
    "delivery_rollback_count": 12,
    "opportunities_count": 12,
    "prepared_count": 12,
    "undecided_count": 12,
    "live_diff_count": 12,
    "send_count": 12,
    "skip_count": 12,
    "close_count": 12,
    "created_count": 12,
    "updated_count": 12,
    "closed_count": 12,
    "skipped_closed_count": 12,
    "invalid_count": 12,
    "result_count": 12,
    "count": 12,
    "action": 24,
    "delivery_status": 16,
    "pending_receipt": 8,
    "available": 8,
    "has_delivery_error": 8,
    "timeout_seconds": 12,
    "phase": 32,
    "mode": 16,
    "plugin_dir": 240,
    "hermes_home": 240,
    "window_start_utc": 40,
    "window_end_utc": 40,
    "goal_id": 96,
    "status": 24,
}


def logger(suffix: str = "") -> logging.Logger:
    """Return a child logger under the plugin's stable namespace."""
    name = f"{LOGGER_NAMESPACE}.{suffix}" if suffix else LOGGER_NAMESPACE
    return logging.getLogger(name)


def normalize_level(value: Any) -> str:
    """Map configuration input to a supported level name; unknown values mean INFO."""
    text = str(value or "").strip().upper()
    return text if text in LEVEL_NAMES else DEFAULT_LEVEL


def configure_level(value: Any) -> str:
    """Apply *value* to the plugin logger only.

    The host owns root handlers, levels and rotation; touching any of them here
    would change every other plugin's logging.
    """
    name = normalize_level(value)
    try:
        logging.getLogger(LOGGER_NAMESPACE).setLevel(getattr(logging, name))
    except Exception:
        return DEFAULT_LEVEL
    return name


def elapsed_ms(started: float) -> int:
    """Return whole milliseconds since *started*, never negative or raising."""
    try:
        return max(0, int((monotonic() - started) * 1000))
    except Exception:
        return 0


def _clean(value: Any, limit: int) -> str:
    """Render one field as a single-line token, capped and never raising."""
    try:
        if isinstance(value, bool):
            text = "true" if value else "false"
        else:
            text = str(value)
    except Exception:
        return "-"
    text = "".join(" " if (not char.isprintable() or char in "\r\n\t") else char for char in text)
    if len(text) > limit:
        text = text[:limit] + "..."
    return text or "-"


def _line(event: str, fields: Mapping[str, Any]) -> str:
    parts = [f"event={_clean(event, 64)}"]
    for key, limit in FIELD_LIMITS.items():
        if key not in fields:
            continue
        value = fields[key]
        if value is None:
            continue
        parts.append(f"{key}={_clean(value, limit)}")
    return " ".join(parts)


def emit(target: logging.Logger, level: int, event: str, **fields: Any) -> None:
    """Write one event record; logging failures never reach the caller."""
    try:
        target.log(level, "%s", _line(event, fields))
    except Exception:
        pass


def debug(target: logging.Logger, event: str, **fields: Any) -> None:
    emit(target, logging.DEBUG, event, **fields)


def info(target: logging.Logger, event: str, **fields: Any) -> None:
    emit(target, logging.INFO, event, **fields)


def warning(target: logging.Logger, event: str, **fields: Any) -> None:
    emit(target, logging.WARNING, event, **fields)


def error(target: logging.Logger, event: str, **fields: Any) -> None:
    emit(target, logging.ERROR, event, **fields)


def stack(exc: BaseException) -> str:
    """Describe *exc* by module file, function and line only — never its text."""
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


def error_event(target: logging.Logger, event: str, exc: BaseException, **fields: Any) -> None:
    """Log a failure with its type and de-texted call stack."""
    fields["error_type"] = type(exc).__name__
    fields["error_stack"] = stack(exc)
    emit(target, logging.ERROR, event, **fields)


def warning_event(target: logging.Logger, event: str, exc: BaseException, **fields: Any) -> None:
    """Log a recoverable degradation with its exception type and call stack."""
    fields["error_type"] = type(exc).__name__
    fields["error_stack"] = stack(exc)
    emit(target, logging.WARNING, event, **fields)


__all__ = [
    "DEFAULT_LEVEL",
    "FIELD_LIMITS",
    "LEVEL_NAMES",
    "LOGGER_NAMESPACE",
    "MAX_TRACE_FRAMES",
    "configure_level",
    "debug",
    "elapsed_ms",
    "emit",
    "error",
    "error_event",
    "info",
    "logger",
    "normalize_level",
    "stack",
    "warning",
    "warning_event",
]

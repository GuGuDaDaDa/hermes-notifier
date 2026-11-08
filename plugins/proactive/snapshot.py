"""Deterministic Stage 1 snapshot assembly and wake-gate decisions."""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import re
import shutil
import sqlite3
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import monotonic
from typing import Any, Iterable, Iterator, Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from hermes_constants import get_hermes_home, reset_hermes_home_override, set_hermes_home_override

from . import log, memory_source, storage


PLUGIN_NAME = "proactive"
UTC = timezone.utc
DAY_SECONDS = 86400
SOURCE_FILENAMES = (
    "conversation.md",
    "tracking.md",
    "sent.md",
    "scheduled-crons.md",
    "memory.md",
    "context.md",
)
NON_HUMAN_SESSION_SURFACES = frozenset({"cron", "subagent", "tool", "webhook", "system"})

DEFAULT_CONFIG: dict[str, Any] = {
    "active_window": "09:00-21:30",
    "timezone": "auto",
    "preferences_file": "PROACTIVE_PREFERENCES.md",
    "retention_days": 14,
    "max_message_chars": 1500,
    "max_earlier_chars": 20000,
    "worker_timeout_seconds": 120,
    "log_level": "INFO",
    "memory_source": "auto",
    "memory_limit": 5,
    "memory_timeout_seconds": 3,
    "memory_tool": "",
    "delivery_platform": "auto",
    "cron_schedule": "30 9,12,15,18,21 * * *",
}

PREFERENCES_TEMPLATE = """\
# Proactive Preferences

## Active window

- Active window: {active_window} ({timezone}).
- Do not wake, scan, or deliver proactive messages outside this window unless an explicit force flag is used.

## Voice and delivery

- Write in the voice of the assistant persona configured for this Hermes profile.
- Merge multiple send-worthy items into one short message.
- Never copy a research worker's draft verbatim; rewrite it in your own voice.

## Hard non-interference rule

Existing healthy scheduled Cron jobs must never be duplicated, pre-announced, or re-reported while their
`last_status` is `ok`. The current Cron inventory is rebuilt into `scheduled-crons.md` on every run;
use that file as the source of truth instead of maintaining a job list here.

Treat a job as covered when it is enabled and healthy. A failing or disabled job may be mentioned as an
anomaly, but its schedule and definition remain owned by the Cron runtime.
"""

FENCED_CODE_RE = re.compile(r"```(?:[^\n]*)\n(.*?)\n```", re.DOTALL)
MIRRORED_MESSAGE_RE = re.compile(r"^\s*(?:\[Cron\b|\[Webhook delivery:)", re.IGNORECASE)
INTERNAL_MESSAGE_RE = re.compile(r"^\s*(?:\[SILENT\]|NO_REPLY)\s*$", re.IGNORECASE)
NON_HUMAN_MESSAGE_LABELS = frozenset({"cron", "subagent", "tool"})
ASCII_WORD_RE = re.compile(r"[A-Za-z][A-Za-z0-9_.+-]{2,}")
CJK_PHRASE_RE = re.compile(r"[\u3400-\u9fff]{2,}")


class SnapshotResult(dict):
    """Dictionary result with attribute access for callers that prefer typed fields."""

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc


class SourceCollector:
    """Assign one run-wide source sequence while retaining a traceable lookup map."""

    def __init__(self) -> None:
        self._next = 1
        self.mapping: dict[int, dict[str, Any]] = {}

    def add(
        self,
        *,
        kind: str,
        origin: str,
        text: str = "",
        timestamp: str | None = None,
        filename: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> str:
        number = self._next
        self._next += 1
        self.mapping[number] = {
            "source": number,
            "kind": kind,
            "origin": origin,
            "timestamp": timestamp,
            "file": filename,
            "text": text,
            "metadata": dict(metadata or {}),
        }
        return f"[source:{number}]"


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)):
        parsed = datetime.fromtimestamp(float(value), tz=UTC)
    else:
        text = str(value).strip()
        if not text:
            return None
        text = text[:-1] + "+00:00" if text.endswith("Z") else text
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _utc_iso(value: datetime, *, microseconds: bool = True) -> str:
    value = value.astimezone(UTC)
    if microseconds:
        return value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def _zone(config: Mapping[str, Any]):
    """Resolve the run's timezone.

    An explicit IANA name wins; ``auto`` (the default) follows the host's
    ``timezone`` setting or ``HERMES_TIMEZONE`` via ``hermes_time``, and finally
    falls back to the server's local zone so an unconfigured host still works.
    """
    raw = str(config.get("timezone") or "auto").strip()
    if raw and raw.lower() not in {"auto", "host", "local"}:
        try:
            return ZoneInfo(raw)
        except ZoneInfoNotFoundError:
            pass
    try:
        from hermes_time import get_timezone

        configured = get_timezone()
        if configured is not None:
            return configured
    except Exception:
        pass
    return datetime.now().astimezone().tzinfo or UTC


def _zone_name(zone) -> str:
    return str(getattr(zone, "key", "") or "").strip()


def _zone_label(zone) -> str:
    return _zone_name(zone) or "local time"


def _utc_offset_label(value: datetime, zone) -> str:
    offset = value.astimezone(zone).utcoffset() or timedelta(0)
    total = int(offset.total_seconds())
    sign = "+" if total >= 0 else "-"
    total = abs(total)
    return f"UTC{sign}{total // 3600:02d}:{(total % 3600) // 60:02d}"


def _display_time(value: datetime, config: Mapping[str, Any]) -> str:
    zone = _zone(config)
    local = value.astimezone(zone)
    return f"{local:%Y-%m-%d %H:%M:%S} ({_zone_label(zone)}, {_utc_offset_label(value, zone)})"


def _display_time_from_text(value: str | None, config: Mapping[str, Any]) -> str:
    parsed = _parse_datetime(value)
    return _display_time(parsed, config) if parsed is not None else "—"


def _active_window_bounds(config: Mapping[str, Any]) -> tuple[int, int]:
    raw = str(config.get("active_window") or DEFAULT_CONFIG["active_window"])
    match = re.fullmatch(r"\s*(\d{1,2}):(\d{2})\s*[-–]\s*(\d{1,2}):(\d{2})\s*", raw)
    if not match:
        return 9 * 60, 21 * 60 + 30
    start_hour, start_minute, end_hour, end_minute = (int(part) for part in match.groups())
    return start_hour * 60 + start_minute, end_hour * 60 + end_minute


def _within_active_window(now: datetime, config: Mapping[str, Any]) -> bool:
    local = now.astimezone(_zone(config))
    minute = local.hour * 60 + local.minute
    start, end = _active_window_bounds(config)
    if start <= end:
        return start <= minute <= end
    return minute >= start or minute <= end


@contextlib.contextmanager
def _home_scope(hermes_home: str | Path | None) -> Iterator[Path]:
    if hermes_home is None:
        yield Path(get_hermes_home())
        return
    home = Path(hermes_home).expanduser().resolve()
    token = set_hermes_home_override(str(home))
    try:
        yield home
    finally:
        reset_hermes_home_override(token)


def _load_yaml_config(path: Path) -> Mapping[str, Any]:
    if not path.is_file():
        return {}
    try:
        import hermes_yaml as yaml

        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}
    if not isinstance(payload, Mapping):
        return {}
    plugins = payload.get("plugins")
    entries = plugins.get("entries") if isinstance(plugins, Mapping) else None
    entry = entries.get(PLUGIN_NAME) if isinstance(entries, Mapping) else None
    if not isinstance(entry, Mapping):
        return {}
    settings = entry.get("settings", entry.get("config", {}))
    return settings if isinstance(settings, Mapping) else {}


def _build_config(home: Path, overrides: Mapping[str, Any] | None) -> dict[str, Any]:
    config = dict(DEFAULT_CONFIG)
    config.update(_load_yaml_config(home / "config.yaml"))
    if overrides:
        config.update({key: value for key, value in overrides.items() if value is not None})
    try:
        config["retention_days"] = max(0, int(config["retention_days"]))
    except (TypeError, ValueError):
        config["retention_days"] = DEFAULT_CONFIG["retention_days"]
    try:
        config["max_message_chars"] = max(100, int(config["max_message_chars"]))
    except (TypeError, ValueError):
        config["max_message_chars"] = DEFAULT_CONFIG["max_message_chars"]
    try:
        config["max_earlier_chars"] = max(0, int(config["max_earlier_chars"]))
    except (TypeError, ValueError):
        config["max_earlier_chars"] = DEFAULT_CONFIG["max_earlier_chars"]
    try:
        config["worker_timeout_seconds"] = max(10, int(config["worker_timeout_seconds"]))
    except (TypeError, ValueError):
        config["worker_timeout_seconds"] = DEFAULT_CONFIG["worker_timeout_seconds"]
    try:
        config["memory_limit"] = max(1, int(config["memory_limit"]))
    except (TypeError, ValueError):
        config["memory_limit"] = DEFAULT_CONFIG["memory_limit"]
    try:
        config["memory_timeout_seconds"] = max(0.1, float(config["memory_timeout_seconds"]))
    except (TypeError, ValueError):
        config["memory_timeout_seconds"] = DEFAULT_CONFIG["memory_timeout_seconds"]
    config["memory_source"] = str(config.get("memory_source") or "").strip()
    config["memory_tool"] = str(config.get("memory_tool") or "").strip()
    config["timezone"] = str(config.get("timezone") or "auto").strip()
    config["delivery_platform"] = str(config.get("delivery_platform") or "auto").strip()
    config["cron_schedule"] = str(
        config.get("cron_schedule") or DEFAULT_CONFIG["cron_schedule"]
    ).strip()
    config["log_level"] = log.normalize_level(config.get("log_level"))
    return config


def _is_production_hermes_home(home: Path) -> bool:
    real_home = (Path.home() / ".hermes").resolve()
    try:
        resolved = home.resolve()
    except OSError:
        return False
    if resolved == real_home:
        return True
    profiles = real_home / "profiles"
    return resolved.parent == profiles


def resolve_preferences_path(home: Path, configured: str | None = None) -> Path:
    """Resolve ``~`` without leaking isolated test homes into the real user home."""
    raw = str(configured or DEFAULT_CONFIG["preferences_file"]).strip()
    path = Path(raw)
    production = _is_production_hermes_home(home)
    if raw.startswith("~"):
        remainder = raw[1:].lstrip("/\\")
        base = Path.home() if production else home
        return (base / remainder).resolve() if remainder else base
    if path.is_absolute():
        return path
    return (home / path).resolve()


def _write_private_file(path: Path, content: str) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    descriptor = os.open(path, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8", closefd=False) as handle:
            handle.write(content)
            if content and not content.endswith("\n"):
                handle.write("\n")
    finally:
        os.close(descriptor)
    os.chmod(path, 0o600)


def ensure_preferences_file(home: Path, config: Mapping[str, Any]) -> Path:
    path = resolve_preferences_path(home, config.get("preferences_file"))
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        zone = _zone(config)
        content = PREFERENCES_TEMPLATE.format(
            active_window=str(config.get("active_window") or DEFAULT_CONFIG["active_window"]),
            timezone=f"{_zone_label(zone)}, {_utc_offset_label(datetime.now(tz=UTC), zone)}",
        )
        _write_private_file(path, content)
    else:
        with contextlib.suppress(OSError):
            os.chmod(path, 0o600)
    return path


def _prune_snapshots(hidden_files: Path, now: datetime, retention_days: int) -> list[Path]:
    logger = log.logger("snapshot")
    if retention_days < 0 or not hidden_files.is_dir():
        return []
    cutoff = now.timestamp() - retention_days * DAY_SECONDS
    removed: list[Path] = []
    failed = 0
    for child in hidden_files.iterdir():
        if not child.is_dir():
            continue
        try:
            modified = child.stat().st_mtime
        except OSError:
            continue
        if modified >= cutoff:
            continue
        shutil.rmtree(child, ignore_errors=True)
        if not child.exists():
            removed.append(child)
        else:
            failed += 1
    if failed:
        log.warning(logger, "snapshot.prune_failed", stage="stage1", count=failed)
    return removed


def _read_human_state_messages(
    state_db: Path,
    *,
    lower_ts: float,
    upper_ts: float | None = None,
    stage: str = "stage1",
) -> list[dict[str, Any]]:
    """Read sanitized human turns from the profile state database in read-only mode.

    Human child sessions are eligible too: a user-started branch under a parent
    conversation still carries the user's replies (for example an explicit
    "already done" confirmation). Runtime-owned sources, mirrored Cron/Webhook
    deliveries, internal control tokens, tool roles, and empty rows are removed
    before callers apply their own logical boundaries.
    """
    logger = log.logger("snapshot")
    if not state_db.is_file():
        return []
    uri = f"file:{urllib.parse.quote(str(state_db))}?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=2.0)
    except sqlite3.Error as exc:
        log.warning_event(
            logger, "state_db.unreadable", exc, stage=stage, reason="open_failed"
        )
        return []
    try:
        conn.row_factory = sqlite3.Row
        tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        if not {"sessions", "messages"} <= tables:
            log.warning(logger, "state_db.unreadable", stage=stage, reason="schema_missing")
            return []
        session_columns = {row[1] for row in conn.execute("PRAGMA table_info(sessions)")}
        message_columns = {row[1] for row in conn.execute("PRAGMA table_info(messages)")}
        if "parent_session_id" not in session_columns:
            log.warning(logger, "state_db.unreadable", stage=stage, reason="schema_missing")
            return []

        title_expr = "title" if "title" in session_columns else "NULL AS title"
        display_expr = "display_name" if "display_name" in session_columns else "NULL AS display_name"
        sessions = conn.execute(
            f"""
            SELECT id, source, parent_session_id, {title_expr}, {display_expr}
              FROM sessions
            """
        ).fetchall()
        human_sessions = {
            str(row["id"]): row
            for row in sessions
            if str(row["source"] or "").strip().lower() not in NON_HUMAN_SESSION_SURFACES
        }
        if not human_sessions:
            return []

        source_expr = "source_label" if "source_label" in message_columns else "NULL AS source_label"
        active_clause = "AND active = 1" if "active" in message_columns else ""
        upper_clause = "AND timestamp <= ?" if upper_ts is not None else ""
        params: tuple[float, ...] = (lower_ts, upper_ts) if upper_ts is not None else (lower_ts,)
        rows = conn.execute(
            f"""
            SELECT id, session_id, role, content, timestamp, {source_expr}
              FROM messages
             WHERE role IN ('user', 'assistant')
               {active_clause}
               AND timestamp > ?
               {upper_clause}
             ORDER BY timestamp ASC, id ASC
            """,
            params,
        ).fetchall()
    except sqlite3.Error as exc:
        log.warning_event(
            logger, "state_db.unreadable", exc, stage=stage, reason="query_failed"
        )
        return []
    finally:
        conn.close()

    records: list[dict[str, Any]] = []
    for row in rows:
        session_id = str(row["session_id"])
        session = human_sessions.get(session_id)
        if session is None:
            continue
        if str(row["source_label"] or "").strip().lower() in NON_HUMAN_MESSAGE_LABELS:
            continue
        content = row["content"]
        if not isinstance(content, str) or not content.strip():
            continue
        if MIRRORED_MESSAGE_RE.match(content) or INTERNAL_MESSAGE_RE.match(content):
            continue
        try:
            timestamp = float(row["timestamp"])
        except (TypeError, ValueError):
            continue
        timestamp_utc = _utc_iso(datetime.fromtimestamp(timestamp, tz=UTC))
        surface = str(session["source"] or "").strip().lower()
        title = str(session["title"] or session["display_name"] or session_id)
        records.append(
            {
                "id": row["id"],
                "session_id": session_id,
                "surface": surface,
                "session_title": title,
                "role": str(row["role"]),
                "content": content,
                "timestamp": timestamp,
                "timestamp_utc": timestamp_utc,
            }
        )
    return records


def _read_state_messages(
    state_db: Path,
    *,
    window_start: datetime,
    window_end: datetime,
) -> list[dict[str, Any]]:
    """Read ``# New`` turns plus the 24-hour ``# Earlier`` context before the watermark.

    The lower bound deliberately reaches one full day before the committed checkpoint;
    ``_render_conversation`` then splits rows at the watermark so fresh developments and
    background context remain distinguishable.
    """
    return _read_human_state_messages(
        state_db,
        lower_ts=(window_start - timedelta(seconds=DAY_SECONDS)).timestamp(),
        upper_ts=window_end.timestamp(),
    )


def read_live_conversation_diff(
    state_db: str | Path,
    window_end_utc: str | datetime,
    *,
    now_utc: datetime | None = None,
) -> list[dict[str, Any]]:
    """Read human conversation rows written strictly after an edition's snapshot boundary.

    Human child sessions are included; runtime mirrors, internal control tokens,
    tool output, and Cron/subagent sessions are removed. The editorial gate uses
    this diff to invalidate candidates that became stale while the snapshot was
    being researched and to close goals the user already confirmed.
    """
    end = _parse_datetime(window_end_utc)
    if end is None:
        return []
    path = Path(state_db)
    upper = now_utc.astimezone(UTC) if isinstance(now_utc, datetime) else None
    return _read_human_state_messages(
        path,
        lower_ts=end.timestamp(),
        upper_ts=upper.timestamp() if upper is not None else None,
        stage="stage3",
    )


def _collapse_code_blocks(text: str) -> str:
    def replace(match: re.Match[str]) -> str:
        body = match.group(1)
        line_count = len(body.splitlines())
        if line_count > 12:
            return f"[代码块已省略，共 {line_count} 行]"
        return match.group(0)

    return FENCED_CODE_RE.sub(replace, text)


def _truncate_message(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    if max_chars <= 1500:
        # Preserve a 700 + "..." + 700 head/tail budget for messages over 1,500 chars.
        half = 700 if max_chars == 1500 else max(1, (max_chars - 3) // 2)
    else:
        half = max(1, (max_chars - 3) // 2)
    return text[:half] + "..." + text[-half:]


def _sanitize_message(content: str, max_chars: int) -> str | None:
    stripped = content.strip()
    if not stripped or stripped.upper() in {"[SILENT]", "NO_REPLY"}:
        return None
    if MIRRORED_MESSAGE_RE.match(stripped):
        return None
    text = _collapse_code_blocks(content.strip())
    return _truncate_message(text, max_chars)


def _conversation_block(record: Mapping[str, Any], marker: str) -> str:
    timestamp = datetime.fromtimestamp(float(record["timestamp"]), tz=UTC)
    origin = f"{record['session_id']}#message:{record['id']}"
    content = str(record["content"])
    return (
        f"## {record['surface']} · {record['session_title']}\n"
        f"- {marker} {_utc_iso(timestamp)} · {record['role']} · {origin}\n\n"
        f"{content}"
    )


def _group_conversation_records(
    records: Iterable[Mapping[str, Any]],
) -> list[tuple[tuple[str, str], list[dict[str, Any]]]]:
    """Group records by surface and session title while preserving first appearance."""
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for record in records:
        key = (str(record["surface"]), str(record["session_title"]))
        groups.setdefault(key, []).append(dict(record))
    return list(groups.items())


def _append_conversation_groups(
    blocks: list[str],
    section: str,
    records: Iterable[Mapping[str, Any]],
    collector: SourceCollector,
) -> None:
    for (surface, session_title), group in _group_conversation_records(records):
        blocks.append(f"# {section} · {surface} ({session_title})")
        for record in group:
            marker = collector.add(
                kind="conversation",
                origin=f"{record['session_id']}#message:{record['id']}",
                text=record["content"],
                timestamp=_utc_iso(datetime.fromtimestamp(float(record["timestamp"]), tz=UTC)),
                filename="conversation.md",
                metadata={"surface": record["surface"], "role": record["role"]},
            )
            blocks.append(_conversation_block(record, marker))


def _render_conversation(
    records: list[dict[str, Any]],
    collector: SourceCollector,
    *,
    window_start: datetime,
    max_message_chars: int,
    max_earlier_chars: int,
) -> tuple[str, list[dict[str, Any]], list[dict[str, Any]]]:
    watermark = window_start.timestamp()
    new_records: list[dict[str, Any]] = []
    earlier_records: list[dict[str, Any]] = []
    for record in records:
        sanitized = _sanitize_message(str(record["content"]), max_message_chars)
        if sanitized is None:
            continue
        candidate = dict(record)
        candidate["content"] = sanitized
        if float(record["timestamp"]) > watermark:
            new_records.append(candidate)
        elif float(record["timestamp"]) > watermark - DAY_SECONDS:
            earlier_records.append(candidate)

    # Select earlier context by rendered size from newest to oldest, then restore chronology.
    selected_earlier: list[dict[str, Any]] = []
    used = 0
    for record in reversed(earlier_records):
        cost = len(_conversation_block(record, "[source:0]")) + 2
        if used + cost > max_earlier_chars:
            continue
        selected_earlier.append(record)
        used += cost
    selected_earlier.reverse()

    blocks: list[str] = []
    if new_records:
        _append_conversation_groups(blocks, "New", new_records, collector)
    else:
        blocks.extend(["# New", "(no new messages)"])

    if selected_earlier:
        _append_conversation_groups(blocks, "Earlier", selected_earlier, collector)
    else:
        blocks.extend(["# Earlier", "(no earlier messages)"])
    return "\n\n".join(blocks) + "\n", new_records, selected_earlier


def _render_tracking(
    goals: list[dict[str, Any]],
    collector: SourceCollector,
    *,
    window_start: datetime,
    config: Mapping[str, Any],
) -> tuple[str, int, int]:
    changed = 0
    time_sensitive = 0
    blocks = ["# Active tracking items"]
    for goal in goals:
        marker_kwargs = {
            "kind": "tracking",
            "origin": str(goal["goal_id"]),
            "text": str(goal.get("title") or ""),
            "timestamp": goal.get("updated_at"),
            "filename": "tracking.md",
        }
        marker_for = getattr(collector, "marker_for", None)
        marker = marker_for(**marker_kwargs) if marker_for is not None else collector.add(**marker_kwargs)
        updated = _parse_datetime(goal.get("updated_at"))
        is_changed = updated is not None and updated > window_start
        if is_changed:
            changed += 1
        if goal.get("momentum_status") == "time_sensitive":
            time_sensitive += 1
        blocks.append(
            "\n".join(
                [
                    f"## {marker} {goal.get('title') or goal['goal_id']}",
                    f"- Goal id: {goal['goal_id']}",
                    f"- Description: {goal.get('description') or '—'}",
                    f"- Momentum status: {goal.get('momentum_status') or '—'}",
                    f"- Status: {goal.get('status') or '—'}",
                    f"- Summary: {goal.get('summary') or '—'}",
                    f"- Updated at: {goal.get('updated_at') or '—'} ({_display_time_from_text(goal.get('updated_at'), config)})",
                    f"- Changed since checkpoint: {'true' if is_changed else 'false'}",
                ]
            )
        )
    if not goals:
        blocks.append("(no active tracking items)")
    return "\n\n".join(blocks) + "\n", changed, time_sensitive


def _render_sent(
    messages: list[dict[str, Any]],
    collector: SourceCollector,
    *,
    config: Mapping[str, Any],
) -> str:
    blocks = [
        "# Delivered proactive messages (last 7 days)",
        "Ordinary chat replies are not listed here; this ledger only contains proactive deliveries.",
    ]
    for item in messages:
        marker = collector.add(
            kind="sent",
            origin=str(item.get("run_id") or item.get("message_id") or item.get("id")),
            text=str(item.get("content") or ""),
            timestamp=item.get("delivered_at"),
            filename="sent.md",
            metadata={"message_id": item.get("message_id"), "cron_job_id": item.get("cron_job_id")},
        )
        blocks.append(
            "\n".join(
                [
                    f"## {marker} {item.get('delivered_at') or '—'}",
                    f"- Run id: {item.get('run_id') or '—'}",
                    f"- Message id: {item.get('message_id') or '—'}",
                    f"- Delivered at: {item.get('delivered_at') or '—'} ({_display_time_from_text(item.get('delivered_at'), config)})",
                    "",
                    str(item.get("content") or ""),
                ]
            )
        )
    if not messages:
        blocks.append("(no delivered proactive messages in the last 7 days)")
    return "\n\n".join(blocks) + "\n"


def _load_cron_jobs(home: Path) -> tuple[list[dict[str, Any]], Path]:
    logger = log.logger("snapshot")
    path = home / "cron" / "jobs.json"
    if not path.is_file():
        return [], path
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        log.warning_event(
            logger, "cron_jobs.unreadable", exc, stage="stage1", reason="unreadable_file"
        )
        return [], path
    try:
        payload = json.loads(raw)
    except ValueError as exc:
        log.warning_event(
            logger, "cron_jobs.unreadable", exc, stage="stage1", reason="invalid_json"
        )
        return [], path
    jobs = payload.get("jobs") if isinstance(payload, Mapping) else payload
    if isinstance(jobs, Mapping):
        jobs = list(jobs.values())
    if not isinstance(jobs, list):
        log.warning(logger, "cron_jobs.unreadable", stage="stage1", reason="invalid_shape")
        return [], path
    return [dict(job) for job in jobs if isinstance(job, Mapping)], path


def _proactive_cron_job(
    jobs: list[dict[str, Any]],
    cron_job_id: str | None = None,
) -> dict[str, Any] | None:
    if cron_job_id:
        for job in jobs:
            if str(job.get("id") or "") == cron_job_id:
                return job
    for job in jobs:
        job_id = str(job.get("id") or "")
        job_name = str(job.get("name") or "")
        if job_name == "proactive_notifier" or job_id == "proactive_notifier":
            return job
    return None


def _cron_archive_candidates(home: Path, job_id: str) -> list[Path]:
    output_dir = home / "cron" / "output" / job_id
    if not output_dir.is_dir():
        return []
    def sort_key(path: Path) -> tuple[float, str]:
        try:
            return path.stat().st_mtime, path.name
        except OSError:
            return 0.0, path.name

    return sorted(
        (path for path in output_dir.glob("*.md") if path.is_file()),
        key=sort_key,
        reverse=True,
    )


def _archive_effective_timestamp(path: Path, zone) -> datetime | None:
    """Return the archive's logical run timestamp for receipt ownership checks.

    The embedded ``Run Time`` and timestamp-shaped stem identify the logical
    run even when a test fixture or copied file has an unrelated mtime. If either
    logical signal predates the receipt, the archive is stale. The filesystem mtime
    is used only when neither logical timestamp can be parsed. Both logical
    signals carry no zone, so they are interpreted in the run's configured zone.
    """
    mtime: datetime | None = None
    try:
        mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
    except OSError:
        pass
    logical: list[datetime] = []
    try:
        stem_time = datetime.strptime(path.stem, "%Y-%m-%d_%H-%M-%S")
    except ValueError:
        pass
    else:
        logical.append(stem_time.replace(tzinfo=zone).astimezone(UTC))
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        log.warning_event(
            log.logger("snapshot"), "archive.unreadable", exc, stage="stage1", reason="unreadable_file"
        )
        return min(logical) if logical else mtime
    match = re.search(r"(?m)^\*\*Run Time:\*\*\s+(.+?)\s*$", text)
    if match is not None:
        try:
            parsed = datetime.fromisoformat(match.group(1).strip())
        except ValueError:
            parsed = None
        if parsed is not None:
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=zone)
            logical.append(parsed.astimezone(UTC))
    return min(logical) if logical else mtime


def _archive_response(path: Path | None) -> str | None:
    if path is None or not path.is_file():
        return None
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        log.warning_event(
            log.logger("snapshot"), "archive.unreadable", exc, stage="stage1", reason="unreadable_file"
        )
        return None
    header = text.split("\n---\n", 1)[0].split("\n## Prompt", 1)[0]
    if any(
        line.startswith(
            (
                "**Status:** no_change",
                "**Status:** silent",
                "Script gate returned `wakeAgent=false`",
            )
        )
        for line in header.splitlines()
    ):
        return None
    matches = list(re.finditer(r"(?m)^## Response\s*$", text))
    if not matches:
        return None
    response = text[matches[-1].end() :].strip()
    if not response or response.startswith("[CRON_FAILURE]"):
        return None
    return response


def _select_delivery_archive(
    home: Path,
    job_id: str,
    receipt: Mapping[str, Any],
    *,
    claimed_message_ids: set[str],
    zone,
) -> tuple[Path | None, str]:
    """Pick an eligible archive that can prove this receipt's delivery.

    Receipt creation time is the ownership boundary: an archive written before the
    receipt belongs to an earlier run and must not satisfy the new pending row. A
    one-minute tolerance keeps same-minute Cron fixtures deterministic. Archives
    already claimed by another delivered receipt are never reused, and every
    candidate is scanned so a later silent wake-gate archive cannot hide an
    earlier real response from the same eligible run.

    The second return value is the safe reason code for delivery reconciliation;
    it is empty when an archive was selected. Archives older than the receipt are
    ignored before reason selection, so the newest eligible archive decides the
    code instead of an older stale one masking it.
    """
    created_at = _parse_datetime(receipt.get("created_at"))
    if created_at is None:
        return None, "stale"
    earliest = created_at.timestamp() - 60
    candidates = _cron_archive_candidates(home, job_id)
    if not candidates:
        return None, "archive_missing"
    # Archives older than the receipt belong to earlier runs. Only the newest
    # eligible archive explains this receipt, so an older stale archive must not
    # mask a later silent/empty result from the same job directory.
    eligible: list[tuple[Path, str]] = []
    for archive in candidates:
        effective = _archive_effective_timestamp(archive, zone)
        if effective is None or effective.timestamp() < earliest:
            continue
        if archive.stem in claimed_message_ids:
            eligible.append((archive, "claimed"))
            continue
        response = _archive_response(archive)
        if response is None:
            eligible.append((archive, "empty"))
            continue
        if response.strip().upper() in {"[SILENT]", "NO_REPLY"}:
            eligible.append((archive, "silent"))
            continue
        return archive, ""
    if not eligible:
        return None, "stale"
    return None, eligible[0][1]


def _reconcile_pending_deliveries(
    home: Path,
    jobs: list[dict[str, Any]],
    now: datetime,
    config: Mapping[str, Any],
) -> int:
    """Resolve queued delivery receipts using the Cron job state and latest run archive.

    A receipt is only considered delivered when the job itself is healthy, has no delivery
    error, and the newest archive contains a real response body. Anything else is treated as
    suppressed or failed so the editorial gate can reconsider the reopened window instead of
    losing a reminder permanently.
    """
    logger = log.logger("snapshot")
    zone = _zone(config)
    rollback_count = 0
    claimed_message_ids = {
        str(item.get("message_id"))
        for item in storage.list_delivered_messages()
        if item.get("message_id")
    }
    for receipt in storage.list_pending_sent_messages():
        run_id = str(receipt.get("run_id") or "")
        job = _proactive_cron_job(jobs, receipt.get("cron_job_id"))
        job_id = str(job.get("id") or "") if job is not None else ""
        archive, reason = (
            _select_delivery_archive(
                home,
                job_id,
                receipt,
                claimed_message_ids=claimed_message_ids,
                zone=zone,
            )
            if job_id
            else (None, "job_missing")
        )
        if job is None:
            reason = "job_missing"
        elif job.get("last_status") != "ok":
            reason = "status_not_ok"
        elif job.get("last_delivery_error"):
            reason = "delivery_error"
        # Empty reason means the archive is eligible; any other code suppresses delivery.
        if not reason and archive is not None:
            delivered = storage.mark_delivery_delivered(
                run_id,
                cron_job_id=job_id,
                message_id=archive.stem,
                delivered_at=_utc_iso(now),
            )
            if delivered:
                claimed_message_ids.add(archive.stem)
                log.info(
                    logger,
                    "delivery.delivered",
                    stage="stage1",
                    run_id=run_id,
                    cron_job_id=job_id,
                    message_id=archive.stem,
                    delivery_status="delivered",
                )
            continue
        if storage.mark_delivery_failed(run_id):
            rollback_count += 1
            log.info(
                logger,
                "delivery.failed",
                stage="stage1",
                run_id=run_id,
                cron_job_id=job_id or None,
                reason=reason,
                delivery_status="failed",
                has_delivery_error=bool(job is not None and job.get("last_delivery_error")),
            )
    return rollback_count


def _schedule_text(job: Mapping[str, Any]) -> str:
    display = job.get("schedule_display")
    if display:
        return str(display)
    schedule = job.get("schedule")
    if isinstance(schedule, Mapping):
        return str(schedule.get("display") or schedule.get("expr") or schedule.get("minutes") or schedule)
    return str(schedule or "—")


def _is_runtime_managed_job(job: Mapping[str, Any]) -> bool:
    """True for this plugin's own Cron job, whichever id the host assigned it."""
    job_id = str(job.get("id") or "")
    job_name = str(job.get("name") or "")
    return job_id == "proactive_notifier" or job_name == "proactive_notifier"


def _render_scheduled_crons(
    jobs: list[dict[str, Any]],
    jobs_path: Path,
    collector: SourceCollector,
    *,
    config: Mapping[str, Any],
) -> tuple[str, list[dict[str, Any]]]:
    blocks = ["# Scheduled Cron jobs"]
    failing: list[dict[str, Any]] = []
    for job in jobs:
        job_id = str(job.get("id") or "—")
        name = str(job.get("name") or job_id)
        marker = collector.add(
            kind="scheduled-cron",
            origin=job_id,
            text=name,
            timestamp=job.get("next_run_at"),
            filename="scheduled-crons.md",
        )
        status = job.get("last_status")
        if status is not None and status != "ok":
            failing.append(job)
        next_run = _parse_datetime(job.get("next_run_at"))
        next_utc = _utc_iso(next_run, microseconds=False) if next_run is not None else "—"
        next_local = _display_time(next_run, config) if next_run is not None else "—"
        lines = [
            f"## {marker} {name}",
            f"- Job id: {job_id}",
            f"- Cadence: {_schedule_text(job)}",
            f"- Next run UTC: {next_utc}",
            f"- Next run local: {next_local}",
            f"- Last status: {status if status is not None else '—'}",
            f"- Definition path: {jobs_path}",
        ]
        if _is_runtime_managed_job(job):
            lines.append("- Managed by the runtime; cron tools cannot change it.")
        blocks.append("\n".join(lines))
    if not jobs:
        blocks.append("(no scheduled Cron jobs found)")
    return "\n\n".join(blocks) + "\n", failing


def _extract_keywords(
    new_messages: Iterable[Mapping[str, Any]],
    goals: Iterable[Mapping[str, Any]],
) -> list[str]:
    text_parts = [str(record.get("content") or "") for record in new_messages]
    text_parts.extend(str(goal.get("title") or "") for goal in goals)
    text = "\n".join(text_parts)
    candidates: list[str] = []
    candidates.extend(ASCII_WORD_RE.findall(text))
    for phrase in CJK_PHRASE_RE.findall(text):
        if len(phrase) > 12:
            candidates.extend(phrase[index : index + 8] for index in range(0, len(phrase) - 7, 4))
        else:
            candidates.append(phrase)
    stopwords = {"the", "and", "for", "with", "that", "this", "from", "user", "assistant"}
    seen: set[str] = set()
    keywords: list[str] = []
    for candidate in candidates:
        normalized = candidate.strip().lower()
        if len(normalized) < 2 or normalized in stopwords or normalized in seen:
            continue
        seen.add(normalized)
        keywords.append(candidate.strip())
    return keywords[:20]


def _render_memory(result: memory_source.MemoryReadResult, collector: SourceCollector) -> str:
    lines = [
        "# Relevant memories",
        "",
        f"<!-- proactive-memory source={result.source} status={result.status} -->",
    ]
    if result.status == memory_source.STATUS_DISABLED:
        lines.append("Memory reading is disabled (memory_source=none); no source was queried.")
        return "\n".join(lines) + "\n"
    if result.status == memory_source.STATUS_UNAVAILABLE:
        lines.append(
            f"Memory source is unavailable (reason: {result.reason}); no memory entries were read."
        )
        return "\n".join(lines) + "\n"
    if result.status == memory_source.STATUS_NO_MATCHES:
        if result.reason == "no_keywords":
            lines.append(
                "Memory source is available; no keywords were extracted this run, so it was not queried."
            )
        else:
            lines.append("Memory source is available, but no entries matched this run's keywords.")
        return "\n".join(lines) + "\n"

    lines.append(f"{len(result.records)} matching entr{'y' if len(result.records) == 1 else 'ies'}.")
    for record in result.records:
        origin = record.origin or record.target
        marker = collector.add(
            kind="memory",
            origin=origin,
            text=record.text,
            timestamp=record.timestamp,
            filename="memory.md",
            metadata=record.metadata,
        )
        block = [f"## {marker} {origin}", f"- target={record.target}"]
        if record.origin and record.origin != record.target:
            block.append(f"- URI: {record.origin}")
        if speaker := record.metadata.get("speaker"):
            block.append(f"- Speaker: {speaker}")
        if record.timestamp:
            block.append(f"- Updated at: {record.timestamp}")
        block.extend(["", record.text])
        lines.extend(["", "\n".join(block)])
    return "\n".join(lines) + "\n"


def _render_context(
    *,
    home: Path,
    now: datetime,
    preferences_path: Path,
    preferences: str,
    jobs: list[dict[str, Any]],
    failing_jobs: list[dict[str, Any]],
    memory_source_name: str,
    memory_status: str,
    collector: SourceCollector,
    config: Mapping[str, Any],
) -> str:
    marker = collector.add(
        kind="runtime-context",
        origin=str(home),
        text="Runtime context",
        timestamp=_utc_iso(now, microseconds=False),
        filename="context.md",
    )
    platforms = sorted(
        {
            str((job.get("origin") or {}).get("platform") or "").lower()
            for job in jobs
            if isinstance(job.get("origin"), Mapping)
        }
        - {""}
    )
    lines = [
        f"## {marker} Runtime context",
        f"- Current UTC: {_utc_iso(now, microseconds=False)}",
        f"- Current local: {_display_time(now, config)}",
        f"- Profile home: {home}",
        f"- Delivery platforms in Cron origins: {', '.join(platforms) if platforms else 'none recorded; delivery follows the Cron origin'}",
        f"- Memory source: {memory_source_name} · status: {memory_status}",
        "- Tool capabilities: follow the host tool registry",
        "- MCP servers: follow the host MCP registry",
        f"- Preferences: {preferences_path}",
        "",
        "## Failing Cron jobs",
    ]
    if failing_jobs:
        for job in failing_jobs:
            lines.append(
                f"- {job.get('id') or '—'} · {job.get('name') or '—'} · last_status={job.get('last_status')}"
            )
    else:
        lines.append("- None")
    lines.extend(["", "## Full preferences", "", preferences.rstrip()])
    return "\n".join(lines) + "\n"


def _create_snapshot_dir(hidden_files: Path, now: datetime, window_start: datetime) -> tuple[Path, str, str]:
    millis = int(now.timestamp() * 1000)
    digest = hashlib.sha256(
        f"{now.isoformat()}|{window_start.isoformat()}".encode("utf-8")
    ).hexdigest()[:6]
    name = f"{millis}-{digest}"
    hidden_files.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(hidden_files, 0o700)
    snapshot_dir = hidden_files / name
    snapshot_dir.mkdir(mode=0o700, exist_ok=True)
    os.chmod(snapshot_dir, 0o700)
    return snapshot_dir, name, f"run-{name}"


def build_snapshot(
    hermes_home: str | Path | None = None,
    now_utc: datetime | str | None = None,
    config: Mapping[str, Any] | None = None,
    preferences_file: str | None = None,
    force: bool = False,
) -> SnapshotResult:
    """Run Stage 1 and record one boundary event describing its outcome."""
    started = monotonic()
    logger = log.logger("snapshot")
    log.info(logger, "stage1.start", stage="stage1")
    try:
        result = _build_snapshot(
            hermes_home=hermes_home,
            now_utc=now_utc,
            config=config,
            preferences_file=preferences_file,
            force=force,
        )
    except Exception as exc:
        log.error_event(
            logger, "stage1.failed", exc, stage="stage1", duration_ms=log.elapsed_ms(started)
        )
        raise
    reason = str(result.get("reason") or "")
    fields = {
        "stage": "stage1",
        "run_id": result.get("run_id"),
        "snapshot_dir": result.get("snapshot_dir"),
        "reason": reason,
        "source_count": result.get("source_count"),
        "new_message_count": result.get("new_message_count"),
        "earlier_message_count": result.get("earlier_message_count"),
        "changed_goal_count": result.get("changed_goal_count"),
        "failing_cron_count": result.get("failing_cron_count"),
        "delivery_rollback_count": result.get("delivery_rollback_count"),
        "duration_ms": log.elapsed_ms(started),
    }
    if reason == "quiet_hours":
        log.info(logger, "stage1.quiet_hours", **fields)
    elif reason == "no_op":
        log.info(logger, "stage1.no_op", **fields)
    else:
        log.info(logger, "stage1.ready", **fields)
    return result


def _build_snapshot(
    hermes_home: str | Path | None = None,
    now_utc: datetime | str | None = None,
    config: Mapping[str, Any] | None = None,
    preferences_file: str | None = None,
    force: bool = False,
) -> SnapshotResult:
    """Build the six-file Stage 1 bundle, or return a quiet/no-op wake-gate result."""
    logger = log.logger("snapshot")
    now = _parse_datetime(now_utc) or datetime.now(tz=UTC)
    with _home_scope(hermes_home) as home:
        cfg = _build_config(home, config)
        log.configure_level(cfg.get("log_level"))
        if preferences_file is not None:
            cfg["preferences_file"] = preferences_file
        preferences_path = ensure_preferences_file(home, cfg)
        preferences = preferences_path.read_text(encoding="utf-8")

        if not force and not _within_active_window(now, cfg):
            return SnapshotResult(
                wake_agent=False,
                snapshot_dir=None,
                files={},
                source_map={},
                source_count=0,
                reason="quiet_hours",
                window_start_utc=None,
                window_end_utc=_utc_iso(now, microseconds=False),
                preferences_path=str(preferences_path),
            )

        storage.init_db()
        cron_jobs, cron_jobs_path = _load_cron_jobs(home)
        delivery_rollback_count = _reconcile_pending_deliveries(home, cron_jobs, now, cfg)
        superseded_run_ids = storage.list_pending_checkpoint_run_ids()
        superseded_count = storage.supersede_pending_checkpoints()
        if superseded_count:
            for superseded_run_id in superseded_run_ids:
                log.info(
                    logger,
                    "checkpoint.superseded",
                    stage="stage1",
                    run_id=superseded_run_id,
                    count=superseded_count,
                    checkpoint_status="superseded",
                )
        watermark_text = storage.latest_committed_watermark()
        window_start = _parse_datetime(watermark_text) or (now - timedelta(hours=24))
        active_goals = storage.list_tracking_items(status="active")

        hidden_files = storage.plugin_data_path() / "hidden_files"
        _prune_snapshots(hidden_files, now, int(cfg["retention_days"]))

        state_db = home / "state.db"
        conversation_records = _read_state_messages(
            state_db,
            window_start=window_start,
            window_end=now,
        )
        failing_jobs = [
            job
            for job in cron_jobs
            if job.get("last_status") is not None and job.get("last_status") != "ok"
        ]

        collector = SourceCollector()
        conversation_text, new_messages, earlier_messages = _render_conversation(
            conversation_records,
            collector,
            window_start=window_start,
            max_message_chars=int(cfg["max_message_chars"]),
            max_earlier_chars=int(cfg["max_earlier_chars"]),
        )
        tracking_text, changed_goals, time_sensitive_goals = _render_tracking(
            active_goals,
            collector,
            window_start=window_start,
            config=cfg,
        )
        sent_cutoff = now - timedelta(days=7)
        sent_rows = [
            row
            for row in storage.list_delivered_messages()
            if (delivered := _parse_datetime(row.get("delivered_at"))) is not None
            and delivered >= sent_cutoff
        ]
        sent_rows.sort(
            key=lambda row: _parse_datetime(row.get("delivered_at")) or datetime.min.replace(tzinfo=UTC),
            reverse=True,
        )
        sent_text = _render_sent(sent_rows, collector, config=cfg)
        scheduled_text, failing_jobs = _render_scheduled_crons(
            cron_jobs, cron_jobs_path, collector, config=cfg
        )

        if (
            len(new_messages) == 0
            and changed_goals == 0
            and time_sensitive_goals == 0
            and len(failing_jobs) == 0
            and delivery_rollback_count == 0
        ):
            return SnapshotResult(
                wake_agent=False,
                snapshot_dir=None,
                files={},
                source_map={},
                source_count=0,
                reason="no_op",
                window_start_utc=_utc_iso(window_start),
                window_end_utc=_utc_iso(now, microseconds=False),
                preferences_path=str(preferences_path),
                new_message_count=0,
                changed_goal_count=0,
                time_sensitive_goal_count=0,
                failing_cron_count=0,
                delivery_rollback_count=delivery_rollback_count,
            )

        keywords = _extract_keywords(new_messages, active_goals)
        selection = memory_source.select_source(cfg.get("memory_source"), home=home)
        memory_result = memory_source.read_memory(
            selection,
            keywords,
            limit=int(cfg["memory_limit"]),
            timeout_seconds=float(cfg["memory_timeout_seconds"]),
            tool_name=str(cfg["memory_tool"]),
            home=home,
        )
        memory_text = _render_memory(memory_result, collector)
        context_text = _render_context(
            home=home,
            now=now,
            preferences_path=preferences_path,
            preferences=preferences,
            jobs=cron_jobs,
            failing_jobs=failing_jobs,
            memory_source_name=memory_result.source,
            memory_status=memory_result.status,
            collector=collector,
            config=cfg,
        )

        snapshot_dir, snapshot_name, run_id = _create_snapshot_dir(hidden_files, now, window_start)
        file_contents = {
            "conversation.md": conversation_text,
            "tracking.md": tracking_text,
            "sent.md": sent_text,
            "scheduled-crons.md": scheduled_text,
            "memory.md": memory_text,
            "context.md": context_text,
        }
        for filename in SOURCE_FILENAMES:
            _write_private_file(snapshot_dir / filename, file_contents[filename])

        return SnapshotResult(
            wake_agent=True,
            snapshot_dir=str(snapshot_dir),
            snapshot_name=snapshot_name,
            run_id=run_id,
            files={name: str(snapshot_dir / name) for name in SOURCE_FILENAMES},
            source_map=collector.mapping,
            source_count=len(collector.mapping),
            reason="snapshot_ready",
            window_start_utc=_utc_iso(window_start),
            window_end_utc=_utc_iso(now, microseconds=False),
            preferences_path=str(preferences_path),
            new_message_count=len(new_messages),
            earlier_message_count=len(earlier_messages),
            changed_goal_count=changed_goals,
            time_sensitive_goal_count=time_sensitive_goals,
            failing_cron_count=len(failing_jobs),
            delivery_rollback_count=delivery_rollback_count,
            worker_timeout_seconds=int(cfg["worker_timeout_seconds"]),
            config=dict(cfg),
        )


def run_tick(
    hermes_home: str | Path | None = None,
    now_utc: datetime | str | None = None,
    llm_client: Any = None,
    force: bool = False,
    snapshot_only: bool = False,
    config: Mapping[str, Any] | None = None,
    preferences_file: str | None = None,
) -> SnapshotResult:
    """Run the deterministic snapshot stage and, unless disabled, Stage 2 research."""
    result = build_snapshot(
        hermes_home=hermes_home,
        now_utc=now_utc,
        config=config,
        preferences_file=preferences_file,
        force=force,
    )
    if not result.wake_agent:
        print('{"wakeAgent": false}')
        return result
    if snapshot_only:
        print(f"Snapshot: {result.snapshot_dir}")
        print(f"Sources: {result.source_count}")
        print(
            json.dumps(
                {
                    "wakeAgent": True,
                    "snapshot_dir": result.snapshot_dir,
                    "source_count": result.source_count,
                    "run_id": result.run_id,
                },
                ensure_ascii=False,
            )
        )
        return result

    from . import workers

    stage2 = workers.run_stage2(result, llm_client=llm_client)
    result.update(stage2)
    print(f"Snapshot: {result.snapshot_dir}")
    print(f"Sources: {result.source_count}")
    if stage2["candidate_count"] == 0:
        print('{"wakeAgent": false}')
        return result

    print("Edition Handoff")
    print(f"Run id: {stage2['run_id']}")
    print(f"Candidates: {stage2['candidate_count']}")
    for candidate in stage2["candidates"]:
        print(f"[item:{candidate['item_num']}] {candidate['title']}")
    print("Call proactivity_read to load this edition and review every candidate.")
    print(
        json.dumps(
            {
                "wakeAgent": True,
                "snapshot_dir": result.snapshot_dir,
                "source_count": result.source_count,
                "run_id": result.run_id,
                "candidate_count": stage2["candidate_count"],
                "next_tool": "proactivity_read",
            },
            ensure_ascii=False,
        )
    )
    return result


__all__ = [
    "DEFAULT_CONFIG",
    "SOURCE_FILENAMES",
    "SnapshotResult",
    "build_snapshot",
    "ensure_preferences_file",
    "read_live_conversation_diff",
    "resolve_preferences_path",
    "run_tick",
]

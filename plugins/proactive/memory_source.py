"""Memory source selection and read adapter for Stage 1.

The adapter owns the only two memory operations Stage 1 needs: resolving which
source this run uses, and reading keyword-matched records from it. All access to
built-in and external memory goes through the host's own memory interfaces, so
authentication, profile binding, parsing and injection scanning stay in one
place outside this plugin.
"""

from __future__ import annotations

import json
import re
import threading
from dataclasses import dataclass, field
from pathlib import Path
from time import monotonic
from typing import Any, Mapping

from . import log


CJK_RE = re.compile(r"[\u3400-\u9fff]")
BUILTIN_SOURCE = "builtin"
NONE_SOURCE = "none"
EXTERNAL_SOURCE = "external"

# Status vocabulary shared by memory.md and context.md.
STATUS_OK = "ok"
STATUS_NO_MATCHES = "no_matches"
STATUS_UNAVAILABLE = "unavailable"
STATUS_DISABLED = "disabled"

# Provider configs that start and own a local server during initialize(). Stage 1
# must not adopt that lifecycle from a short-lived cron child. Providers without a
# config marker can opt in explicitly through ``requires_local_service``.
MANAGED_LOCAL_DEPLOYMENTS = frozenset({"quick_local"})

_PROVIDER_RESULT_KEYS = ("memories", "results", "items", "data")


@dataclass(frozen=True)
class MemorySelection:
    """Which source this run reads, and why it was chosen."""

    category: str
    name: str
    reason: str


@dataclass(frozen=True)
class MemoryRecord:
    """One memory entry selected for the snapshot."""

    text: str
    target: str = "provider"
    origin: str = ""
    timestamp: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MemoryReadResult:
    """Outcome of one memory read, including the empty-state discriminator."""

    source: str
    status: str
    reason: str
    records: tuple[MemoryRecord, ...] = ()
    error_type: str = ""


def select_source(raw_value: Any, *, home: Path) -> MemorySelection:
    """Resolve the configured ``memory_source`` without a fallback chain."""
    value = str(raw_value or "").strip()
    lowered = value.lower()
    if not value:
        # No source can be named for an empty setting; "none" keeps the header
        # vocabulary valid while the reason code carries the config error.
        return MemorySelection(EXTERNAL_SOURCE, NONE_SOURCE, "invalid_config")
    if lowered == "none":
        return MemorySelection(NONE_SOURCE, NONE_SOURCE, "configured_none")
    if lowered in {"builtin", "built-in"}:
        return MemorySelection(BUILTIN_SOURCE, BUILTIN_SOURCE, "configured_builtin")
    if lowered == "auto":
        provider = _profile_memory_provider(home)
        if _is_core_provider(provider):
            return MemorySelection(BUILTIN_SOURCE, BUILTIN_SOURCE, "auto_builtin")
        return MemorySelection(EXTERNAL_SOURCE, provider, "auto_provider")
    return MemorySelection(EXTERNAL_SOURCE, value, "configured_provider")


def read_memory(
    selection: MemorySelection,
    keywords: list[str],
    *,
    limit: int,
    timeout_seconds: float,
    tool_name: str,
    home: Path,
) -> MemoryReadResult:
    """Read records for *keywords* from the selected source and emit one result event."""
    logger = log.logger("memory_source")
    log.info(
        logger,
        "memory_source.resolved",
        stage="stage1",
        source=selection.name,
        status=STATUS_OK,
        reason=selection.reason,
    )
    started = monotonic()
    if selection.category == NONE_SOURCE:
        result = MemoryReadResult(selection.name, STATUS_DISABLED, selection.reason)
    elif selection.category == BUILTIN_SOURCE:
        result = _read_builtin(keywords, limit=limit, selection=selection)
    elif selection.category == EXTERNAL_SOURCE:
        result = _read_external(
            selection,
            keywords,
            limit=limit,
            timeout_seconds=timeout_seconds,
            tool_name=tool_name,
            home=home,
        )
    else:
        result = MemoryReadResult(selection.name, STATUS_UNAVAILABLE, "invalid_config")

    fields = {
        "stage": "stage1",
        "source": result.source,
        "status": result.status,
        "reason": result.reason,
        "result_count": len(result.records),
        "duration_ms": log.elapsed_ms(started),
    }
    if result.status == STATUS_UNAVAILABLE:
        if result.error_type:
            fields["error_type"] = result.error_type
        event = "memory_source.timeout" if result.reason == "timeout" else "memory_source.unavailable"
        log.warning(logger, event, **fields)
    else:
        log.info(logger, "memory_source.read", **fields)
    return result


def _read_builtin(
    keywords: list[str],
    *,
    limit: int,
    selection: MemorySelection,
) -> MemoryReadResult:
    try:
        from tools.memory_tool import load_on_disk_store
    except Exception as exc:  # pragma: no cover - host package always present
        return MemoryReadResult(
            selection.name, STATUS_UNAVAILABLE, "builtin_unavailable", error_type=type(exc).__name__
        )
    try:
        store = load_on_disk_store()
    except Exception as exc:
        return MemoryReadResult(
            selection.name, STATUS_UNAVAILABLE, "builtin_unreadable", error_type=type(exc).__name__
        )
    if not keywords:
        return MemoryReadResult(selection.name, STATUS_NO_MATCHES, "no_keywords")

    candidates: list[tuple[str, str]] = []
    if store.target_enabled("memory"):
        candidates.extend(("memory", str(entry)) for entry in store.memory_entries)
    if store.target_enabled("user"):
        candidates.extend(("user", str(entry)) for entry in store.user_entries)

    seen: set[tuple[str, str]] = set()
    records: list[MemoryRecord] = []
    for target, text in candidates:
        if (target, text) in seen or not text.strip() or not _entry_is_safe(text):
            continue
        if not any(_keyword_matches(keyword, text) for keyword in keywords):
            continue
        seen.add((target, text))
        records.append(MemoryRecord(text=text, target=target, origin=target))
        if len(records) >= limit:
            break
    if not records:
        return MemoryReadResult(selection.name, STATUS_NO_MATCHES, "no_keyword_matches")
    return MemoryReadResult(selection.name, STATUS_OK, "matched", tuple(records))


def _entry_is_safe(text: str) -> bool:
    """Drop entries the host's injection scanner blocks for memory writes."""
    try:
        from tools.threat_patterns import scan_for_threats

        return not scan_for_threats(text, scope="strict")
    except Exception:  # pragma: no cover - host scanner always present
        return True


def _read_external(
    selection: MemorySelection,
    keywords: list[str],
    *,
    limit: int,
    timeout_seconds: float,
    tool_name: str,
    home: Path,
) -> MemoryReadResult:
    if selection.reason == "invalid_config":
        return MemoryReadResult(selection.name, STATUS_UNAVAILABLE, "invalid_config")

    try:
        from plugins.memory import find_provider_dir, find_provider_entry_point, load_memory_provider
    except Exception as exc:  # pragma: no cover - host package always present
        return MemoryReadResult(
            selection.name, STATUS_UNAVAILABLE, "provider_not_installed", error_type=type(exc).__name__
        )

    name = selection.name
    try:
        installed = find_provider_dir(name) is not None or find_provider_entry_point(name) is not None
    except Exception:
        installed = False
    try:
        provider = load_memory_provider(name, register_skills=False)
    except Exception as exc:
        return MemoryReadResult(name, STATUS_UNAVAILABLE, "provider_load_failed", error_type=type(exc).__name__)
    if provider is None:
        reason = "provider_load_failed" if installed else "provider_not_installed"
        return MemoryReadResult(name, STATUS_UNAVAILABLE, reason)

    if _requires_local_service(provider, name, home):
        _safe_shutdown(provider)
        return MemoryReadResult(name, STATUS_UNAVAILABLE, "local_service_required")

    # Closing is one-shot: a timed-out worker may still reach its finally block
    # after the adapter's own best-effort close has already run.
    close_lock = threading.Lock()
    closed = False

    def close_once() -> str:
        nonlocal closed
        with close_lock:
            if closed:
                return ""
            closed = True
        try:
            provider.shutdown()
        except Exception as exc:
            return type(exc).__name__
        return ""

    if not keywords:
        try:
            available = bool(provider.is_available())
        except Exception as exc:
            if shutdown_error := close_once():
                _log_shutdown_failure(name, STATUS_UNAVAILABLE, shutdown_error)
            return MemoryReadResult(
                name, STATUS_UNAVAILABLE, "provider_unavailable", error_type=type(exc).__name__
            )
        error_type = close_once()
        if error_type:
            _log_shutdown_failure(name, STATUS_NO_MATCHES, error_type)
        if not available:
            return MemoryReadResult(name, STATUS_UNAVAILABLE, "provider_unavailable")
        return MemoryReadResult(name, STATUS_NO_MATCHES, "no_keywords")

    outcome: dict[str, Any] = {}
    finished = threading.Event()
    session_id = "proactive-stage1"
    query = " ".join(keywords)

    def run() -> None:
        stage_reason = "provider_unavailable"
        try:
            if not provider.is_available():
                outcome.update(status=STATUS_UNAVAILABLE, reason="provider_unavailable")
                return
            stage_reason = "provider_initialize_failed"
            provider.initialize(
                session_id,
                hermes_home=str(home),
                platform="cli",
                agent_context="cron",
            )
            stage_reason = "provider_query_failed"
            records = _query_provider(
                provider,
                query,
                limit=limit,
                tool_name=tool_name,
                session_id=session_id,
            )
        except Exception as exc:
            outcome.update(
                status=STATUS_UNAVAILABLE,
                reason=stage_reason,
                error_type=type(exc).__name__,
            )
        else:
            if records:
                outcome.update(status=STATUS_OK, reason="matched", records=records)
            else:
                outcome.update(status=STATUS_NO_MATCHES, reason="no_results", records=())
        finally:
            if shutdown_error := close_once():
                outcome["shutdown_error_type"] = shutdown_error
            finished.set()

    def close_after_timeout() -> None:
        if shutdown_error := close_once():
            _log_shutdown_failure(name, STATUS_UNAVAILABLE, shutdown_error)

    thread = threading.Thread(target=run, name="proactive-memory-read", daemon=True)
    thread.start()
    if not finished.wait(timeout_seconds):
        # Do not join the wedged worker; a separate best-effort close still gives the
        # provider a chance to release sockets before this cron child exits.
        threading.Thread(
            target=close_after_timeout,
            name="proactive-memory-shutdown",
            daemon=True,
        ).start()
        return MemoryReadResult(name, STATUS_UNAVAILABLE, "timeout", error_type="TimeoutError")

    if shutdown_error := outcome.get("shutdown_error_type"):
        _log_shutdown_failure(name, str(outcome.get("status") or STATUS_UNAVAILABLE), shutdown_error)
    return MemoryReadResult(
        name,
        str(outcome.get("status") or STATUS_UNAVAILABLE),
        str(outcome.get("reason") or "provider_unavailable"),
        tuple(outcome.get("records") or ()),
        str(outcome.get("error_type") or ""),
    )


def _log_shutdown_failure(source: str, status: str, error_type: str) -> None:
    log.warning(
        log.logger("memory_source"),
        "memory_source.shutdown_failed",
        stage="stage1",
        source=source,
        status=status,
        reason="shutdown_failed",
        error_type=error_type,
    )


def _query_provider(
    provider: Any,
    query: str,
    *,
    limit: int,
    tool_name: str,
    session_id: str,
) -> tuple[MemoryRecord, ...]:
    schemas = []
    try:
        schemas = list(provider.get_tool_schemas() or [])
    except Exception:
        schemas = []
    names = {str(schema.get("name") or "") for schema in schemas if isinstance(schema, Mapping)}
    if tool_name and tool_name in names:
        raw = provider.handle_tool_call(tool_name, {"query": query, "limit": limit})
        return _records_from_tool_output(raw, limit=limit)
    text = provider.prefetch(query, session_id=session_id)
    if not text or not str(text).strip():
        return ()
    return (MemoryRecord(text=str(text), target="provider", origin="prefetch"),)


def _records_from_tool_output(raw: Any, *, limit: int) -> tuple[MemoryRecord, ...]:
    try:
        payload = json.loads(str(raw))
    except (TypeError, ValueError) as exc:
        raise ValueError("provider returned non-JSON tool output") from exc
    if not isinstance(payload, Mapping):
        raise ValueError("provider tool output is not an object")
    result = payload.get("result", payload)
    items: Any = None
    if isinstance(result, Mapping):
        for key in _PROVIDER_RESULT_KEYS:
            if isinstance(result.get(key), list):
                items = result[key]
                break
    elif isinstance(result, list):
        items = result
    if not isinstance(items, list):
        return ()
    records: list[MemoryRecord] = []
    for item in items:
        if len(records) >= limit:
            break
        if isinstance(item, Mapping):
            text = (
                item.get("abstract")
                or item.get("summary")
                or item.get("content")
                or item.get("text")
                or ""
            )
            records.append(
                MemoryRecord(
                    text=str(text),
                    target="provider",
                    origin=str(item.get("uri") or item.get("path") or item.get("id") or ""),
                    timestamp=str(item.get("updated_at") or item.get("created_at") or "") or None,
                    metadata={
                        "speaker": item.get("speaker"),
                        "score": item.get("score"),
                    },
                )
            )
        else:
            records.append(MemoryRecord(text=str(item), target="provider"))
    return tuple(record for record in records if record.text.strip())


def _keyword_matches(keyword: str, text: str) -> bool:
    """ASCII terms match case-insensitively; CJK phrases match verbatim."""
    if not keyword:
        return False
    if CJK_RE.search(keyword):
        return keyword in text
    return keyword.casefold() in text.casefold()


def _profile_config(home: Path) -> Mapping[str, Any]:
    path = home / "config.yaml"
    if not path.is_file():
        return {}
    try:
        import hermes_yaml as yaml

        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}
    return payload if isinstance(payload, Mapping) else {}


def _profile_memory_provider(home: Path) -> str:
    memory = _profile_config(home).get("memory")
    if not isinstance(memory, Mapping):
        return ""
    return str(memory.get("provider") or "").strip()


def _profile_memory_section(home: Path, name: str) -> Mapping[str, Any]:
    memory = _profile_config(home).get("memory")
    if not isinstance(memory, Mapping):
        return {}
    section = memory.get(name)
    return section if isinstance(section, Mapping) else {}


def _is_core_provider(name: str) -> bool:
    try:
        from agent.memory_provider import is_core_memory_provider

        return is_core_memory_provider(name)
    except Exception:  # pragma: no cover - host module always present
        return str(name or "").strip().lower() in {"", "default", "builtin", "built-in", "none"}


def _requires_local_service(provider: Any, name: str, home: Path) -> bool:
    flag = getattr(provider, "requires_local_service", None)
    if callable(flag):
        try:
            flag = flag()
        except Exception:
            flag = False
    if flag:
        return True
    deployment = str(_profile_memory_section(home, name).get("deployment") or "").strip().lower()
    return deployment in MANAGED_LOCAL_DEPLOYMENTS


def _safe_shutdown(provider: Any) -> None:
    try:
        provider.shutdown()
    except Exception as exc:
        _log_shutdown_failure(
            str(getattr(provider, "name", "") or "-"),
            STATUS_UNAVAILABLE,
            type(exc).__name__,
        )


__all__ = [
    "BUILTIN_SOURCE",
    "EXTERNAL_SOURCE",
    "NONE_SOURCE",
    "STATUS_DISABLED",
    "STATUS_NO_MATCHES",
    "STATUS_OK",
    "STATUS_UNAVAILABLE",
    "MemoryReadResult",
    "MemoryRecord",
    "MemorySelection",
    "read_memory",
    "select_source",
]

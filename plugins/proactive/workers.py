"""Stage 2 partitioned read-only research and evidence compilation."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from typing import Any, Mapping

from . import log
from . import snapshot as snapshot_module
from . import storage
from .schemas import WORKER1_OPPORTUNITIES_SCHEMA, WORKER2_PREPARED_SCHEMA


WORKER_TEMPERATURE = 0.1
DEFAULT_WORKER_TIMEOUT_SECONDS = 120.0
SOURCE_RE = re.compile(r"\[source:(\d+)\]")
_HOST_LLM: Any = None

WORKER1_INSTRUCTIONS = """\
You are the conversation follow-up research worker for a proactive notifier.
Read only the supplied conversation, active tracking, scheduled Cron, and
non-interference rules. Identify unresolved commitments, external blockers, and
time anchors that deserve a follow-up.

Rules:
- Never close, reopen, or edit a closed tracking item.
- New tracking items must be active and must have a clear future anchor, external
  blocker, or explicit user commitment.
- Existing active items may have their title, description, summary, and momentum
  updated conservatively.
- Do not propose items already covered by a healthy scheduled Cron job.
- Every candidate must cite one or more [source:N] markers from the supplied text.
- Return JSON only, matching the requested schema.
"""

WORKER2_INSTRUCTIONS = """\
You are the time-sensitivity and prepared-draft research worker for a proactive
notifier. Read only the supplied updated tracking, memories, runtime context, and
recent proactive delivery ledger. Propose deadline, state-change, and broken-Cron
candidates. Do not re-scan raw conversation turns.

Rules:
- Every candidate must include a short, natural initial draft.
- Do not duplicate covered healthy scheduled Cron jobs.
- Every candidate must cite one or more [source:N] markers from the supplied text.
- Return JSON only, matching the requested schema.
"""


class WorkerOutputError(ValueError):
    """Raised when a structured worker response cannot be safely normalized."""


class SourceRegistry:
    """Track run-wide source numbers while allowing newly tracked goals to append evidence."""

    def __init__(self, mapping: Mapping[int, Mapping[str, Any]] | None = None) -> None:
        self.mapping: dict[int, dict[str, Any]] = {
            int(number): dict(entry) for number, entry in (mapping or {}).items()
        }
        self._next = max(self.mapping, default=0) + 1

    def marker_for(
        self,
        *,
        kind: str,
        origin: str,
        text: str,
        timestamp: str | None,
        filename: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> str:
        for number, entry in self.mapping.items():
            if (
                entry.get("kind") == kind
                and entry.get("origin") == origin
                and entry.get("file") == filename
            ):
                return f"[source:{number}]"
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


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        log.warning_event(
            log.logger("workers"),
            "snapshot.file_missing",
            exc,
            stage="stage2",
            reason="missing_snapshot_file",
        )
        return ""


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
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
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def set_host_llm(llm_client: Any) -> None:
    """Remember the host-owned LLM client supplied during plugin registration."""
    global _HOST_LLM
    _HOST_LLM = llm_client


def _resolve_llm(llm_client: Any) -> Any:
    if llm_client is not None:
        return llm_client
    if _HOST_LLM is not None:
        return _HOST_LLM
    from agent.plugin_llm import PluginLlm

    return PluginLlm(plugin_id="proactive")


def _extract_non_interference(preferences: str) -> str:
    lines = preferences.splitlines()
    start = None
    for index, line in enumerate(lines):
        if line.strip().startswith("## ") and "non-interference" in line.lower():
            start = index
            break
    if start is None:
        return preferences.strip()
    end = len(lines)
    for index in range(start + 1, len(lines)):
        if lines[index].strip().startswith("## "):
            end = index
            break
    return "\n".join(lines[start:end]).strip()


def _worker1_input(snapshot_dir: Path, preferences_path: Path) -> str:
    conversation = _read_text(snapshot_dir / "conversation.md")
    tracking = _read_text(snapshot_dir / "tracking.md")
    scheduled = _read_text(snapshot_dir / "scheduled-crons.md")
    preferences = _read_text(preferences_path)
    rules = _extract_non_interference(preferences)
    return "\n\n".join(
        [
            "## conversation.md\n\n" + conversation.rstrip(),
            "## tracking.md\n\n" + tracking.rstrip(),
            "## scheduled-crons.md\n\n" + scheduled.rstrip(),
            "## Non-interference rules from preferences\n\n" + rules,
        ]
    )


def _worker2_input(snapshot_dir: Path) -> str:
    tracking = _read_text(snapshot_dir / "tracking.md")
    memory = _read_text(snapshot_dir / "memory.md")
    context = _read_text(snapshot_dir / "context.md")
    sent = _read_text(snapshot_dir / "sent.md")
    return "\n\n".join(
        [
            "## tracking.md\n\n" + tracking.rstrip(),
            "## memory.md\n\n" + memory.rstrip(),
            "## context.md\n\n" + context.rstrip(),
            "## sent.md\n\n" + sent.rstrip(),
        ]
    )


def _structured_payload(result: Any) -> dict[str, Any]:
    parsed = getattr(result, "parsed", None)
    if parsed is None and isinstance(result, Mapping):
        parsed = result
    if parsed is None:
        text = getattr(result, "text", None)
        if text:
            try:
                parsed = json.loads(text)
            except (TypeError, ValueError) as exc:
                raise WorkerOutputError("worker response was not valid JSON") from exc
    if not isinstance(parsed, Mapping):
        raise WorkerOutputError("worker response must be a JSON object")
    return dict(parsed)


def _raw_candidate_count(result: Any) -> int | None:
    """Count candidates straight from the structured reply without validating them."""
    payload = getattr(result, "parsed", None)
    if not isinstance(payload, Mapping):
        return None
    candidates = payload.get("candidates")
    return len(candidates) if isinstance(candidates, list) else None


def _require_text(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise WorkerOutputError(f"candidate field {field!r} is required")
    return text


def _extract_source_numbers(ref: Any) -> list[int]:
    if isinstance(ref, int):
        return [ref]
    text = str(ref).strip()
    if text.isdigit():
        return [int(text)]
    matches = re.findall(r"\[?\s*source:\s*(\d+)\s*\]?|\[(\d+)\]", text, re.IGNORECASE)
    nums: list[int] = []
    for g1, g2 in matches:
        val = g1 or g2
        if val:
            nums.append(int(val))
    return nums


def _normalize_candidate(
    candidate: Mapping[str, Any],
    *,
    require_draft: bool,
    valid_sources: Mapping[int, Any] | None = None,
) -> dict[str, Any]:
    priority = str(candidate.get("priority") or "").strip()
    if priority not in ("regular", "high"):
        raise WorkerOutputError("candidate priority must be regular or high")
    refs = candidate.get("evidence_refs")
    if not isinstance(refs, list) or not refs:
        raise WorkerOutputError("candidate evidence_refs must be a non-empty list")
    normalized_refs: list[str] = []
    for ref in refs:
        nums = _extract_source_numbers(ref)
        if not nums:
            raise WorkerOutputError(f"invalid evidence_ref {ref!r}; expected [source:N]")
        for num in nums:
            if valid_sources is not None and num not in valid_sources:
                raise WorkerOutputError(f"evidence_ref [source:{num}] does not exist in snapshot")
            tag = f"[source:{num}]"
            if tag not in normalized_refs:
                normalized_refs.append(tag)
    if not normalized_refs:
        raise WorkerOutputError("candidate evidence_refs must be a non-empty list")
    normalized = {
        "title": _require_text(candidate.get("title"), "title"),
        "priority": priority,
        "relevant_until": _require_text(candidate.get("relevant_until"), "relevant_until"),
        "why_it_matters": _require_text(candidate.get("why_it_matters"), "why_it_matters"),
        "evidence_refs": normalized_refs,
        "proposed_action": _require_text(candidate.get("proposed_action"), "proposed_action"),
        "what_remains": _require_text(candidate.get("what_remains"), "what_remains"),
    }
    if require_draft:
        normalized["draft"] = _require_text(candidate.get("draft"), "draft")
    return normalized


def _render_candidate(item: Mapping[str, Any]) -> str:
    refs = item.get("evidence_refs")
    if isinstance(refs, str):
        try:
            refs = json.loads(refs)
        except ValueError:
            refs = [refs]
    refs_text = ", ".join(str(ref) for ref in (refs or [])) or "—"
    lines = [
        f"## [item:{item['item_num']}] {item['title']}",
        f"- Priority: {item['priority']}",
        f"- Relevant until: {item['relevant_until']}",
        f"- Why it matters: {item['why_it_matters']}",
        f"- Evidence refs: {refs_text}",
        f"- Proposed action: {item['proposed_action']}",
        f"- What remains: {item['what_remains']}",
    ]
    if item.get("draft") is not None:
        lines.extend(["", "Draft:", str(item["draft"])])
    return "\n".join(lines)


def _render_opportunities(items: list[dict]) -> str:
    blocks = ["# Opportunities"]
    blocks.extend(_render_candidate(item) for item in items)
    if not items:
        blocks.append("(no conversation-driven candidates)")
    return "\n\n".join(blocks) + "\n"


def _render_prepared(items: list[dict]) -> str:
    blocks = ["# Prepared drafts"]
    blocks.extend(_render_candidate(item) for item in items)
    if not items:
        blocks.append("(no time-sensitive candidates)")
    return "\n\n".join(blocks) + "\n"


def _extract_source_block(snapshot_dir: Path, number: int, entry: Mapping[str, Any]) -> str | None:
    """Return the original snapshot block containing the marker, if it can be isolated."""
    path = snapshot_dir / str(entry.get("file") or "")
    text = _read_text(path)
    if not text:
        return None
    marker = f"[source:{number}]"
    lines = text.splitlines()
    marker_index = next((index for index, line in enumerate(lines) if marker in line), None)
    if marker_index is None:
        return None

    marker_line = lines[marker_index].strip()
    if marker_line.startswith("## "):
        start = marker_index
    else:
        start = marker_index
        while start > 0 and not lines[start].lstrip().startswith("## "):
            start -= 1

    # Tracking, memory, sent, scheduled, and context blocks begin with a
    # source-bearing heading. Conversation blocks begin with a surface heading
    # and carry their marker on a nested metadata line instead.
    conversation_block = not marker_line.startswith("## ")
    end = len(lines)
    for index in range(start + 1, len(lines)):
        stripped = lines[index].lstrip()
        if conversation_block:
            next_group = stripped in {"# New", "# Earlier"} or stripped.startswith(
                ("# New ·", "# Earlier ·")
            )
            next_message = stripped.startswith("## ") and any(
                "- [source:" in following for following in lines[index + 1 : index + 3]
            )
            if next_group or next_message:
                end = index
                break
        elif stripped.startswith("## [source:"):
            end = index
            break

    block = "\n".join(lines[start:end]).strip()
    return block or None


def _render_source_block(
    snapshot_dir: Path,
    number: int,
    entry: Mapping[str, Any],
) -> str:
    block = _extract_source_block(snapshot_dir, number, entry)
    if block is None:
        text = str(entry.get("text") or "").strip()
        block = "\n".join(
            [
                f"- Snapshot file: {entry.get('file') or '—'}",
                f"- Timestamp: {entry.get('timestamp') or '—'}",
                f"- Origin: {entry.get('origin') or '—'}",
                "",
                text or "(no inline text)",
            ]
        )
        return f"## [source:{number}] {entry.get('kind') or 'unknown'} · {entry.get('origin') or '—'}\n\n{block}"
    if block.splitlines() and block.splitlines()[0].startswith("## [source:"):
        return block
    return (
        f"## [source:{number}] {entry.get('kind') or 'unknown'} · {entry.get('origin') or '—'}\n\n"
        f"{block}"
    )


def _extract_ref_numbers(refs: Any) -> set[int]:
    if isinstance(refs, str):
        try:
            decoded = json.loads(refs)
        except ValueError:
            values = [refs]
        else:
            values = decoded if isinstance(decoded, list) else [refs]
    elif isinstance(refs, list):
        values = refs
    else:
        values = []
    numbers: set[int] = set()
    for ref in values:
        for num in _extract_source_numbers(ref):
            numbers.add(num)
    return numbers


def _render_sources(items: list[dict], registry: SourceRegistry, snapshot_dir: Path) -> str:
    cited: set[int] = set()
    for item in items:
        cited.update(_extract_ref_numbers(item.get("evidence_refs")))
    blocks = ["# Sources", "", "## Cited evidence"]
    valid_numbers = sorted(number for number in cited if number in registry.mapping)
    for number in valid_numbers:
        blocks.append(_render_source_block(snapshot_dir, number, registry.mapping[number]))
    if not valid_numbers:
        blocks.append("(no cited evidence)")

    sent_entries = sorted(
        (
            (number, entry)
            for number, entry in registry.mapping.items()
            if entry.get("kind") == "sent"
        ),
        key=lambda pair: pair[0],
    )
    blocks.extend(
        [
            "",
            "## 7-day sent summary",
            "",
            f"Delivered proactive messages in the last 7 days: {len(sent_entries)}",
        ]
    )
    for number, entry in sent_entries:
        blocks.append(
            f"- [source:{number}] {entry.get('timestamp') or '—'} · {entry.get('text') or ''}".rstrip()
        )
    return "\n\n".join(blocks) + "\n"


def _call_worker(
    llm: Any,
    *,
    worker: str,
    purpose: str,
    schema_name: str,
    instructions: str,
    text: str,
    json_schema: Mapping[str, Any],
    timeout: float,
    run_id: str,
) -> Any:
    """Run one structured worker call with lifecycle logging around it."""
    logger = log.logger("workers")
    log.info(
        logger,
        "worker.started",
        stage="stage2",
        run_id=run_id,
        worker=worker,
        purpose=purpose,
        schema=schema_name,
        timeout_seconds=timeout,
    )
    started = monotonic()
    try:
        response = llm.complete_structured(
            instructions=instructions,
            input=[{"type": "text", "text": text}],
            json_schema=json_schema,
            schema_name=schema_name,
            temperature=WORKER_TEMPERATURE,
            timeout=timeout,
            purpose=purpose,
        )
    except Exception as exc:
        log.error_event(
            logger,
            "worker.failed",
            exc,
            stage="stage2",
            run_id=run_id,
            worker=worker,
            purpose=purpose,
            duration_ms=log.elapsed_ms(started),
        )
        raise
    log.info(
        logger,
        "worker.finished",
        stage="stage2",
        run_id=run_id,
        worker=worker,
        purpose=purpose,
        duration_ms=log.elapsed_ms(started),
        candidate_count=_raw_candidate_count(response),
    )
    return response


def _parse_worker_payload(response: Any, *, worker: str, run_id: str) -> dict[str, Any]:
    try:
        return _structured_payload(response)
    except WorkerOutputError as exc:
        log.error_event(
            log.logger("workers"),
            "worker.parse_failed",
            exc,
            stage="stage2",
            run_id=run_id,
            worker=worker,
        )
        raise


def _normalize_candidates(
    payload: Mapping[str, Any],
    *,
    worker: str,
    worker_kind: str,
    require_draft: bool,
    valid_sources: Mapping[int, Any],
    run_id: str,
) -> list[dict[str, Any]]:
    try:
        return [
            _normalize_candidate(candidate, require_draft=require_draft, valid_sources=valid_sources)
            for candidate in (payload.get("candidates") or [])
            if isinstance(candidate, Mapping)
        ]
    except WorkerOutputError as exc:
        log.error_event(
            log.logger("workers"),
            "worker.normalize_failed",
            exc,
            stage="stage2",
            run_id=run_id,
            worker=worker,
            worker_kind=worker_kind,
        )
        raise


def _apply_tracking_upserts(upserts: list[dict], *, run_id: str) -> list[dict]:
    """Apply worker upserts and log outcome counts only — never goal text.

    ``reason="closed"`` means the target was already closed and the upsert was
    skipped, so it is reported as ``skipped_closed_count`` rather than implying
    the research worker closed a goal.
    """
    outcomes = storage.apply_worker_tracking_upserts(list(upserts or []))
    counts = {"created": 0, "updated": 0, "skipped_closed": 0, "invalid": 0}
    for outcome in outcomes:
        reason = str(outcome.get("reason") or "")
        if reason in {"created", "updated"}:
            counts[reason] += 1
        elif reason == "closed":
            counts["skipped_closed"] += 1
        else:
            counts["invalid"] += 1
    log.info(
        log.logger("workers"),
        "tracking.upserts",
        stage="stage2",
        run_id=run_id,
        created_count=counts["created"],
        updated_count=counts["updated"],
        skipped_closed_count=counts["skipped_closed"],
        invalid_count=counts["invalid"],
    )
    return outcomes


def run_stage2(snapshot_result: Mapping[str, Any], *, llm_client: Any = None) -> dict[str, Any]:
    """Run Stage 2 and record run-level start, outcome and failure events."""
    logger = log.logger("workers")
    run_id = str(snapshot_result.get("run_id") or "")
    started = monotonic()
    log.info(
        logger,
        "stage2.start",
        stage="stage2",
        run_id=run_id,
        snapshot_dir=snapshot_result.get("snapshot_dir"),
        timeout_seconds=snapshot_result.get("worker_timeout_seconds"),
    )
    try:
        result = _run_stage2(snapshot_result, llm_client=llm_client)
    except Exception as exc:
        log.error_event(
            logger,
            "stage2.failed",
            exc,
            stage="stage2",
            run_id=run_id,
            duration_ms=log.elapsed_ms(started),
        )
        raise
    log.info(
        logger,
        "stage2.finished",
        stage="stage2",
        run_id=run_id,
        candidate_count=result["candidate_count"],
        checkpoint_status=result["checkpoint_status"],
        duration_ms=log.elapsed_ms(started),
    )
    return result


def _run_stage2(snapshot_result: Mapping[str, Any], *, llm_client: Any = None) -> dict[str, Any]:
    """Run both partitioned workers, persist candidates, and decide the wake gate."""
    logger = log.logger("workers")
    llm = _resolve_llm(llm_client)
    snapshot_dir = Path(snapshot_result["snapshot_dir"])
    run_id = str(snapshot_result["run_id"])
    window_start = _parse_timestamp(snapshot_result.get("window_start_utc"))
    if window_start is None:
        window_start = datetime.now(timezone.utc)
    registry = SourceRegistry(snapshot_result.get("source_map") or {})

    storage.create_checkpoint(
        run_id,
        str(snapshot_dir),
        str(snapshot_result.get("window_start_utc") or ""),
        str(snapshot_result.get("window_end_utc") or ""),
        status="pending",
        candidate_count=0,
    )
    log.info(
        logger,
        "checkpoint.created",
        stage="stage2",
        run_id=run_id,
        snapshot_dir=str(snapshot_dir),
        window_start_utc=snapshot_result.get("window_start_utc"),
        window_end_utc=snapshot_result.get("window_end_utc"),
        checkpoint_status="pending",
    )

    worker_timeout = float(
        snapshot_result.get("worker_timeout_seconds") or DEFAULT_WORKER_TIMEOUT_SECONDS
    )

    worker1_response = _call_worker(
        llm,
        worker="worker1",
        purpose="proactive_worker1_opportunities",
        schema_name="worker1_opportunities_schema",
        instructions=WORKER1_INSTRUCTIONS,
        text=_worker1_input(snapshot_dir, Path(snapshot_result["preferences_path"])),
        json_schema=WORKER1_OPPORTUNITIES_SCHEMA,
        timeout=worker_timeout,
        run_id=run_id,
    )
    worker1_payload = _parse_worker_payload(worker1_response, worker="worker1", run_id=run_id)
    _apply_tracking_upserts(worker1_payload.get("tracking_upserts") or [], run_id=run_id)

    active_goals = storage.list_tracking_items(status="active")
    updated_tracking, _, _ = snapshot_module._render_tracking(
        active_goals,
        registry,
        window_start=window_start,
        config=snapshot_result.get("config") or snapshot_module.DEFAULT_CONFIG,
    )
    snapshot_module._write_private_file(snapshot_dir / "tracking.md", updated_tracking)

    worker2_response = _call_worker(
        llm,
        worker="worker2",
        purpose="proactive_worker2_prepared",
        schema_name="worker2_prepared_schema",
        instructions=WORKER2_INSTRUCTIONS,
        text=_worker2_input(snapshot_dir),
        json_schema=WORKER2_PREPARED_SCHEMA,
        timeout=worker_timeout,
        run_id=run_id,
    )
    worker2_payload = _parse_worker_payload(worker2_response, worker="worker2", run_id=run_id)

    worker1_candidates = _normalize_candidates(
        worker1_payload,
        worker="worker1",
        worker_kind="opportunities",
        require_draft=False,
        valid_sources=registry.mapping,
        run_id=run_id,
    )
    worker2_candidates = _normalize_candidates(
        worker2_payload,
        worker="worker2",
        worker_kind="prepared",
        require_draft=True,
        valid_sources=registry.mapping,
        run_id=run_id,
    )

    item_num = 1
    for candidate in worker1_candidates:
        storage.add_candidate_item(run_id, item_num, "opportunities", candidate)
        item_num += 1
    for candidate in worker2_candidates:
        storage.add_candidate_item(run_id, item_num, "prepared", candidate)
        item_num += 1

    persisted = storage.list_candidate_items(run_id)
    opportunities = [item for item in persisted if item["worker_kind"] == "opportunities"]
    prepared = [item for item in persisted if item["worker_kind"] == "prepared"]
    candidate_count = len(persisted)

    opportunities_text = _render_opportunities(opportunities)
    prepared_text = _render_prepared(prepared)
    sources_text = _render_sources(persisted, registry, snapshot_dir)
    snapshot_module._write_private_file(snapshot_dir / "opportunities.md", opportunities_text)
    snapshot_module._write_private_file(snapshot_dir / "prepared.md", prepared_text)
    snapshot_module._write_private_file(snapshot_dir / "sources.md", sources_text)

    # Zero candidates can advance the scan watermark immediately; any candidate
    # stays pending until the editorial gate records every send/skip decision.
    if candidate_count == 0:
        storage.commit_checkpoint(run_id, candidate_count=0)
        checkpoint_status = "committed"
    else:
        storage.set_checkpoint_pending(run_id, candidate_count)
        checkpoint_status = "pending"

    log.info(
        logger,
        "stage2.candidates",
        stage="stage2",
        run_id=run_id,
        candidate_count=candidate_count,
        source_count=len(registry.mapping),
        checkpoint_status=checkpoint_status,
        opportunities_count=len(opportunities),
        prepared_count=len(prepared),
    )
    if checkpoint_status == "committed":
        log.info(
            logger,
            "stage2.committed",
            stage="stage2",
            run_id=run_id,
            candidate_count=0,
            checkpoint_status="committed",
        )
    else:
        log.info(
            logger,
            "stage2.pending",
            stage="stage2",
            run_id=run_id,
            candidate_count=candidate_count,
            checkpoint_status="pending",
        )
        # Candidates stay pending for the editorial gate, which emits the handoff.
        log.info(
            logger,
            "stage2.handoff",
            stage="stage2",
            run_id=run_id,
            candidate_count=candidate_count,
            checkpoint_status="pending",
        )

    return {
        "run_id": run_id,
        "candidate_count": candidate_count,
        "checkpoint_status": checkpoint_status,
        "candidates": persisted,
        "opportunities": opportunities_text,
        "prepared": prepared_text,
        "sources": sources_text,
        "source_map": registry.mapping,
        "source_count": len(registry.mapping),
    }


__all__ = [
    "SourceRegistry",
    "WorkerOutputError",
    "WORKER1_INSTRUCTIONS",
    "WORKER2_INSTRUCTIONS",
    "run_stage2",
    "set_host_llm",
]

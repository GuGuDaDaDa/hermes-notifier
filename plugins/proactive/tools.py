"""JSON-returning agent tools for the proactive plugin."""

from __future__ import annotations

import json
from pathlib import Path
from time import monotonic
from typing import Any

from hermes_constants import get_hermes_home

from . import log, snapshot, storage


def _payload(args: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
    data: dict[str, Any] = {}
    if isinstance(args, dict):
        data.update(args)
    elif isinstance(args, str) and args.strip():
        decoded = json.loads(args)
        if not isinstance(decoded, dict):
            raise ValueError("args must be a JSON object")
        data.update(decoded)
    elif args is not None:
        raise ValueError("args must be a dict or JSON object string")
    data.update({key: value for key, value in kwargs.items() if value is not None})
    return data


def _error(message: str, action: str | None = None) -> str:
    payload: dict[str, Any] = {"ok": False, "error": message}
    if action:
        payload["action"] = action
    return json.dumps(payload, ensure_ascii=False)


def _read_text(path: Path, *, run_id: str | None = None) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        log.warning_event(
            log.logger("editorial"),
            "editorial.file_missing",
            exc,
            stage="stage3",
            run_id=run_id,
            reason="missing_snapshot_file",
        )
        return ""


def handle_proactivity_read(args: Any = None, **kwargs: Any) -> str:
    """Load a pending edition, its evidence, preferences, and post-snapshot conversation."""
    started = monotonic()
    logger = log.logger("editorial")
    attempted_run_id = ""
    try:
        payload = _payload(args, kwargs)
        run_id = str(payload.get("run_id") or "").strip()
        attempted_run_id = run_id
        storage.init_db()
        if run_id:
            checkpoint = storage.get_checkpoint(run_id)
            if checkpoint is None:
                raise KeyError(f"unknown run_id: {run_id}")
        else:
            checkpoint = storage.latest_pending_checkpoint()
            if checkpoint is None:
                raise LookupError("no pending checkpoint found")

        home = Path(get_hermes_home())
        snapshot_dir = Path(str(checkpoint["snapshot_dir"]))
        preferences_path = snapshot.ensure_preferences_file(
            home,
            snapshot._build_config(home, None),
        )
        live_diff = snapshot.read_live_conversation_diff(
            home / "state.db",
            checkpoint["window_end_utc"],
        )
        undecided = storage.list_undecided_candidate_items(checkpoint["run_id"])
        log.info(
            logger,
            "editorial.read",
            stage="stage3",
            run_id=checkpoint["run_id"],
            snapshot_dir=str(snapshot_dir),
            undecided_count=len(undecided),
            live_diff_count=len(live_diff),
            duration_ms=log.elapsed_ms(started),
        )
        return json.dumps(
            {
                "ok": True,
                "run_id": checkpoint["run_id"],
                "checkpoint": checkpoint,
                "opportunities": _read_text(
                    snapshot_dir / "opportunities.md", run_id=checkpoint["run_id"]
                ),
                "prepared": _read_text(snapshot_dir / "prepared.md", run_id=checkpoint["run_id"]),
                "sources": _read_text(snapshot_dir / "sources.md", run_id=checkpoint["run_id"]),
                "preferences": _read_text(preferences_path),
                "preferences_path": str(preferences_path),
                "candidate_items": undecided,
                "undecided_items": undecided,
                "live_conversation_diff": live_diff,
            },
            ensure_ascii=False,
        )
    except Exception as exc:
        log.error_event(
            logger,
            "editorial.read_failed",
            exc,
            stage="stage3",
            run_id=attempted_run_id or None,
            action="read",
            duration_ms=log.elapsed_ms(started),
        )
        return _error(str(exc), "read")


def handle_proactivity_select(args: Any = None, **kwargs: Any) -> str:
    """Record the editorial gate's complete decision set and commit the edition."""
    started = monotonic()
    logger = log.logger("editorial")
    attempted_run_id = ""
    try:
        payload = _payload(args, kwargs)
        attempted_run_id = str(payload.get("run_id") or "")
        storage.init_db()
        result = storage.commit_editorial_decisions(
            attempted_run_id,
            payload.get("decisions"),
            final_message=payload.get("final_message"),
            close_goal_ids=payload.get("close_goal_ids"),
        )
        decisions = result.get("decisions") or []
        send_count = sum(1 for entry in decisions if entry.get("decision") == "send")
        skip_count = sum(1 for entry in decisions if entry.get("decision") == "skip")
        log.info(
            logger,
            "editorial.select",
            stage="stage3",
            run_id=attempted_run_id,
            action=str(result.get("action") or ""),
            send_count=send_count,
            skip_count=skip_count,
            close_count=len(result.get("closed_goal_ids") or []),
            checkpoint_status="committed",
            pending_receipt=result.get("action") == "deliver",
            duration_ms=log.elapsed_ms(started),
        )
        return json.dumps(result, ensure_ascii=False)
    except Exception as exc:
        log.error_event(
            logger,
            "editorial.select_failed",
            exc,
            stage="stage3",
            run_id=attempted_run_id or None,
            action="select",
            duration_ms=log.elapsed_ms(started),
        )
        return _error(str(exc), "select")


def _tracking_result(
    action: str,
    payload: dict[str, Any],
    *,
    item: dict[str, Any] | None = None,
) -> str:
    """Log one tracking state transition (no titles or summaries) and return the JSON payload."""
    log.info(
        log.logger("editorial"),
        "tracking.manage",
        stage="stage3",
        action=action,
        goal_id=(item or {}).get("goal_id"),
        status=(item or {}).get("status"),
    )
    return json.dumps(payload, ensure_ascii=False)


def handle_tracking_manage(args: Any = None, **kwargs: Any) -> str:
    """Dispatch ``tracking_manage`` and always return a valid JSON string."""
    started = monotonic()
    logger = log.logger("editorial")
    action: str | None = None
    try:
        payload = _payload(args, kwargs)
        action = str(payload.get("action") or "").strip().lower()
        if not action:
            raise ValueError(
                "action is required; expected one of list, get, add, update, upsert, close, reopen"
            )

        storage.init_db()
        if action == "list":
            status = payload.get("status")
            status = str(status).strip().lower() if status is not None else None
            items = storage.list_tracking_items(status=status)
            return json.dumps(
                {"ok": True, "action": "list", "count": len(items), "items": items},
                ensure_ascii=False,
            )
        if action == "get":
            goal_id = str(payload.get("goal_id") or "").strip()
            if not goal_id:
                raise ValueError("goal_id is required for get")
            return json.dumps(
                {"ok": True, "action": "get", "item": storage.get_tracking_item(goal_id)},
                ensure_ascii=False,
            )
        if action in {"add", "update", "upsert"}:
            item = storage.upsert_tracking_item(
                goal_id=str(payload.get("goal_id") or ""),
                title=payload.get("title"),
                description=payload.get("description"),
                summary=payload.get("summary"),
                momentum_status=payload.get("momentum_status"),
                source_ref=payload.get("source_ref"),
            )
            return _tracking_result(action, {"ok": True, "action": action, "item": item}, item=item)
        if action == "close":
            item = storage.close_tracking_item(
                str(payload.get("goal_id") or ""),
                summary=payload.get("summary"),
            )
            return _tracking_result("close", {"ok": True, "action": "close", "item": item}, item=item)
        if action == "reopen":
            item = storage.reopen_tracking_item(
                str(payload.get("goal_id") or ""),
                summary=payload.get("summary"),
                momentum_status=payload.get("momentum_status"),
            )
            return _tracking_result(
                "reopen", {"ok": True, "action": "reopen", "item": item}, item=item
            )
        raise ValueError(
            f"unknown action {action!r}; expected one of list, get, add, update, upsert, close, reopen"
        )
    except Exception as exc:
        log.error_event(
            logger,
            "tracking.manage_failed",
            exc,
            stage="stage3",
            action=action,
            duration_ms=log.elapsed_ms(started),
        )
        return _error(str(exc), action)

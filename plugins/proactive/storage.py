"""Durable SQLite state for the proactive plugin."""

from __future__ import annotations

import contextlib
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

from plugins.plugin_storage import plugin_data_dir, plugin_db

from .schemas import DECISION_REASONS


PLUGIN_NAME = "proactive"
DB_FILENAME = "data.db"

VALID_STATUSES = ("active", "closed")
MOMENTUM_STATUSES = ("on_track", "waiting", "stalled", "time_sensitive")
DECISION_VALUES = ("send", "skip")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS checkpoints (
    run_id TEXT PRIMARY KEY,
    snapshot_dir TEXT NOT NULL,
    window_start_utc TEXT NOT NULL,
    window_end_utc TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'committed', 'superseded', 'failed')),
    candidate_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    committed_at TEXT
);

CREATE TABLE IF NOT EXISTS tracking_items (
    goal_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    summary TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'closed')) DEFAULT 'active',
    momentum_status TEXT NOT NULL DEFAULT 'on_track',
    source_ref TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    closed_at TEXT
);

CREATE TABLE IF NOT EXISTS candidate_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES checkpoints(run_id),
    item_num INTEGER NOT NULL,
    worker_kind TEXT NOT NULL CHECK (worker_kind IN ('opportunities', 'prepared')),
    title TEXT NOT NULL,
    priority TEXT NOT NULL CHECK (priority IN ('regular', 'high')),
    relevant_until TEXT NOT NULL,
    why_it_matters TEXT NOT NULL,
    evidence_refs TEXT NOT NULL,
    proposed_action TEXT NOT NULL,
    what_remains TEXT NOT NULL,
    draft TEXT,
    decision TEXT CHECK (decision IN ('send', 'skip')),
    decision_reason TEXT CHECK (decision_reason IN ('time_sensitive', 'changed', 'already_handled', 'moot', 'unconfirmed', 'preferences', 'not_useful_now')),
    decision_note TEXT,
    decided_at TEXT,
    UNIQUE (run_id, item_num)
);

CREATE TABLE IF NOT EXISTS sent_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL UNIQUE REFERENCES checkpoints(run_id),
    cron_job_id TEXT,
    message_id TEXT,
    content TEXT NOT NULL,
    delivery_status TEXT NOT NULL CHECK (delivery_status IN ('pending', 'delivered', 'failed')),
    created_at TEXT NOT NULL,
    delivered_at TEXT
);
"""


def utc_now_iso() -> str:
    """Return a UTC timestamp with microsecond precision and an explicit ``Z`` suffix."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _advance_timestamp(previous: str | None) -> str:
    """Return a timestamp strictly later than *previous* even for back-to-back updates."""
    now = datetime.now(timezone.utc)
    prior = _parse_timestamp(previous)
    if prior is not None and now <= prior:
        now = prior + timedelta(microseconds=1)
    return now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


@contextlib.contextmanager
def _connection() -> Iterator[sqlite3.Connection]:
    with contextlib.closing(plugin_db(PLUGIN_NAME, DB_FILENAME)) as conn:
        conn.row_factory = sqlite3.Row
        yield conn


@contextlib.contextmanager
def _write_connection() -> Iterator[sqlite3.Connection]:
    with _connection() as conn:
        try:
            with conn:
                yield conn
        except BaseException:
            conn.rollback()
            raise


def plugin_data_path():
    """Return the profile-scoped directory that owns the database and hidden snapshots."""
    return plugin_data_dir(PLUGIN_NAME)


def init_db() -> None:
    """Create the durable schema idempotently.

    No fixture rows are inserted: tracking items belong to the installing user
    and must be created explicitly through ``tracking_manage`` or the CLI.
    """
    with _write_connection() as conn:
        conn.executescript(SCHEMA_SQL)


def latest_committed_watermark() -> str | None:
    """Return the newest committed scan boundary, or ``None`` before any completed run."""
    with _connection() as conn:
        values = [
            row["window_end_utc"]
            for row in conn.execute(
                "SELECT window_end_utc FROM checkpoints WHERE status = 'committed'"
            ).fetchall()
        ]
    candidates = [(value, _parse_timestamp(value)) for value in values]
    candidates = [(value, stamp) for value, stamp in candidates if stamp is not None]
    if not candidates:
        return None
    return max(candidates, key=lambda entry: entry[1])[0]


def latest_pending_checkpoint() -> dict | None:
    """Return the newest edition still awaiting an editorial decision."""
    with _connection() as conn:
        row = conn.execute(
            """
            SELECT * FROM checkpoints
             WHERE status = 'pending'
             ORDER BY created_at DESC, rowid DESC
             LIMIT 1
            """
        ).fetchone()
        return _row_to_dict(row)


def list_pending_checkpoint_run_ids() -> list[str]:
    """Return every edition still awaiting an editorial decision, oldest first."""
    with _connection() as conn:
        rows = conn.execute(
            """
            SELECT run_id FROM checkpoints
             WHERE status = 'pending'
             ORDER BY created_at ASC, rowid ASC
            """
        ).fetchall()
    return [str(row["run_id"]) for row in rows]


def supersede_pending_checkpoints() -> int:
    """Retire interrupted editions so the next scan resumes from the committed watermark."""
    with _write_connection() as conn:
        cursor = conn.execute(
            "UPDATE checkpoints SET status = 'superseded' WHERE status = 'pending'"
        )
        return cursor.rowcount


def list_delivered_messages() -> list[dict]:
    """Return all delivered proactive messages; callers apply their own time window."""
    with _connection() as conn:
        rows = conn.execute(
            """
            SELECT * FROM sent_log
             WHERE delivery_status = 'delivered'
               AND delivered_at IS NOT NULL
             ORDER BY delivered_at DESC, id DESC
            """
        ).fetchall()
        return [dict(row) for row in rows]


def list_pending_sent_messages() -> list[dict]:
    """Return delivery receipts that still need confirmation from the Cron runtime."""
    with _connection() as conn:
        rows = conn.execute(
            """
            SELECT s.*, c.status AS checkpoint_status, c.window_end_utc
              FROM sent_log AS s
              JOIN checkpoints AS c ON c.run_id = s.run_id
             WHERE s.delivery_status = 'pending'
             ORDER BY s.created_at ASC, s.id ASC
            """
        ).fetchall()
        return [dict(row) for row in rows]


def mark_delivery_delivered(
    run_id: str,
    *,
    cron_job_id: str,
    message_id: str,
    delivered_at: str,
) -> bool:
    """Promote one pending receipt after the Cron archive confirms a real delivery."""
    run_id = _require_text(run_id, "run_id")
    with _write_connection() as conn:
        cursor = conn.execute(
            """
            UPDATE sent_log
               SET delivery_status = 'delivered',
                   delivered_at = ?,
                   cron_job_id = ?,
                   message_id = ?
             WHERE run_id = ? AND delivery_status = 'pending'
            """,
            (delivered_at, cron_job_id, message_id, run_id),
        )
        return cursor.rowcount == 1


def mark_delivery_failed(run_id: str) -> bool:
    """Fail one pending receipt and reopen the checkpoint whose window must be rescanned.

    The checkpoint deliberately moves to ``superseded`` rather than remaining committed:
    watermark resolution then falls back to the previous committed boundary, which is the
    only safe way to ensure a suppressed reminder is considered again. The two writes share
    one transaction so a failed receipt can never leave its window permanently advanced.
    """
    run_id = _require_text(run_id, "run_id")
    with _write_connection() as conn:
        cursor = conn.execute(
            """
            UPDATE sent_log
               SET delivery_status = 'failed'
             WHERE run_id = ? AND delivery_status = 'pending'
            """,
            (run_id,),
        )
        if cursor.rowcount != 1:
            return False
        conn.execute(
            """
            UPDATE checkpoints
               SET status = 'superseded'
             WHERE run_id = ? AND status = 'committed'
            """,
            (run_id,),
        )
        return True


def delivery_counts() -> dict[str, int]:
    """Count receipts by delivery state for operator-facing status output."""
    counts = {"pending": 0, "delivered": 0, "failed": 0}
    with _connection() as conn:
        rows = conn.execute(
            "SELECT delivery_status, COUNT(*) AS count FROM sent_log GROUP BY delivery_status"
        ).fetchall()
    for row in rows:
        counts[str(row["delivery_status"])] = int(row["count"])
    return counts


def list_recent_checkpoints(limit: int = 5) -> list[dict]:
    """Return recent editions newest-first for status and history rendering."""
    with _connection() as conn:
        rows = conn.execute(
            """
            SELECT * FROM checkpoints
             ORDER BY created_at DESC, rowid DESC
             LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()
        return [dict(row) for row in rows]


def create_checkpoint(
    run_id: str,
    snapshot_dir: str,
    window_start_utc: str,
    window_end_utc: str,
    *,
    status: str = "pending",
    candidate_count: int = 0,
    created_at: str | None = None,
    committed_at: str | None = None,
) -> dict:
    """Insert one edition checkpoint before any candidate rows are attached to it."""
    run_id = _require_text(run_id, "run_id")
    if status not in ("pending", "committed", "superseded", "failed"):
        raise ValueError("invalid checkpoint status")
    now = created_at or utc_now_iso()
    with _write_connection() as conn:
        conn.execute(
            """
            INSERT INTO checkpoints (
                run_id, snapshot_dir, window_start_utc, window_end_utc,
                status, candidate_count, created_at, committed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                snapshot_dir,
                window_start_utc,
                window_end_utc,
                status,
                max(0, int(candidate_count)),
                now,
                committed_at,
            ),
        )
        row = conn.execute("SELECT * FROM checkpoints WHERE run_id = ?", (run_id,)).fetchone()
    result = _row_to_dict(row)
    assert result is not None
    return result


def get_checkpoint(run_id: str) -> dict | None:
    with _connection() as conn:
        row = conn.execute("SELECT * FROM checkpoints WHERE run_id = ?", (run_id,)).fetchone()
        return _row_to_dict(row)


def set_checkpoint_pending(run_id: str, candidate_count: int) -> None:
    """Keep the checkpoint unresolved so the editorial gate owns the watermark decision."""
    with _write_connection() as conn:
        conn.execute(
            """
            UPDATE checkpoints
               SET status = 'pending', candidate_count = ?, committed_at = NULL
             WHERE run_id = ?
            """,
            (max(0, int(candidate_count)), run_id),
        )


def commit_checkpoint(
    run_id: str,
    *,
    candidate_count: int = 0,
    committed_at: str | None = None,
) -> None:
    """Advance the scan watermark only after a run has no unresolved candidates."""
    with _write_connection() as conn:
        conn.execute(
            """
            UPDATE checkpoints
               SET status = 'committed', candidate_count = ?, committed_at = ?
             WHERE run_id = ?
            """,
            (max(0, int(candidate_count)), committed_at or utc_now_iso(), run_id),
        )


def add_candidate_item(run_id: str, item_num: int, worker_kind: str, candidate: dict) -> dict:
    """Persist one numbered candidate with evidence references encoded as JSON text."""
    if worker_kind not in ("opportunities", "prepared"):
        raise ValueError("worker_kind must be opportunities or prepared")
    refs = candidate.get("evidence_refs") or []
    if not isinstance(refs, str):
        refs = json.dumps([str(ref) for ref in refs], ensure_ascii=False)
    with _write_connection() as conn:
        conn.execute(
            """
            INSERT INTO candidate_items (
                run_id, item_num, worker_kind, title, priority, relevant_until,
                why_it_matters, evidence_refs, proposed_action, what_remains, draft
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                int(item_num),
                worker_kind,
                str(candidate.get("title") or ""),
                str(candidate.get("priority") or "regular"),
                str(candidate.get("relevant_until") or ""),
                str(candidate.get("why_it_matters") or ""),
                refs,
                str(candidate.get("proposed_action") or ""),
                str(candidate.get("what_remains") or ""),
                candidate.get("draft"),
            ),
        )
        row = conn.execute(
            "SELECT * FROM candidate_items WHERE run_id = ? AND item_num = ?",
            (run_id, int(item_num)),
        ).fetchone()
    result = _row_to_dict(row)
    assert result is not None
    return result


def list_candidate_items(run_id: str) -> list[dict]:
    with _connection() as conn:
        rows = conn.execute(
            "SELECT * FROM candidate_items WHERE run_id = ? ORDER BY item_num ASC",
            (run_id,),
        ).fetchall()
        return [dict(row) for row in rows]


def list_undecided_candidate_items(run_id: str) -> list[dict]:
    """Return only candidates that have not received a recorded disposition yet."""
    with _connection() as conn:
        rows = conn.execute(
            """
            SELECT * FROM candidate_items
             WHERE run_id = ? AND decision IS NULL
             ORDER BY item_num ASC
            """,
            (run_id,),
        ).fetchall()
        return [dict(row) for row in rows]


def _normalize_item_num(value: object) -> int:
    if isinstance(value, bool):
        raise ValueError("item_num must be a positive integer")
    if isinstance(value, int):
        number = value
    elif isinstance(value, str) and value.strip().isdigit():
        number = int(value.strip())
    else:
        raise ValueError("item_num must be a positive integer")
    if number <= 0:
        raise ValueError("item_num must be a positive integer")
    return number


def _normalize_decisions(decisions: object, expected_numbers: set[int]) -> list[dict]:
    if not isinstance(decisions, list):
        raise ValueError("decisions must be an array")

    normalized: list[dict] = []
    seen: set[int] = set()
    for raw in decisions:
        if not isinstance(raw, dict):
            raise ValueError("each decision must be an object")
        item_num = _normalize_item_num(raw.get("item_num"))
        if item_num in seen:
            raise ValueError(f"duplicate item_num {item_num}")
        if item_num not in expected_numbers:
            raise ValueError(f"unknown item_num {item_num}")
        decision = str(raw.get("decision") or "").strip().lower()
        if decision not in DECISION_VALUES:
            raise ValueError(
                f"item {item_num}: decision must be one of {DECISION_VALUES}, got {decision!r}"
            )
        reason = str(raw.get("reason") or "").strip().lower()
        if reason not in DECISION_REASONS:
            raise ValueError(
                f"item {item_num}: reason must be one of {DECISION_REASONS}, got {reason!r}"
            )
        note = raw.get("note")
        if note is not None and not isinstance(note, str):
            raise ValueError(f"item {item_num}: note must be a string")
        seen.add(item_num)
        normalized.append(
            {
                "item_num": item_num,
                "decision": decision,
                "reason": reason,
                "note": note.strip() if isinstance(note, str) else None,
            }
        )

    missing = sorted(expected_numbers - seen)
    if missing:
        raise ValueError(f"missing decisions for item_num: {', '.join(map(str, missing))}")
    return sorted(normalized, key=lambda entry: entry["item_num"])


def commit_editorial_decisions(
    run_id: str,
    decisions: object,
    *,
    final_message: str | None = None,
    close_goal_ids: object = None,
) -> dict:
    """Validate and atomically record one edition's complete editorial decision.

    Every candidate in the run must be covered before any mutation occurs. The
    candidate decisions, verified goal closures, checkpoint commitment, and
    pending delivery row share one transaction so a failure cannot leave the
    watermark advanced without an auditable disposition or delivery record.
    """
    run_id = _require_text(run_id, "run_id")
    if close_goal_ids is None:
        goal_ids: list[str] = []
    elif isinstance(close_goal_ids, list):
        goal_ids = []
        for value in close_goal_ids:
            if not isinstance(value, str) or not value.strip():
                raise ValueError("close_goal_ids must contain non-empty goal ids")
            goal_ids.append(value.strip())
    else:
        raise ValueError("close_goal_ids must be an array of goal ids")

    with _write_connection() as conn:
        checkpoint = conn.execute(
            "SELECT * FROM checkpoints WHERE run_id = ?", (run_id,)
        ).fetchone()
        if checkpoint is None:
            raise KeyError(f"unknown run_id: {run_id}")
        if checkpoint["status"] != "pending":
            raise ValueError(
                f"checkpoint {run_id} is {checkpoint['status']}, expected pending"
            )

        candidate_rows = conn.execute(
            "SELECT * FROM candidate_items WHERE run_id = ? ORDER BY item_num ASC",
            (run_id,),
        ).fetchall()
        if not candidate_rows:
            raise ValueError(f"checkpoint {run_id} has no candidates")
        expected_numbers = {int(row["item_num"]) for row in candidate_rows}
        normalized = _normalize_decisions(decisions, expected_numbers)

        any_send = any(entry["decision"] == "send" for entry in normalized)
        message = (final_message or "").strip() if isinstance(final_message, str) else ""
        if any_send and (not message or message.upper() in {"[SILENT]", "NO_REPLY"}):
            raise ValueError("final_message is required and must be non-empty when any decision is send")

        goals: list[sqlite3.Row] = []
        seen_goals: set[str] = set()
        for goal_id in goal_ids:
            if goal_id in seen_goals:
                continue
            seen_goals.add(goal_id)
            row = conn.execute(
                "SELECT * FROM tracking_items WHERE goal_id = ?", (goal_id,)
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown close goal_id: {goal_id}")
            goals.append(row)

        decided_at = utc_now_iso()
        for entry in normalized:
            conn.execute(
                """
                UPDATE candidate_items
                   SET decision = ?, decision_reason = ?, decision_note = ?, decided_at = ?
                 WHERE run_id = ? AND item_num = ?
                """,
                (
                    entry["decision"],
                    entry["reason"],
                    entry["note"],
                    decided_at,
                    run_id,
                    entry["item_num"],
                ),
            )

        for row in goals:
            updated_at = _advance_timestamp(row["updated_at"])
            # Re-closing keeps the original closure time while still advancing updated_at.
            closed_at = row["closed_at"] or updated_at
            conn.execute(
                """
                UPDATE tracking_items
                   SET status = 'closed', updated_at = ?, closed_at = ?
                 WHERE goal_id = ?
                """,
                (updated_at, closed_at, row["goal_id"]),
            )

        committed_at = utc_now_iso()
        cursor = conn.execute(
            """
            UPDATE checkpoints
               SET status = 'committed', committed_at = ?
             WHERE run_id = ? AND status = 'pending'
            """,
            (committed_at, run_id),
        )
        if cursor.rowcount != 1:
            raise ValueError(f"checkpoint {run_id} could not be committed")

        if any_send:
            conn.execute(
                """
                INSERT INTO sent_log (
                    run_id, content, delivery_status, created_at
                ) VALUES (?, ?, 'pending', ?)
                """,
                (run_id, message, committed_at),
            )

    result: dict = {
        "ok": True,
        "run_id": run_id,
        "action": "deliver" if any_send else "silent",
        "committed_at": committed_at,
        "decisions": normalized,
        "closed_goal_ids": [row["goal_id"] for row in goals],
    }
    if any_send:
        result["final_message"] = message
    else:
        result["reply_token"] = "[SILENT]"
    return result


def _normalize_source_ref(val: Any) -> str | None:
    if val is None:
        return None
    if isinstance(val, (list, tuple)):
        joined = ", ".join(str(item).strip() for item in val if str(item).strip())
        return joined or None
    text = str(val).strip()
    return text or None


def apply_worker_tracking_upserts(upserts: list[dict]) -> list[dict]:
    """Apply research-worker upserts while preserving every closed item verbatim.

    Worker-provided status and closure fields are deliberately ignored: only the
    editorial gate or an explicit user action may close or reopen a goal.
    """
    outcomes: list[dict] = []
    with _write_connection() as conn:
        for raw in upserts or []:
            if not isinstance(raw, dict):
                outcomes.append({"goal_id": "", "applied": False, "reason": "invalid"})
                continue
            goal_id = str(raw.get("goal_id") or "").strip()
            if not goal_id:
                outcomes.append({"goal_id": "", "applied": False, "reason": "missing_goal_id"})
                continue
            existing = conn.execute(
                "SELECT * FROM tracking_items WHERE goal_id = ?", (goal_id,)
            ).fetchone()
            if existing is not None and existing["status"] == "closed":
                outcomes.append({"goal_id": goal_id, "applied": False, "reason": "closed"})
                continue

            momentum = raw.get("momentum_status")
            if momentum is not None and momentum not in MOMENTUM_STATUSES:
                outcomes.append({"goal_id": goal_id, "applied": False, "reason": "invalid_momentum"})
                continue

            item_source_ref = _normalize_source_ref(raw.get("source_ref"))
            if existing is None:
                title = str(raw.get("title") or "").strip()
                description = str(raw.get("description") or "").strip()
                summary = str(raw.get("summary") or "").strip()
                if not title or not description or not summary:
                    outcomes.append(
                        {"goal_id": goal_id, "applied": False, "reason": "incomplete_new_goal"}
                    )
                    continue
                now = utc_now_iso()
                conn.execute(
                    """
                    INSERT INTO tracking_items (
                        goal_id, title, description, summary, status, momentum_status,
                        source_ref, created_at, updated_at, closed_at
                    ) VALUES (?, ?, ?, ?, 'active', ?, ?, ?, ?, NULL)
                    """,
                    (
                        goal_id,
                        title,
                        description,
                        summary,
                        momentum or "on_track",
                        item_source_ref,
                        now,
                        now,
                    ),
                )
                outcomes.append({"goal_id": goal_id, "applied": True, "reason": "created"})
                continue

            title = existing["title"]
            description = existing["description"]
            summary = existing["summary"]
            source_ref = existing["source_ref"]
            if raw.get("title") is not None:
                title = str(raw.get("title")).strip() or title
            if raw.get("description") is not None:
                description = str(raw.get("description")).strip() or description
            if raw.get("summary") is not None:
                summary = str(raw.get("summary")).strip() or summary
            if raw.get("source_ref") is not None:
                source_ref = item_source_ref
            conn.execute(
                """
                UPDATE tracking_items
                   SET title = ?, description = ?, summary = ?, momentum_status = ?,
                       source_ref = ?, updated_at = ?
                 WHERE goal_id = ? AND status = 'active'
                """,
                (
                    title,
                    description,
                    summary,
                    existing["momentum_status"] if momentum is None else momentum,
                    source_ref,
                    _advance_timestamp(existing["updated_at"]),
                    goal_id,
                ),
            )
            outcomes.append({"goal_id": goal_id, "applied": True, "reason": "updated"})
    return outcomes


def _row_to_dict(row: sqlite3.Row | None) -> dict | None:
    return dict(row) if row is not None else None


def list_tracking_items(status: str | None = None) -> list[dict]:
    if status is not None and status not in VALID_STATUSES:
        raise ValueError(f"status must be one of {VALID_STATUSES}")
    query = "SELECT * FROM tracking_items"
    params: tuple[str, ...] = ()
    if status is not None:
        query += " WHERE status = ?"
        params = (status,)
    query += " ORDER BY updated_at DESC, goal_id ASC"
    with _connection() as conn:
        return [dict(row) for row in conn.execute(query, params).fetchall()]


def get_tracking_item(goal_id: str) -> dict | None:
    with _connection() as conn:
        row = conn.execute(
            "SELECT * FROM tracking_items WHERE goal_id = ?", (goal_id,)
        ).fetchone()
        return _row_to_dict(row)


def _require_text(value: str | None, field: str) -> str:
    text = (value or "").strip()
    if not text:
        raise ValueError(f"{field} is required")
    return text


def upsert_tracking_item(
    goal_id: str,
    title: str | None = None,
    description: str | None = None,
    summary: str | None = None,
    momentum_status: str | None = None,
    source_ref: str | None = None,
) -> dict:
    goal_id = _require_text(goal_id, "goal_id")
    if momentum_status is not None and momentum_status not in MOMENTUM_STATUSES:
        raise ValueError(f"momentum_status must be one of {MOMENTUM_STATUSES}")

    norm_source_ref = _normalize_source_ref(source_ref)
    existing = get_tracking_item(goal_id)
    if existing is None:
        new_title = _require_text(title, "title")
        new_description = _require_text(description, "description")
        new_summary = _require_text(summary, "summary")
        now = utc_now_iso()
        with _write_connection() as conn:
            conn.execute(
                """
                INSERT INTO tracking_items (
                    goal_id, title, description, summary, status, momentum_status,
                    source_ref, created_at, updated_at, closed_at
                ) VALUES (?, ?, ?, ?, 'active', ?, ?, ?, ?, NULL)
                """,
                (
                    goal_id,
                    new_title,
                    new_description,
                    new_summary,
                    momentum_status or "on_track",
                    norm_source_ref,
                    now,
                    now,
                ),
            )
    else:
        new_title = existing["title"] if title is None else _require_text(title, "title")
        new_description = (
            existing["description"]
            if description is None
            else _require_text(description, "description")
        )
        new_summary = existing["summary"] if summary is None else _require_text(summary, "summary")
        updated_at = _advance_timestamp(existing["updated_at"])
        with _write_connection() as conn:
            conn.execute(
                """
                UPDATE tracking_items
                   SET title = ?, description = ?, summary = ?, momentum_status = ?,
                       source_ref = ?, updated_at = ?
                 WHERE goal_id = ?
                """,
                (
                    new_title,
                    new_description,
                    new_summary,
                    existing["momentum_status"] if momentum_status is None else momentum_status,
                    existing["source_ref"] if source_ref is None else norm_source_ref,
                    updated_at,
                    goal_id,
                ),
            )

    result = get_tracking_item(goal_id)
    assert result is not None
    return result


def close_tracking_item(goal_id: str, summary: str | None = None) -> dict:
    goal_id = _require_text(goal_id, "goal_id")
    existing = get_tracking_item(goal_id)
    if existing is None:
        raise KeyError(f"unknown goal_id: {goal_id}")

    updated_at = _advance_timestamp(existing["updated_at"])
    # Re-closing an already closed item keeps the original closure time while still advancing updated_at.
    closed_at = existing["closed_at"] or updated_at
    with _write_connection() as conn:
        conn.execute(
            """
            UPDATE tracking_items
               SET status = 'closed', summary = ?, updated_at = ?, closed_at = ?
             WHERE goal_id = ?
            """,
            (existing["summary"] if summary is None else summary, updated_at, closed_at, goal_id),
        )
    result = get_tracking_item(goal_id)
    assert result is not None
    return result


def reopen_tracking_item(
    goal_id: str,
    summary: str | None = None,
    momentum_status: str | None = None,
) -> dict:
    goal_id = _require_text(goal_id, "goal_id")
    if momentum_status is not None and momentum_status not in MOMENTUM_STATUSES:
        raise ValueError(f"momentum_status must be one of {MOMENTUM_STATUSES}")
    existing = get_tracking_item(goal_id)
    if existing is None:
        raise KeyError(f"unknown goal_id: {goal_id}")

    updated_at = _advance_timestamp(existing["updated_at"])
    with _write_connection() as conn:
        conn.execute(
            """
            UPDATE tracking_items
               SET status = 'active', summary = ?, momentum_status = ?,
                   updated_at = ?, closed_at = NULL
             WHERE goal_id = ?
            """,
            (
                existing["summary"] if summary is None else summary,
                existing["momentum_status"] if momentum_status is None else momentum_status,
                updated_at,
                goal_id,
            ),
        )
    result = get_tracking_item(goal_id)
    assert result is not None
    return result

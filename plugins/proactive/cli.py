"""CLI and slash-command surfaces for proactive tracking items."""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import shlex
from pathlib import Path
from typing import Any

from hermes_constants import get_hermes_home

from . import snapshot, storage


CRON_JOB_NAME = "proactive_notifier"
DEFAULT_CRON_SCHEDULE = "30 9,12,15,18,21 * * *"
CRON_SKILLS = ["proactive:editorial-gate"]
CRON_TOOLSETS = ["proactivity", "session_search", "file"]
BRIDGE_FILENAME = "proactive-tick.py"
BRIDGE_STUB_PATH = Path(__file__).resolve().parent / "assets" / BRIDGE_FILENAME
FALLBACK_DIR_SENTINEL = 'FALLBACK_PLUGIN_DIR = ""  # @PROACTIVE_PLUGIN_DIR@'


def _load_bridge_template() -> str:
    """Load the pre-run bridge stub verbatim; fail loudly when missing or malformed."""
    try:
        text = BRIDGE_STUB_PATH.read_text(encoding="utf-8")
    except OSError as exc:
        raise RuntimeError(f"proactive bridge stub missing: {BRIDGE_STUB_PATH}") from exc
    if not text.strip():
        raise RuntimeError(f"proactive bridge stub is empty: {BRIDGE_STUB_PATH}")
    if text.count(FALLBACK_DIR_SENTINEL) != 1:
        raise RuntimeError(
            f"proactive bridge stub must contain fallback marker once: {BRIDGE_STUB_PATH}"
        )
    try:
        compile(text, str(BRIDGE_STUB_PATH), "exec")
    except SyntaxError as exc:
        raise RuntimeError(f"proactive bridge stub has invalid syntax: {BRIDGE_STUB_PATH}") from exc
    return text


# Validate the asset at import time so incomplete plugin installs fail during registration.
_load_bridge_template()

HELP_TEXT = """\
/proactive — proactive tracking and snapshot controls

  track list [--status active|closed]
  track get <goal_id>
  track add <goal_id> --title T --description D --summary S [--momentum-status STATUS]
  track update <goal_id> [--title T] [--description D] [--summary S] [--momentum-status STATUS]
  track upsert <goal_id> --title T --description D --summary S [--momentum-status STATUS]
  track close <goal_id> [--summary S]
  track reopen <goal_id> [--summary S] [--momentum-status STATUS]
  setup-cron
  status
  history
  tick [--snapshot-only] [--force]
"""


def _display_time(value: str | None, home: Path) -> str:
    """Render one stored UTC timestamp in the profile's configured timezone."""
    return snapshot._display_time_from_text(value, snapshot._build_config(home, None))


def _load_jobs_file(home: Path) -> list[dict[str, Any]]:
    path = home / "cron" / "jobs.json"
    if not path.is_file():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    jobs = payload.get("jobs") if isinstance(payload, dict) else payload
    if isinstance(jobs, dict):
        jobs = list(jobs.values())
    if not isinstance(jobs, list):
        return []
    return [dict(job) for job in jobs if isinstance(job, dict)]


def _proactive_job(home: Path) -> dict[str, Any] | None:
    for job in _load_jobs_file(home):
        if str(job.get("name") or "") == CRON_JOB_NAME:
            return job
    return None


def _origin_from_mapping(platform: str, data: dict[str, Any]) -> dict[str, Any] | None:
    """Normalize one platform-origin record into the fields Cron expects."""
    chat_id = data.get("chat_id") or data.get("id")
    if not chat_id:
        return None
    return {
        "platform": platform,
        "chat_id": str(chat_id),
        "chat_name": data.get("name") or data.get("chat_name") or str(chat_id),
        "thread_id": data.get("thread_id"),
        "user_id": data.get("user_id"),
    }


def _platform_matches(platform: str, requested: str) -> bool:
    return requested.strip().lower() in {"", "auto", "any"} or platform.strip().lower() == requested.strip().lower()


def _delivery_origin(home: Path, platform: str = "auto") -> dict[str, Any] | None:
    """Resolve the Cron delivery origin from the host's own configuration.

    ``auto`` follows the first human platform recorded by an existing Cron job
    (preferring jobs that already deliver to their origin), then the host's
    channel directory, then ``platforms.<name>.home_channel``. A named platform
    restricts every lookup to that platform. Nothing here is hardcoded to one
    messaging product.
    """
    requested = str(platform or "auto").strip()

    candidates: list[tuple[bool, dict[str, Any]]] = []
    for job in _load_jobs_file(home):
        origin = job.get("origin")
        if not isinstance(origin, dict):
            continue
        name = str(origin.get("platform") or "").strip()
        if not name or not _platform_matches(name, requested):
            continue
        normalized = _origin_from_mapping(name, origin)
        if normalized is not None:
            candidates.append((job.get("deliver") == "origin", normalized))
    if candidates:
        candidates.sort(key=lambda entry: entry[0], reverse=True)
        return candidates[0][1]

    directory = home / "channel_directory.json"
    if directory.is_file():
        try:
            payload = json.loads(directory.read_text(encoding="utf-8"))
            platforms = payload.get("platforms") or {}
            if isinstance(platforms, dict):
                for name, entries in platforms.items():
                    if not _platform_matches(str(name), requested):
                        continue
                    for entry in entries if isinstance(entries, list) else []:
                        if isinstance(entry, dict):
                            normalized = _origin_from_mapping(str(name), entry)
                            if normalized is not None:
                                return normalized
        except (OSError, ValueError):
            pass

    config_path = home / "config.yaml"
    if config_path.is_file():
        try:
            import hermes_yaml as yaml

            config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
            platforms = (config.get("platforms") or {}) if isinstance(config, dict) else {}
            if isinstance(platforms, dict):
                for name, section in platforms.items():
                    if not _platform_matches(str(name), requested):
                        continue
                    channel = section.get("home_channel") if isinstance(section, dict) else None
                    if isinstance(channel, dict):
                        normalized = _origin_from_mapping(str(name), channel)
                        if normalized is not None:
                            return normalized
        except Exception:
            pass

    if snapshot._is_production_hermes_home(home):
        default_root = (Path.home() / ".hermes").resolve()
        if home.resolve() != default_root:
            return _delivery_origin(default_root, requested)
    return None


def _bridge_script(plugin_dir: Path) -> str:
    template = _load_bridge_template()
    replacement = f"FALLBACK_PLUGIN_DIR = {repr(str(plugin_dir))}"
    return template.replace(FALLBACK_DIR_SENTINEL, replacement, 1)


def _install_bridge(home: Path) -> Path:
    scripts_dir = home / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = scripts_dir / BRIDGE_FILENAME
    content = _bridge_script(Path(__file__).resolve().parent)
    temporary = path.with_name(path.name + ".tmp")
    with open(temporary, "w", encoding="utf-8") as handle:
        handle.write(content)
        if not content.endswith("\n"):
            handle.write("\n")
    os.chmod(temporary, 0o755)
    os.replace(temporary, path)
    os.chmod(path, 0o755)
    return path


def _setup_cron(home: Path | None = None) -> dict[str, Any]:
    home = (home or Path(get_hermes_home())).expanduser().resolve()
    config = snapshot._build_config(home, None)
    bridge = _install_bridge(home)
    origin = _delivery_origin(home, str(config.get("delivery_platform") or "auto"))
    schedule = str(config.get("cron_schedule") or DEFAULT_CRON_SCHEDULE).strip()
    timezone_name = snapshot._zone_name(snapshot._zone(config))

    from cron import jobs as cron_jobs

    prompt = (
        "Review the pending proactive edition with proactivity_read, decide every candidate "
        "with proactivity_select, and output only the final rewritten reminder or [SILENT]."
    )
    with cron_jobs.use_cron_store(home):
        jobs = cron_jobs.load_jobs()
        existing = next((job for job in jobs if job.get("name") == CRON_JOB_NAME), None)
        fields = {
            "name": CRON_JOB_NAME,
            "prompt": prompt,
            "schedule": schedule,
            "timezone": timezone_name or None,
            "script": BRIDGE_FILENAME,
            "skills": list(CRON_SKILLS),
            "enabled_toolsets": list(CRON_TOOLSETS),
            "deliver": "origin",
            "attach_to_session": True,
            "model": None,
            "provider": None,
            "reasoning_effort": None,
        }
        if origin is not None:
            fields["origin"] = origin
        if existing is None:
            job = cron_jobs.create_job(**fields)
        else:
            job = cron_jobs.update_job(existing["id"], fields)
            if job is None:
                raise RuntimeError(f"failed to update cron job {existing['id']}")

    return {
        "job": job,
        "bridge_path": str(bridge),
        "next_run_at": job.get("next_run_at"),
    }


def _render_status(home: Path) -> str:
    storage.init_db()
    watermark = storage.latest_committed_watermark()
    active_count = len(storage.list_tracking_items(status="active"))
    editions = storage.list_recent_checkpoints(limit=1)
    counts = storage.delivery_counts()
    job = _proactive_job(home)

    lines = [
        "Proactive status",
        f"Active watermark: {_display_time(watermark, home)}",
        f"Active tracked goals: {active_count}",
    ]
    if editions:
        edition = editions[0]
        candidates = storage.list_candidate_items(edition["run_id"])
        sent = sum(1 for item in candidates if item.get("decision") == "send")
        skipped = sum(1 for item in candidates if item.get("decision") == "skip")
        lines.append(
            f"Recent edition: {edition['run_id']} ({edition['status']}) — "
            f"{len(candidates)} candidate(s), {sent} send, {skipped} skip"
        )
    else:
        lines.append("Recent edition: —")
    lines.append(
        f"Delivered receipts: {counts.get('delivered', 0)} total; "
        f"pending={counts.get('pending', 0)}, failed={counts.get('failed', 0)}"
    )
    if job is None:
        lines.append("Bound Cron job: not configured")
    else:
        lines.append(
            f"Bound Cron job: {CRON_JOB_NAME} ({job.get('id', '—')}) — "
            f"next run {_display_time(job.get('next_run_at'), home)}"
        )
    return "\n".join(lines)


def _render_history(home: Path, *, limit: int = 5) -> str:
    storage.init_db()
    editions = storage.list_recent_checkpoints(limit=limit)
    lines = ["Proactive history", "Recent editions:"]
    if not editions:
        lines.append("- (no editions yet)")
    for edition in editions:
        candidates = storage.list_candidate_items(edition["run_id"])
        lines.append(
            f"- {edition['run_id']} [{edition['status']}] "
            f"{_display_time(edition['window_start_utc'], home)} → {_display_time(edition['window_end_utc'], home)}"
        )
        if not candidates:
            lines.append("  (no candidate items)")
        for item in candidates:
            decision = item.get("decision") or "undecided"
            reason = item.get("decision_reason") or "—"
            lines.append(
                f"  [item:{item['item_num']}] {decision} · {reason} · {item.get('title') or '—'}"
            )

    delivered = storage.list_delivered_messages()[:limit]
    lines.append("Recent delivered messages:")
    if not delivered:
        lines.append("- (no delivered messages yet)")
    for item in delivered:
        lines.append(
            f"- {_display_time(item.get('delivered_at'), home)} · run {item.get('run_id')} · "
            f"message {item.get('message_id') or '—'}"
        )
        content = str(item.get("content") or "").strip().replace("\n", " ")
        if content:
            lines.append(f"  {content}")
    return "\n".join(lines)


def _render_item(item: dict[str, Any], home: Path, *, heading: str | None = None) -> str:
    lines = []
    if heading:
        lines.append(heading)
    lines.extend(
        [
            f"  {item['title']}",
            f"  Goal id: {item['goal_id']}",
            f"  Status: {item['status']}",
            f"  Momentum: {item['momentum_status']}",
            f"  Summary: {item['summary']}",
            f"  Description: {item['description']}",
            f"  Created at: {_display_time(item['created_at'], home)}",
            f"  Updated at: {_display_time(item['updated_at'], home)}",
        ]
    )
    if item.get("closed_at"):
        lines.append(f"  Closed at: {_display_time(item['closed_at'], home)}")
    return "\n".join(lines)


def _render_list(items: list[dict[str, Any]], home: Path) -> str:
    if not items:
        return "No tracked goals."
    blocks = [f"Tracked goals ({len(items)}):"]
    for item in items:
        blocks.append(_render_item(item, home))
    return "\n\n".join(blocks)


def _execute(
    action: str,
    *,
    home: Path | None = None,
    goal_id: str | None = None,
    title: str | None = None,
    description: str | None = None,
    summary: str | None = None,
    momentum_status: str | None = None,
    source_ref: str | None = None,
    status: str | None = None,
) -> str:
    home = home or Path(get_hermes_home())
    storage.init_db()
    if action == "list":
        return _render_list(storage.list_tracking_items(status=status), home)
    if action == "get":
        item = storage.get_tracking_item(goal_id or "")
        if item is None:
            return f"No tracked goal found: {goal_id}"
        return _render_item(item, home)
    if action in {"add", "update", "upsert"}:
        item = storage.upsert_tracking_item(
            goal_id or "",
            title=title,
            description=description,
            summary=summary,
            momentum_status=momentum_status,
            source_ref=source_ref,
        )
        return _render_item(item, home, heading=f"Tracked goal saved: {item['goal_id']}")
    if action == "close":
        item = storage.close_tracking_item(goal_id or "", summary=summary)
        return _render_item(item, home, heading=f"Tracked goal closed: {item['goal_id']}")
    if action == "reopen":
        item = storage.reopen_tracking_item(
            goal_id or "",
            summary=summary,
            momentum_status=momentum_status,
        )
        return _render_item(item, home, heading=f"Tracked goal reopened: {item['goal_id']}")
    raise ValueError(f"unknown track action: {action}")


def _add_common_item_args(parser: argparse.ArgumentParser, *, allow_momentum: bool = True) -> None:
    parser.add_argument("--title")
    parser.add_argument("--description")
    parser.add_argument("--summary")
    parser.add_argument("--source-ref", "--source_ref")
    if allow_momentum:
        parser.add_argument("--momentum-status", "--momentum_status", choices=storage.MOMENTUM_STATUSES)


def _add_track_action_arguments(parser: argparse.ArgumentParser, action: str) -> None:
    if action == "list":
        parser.add_argument("--status", choices=storage.VALID_STATUSES)
        return
    if action in {"get", "close", "reopen"}:
        parser.add_argument("goal_id")
        if action != "get":
            parser.add_argument("--summary")
        if action == "reopen":
            parser.add_argument("--momentum-status", "--momentum_status", choices=storage.MOMENTUM_STATUSES)
        return
    if action in {"add", "update", "upsert"}:
        parser.add_argument("goal_id")
        _add_common_item_args(parser)
        return
    raise ValueError(f"unknown track action: {action}")


def setup_cli(subparser: argparse.ArgumentParser) -> None:
    """Build the ``hermes proactive ...`` argparse tree."""
    subs = subparser.add_subparsers(dest="proactive_command")

    p_tick = subs.add_parser("tick", help="Run the deterministic Stage 1 snapshot builder")
    p_tick.add_argument("--snapshot-only", action="store_true", help="Build inputs without Stage 2")
    p_tick.add_argument("--force", action="store_true", help="Bypass the active-window gate")

    subs.add_parser("setup-cron", help="Install the Cron bridge and upsert the proactive job")
    subs.add_parser("status", help="Show watermark, delivery, and Cron schedule status")
    subs.add_parser("history", help="Show recent editions, decisions, and deliveries")

    track = subs.add_parser("track", help="Manage durable tracked goals")
    track_subs = track.add_subparsers(dest="track_action")

    track_help = {
        "list": "List tracked goals",
        "get": "Show one tracked goal",
        "add": "Create a tracked goal",
        "update": "Update a tracked goal",
        "upsert": "Create or update a tracked goal",
        "close": "Close a tracked goal",
        "reopen": "Reopen a tracked goal",
    }
    for name, help_text in track_help.items():
        parser = track_subs.add_parser(name, help=help_text)
        _add_track_action_arguments(parser, name)

    subparser.set_defaults(func=handle_cli)


def handle_cli(args: argparse.Namespace) -> int:
    command = getattr(args, "proactive_command", None)
    home = Path(get_hermes_home()).expanduser().resolve()
    if command == "tick":
        try:
            snapshot.run_tick(
                force=bool(getattr(args, "force", False)),
                snapshot_only=bool(getattr(args, "snapshot_only", False)),
            )
        except Exception as exc:
            print(f"Error: {exc}")
            return 1
        return 0
    if command == "setup-cron":
        try:
            result = _setup_cron()
            job = result["job"]
            print(f"Installed Cron bridge: {result['bridge_path']}")
            print(
                f"Configured {CRON_JOB_NAME} ({job['id']}); next run "
                f"{_display_time(result.get('next_run_at'), home)}"
            )
        except Exception as exc:
            print(f"Error: {exc}")
            return 1
        return 0
    if command in {"status", "history"}:
        try:
            rendered = _render_status(home) if command == "status" else _render_history(home)
        except Exception as exc:
            print(f"Error: {exc}")
            return 1
        print(rendered)
        return 0

    action = getattr(args, "track_action", None)
    if not action:
        print("Usage: hermes proactive {track|tick|setup-cron|status|history}")
        return 2
    try:
        rendered = _execute(
            action,
            home=home,
            goal_id=getattr(args, "goal_id", None),
            title=getattr(args, "title", None),
            description=getattr(args, "description", None),
            summary=getattr(args, "summary", None),
            momentum_status=getattr(args, "momentum_status", None),
            source_ref=getattr(args, "source_ref", None),
            status=getattr(args, "status", None),
        )
    except Exception as exc:
        print(f"Error: {exc}")
        return 1
    print(rendered)
    return 0


class _SlashParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ValueError(message)


def _parse_slash_tokens(action: str, tokens: list[str]) -> argparse.Namespace:
    parser = _SlashParser(prog=f"/proactive track {action}", add_help=False)
    _add_track_action_arguments(parser, action)
    return parser.parse_args(tokens)


def _parse_slash_tick(tokens: list[str]) -> argparse.Namespace:
    parser = _SlashParser(prog="/proactive tick", add_help=False)
    parser.add_argument("--snapshot-only", action="store_true")
    parser.add_argument("--force", action="store_true")
    return parser.parse_args(tokens)


def handle_slash(raw_args: str = "", **kwargs: Any) -> str:
    try:
        tokens = shlex.split(raw_args)
    except ValueError as exc:
        return f"Error: {exc}"
    if not tokens or tokens[0] in {"help", "-h", "--help"}:
        return HELP_TEXT
    if tokens[0] == "tick":
        try:
            parsed = _parse_slash_tick(tokens[1:])
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                snapshot.run_tick(
                    force=bool(parsed.force),
                    snapshot_only=bool(parsed.snapshot_only),
                )
            return output.getvalue().rstrip()
        except Exception as exc:
            return f"Error: {exc}"
    if tokens[0] in {"setup-cron", "status", "history"}:
        try:
            home = Path(get_hermes_home()).expanduser().resolve()
            if tokens[0] == "setup-cron":
                result = _setup_cron(home)
                job = result["job"]
                return (
                    f"Installed Cron bridge: {result['bridge_path']}\n"
                    f"Configured {CRON_JOB_NAME} ({job['id']}); next run "
                    f"{_display_time(result.get('next_run_at'), home)}"
                )
            return _render_status(home) if tokens[0] == "status" else _render_history(home)
        except Exception as exc:
            return f"Error: {exc}"
    if tokens[0] != "track":
        return f"Unknown command: {tokens[0]}\n\n{HELP_TEXT}"
    if len(tokens) < 2 or tokens[1] in {"help", "-h", "--help"}:
        return HELP_TEXT
    action = tokens[1]
    try:
        parsed = _parse_slash_tokens(action, tokens[2:])
        return _execute(
            action,
            home=Path(get_hermes_home()),
            goal_id=getattr(parsed, "goal_id", None),
            title=getattr(parsed, "title", None),
            description=getattr(parsed, "description", None),
            summary=getattr(parsed, "summary", None),
            momentum_status=getattr(parsed, "momentum_status", None),
            source_ref=getattr(parsed, "source_ref", None),
            status=getattr(parsed, "status", None),
        )
    except Exception as exc:
        return f"Error: {exc}"


setup_fn = setup_cli
handler_fn = handle_cli

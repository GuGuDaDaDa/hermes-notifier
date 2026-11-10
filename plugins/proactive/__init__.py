"""Native Hermes plugin for durable proactive tracking."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from hermes_constants import get_hermes_home

from . import bridge, cli, log, snapshot, storage, workers
from .cli import handle_cli, handle_slash, setup_cli
from .schemas import (
    PROACTIVITY_READ_SCHEMA,
    PROACTIVITY_SELECT_SCHEMA,
    TRACKING_MANAGE_SCHEMA,
)
from .tools import (
    handle_proactivity_read,
    handle_proactivity_select,
    handle_tracking_manage,
)


SKILL_PATH = Path(__file__).resolve().parent / "skills" / "editorial-gate" / "SKILL.md"
CONVERSATION_PROMPT_SECTION_ID = "proactive-conversation"
CONVERSATION_PROMPT_SECTION = (
    "Proactive follow-ups are a quiet background notebook. When a user completes or "
    "explicitly drops a topic in conversation, close only that topic quietly via "
    "`tracking_manage`; when they pause or defer it, update it with `momentum_status` "
    "(`waiting` or `stalled`) instead of closing. Fetch `tracking_manage` via "
    "`tool_search`/`tool_describe`/`tool_call` if deferred — never invoke `terminal`, "
    "shell CLIs, or direct SQLite commands to inspect or modify tracking state. Reply "
    "naturally in one short sentence matching the user's language, without echoing internal "
    "fields (`goal_id`, `status`, `closed`, `active`, `momentum_status`), pipeline "
    "mechanics, reminder-suppression notices, or unrelated items still being tracked."
)


def _conversation_prompt_section(session_info: Mapping[str, Any]) -> str:
    """Keep chat-reply rules out of Cron and other non-human runtime sessions."""
    platform = str((session_info or {}).get("platform") or "").strip().lower()
    if platform in snapshot.NON_HUMAN_SESSION_SURFACES:
        return ""
    return CONVERSATION_PROMPT_SECTION


def configure_logging(hermes_home: str | Path | None = None) -> str:
    """Apply the plugin's configured log level to the ``proactive`` logger.

    Used by the standalone Cron bridge, which starts before any plugin module is
    loaded and therefore cannot rely on :func:`register`.
    """
    try:
        home = Path(hermes_home or get_hermes_home()).expanduser()
        return log.configure_level(snapshot._build_config(home, None).get("log_level"))
    except Exception:
        return log.DEFAULT_LEVEL


def _apply_configured_level(ctx) -> None:
    """Prefer the host-provided plugin setting; fall back to config.yaml settings."""
    level = None
    getter = getattr(ctx, "get_config", None)
    if callable(getter):
        try:
            level = getter("log_level", None)
        except Exception:
            level = None
    if level is None:
        configure_logging()
    else:
        log.configure_level(level)


def register(ctx) -> None:
    """Initialize durable state and register the plugin's tools, skill, CLI, and slash command."""
    storage.init_db()
    _apply_configured_level(ctx)
    workers.set_host_llm(getattr(ctx, "llm", None))
    ctx.register_tool(
        name="proactivity_read",
        toolset="proactivity",
        schema=PROACTIVITY_READ_SCHEMA,
        handler=handle_proactivity_read,
        description="Load a pending proactive edition and its freshness evidence.",
        emoji="📰",
    )
    ctx.register_tool(
        name="proactivity_select",
        toolset="proactivity",
        schema=PROACTIVITY_SELECT_SCHEMA,
        handler=handle_proactivity_select,
        description="Record editorial decisions, close verified goals, and commit the edition.",
        emoji="✅",
    )
    ctx.register_tool(
        name="tracking_manage",
        toolset="proactivity",
        schema=TRACKING_MANAGE_SCHEMA,
        handler=handle_tracking_manage,
        description="Quietly manage background proactive tracking items.",
        emoji="🎯",
    )
    ctx.register_skill(
        name="editorial-gate",
        path=SKILL_PATH,
        description="Review a pending proactive edition and decide every candidate before delivery.",
    )
    register_prompt_section = getattr(ctx, "register_system_prompt_section", None)
    if callable(register_prompt_section):
        register_prompt_section(
            id=CONVERSATION_PROMPT_SECTION_ID,
            content=_conversation_prompt_section,
            position="after_memory",
        )
    ctx.register_cli_command(
        name="proactive",
        help="Manage proactive tracking items",
        setup_fn=setup_cli,
        handler_fn=handle_cli,
        description="Inspect and update durable proactive tracked goals.",
    )
    ctx.register_command(
        "proactive",
        handler=handle_slash,
        description="Manage durable proactive tracked goals.",
        args_hint=(
            "track <list|get|add|update|upsert|close|reopen> | tick [--snapshot-only] [--force] | "
            "setup-cron | status | history"
        ),
    )

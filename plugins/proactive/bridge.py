"""In-package Cron bridge runner invoked by the thin pre-run script stub."""

from __future__ import annotations

import time
from pathlib import Path

from . import log, snapshot


def _hydrate_credentials(home: Path) -> None:
    """Load the profile's ``.env`` layers into this pre-run process.

    The Cron scheduler spawns pre-run scripts through a sanitized child environment
    that deliberately strips every declared provider credential. A ``config.yaml``
    whose main-model key is written as ``${VAR}`` therefore expands to nothing,
    stays a literal placeholder, and is sent to the provider as an invalid key —
    Stage 2 then dies with an authentication error it cannot explain. The Cron
    main-agent process reloads ``.env`` itself before any provider call; this
    bridge starts earlier and must do the same, or the whole run has no credential
    to authenticate with.

    A failure here is not fatal on its own: the run still proceeds and the
    unresolved-reference guard in Stage 2 reports the missing variable name.
    """
    logger = log.logger("bridge")
    try:
        from hermes_cli.env_loader import load_hermes_dotenv
    except Exception as exc:
        log.warning_event(logger, "bridge.dotenv_unavailable", exc, stage="bridge")
        return
    started = time.monotonic()
    try:
        load_hermes_dotenv(hermes_home=home)
    except Exception as exc:
        log.error_event(
            logger,
            "bridge.dotenv_failed",
            exc,
            stage="bridge",
            hermes_home=str(home),
            duration_ms=log.elapsed_ms(started),
        )
        return
    log.info(
        logger,
        "bridge.dotenv_loaded",
        stage="bridge",
        hermes_home=str(home),
        duration_ms=log.elapsed_ms(started),
    )


def run_bridge(
    home: str | Path,
    *,
    plugin_dir: str | Path,
    started: float,
) -> int:
    """Configure plugin logging, run Stage 1/2 tick, and emit lifecycle events."""
    logger = log.logger("bridge")
    try:
        resolved_home = Path(home).expanduser().resolve()
        resolved_plugin = Path(plugin_dir).expanduser().resolve()
        from . import configure_logging

        # Re-apply via the plugin's shared config loader so runtime level matches config.yaml.
        configure_logging(resolved_home)
        log.info(
            logger,
            "bridge.plugin_selected",
            stage="bridge",
            plugin_dir=str(resolved_plugin),
        )
        _hydrate_credentials(resolved_home)
        snapshot.run_tick(hermes_home=str(resolved_home))
    except Exception as exc:
        log.error_event(
            logger,
            "bridge.failed",
            exc,
            stage="bridge",
            phase="pipeline",
            duration_ms=log.elapsed_ms(started),
        )
        raise
    log.info(
        logger,
        "bridge.finished",
        stage="bridge",
        duration_ms=log.elapsed_ms(started),
    )
    return 0


__all__ = ["run_bridge"]

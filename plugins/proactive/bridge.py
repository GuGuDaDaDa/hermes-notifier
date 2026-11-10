"""In-package Cron bridge runner invoked by the thin pre-run script stub."""

from __future__ import annotations

from pathlib import Path

from . import log, snapshot


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

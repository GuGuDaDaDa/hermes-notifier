"""Tool parameter schemas exposed by the proactive plugin."""

from __future__ import annotations


DECISION_REASONS = [
    "time_sensitive",
    "changed",
    "already_handled",
    "moot",
    "unconfirmed",
    "preferences",
    "not_useful_now",
]


PROACTIVITY_READ_SCHEMA = {
    "name": "proactivity_read",
    "description": (
        "Load the latest pending proactive edition, or an explicitly requested run. Returns "
        "the researched opportunities, prepared drafts, cited sources, user preferences, "
        "undecided candidate items, and human conversation added after the snapshot boundary."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "run_id": {
                "type": "string",
                "description": "Optional edition run id; defaults to the latest pending checkpoint.",
            }
        },
        "additionalProperties": False,
    },
}


PROACTIVITY_SELECT_SCHEMA = {
    "name": "proactivity_select",
    "description": (
        "Record a send or skip decision with an enumerated reason for every candidate in one "
        "edition, optionally close verified goals, commit the checkpoint, and queue the final "
        "rewritten message for delivery when anything is sent."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "run_id": {
                "type": "string",
                "description": "Edition run id returned by proactivity_read.",
            },
            "decisions": {
                "type": "array",
                "description": "Exactly one decision for every [item:N] in the edition.",
                "items": {
                    "type": "object",
                    "properties": {
                        "item_num": {
                            "type": "integer",
                            "minimum": 1,
                            "description": "Candidate number from [item:N].",
                        },
                        "decision": {
                            "type": "string",
                            "enum": ["send", "skip"],
                            "description": "Whether to deliver this candidate in the final message.",
                        },
                        "reason": {
                            "type": "string",
                            "enum": DECISION_REASONS,
                            "description": "Auditable rationale for the decision.",
                        },
                        "note": {
                            "type": "string",
                            "description": "Optional short explanation for the decision.",
                        },
                    },
                    "required": ["item_num", "decision", "reason"],
                    "additionalProperties": False,
                },
            },
            "final_message": {
                "type": "string",
                "description": (
                    "The single rewritten message to deliver. Required and non-empty when any "
                    "decision is send."
                ),
            },
            "close_goal_ids": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Tracked goal ids verified as completed or no longer needed.",
            },
        },
        "required": ["run_id", "decisions"],
        "additionalProperties": False,
    },
}


TRACKING_MANAGE_SCHEMA = {
    "name": "tracking_manage",
    "description": (
        "List, inspect, create, update, close, or reopen durable proactive tracking items. "
        "Use close only after the item is verified complete; use add, update, or upsert for "
        "new or active items."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["list", "get", "add", "update", "upsert", "close", "reopen"],
                "description": "Operation to perform.",
            },
            "goal_id": {
                "type": "string",
                "description": "Stable tracking item identifier.",
            },
            "title": {
                "type": "string",
                "description": "Human-readable title. Required when creating a new item.",
            },
            "description": {
                "type": "string",
                "description": "What is being tracked. Required when creating a new item.",
            },
            "summary": {
                "type": "string",
                "description": "Current progress summary. Required when creating a new item.",
            },
            "momentum_status": {
                "type": "string",
                "enum": ["on_track", "waiting", "stalled", "time_sensitive"],
                "description": "Current momentum of the tracked item.",
            },
            "source_ref": {
                "type": "string",
                "description": "Optional source reference or evidence pointer.",
            },
            "status": {
                "type": "string",
                "enum": ["active", "closed"],
                "description": "Optional status filter for the list action.",
            },
        },
        "required": ["action"],
        "additionalProperties": False,
    },
}


CANDIDATE_SCHEMA = {
    "type": "object",
    "required": [
        "title",
        "priority",
        "relevant_until",
        "why_it_matters",
        "evidence_refs",
        "proposed_action",
        "what_remains",
    ],
    "properties": {
        "title": {"type": "string"},
        "priority": {"type": "string", "enum": ["regular", "high"]},
        "relevant_until": {"type": "string"},
        "why_it_matters": {"type": "string"},
        "evidence_refs": {"type": "array", "items": {"type": "string"}},
        "proposed_action": {"type": "string"},
        "what_remains": {"type": "string"},
    },
}


WORKER1_OPPORTUNITIES_SCHEMA = {
    "name": "worker1_opportunities_schema",
    "type": "object",
    "required": ["tracking_upserts", "candidates"],
    "properties": {
        "tracking_upserts": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["goal_id", "title", "description", "summary", "momentum_status"],
                "properties": {
                    "goal_id": {"type": "string"},
                    "title": {"type": "string"},
                    "description": {"type": "string"},
                    "summary": {"type": "string"},
                    "momentum_status": {
                        "type": "string",
                        "enum": ["on_track", "waiting", "stalled", "time_sensitive"],
                    },
                    "source_ref": {
                        "anyOf": [
                            {"type": "string"},
                            {"type": "array", "items": {"type": "string"}},
                            {"type": "null"},
                        ],
                    },
                },
            },
        },
        "candidates": {"type": "array", "items": CANDIDATE_SCHEMA},
    },
}


PREPARED_CANDIDATE_SCHEMA = {
    "type": "object",
    "required": CANDIDATE_SCHEMA["required"] + ["draft"],
    "properties": {
        **CANDIDATE_SCHEMA["properties"],
        "draft": {"type": "string"},
    },
}


WORKER2_PREPARED_SCHEMA = {
    "name": "worker2_prepared_schema",
    "type": "object",
    "required": ["candidates"],
    "properties": {
        "candidates": {"type": "array", "items": PREPARED_CANDIDATE_SCHEMA},
    },
}

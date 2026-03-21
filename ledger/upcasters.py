"""
ledger/upcasters.py — Registered upcasters for schema evolution.
=================================================================
Upcasters transform old event payloads to the current version AT READ TIME.
They NEVER write to the events table. The stored event is immutable.

Registered upcasters:
  CreditAnalysisCompleted v1 → v2: add model_versions={} if absent
  DecisionGenerated v1 → v2:       add model_versions={} if absent

Rule: only infer fields when deterministic. Use null/empty dict for unknowable fields.
"""
from __future__ import annotations


class UpcasterRegistry:
    """Apply on load_stream() and load_all() — never on append()."""

    def upcast(self, event: dict) -> dict:
        et = event.get("event_type")
        ver = event.get("event_version", 1)

        if et == "CreditAnalysisCompleted" and ver < 2:
            event = dict(event)
            event["event_version"] = 2
            p = dict(event.get("payload", {}))
            # v1 → v2: adds model_versions (inferred as empty dict — genuinely unknowable)
            p.setdefault("model_versions", {})
            # Also backfill regulatory_basis if absent (added in same schema revision)
            p.setdefault("regulatory_basis", [])
            event["payload"] = p

        if et == "DecisionGenerated" and ver < 2:
            event = dict(event)
            event["event_version"] = 2
            p = dict(event.get("payload", {}))
            # v1 → v2: adds model_versions
            p.setdefault("model_versions", {})
            event["payload"] = p

        return event

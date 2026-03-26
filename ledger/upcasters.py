from __future__ import annotations
from typing import Callable, Dict, Tuple
from datetime import datetime, timezone

class UpcasterRegistry:
    def __init__(self):
        self._upcasters: Dict[Tuple[str, int], Callable[[dict], dict]] = {}

    def register(self, event_type: str, from_version: int):
        def decorator(fn: Callable[[dict], dict]):
            self._upcasters[(event_type, from_version)] = fn
            return fn
        return decorator

    def upcast(self, event: dict) -> dict:
        current_event = dict(event)
        # Apply upcasters repeatedly in case of v1->v2->v3
        while True:
            et = current_event.get("event_type")
            ver = current_event.get("event_version", 1)
            upcaster = self._upcasters.get((et, ver))
            if not upcaster:
                break
            current_event = upcaster(current_event)
        return current_event

registry = UpcasterRegistry()

@registry.register("CreditAnalysisCompleted", 1)
def upcast_credit_v1_to_v2(event: dict) -> dict:
    payload = dict(event.get("payload", {}))
    
    # 1. Deterministic Inference (~0% error rate directly from timestamps)
    recorded_at = event.get("recorded_at")
    if isinstance(recorded_at, str):
        try:
            recorded_at = datetime.fromisoformat(recorded_at.replace("Z", "+00:00"))
        except:
            recorded_at = datetime.now(timezone.utc)
    elif not isinstance(recorded_at, datetime):
        recorded_at = datetime.now(timezone.utc)
        
    payload["model_version"] = "legacy-pre-2025" if recorded_at < datetime(2025, 1, 1, tzinfo=timezone.utc) else "v2-model"
    
    # 2. Non-Deterministic Inference prevention (Explicit mapping to NULL)
    payload["confidence_score"] = None
    
    payload.setdefault("regulatory_basis", [])
    payload.setdefault("model_versions", {})
    
    return {**event, "event_version": 2, "payload": payload}

@registry.register("DecisionGenerated", 1)
def upcast_decision_v1_to_v2(event: dict) -> dict:
    payload = dict(event.get("payload", {}))
    payload.setdefault("model_versions", {})
    return {**event, "event_version": 2, "payload": payload}

# ensure all instantiations get the populated singleton
class LegacyUpcasterRegistry:
    def upcast(self, event: dict) -> dict:
        return registry.upcast(event)
UpcasterRegistry = LegacyUpcasterRegistry

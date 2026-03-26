# DOMAIN_NOTES

This note answers the six graded Domain Reconnaissance questions from the Week 5 challenge brief.

One repo-specific note up front: the challenge brief frames the early design around four high-level aggregates: `LoanApplication`, `AgentSession`, `ComplianceRecord`, and `AuditLedger`. This starter later expands the implementation surface into seven stream families: `loan-*`, `docpkg-*`, `agent-*`, `credit-*`, `fraud-*`, `compliance-*`, and `audit-*`. The reasoning below answers the graded four-aggregate questions directly, while still mapping cleanly to the starter kit's more detailed stream layout.

## 1. EDA vs ES Distinction

A callback-based trace collector such as LangChain callbacks is Event-Driven Architecture, not Event Sourcing.

Why it is EDA:
- The callback payload is emitted as a notification side effect.
- The system's source of truth still lives somewhere else, usually mutable tables or in-memory state.
- If the callback sink is unavailable, delayed, or loses data, the business state can still move forward, but the history becomes incomplete.
- Reconstructing the exact decision state from first principles is usually impossible because the callback log is observational, not authoritative.

What would change if I redesigned it around The Ledger:
- The authoritative write path would become: command received -> aggregate rehydrated from stream -> business rules checked -> domain events appended transactionally.
- Every material agent action would be written to the event store before downstream effects are considered complete.
- The callback system, if still useful, would become a projection or outbox consumer of the event store rather than the place where the history originates.
- Agent memory would move from process-local state to the `agent-{type}-{session_id}` stream, so restart recovery becomes replay, not best-effort reconstruction.

What I gain:
- Reproducibility: I can replay the application stream and agent session streams to reconstruct the exact state at any past point.
- Auditability: the events are the database, so audit is architectural, not an annotation bolted on later.
- Causal reasoning: `correlation_id` and `causation_id` chains let me answer "what led to this decision?" across streams.
- Operational recovery: if the process crashes, the system can recover from persisted facts rather than opaque logs.

In short: callbacks tell me that something happened; an event store is the reason I can prove what happened and rebuild state from it.

## 2. The Aggregate Question

The four high-level aggregate boundaries I would use are:
- `LoanApplication` for the customer-facing application lifecycle and binding business state.
- `AgentSession` for the work history and memory of each agent run.
- `ComplianceRecord` for regulation evaluation and rule-level evidence.
- `AuditLedger` for tamper-evident, cross-stream audit checkpoints.

The alternative boundary I considered and rejected was merging `ComplianceRecord` into `LoanApplication`.

Why I rejected it:
- Compliance evaluation is a separate consistency concern from the loan lifecycle. Its core invariant is "no clearance without all required checks and rule-version evidence," not "what is the application's current business state?"
- If compliance lives inside the loan stream, every rule-level write contends with unrelated loan writes such as human review, approval, decline, or agent-triggered state transitions.
- A six-rule compliance run would artificially heat the `loan-{id}` stream and raise collision rates for unrelated business actions.

Specific coupling problem this boundary prevents:
- Suppose the ComplianceAgent is appending `ComplianceRulePassed(REG-002)` while the DecisionOrchestrator is trying to append `DecisionGenerated` after reading the same application version.
- If both are writing to one merged `loan-{id}` stream, the orchestrator can lose on optimistic concurrency for a change that is not conceptually the same invariant.
- That creates a retry loop where decision-generation is coupled to rule-by-rule compliance bookkeeping.
- By separating `compliance-{application_id}` from `loan-{application_id}`, I keep the invariants local: the compliance stream owns rule completeness, and the loan stream only advances once compliance has produced a stable outcome.

This boundary choice prevents the loan application stream from becoming a hot spot and keeps regulatory evidence writes from serializing the rest of the business lifecycle.

## 3. Concurrency In Practice

Scenario: two AI agents both process the same loan application and call `append(..., expected_version=3)` on the same stream.

Exact sequence of operations:
1. Both agents load `loan-{application_id}` and see version 3.
2. Both agents independently decide to append a new event based on that state.
3. Agent A enters the append transaction first.
4. The store locks the `event_streams` row for that stream with `SELECT ... FOR UPDATE`.
5. Agent A sees `current_version = 3`, which matches `expected_version = 3`.
6. Agent A inserts the new event at `stream_position = 4`, writes any outbox rows in the same transaction, updates `current_version` to 4, and commits.
7. Agent B reaches the same append path and blocks until Agent A's row lock is released.
8. After Agent A commits, Agent B acquires the row lock and re-reads the stream metadata row.
9. Agent B now sees `current_version = 4`.
10. Because `actual_version != expected_version`, the store raises `OptimisticConcurrencyError(stream_id, expected=3, actual=4)`.
11. Agent B does not insert anything. No silent overwrite occurs.

**What the losing agent must execute next (Reload-and-Retry Sequence):**
1. The losing agent catches the `OptimisticConcurrencyError`.
2. It initiates a reload, pulling the latest `loan-{id}` stream history (which now includes Agent A's event at version 4).
3. The aggregate is rehydrated to its new latest state.
4. The business intent rule is re-evaluated against the new state (e.g., is this command still valid?).
5. If still valid, the agent issues a new append targeting `expected_version = 4`.
- For MCP-facing tooling I would expose a structured recovery hint such as `suggested_action = "reload_stream_and_retry"`.

What the losing agent must do next:
- Reload the stream at version 4.
- Rehydrate the aggregate from the authoritative event history.
- Re-run the business logic against the new state.
- Decide whether the intended action is still relevant.

That last step matters. Retry does not mean "blindly append the same event again." It means "re-evaluate using the new truth."

## 4. Projection Lag And Its Consequences

If the `LoanApplication` projection lags by 200 ms and a loan officer queries "available credit limit" immediately after a disbursement event is committed, the projection may still show the old limit.

The system behavior I want is:
- The command succeeds immediately because the write side is strongly consistent.
- The command response includes enough information for the client to know that the write committed, for example `stream_version`, `event_id`, and ideally the `global_position`.
- The read model remains eventually consistent for a short period.

How I communicate this to the UI:
- The UI should not treat the stale projection as an error. It should treat it as a normal "update pending" state.
- After the command returns, the client can either:
  - optimistically overlay the new value in the UI until the projection catches up, or
  - poll a health/read endpoint until the relevant projection checkpoint is at or beyond the event's `global_position`.
- The response contract should explicitly include freshness metadata such as:
  - `read_model_status = "stale"`
  - `projection_last_seen_position`
  - `pending_global_position`

What the UI should say:
- Something like: "Update recorded. Dashboard is catching up." That is better than pretending the old limit is authoritative.

The architectural point is that the write side must never read from the projection to validate commands. Projections are for serving queries, not for making authoritative decisions.

## 5. The Upcasting Scenario

The brief's example event evolves from:

```json
{
  "application_id": "...",
  "decision": "...",
  "reason": "..."
}
```

to:

```json
{
  "application_id": "...",
  "decision": "...",
  "reason": "...",
  "model_version": "...",
  "confidence_score": null,
  "regulatory_basis": []
}
```

I would upcast it at read time, not by mutating stored rows.

Example upcaster:

```python
from datetime import datetime, timezone


def infer_model_version(recorded_at: datetime) -> str:
    jan_2025 = datetime(2025, 1, 1, tzinfo=timezone.utc)
    jan_2026 = datetime(2026, 1, 1, tzinfo=timezone.utc)

    if recorded_at < jan_2025:
        return "legacy-pre-2025"
    if recorded_at < jan_2026:
        return "credit-model-2025"
    return "credit-model-2026"


def upcast_credit_decision_v1_to_v2(event: dict) -> dict:
    payload = dict(event["payload"])
    payload["model_version"] = infer_model_version(event["recorded_at"])
    payload["confidence_score"] = None
    payload["regulatory_basis"] = payload.get("regulatory_basis", [])
    return {
        **event,
        "event_version": 2,
        "payload": payload,
    }
```

**Reasoning for Null over Fabrication:**
We choose to assign `confidence_score = null` rather than fabricating a median value like `0.85`. The downstream consequence of fabrication is severe: if an automated compliance policy audits this decision in the future, it might incorrectly validate a high-risk manual override because the false `0.85` score meets a minimum threshold rule. The explicit `null` preserves the honest state that the model's confidence was historically untracked, forcing the policy engine to fall back to a manual verification branch instead of silently masking the gap.

Inference strategy for historical `model_version`:
- Use deployment history if there was exactly one active production model for that time window.
- If the production history is ambiguous, do not fabricate specificity. Use a coarse but honest value such as `legacy-pre-2025` or even `legacy-unknown`.
- `confidence_score` should be `null` if the original event never captured it. Fabricating a numeric confidence would create false precision and contaminate downstream audit work.

My rule is:
- Infer only when the inference is deterministic from operational history.
- Use `null` or a clearly-labeled legacy sentinel when the field is genuinely unknowable.

That preserves the core event-sourcing guarantee: the past stays immutable, and schema evolution is honest about uncertainty.

## 6. The Marten Async Daemon Parallel

Marten's distributed async daemon solves one key problem: multiple nodes can project in parallel without two nodes processing the same shard of work and corrupting checkpoint semantics.

In a Python implementation I would mirror that pattern with:
- A projection worker process on each node.
- Sharded projection ownership, for example by projection name plus stream hash range or by explicit shard rows in a lease table.
- A PostgreSQL coordination primitive:
  - either `pg_try_advisory_lock(...)`, or
  - a lease table claimed with `SELECT ... FOR UPDATE SKIP LOCKED`.

My preferred primitive here is PostgreSQL advisory locks for leader/shard ownership and a checkpoint table for progress.

How it would work:
1. Each worker tries to claim one or more projection shards.
2. A successful claim means that node is the only processor for that shard.
3. The worker reads events after the shard's stored checkpoint.
4. It applies events idempotently to the projection.
5. It advances the checkpoint in the same transaction as the projection update.
6. If the worker dies, its advisory lock is released automatically when the DB session drops, and another node can take over.

Failure mode this guards against:
- Split-brain projection execution, where two nodes process the same event range and both advance the checkpoint.
- Without coordination, that can produce duplicate side effects, out-of-order projection state, or a checkpoint that claims progress farther than the projection table actually reflects.

The combination of shard ownership plus transactional checkpoint updates is the Python equivalent of the safety Marten gives you out of the box.

## Practical Mapping To This Starter

For this repo specifically, the immediate design implications are:
- `loan-*` stays the authoritative application lifecycle stream.
- `agent-*` is the Gas Town memory stream and must be written before work, not after.
- `compliance-*` stays separate from `loan-*` to avoid unnecessary write contention.
- `audit-*` remains a separate integrity stream because tamper evidence is cross-cutting, not just another application field.
- The more detailed starter streams such as `docpkg-*`, `credit-*`, and `fraud-*` are refinements of the same aggregate-boundary logic, not a contradiction of it.

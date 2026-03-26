# Submission Checklist: The Ledger (Level 5)

This checklist directly maps the required rubric deliverables to their finalized files and artifacts in this workspace. 

## 1. Core Documentation Deliverables

| Deliverable | File Location | Description |
|---|---|---|
| **Domain Notes (Complete, finalized)** | [`DOMAIN_NOTES.md`](DOMAIN_NOTES.md) | Domain philosophy, EDA vs. ES comparison, and aggregate definitions. |
| **Design Specification (Quantitative & Finalized)** | [`DESIGN.md`](DESIGN.md) | Full architectural plan, including Level-5 additions: column justifications, distributed daemon strategy, lag SLOs, and tradeoff reflection. |
| **Consolidated Final Report** | [`FINAL_REPORT.md`](FINAL_REPORT.md) | Interim to Final sprint narrative, plus the complete **Assessment Rubric Evidence Matrix** (Section 8). |

## 2. Evidence Artifacts (Proof of Level 5 Capability)

| Category | Artifact File | Description |
|---|---|---|
| **Concurrency & OCC** | [`artifacts/concurrency_load_test.txt`](artifacts/concurrency_load_test.txt) | Double-decision test output showing strict DB-level OCC locking to prevent silent overwrites. |
| **Projection Lag & SLOs** | [`artifacts/projection_lag_measurements.txt`](artifacts/projection_lag_measurements.txt) | Load test measurements proving daemon achieves P95 lag < 250ms and absolute max < 1000ms. |
| **Immutability & Integrity** | [`artifacts/immutability_and_hash_chain.txt`](artifacts/immutability_and_hash_chain.txt) | Upcasting execution without data mutation + hash chain tamper detection identifying a simulated raw DBA database edit. |
| **MCP Integration Trace** | [`artifacts/mcp_lifecycle_trace.txt`](artifacts/mcp_lifecycle_trace.txt) | Full lifecycle API walkthrough using solely MCP boundaries, obscuring domain complexity from the client. |
| **Bonus (What-If & Reg)** | [`artifacts/counterfactual_and_regulatory.txt`](artifacts/counterfactual_and_regulatory.txt) | Evidence of causal chain filtering in what-if projections, plus the generated regulatory compliance package format. |

## 3. Architecture Diagrams

Included embedded or referenced within `DESIGN.md` and the workspace:
- `architecture.md` (Mermaid source)
- `architecture deep.jpg` (High-resolution structure)
- `image.png` (High-level data flow)
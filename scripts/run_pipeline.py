"""
scripts/run_pipeline.py — Process one application through one or more agents.

Usage:
  python scripts/run_pipeline.py --application APEX-0007 --phase all
  python scripts/run_pipeline.py --application APEX-0007 --phase credit
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

import asyncpg
from dotenv import load_dotenv

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from ledger.agents.credit_analysis_agent import CreditAnalysisAgent
from ledger.agents.stub_agents import (
    ComplianceAgent,
    DecisionOrchestratorAgent,
    DocumentProcessingAgent,
    FraudDetectionAgent,
)
from ledger.event_store import EventStore
from ledger.llm import create_llm_client, resolve_model_name
from ledger.registry.client import ApplicantRegistryClient


PHASE_ORDER = ["document", "credit", "fraud", "compliance", "decision"]


def _build_agent(phase: str, store, registry, client, model: str):
    if phase == "document":
        return DocumentProcessingAgent("AGENT-CLI-DOCUMENT", "document_processing", store, registry, client, model=model)
    if phase == "credit":
        return CreditAnalysisAgent("AGENT-CLI-CREDIT", "credit_analysis", store, registry, client, model=model)
    if phase == "fraud":
        return FraudDetectionAgent("AGENT-CLI-FRAUD", "fraud_detection", store, registry, client, model=model)
    if phase == "compliance":
        return ComplianceAgent("AGENT-CLI-COMPLIANCE", "compliance", store, registry, client, model=model)
    if phase == "decision":
        return DecisionOrchestratorAgent("AGENT-CLI-DECISION", "decision_orchestrator", store, registry, client, model=model)
    raise ValueError(f"Unknown phase: {phase}")


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--application", required=True)
    parser.add_argument("--phase", default="all", choices=["all", *PHASE_ORDER])
    parser.add_argument(
        "--db-url",
        default=os.environ.get("DB_URL")
        or os.environ.get("DATABASE_URL")
        or "postgresql://postgres:postgres@localhost:5433/ledger",
    )
    args = parser.parse_args()

    db_url = args.db_url
    print(f"Connecting to database: {db_url}")

    try:
        client, provider = create_llm_client()
        print(f"LLM provider: {provider}")
    except Exception as exc:
        client = None
        print(f"LLM client unavailable ({exc}). Running in fallback mode.")

    model = resolve_model_name("claude-sonnet-4-20250514")

    pool = await asyncpg.create_pool(db_url)
    store = EventStore(db_url)
    await store.connect()

    try:
        registry = ApplicantRegistryClient(pool)
        phases = PHASE_ORDER if args.phase == "all" else [args.phase]

        print(f"--- Running phases: {', '.join(phases)} for application {args.application} ---")
        last_result = None

        for phase in phases:
            print(f"\n>>> Phase: {phase}")
            agent = _build_agent(phase, store, registry, client, model)
            last_result = await agent.process_application(args.application)
            print(
                "Phase result:",
                {
                    "session_id": last_result.get("session_id"),
                    "next_agent": last_result.get("next_agent") or last_result.get("next_agent_triggered"),
                    "output_events": last_result.get("output_events") or last_result.get("output_events_written"),
                },
            )

        print("\nPipeline Result:")
        print("  Status: SUCCESS")
        if last_result is not None:
            print(f"  Final Session ID: {last_result.get('session_id')}")
            print(f"  Final Next Agent: {last_result.get('next_agent') or last_result.get('next_agent_triggered')}")
    finally:
        await store.close()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())

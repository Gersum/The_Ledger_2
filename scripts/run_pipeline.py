"""
scripts/run_pipeline.py — Process one application through all agents.
Usage: python scripts/run_pipeline.py --application APEX-0007 [--phase all|document|credit|fraud|compliance|decision]
"""
import argparse, asyncio, os, sys
from pathlib import Path; sys.path.insert(0, str(Path(__file__).parent.parent))
from dotenv import load_dotenv; load_dotenv()

from ledger.llm import create_llm_client, resolve_model_name

async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--application", required=True)
    p.add_argument("--phase", default="all")
    p.add_argument("--db-url", default=os.environ.get("DB_URL") or os.environ.get("DATABASE_URL") or "postgresql://postgres:postgres@localhost:5433/ledger")
    args = p.parse_args()

    client, provider = create_llm_client()
    model = resolve_model_name("claude-sonnet-4-20250514")

    import asyncpg
    from ledger.event_store import EventStore
    from ledger.registry.client import ApplicantRegistryClient
    from ledger.agents.base_agent import CreditAnalysisAgent

    # Use DB_URL if provided, else fall back to env or default
    db_url = args.db_url or os.environ.get("DATABASE_URL") or os.environ.get("DB_URL")
    if not db_url:
        print("Error: DATABASE_URL or DB_URL environment variable must be set.")
        return

    print(f"Connecting to database: {db_url}")
    pool = await asyncpg.create_pool(db_url)
    
    try:
        store = EventStore(db_url)
        await store.connect()
        registry = ApplicantRegistryClient(pool)
        
        # Initialize the target agent (currently only Credit is implemented)
        agent_id = "AGENT-CLI-PIPELINE"
        agent = CreditAnalysisAgent(agent_id, "credit_analysis", store, registry, client, model=model)
        
        print(f"--- Running Pipeline Phase: {args.phase} for Application: {args.application} ---")
        
        # In a real system, we'd map 'phase' to different agents. 
        # For now, we run the CreditAnalysisAgent as the entry point.
        result = await agent.process_application(args.application)
        
        print("\nPipeline Result:")
        print(f"  Session ID: {result.get('session_id')}")
        print(f"  Status: SUCCESS")
        print(f"  Next Agent: {result.get('next_agent_triggered')}")
        
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())

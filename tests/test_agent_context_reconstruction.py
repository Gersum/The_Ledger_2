from src.models.agent_context import AgentContext

def test_agent_context_reconstruction():
    ctx = AgentContext(session_id="s1", application_id="a1")
    
    # Add 5 events
    ctx.add_event("Init", "SUCCESS", "started", 100)
    ctx.add_event("Step1", "SUCCESS", "step 1 done", 100)
    ctx.add_event("Step2", "ERROR", "failed to parse", 200)
    ctx.add_event("Step3", "SUCCESS", "step 3 done", 100)
    ctx.add_event("Step4", "PENDING", "waiting", 50)
    ctx.add_event("Step5", "SUCCESS", "step 5 done", 100)
    
    summary = ctx.summarize(max_tokens=4000)
    
    assert "failed to parse" in summary # ERROR retained
    assert "waiting" in summary # PENDING retained
    assert "step 5 done" in summary # Last 3 includes this
    assert "started" not in summary # Early success omitted
    
    # Test token aware reconciliation
    ctx.add_event("HugeStep", "ERROR", "huge error", 5000)
    ctx.summarize(max_tokens=4000)
    assert ctx.needs_reconciliation is True

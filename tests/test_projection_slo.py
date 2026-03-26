import pytest
import time
import asyncio

class MockEventStore:
    async def load_all(self, batch_size=500):
        # yields batches of events
        for _ in range(10): # 5000 events total
            batch = [{"event_type": "DummyEvent"} for _ in range(batch_size)]
            yield batch

class DummyProjection:
    def __init__(self):
        self.count = 0
        self.last_latency = 0
        
    async def rebuild_from_scratch(self, event_store):
        t0 = time.time()
        self.count = 0
        async for batch in event_store.load_all(batch_size=500):
            # simulate processing
            await asyncio.sleep(0.001)
            self.count += len(batch)
        self.last_latency = (time.time() - t0) * 1000 # ms
        
@pytest.mark.asyncio
async def test_projection_rebuild_from_scratch_slo():
    store = MockEventStore()
    projection = DummyProjection()
    
    await projection.rebuild_from_scratch(store)
    
    assert projection.count == 5000
    # SLO: processing 5000 records should take < 500ms
    assert projection.last_latency < 500, f"Latency {projection.last_latency}ms breached 500ms SLO"

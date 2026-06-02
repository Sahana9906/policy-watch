import pytest

from orchestrator.listener import OrchestrationEvent, dispatch_orchestration_event


pytestmark = pytest.mark.asyncio


async def test_workflow_listener_dispatches_workflow_events(monkeypatch):
    calls = []

    async def fake_dispatch(payload):
        calls.append(payload)
        return {"dispatched": True}

    monkeypatch.setattr("orchestrator.listener.dispatch_event", fake_dispatch)

    payload = {
        "workflow_run_id": 123,
        "current_stage": "regulation_detected",
    }
    await dispatch_orchestration_event(
        OrchestrationEvent(
            channel="workflow_runs",
            payload=payload,
            raw_payload="{}",
            pid=1,
        )
    )

    assert calls == [payload]


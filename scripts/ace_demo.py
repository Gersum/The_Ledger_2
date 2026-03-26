#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone


def call_json(base_url: str, method: str, path: str, payload: dict | None = None) -> tuple[dict, float]:
    url = f"{base_url.rstrip('/')}{path}"
    data = None
    headers = {"Content-Type": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(url=url, method=method, data=data, headers=headers)
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
    elapsed = time.perf_counter() - started

    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        parsed = {"status": "error", "reason": f"non-json response: {body[:200]}"}

    return parsed, elapsed


def print_step(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run 6-step readiness demo against Ledger UI API")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000", help="Ledger UI API base URL")
    args = parser.parse_args()

    base = args.base_url

    print_step("Step 1: Complete Decision History (<60s)")
    full, full_t = call_json(base, "POST", "/api/run_full_pipeline")
    app_id = full.get("app_id")
    if full.get("status") != "ok" or not app_id:
        print(f"FAIL: run_full_pipeline failed in {full_t:.2f}s -> {full}")
        return 1
    history, hist_t = call_json(base, "GET", f"/api/history/{app_id}")
    ok1 = (
        history.get("status") == "ok"
        and history.get("timing", {}).get("under_60_seconds")
        and history.get("integrity", {}).get("valid")
    )
    print(f"application_id={app_id}")
    print(f"pipeline_time={full_t:.2f}s history_time={hist_t:.2f}s")
    print(f"events={history.get('counts', {}).get('total_events')} causal_links={history.get('counts', {}).get('causal_links')}")
    print("PASS" if ok1 else "FAIL", json.dumps(history.get("timing", {}), default=str))

    print_step("Step 2: Concurrency Under Pressure")
    occ, occ_t = call_json(base, "POST", "/api/concurrency_test")
    results = occ.get("results") or []
    occ_seen = any(
        any("OCC Error" in log for log in result.get("logs", []))
        for result in results
        if isinstance(result, dict)
    )
    retry_seen = any(
        any("retrying" in log.lower() for log in result.get("logs", []))
        for result in results
        if isinstance(result, dict)
    )
    ok2 = occ.get("status") == "ok" and occ_seen and retry_seen
    print(f"time={occ_t:.2f}s stream={occ.get('stream_id')}")
    print(f"occ_seen={occ_seen} retry_seen={retry_seen}")
    print("PASS" if ok2 else "FAIL")

    print_step("Step 3: Temporal Compliance Query")
    as_of = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
    encoded = urllib.parse.quote(as_of, safe=":")
    snap, snap_t = call_json(base, "GET", f"/api/applications/{app_id}/compliance?as_of={encoded}")
    ok3 = snap.get("status") == "ok" and isinstance(snap.get("snapshot", {}).get("rules"), list)
    print(f"time={snap_t:.2f}s resource={snap.get('resource')}")
    print(f"rules={len(snap.get('snapshot', {}).get('rules', []))} verdict={snap.get('snapshot', {}).get('overall_verdict')}")
    print("PASS" if ok3 else "FAIL")

    print_step("Step 4: Upcasting and Immutability")
    upcast, up_t = call_json(base, "POST", "/api/upcast_probe")
    ok4 = upcast.get("status") == "ok" and bool(upcast.get("immutability_verified"))
    print(f"time={up_t:.2f}s")
    print(
        "loaded_v=", upcast.get("loaded_event_version"),
        "stored_v=", upcast.get("stored_event_version"),
        "immutability_verified=", upcast.get("immutability_verified"),
    )
    print("PASS" if ok4 else "FAIL")

    print_step("Step 5: Gas Town Recovery")
    crash, crash_t = call_json(base, "POST", f"/api/simulate_agent_crash?application_id={app_id}&agent_type=fraud_detection")
    resumed_state = (((crash.get("resumed_context") or {}).get("reconstructed") or {}).get("session_state"))
    source = (((crash.get("resumed_context") or {}).get("session") or {}).get("context_source"))
    ok5 = (
        crash.get("status") == "ok"
        and resumed_state == "completed"
        and isinstance(source, str)
        and source.startswith("prior_session_replay:")
    )
    print(f"time={crash_t:.2f}s crashed={crash.get('crashed_session_id')} resumed={crash.get('recovered_session_id')}")
    print(f"resumed_state={resumed_state} context_source={source}")
    print("PASS" if ok5 else "FAIL")

    print_step("Step 6: What-If Counterfactual")
    whatif, whatif_t = call_json(base, "GET", f"/api/what_if/{app_id}?risk_tier=HIGH&assume_confidence=0.65")
    ok6 = whatif.get("status") == "ok" and "counterfactual" in whatif
    print(f"time={whatif_t:.2f}s")
    print(
        f"baseline={whatif.get('baseline', {}).get('final_recommendation')} -> "
        f"counterfactual={whatif.get('counterfactual', {}).get('final_recommendation')}"
    )
    print(f"delta={whatif.get('delta')}")
    print("PASS" if ok6 else "FAIL")

    overall = all([ok1, ok2, ok3, ok4, ok5, ok6])
    print_step("Summary")
    print(f"overall={'PASS' if overall else 'FAIL'}")
    return 0 if overall else 2


if __name__ == "__main__":
    raise SystemExit(main())

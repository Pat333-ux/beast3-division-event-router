import os
import json
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional
import httpx
from fastapi import FastAPI, Request, HTTPException, Header
from google.cloud import firestore

app = FastAPI(title="BEAST 3.0 21-Division Event Router", version="3.0.0")

GCP_PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "")
MAX_RETRIES = int(os.environ.get("BEAST_MAX_RETRIES", "3"))
SLA_TIMEOUT_MINUTES = int(os.environ.get("BEAST_SLA_MINUTES", "15"))

# Division Routing Table (DIV-01 to DIV-21)
DIVISION_MATRIX: Dict[str, Dict[str, Any]] = {
    "treasury": {"id": "DIV-01", "name": "Treasury & LUCR Allocation", "priority": 1, "webhook": os.environ.get("DIV_01_URL", "")},
    "governance": {"id": "DIV-02", "name": "Constitutional Governance", "priority": 1, "webhook": os.environ.get("DIV_02_URL", "")},
    "family_first": {"id": "DIV-03", "name": "Family First Aid & Benefits", "priority": 1, "webhook": os.environ.get("DIV_03_URL", "")},
    "program_300": {"id": "DIV-04", "name": "Program 300 & Disability Pathway", "priority": 1, "webhook": os.environ.get("DIV_04_URL", "")},
    "municipal": {"id": "DIV-05", "name": "Municipal Task Force Interface", "priority": 2, "webhook": os.environ.get("DIV_05_URL", "")},
    "external_relations": {"id": "DIV-06", "name": "External Relations & Comms", "priority": 3, "webhook": os.environ.get("DIV_06_URL", "")},
    "emergency": {"id": "DIV-07", "name": "Emergency Response & Rapid Aid", "priority": 0, "webhook": os.environ.get("DIV_07_URL", "")},
    "qa_audit": {"id": "DIV-08", "name": "Quality Assurance & Telemetry", "priority": 2, "webhook": os.environ.get("DIV_08_URL", "")},
}

db = firestore.Client(project=GCP_PROJECT) if GCP_PROJECT else None


def compute_idempotency_key(event_id: str, payload: Dict[str, Any]) -> str:
    """Generates a deterministic SHA-256 lock key to prevent duplicate execution."""
    raw = f"{event_id}:{json.dumps(payload, sort_keys=True)}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def resolve_division(domain: str, urgency: str = "normal") -> Dict[str, Any]:
    """Deterministically maps an incoming task domain to its target BEAST division."""
    if urgency == "critical":
        return DIVISION_MATRIX["emergency"]
    return DIVISION_MATRIX.get(domain.lower(), {
        "id": "DIV-21",
        "name": "System Orchestration & Fallback Triage",
        "priority": 2,
        "webhook": os.environ.get("DIV_21_URL", ""),
    })


async def execute_division_dispatch(task_id: str, division: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    """Dispatches task payload to target division microservice if webhook is configured."""
    webhook_url = division.get("webhook", "")
    if not webhook_url:
        return {"dispatched": True, "mode": "ledger_queued", "status_code": 202}

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(
            webhook_url,
            json={"task_id": task_id, "division_id": division["id"], "payload": payload},
        )
        resp.raise_for_status()
        return {"dispatched": True, "mode": "webhook_live", "status_code": resp.status_code}


@app.get("/health")
async def health():
    return {
        "status": "operational",
        "service": "BEAST 3.0 21-Division Event Router",
        "active_divisions_configured": len(DIVISION_MATRIX),
    }


@app.post("/events/route")
async def route_event(
    request: Request,
    ce_id: Optional[str] = Header(default=None),
    ce_type: Optional[str] = Header(default="beast.task.created"),
):
    """Ingests direct API events or Firestore Eventarc CloudEvents with idempotency protection."""
    body = await request.json()
    event_id = ce_id or body.get("event_id") or hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()[:16]
    domain = body.get("domain", "orchestration")
    urgency = body.get("urgency", "normal")
    payload = body.get("payload", body)

    idem_key = compute_idempotency_key(event_id, payload)
    division = resolve_division(domain, urgency)
    now = datetime.now(timezone.utc)
    sla_deadline = now + timedelta(minutes=SLA_TIMEOUT_MINUTES)

    if db:
        lock_ref = db.collection("beast_idempotency_locks").document(idem_key)
        if lock_ref.get().exists:
            return {"status": "duplicate_ignored", "idempotency_key": idem_key}

        lock_ref.set({
            "event_id": event_id,
            "ce_type": ce_type,
            "division_id": division["id"],
            "created_at": now,
        })

        task_ref = db.collection("beast_division_tasks").document(idem_key)
        task_ref.set({
            "task_id": idem_key,
            "event_id": event_id,
            "domain": domain,
            "division_id": division["id"],
            "division_name": division["name"],
            "priority": division["priority"],
            "status": "ROUTED",
            "retry_count": 0,
            "sla_deadline": sla_deadline,
            "payload": payload,
            "updated_at": now,
        })

    try:
        dispatch_result = await execute_division_dispatch(idem_key, division, payload)
        if db:
            db.collection("beast_division_tasks").document(idem_key).update({
                "status": "EXECUTING",
                "dispatch_meta": dispatch_result,
                "updated_at": datetime.now(timezone.utc),
            })
        return {
            "status": "routed",
            "task_id": idem_key,
            "division": division["id"],
            "dispatch": dispatch_result,
        }
    except Exception as exc:
        if db:
            db.collection("beast_division_tasks").document(idem_key).update({
                "status": "RETRY_PENDING",
                "last_error": str(exc),
                "updated_at": datetime.now(timezone.utc),
            })
        raise HTTPException(status_code=502, detail=f"Division dispatch failed, queued for watchdog: {exc}")


@app.post("/watchdog/heal")
async def self_healing_watchdog():
    """Cloud Scheduler endpoint: detects stalled or failed division tasks and auto-recovers."""
    if not db:
        return {"status": "skipped", "reason": "Firestore not bound"}

    now = datetime.now(timezone.utc)
    stalled_tasks = (
        db.collection("beast_division_tasks")
        .where("status", "in", ["RETRY_PENDING", "ROUTED", "EXECUTING"])
        .where("sla_deadline", "<", now)
        .limit(50)
        .stream()
    )

    healed, escalated = [], []

    for doc in stalled_tasks:
        task = doc.to_dict()
        task_id = doc.id
        retries = task.get("retry_count", 0)

        if retries >= MAX_RETRIES:
            doc.reference.update({
                "status": "DEAD_LETTER_ESCALATED",
                "escalated_at": now,
            })
            escalated.append(task_id)
            continue

        division = resolve_division(task.get("domain", "orchestration"))
        try:
            res = await execute_division_dispatch(task_id, division, task.get("payload", {}))
            doc.reference.update({
                "status": "EXECUTING",
                "retry_count": retries + 1,
                "sla_deadline": now + timedelta(minutes=SLA_TIMEOUT_MINUTES),
                "dispatch_meta": res,
                "updated_at": now,
            })
            healed.append(task_id)
        except Exception as exc:
            doc.reference.update({
                "status": "RETRY_PENDING",
                "retry_count": retries + 1,
                "last_error": str(exc),
                "updated_at": now,
            })

    return {
        "status": "watchdog_sweep_complete",
        "healed_count": len(healed),
        "escalated_count": len(escalated),
        "healed_tasks": healed,
        "escalated_tasks": escalated,
    }


from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from typing import Optional
from datetime import datetime
from .schemas import EventBatch, Report
from . import crud
from .db import migrate
from .utils import parse_relative_window

app = FastAPI(title="AdPulse++", version="1.0.0")

@app.on_event("startup")
def _startup():
    migrate()

@app.get("/healthz")
def healthz():
    return {"status":"ok"}

@app.post("/events")
def ingest(batch: EventBatch):
    # Insert events
    inserted = crud.insert_events([e.model_dump() for e in batch.events])
    # Async MV refresh would be better; do simple refresh when small volumes
    if inserted <= 5000:
        try:
            crud.refresh_materialized_view()
        except Exception:
            pass
    return {"inserted": inserted}

@app.get("/report")
def report(campaign_id: str,
           start: Optional[str] = Query(None, description="ISO timestamp or relative like -1h"),
           end: Optional[str] = Query(None, description="ISO timestamp")):
    s = parse_relative_window(start) if start and start.startswith('-') else (datetime.fromisoformat(start.replace('Z','')) if start else None)
    e = datetime.fromisoformat(end.replace('Z','')) if end else None
    data = crud.get_report(campaign_id, s, e)
    return JSONResponse({
        "campaign_id": campaign_id,
        **data
    })

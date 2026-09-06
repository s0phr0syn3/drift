"""Alerts API endpoints."""

from datetime import datetime
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel
from sqlalchemy.orm import Session

from drift.storage.models import get_engine, AlertRule, AlertEvent
from drift.analysis.alerts import (
    get_alert_rules,
    create_alert_rule,
    get_alert_events,
    acknowledge_alert,
    run_anomaly_check,
)
from drift.storage.queries import get_monitored_database_by_name


router = APIRouter()


# Pydantic models
class AlertRuleCreate(BaseModel):
    name: str
    rule_type: Literal["latency_increase", "cache_drop", "temp_disk"]
    threshold: dict
    notification: dict | None = None
    enabled: bool = True


class AlertRuleResponse(BaseModel):
    id: int
    name: str
    rule_type: str
    threshold: dict
    notification: dict | None
    enabled: bool
    created_at: datetime


class AlertEventResponse(BaseModel):
    id: int
    rule_id: int | None
    database_id: int | None
    queryid: str | None
    triggered_at: datetime
    details: dict | None
    acknowledged: bool


class AcknowledgeRequest(BaseModel):
    acknowledged: bool = True


class CheckResult(BaseModel):
    events_created: int
    events: list[AlertEventResponse]


def get_db_session(request: Request):
    """Dependency to get database session."""
    config = request.app.state.config
    engine = get_engine(config.storage.dsn)
    session = Session(engine)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@router.get("/alerts/rules", response_model=list[AlertRuleResponse])
def list_rules(
    enabled_only: bool = Query(False, description="Only return enabled rules"),
    session: Session = Depends(get_db_session),
):
    """List all alert rules."""
    rules = get_alert_rules(session, enabled_only=enabled_only)
    return [
        AlertRuleResponse(
            id=r.id,
            name=r.name,
            rule_type=r.rule_type,
            threshold=r.threshold,
            notification=r.notification,
            enabled=r.enabled,
            created_at=r.created_at,
        )
        for r in rules
    ]


@router.post("/alerts/rules", response_model=AlertRuleResponse, status_code=201)
def create_rule(
    rule: AlertRuleCreate,
    session: Session = Depends(get_db_session),
):
    """Create a new alert rule."""
    new_rule = create_alert_rule(
        session,
        name=rule.name,
        rule_type=rule.rule_type,
        threshold=rule.threshold,
        notification=rule.notification,
        enabled=rule.enabled,
    )
    return AlertRuleResponse(
        id=new_rule.id,
        name=new_rule.name,
        rule_type=new_rule.rule_type,
        threshold=new_rule.threshold,
        notification=new_rule.notification,
        enabled=new_rule.enabled,
        created_at=new_rule.created_at,
    )


@router.get("/alerts/rules/{rule_id}", response_model=AlertRuleResponse)
def get_rule(
    rule_id: int,
    session: Session = Depends(get_db_session),
):
    """Get a specific alert rule."""
    rule = session.get(AlertRule, rule_id)
    if not rule:
        raise HTTPException(status_code=404, detail=f"Alert rule {rule_id} not found")
    return AlertRuleResponse(
        id=rule.id,
        name=rule.name,
        rule_type=rule.rule_type,
        threshold=rule.threshold,
        notification=rule.notification,
        enabled=rule.enabled,
        created_at=rule.created_at,
    )


@router.put("/alerts/rules/{rule_id}", response_model=AlertRuleResponse)
def update_rule(
    rule_id: int,
    update: AlertRuleCreate,
    session: Session = Depends(get_db_session),
):
    """Update an alert rule."""
    rule = session.get(AlertRule, rule_id)
    if not rule:
        raise HTTPException(status_code=404, detail=f"Alert rule {rule_id} not found")

    rule.name = update.name
    rule.rule_type = update.rule_type
    rule.threshold = update.threshold
    rule.notification = update.notification
    rule.enabled = update.enabled

    return AlertRuleResponse(
        id=rule.id,
        name=rule.name,
        rule_type=rule.rule_type,
        threshold=rule.threshold,
        notification=rule.notification,
        enabled=rule.enabled,
        created_at=rule.created_at,
    )


@router.delete("/alerts/rules/{rule_id}", status_code=204)
def delete_rule(
    rule_id: int,
    session: Session = Depends(get_db_session),
):
    """Delete an alert rule."""
    rule = session.get(AlertRule, rule_id)
    if not rule:
        raise HTTPException(status_code=404, detail=f"Alert rule {rule_id} not found")
    session.delete(rule)


@router.get("/alerts/events", response_model=list[AlertEventResponse])
def list_events(
    acknowledged: bool | None = Query(None, description="Filter by acknowledged status"),
    limit: int = Query(100, ge=1, le=1000),
    session: Session = Depends(get_db_session),
):
    """List alert events."""
    events = get_alert_events(session, acknowledged=acknowledged, limit=limit)
    return [
        AlertEventResponse(
            id=e.id,
            rule_id=e.rule_id,
            database_id=e.database_id,
            queryid=str(e.queryid) if e.queryid else None,
            triggered_at=e.triggered_at,
            details=e.details,
            acknowledged=e.acknowledged,
        )
        for e in events
    ]


@router.post("/alerts/events/{event_id}/acknowledge", response_model=AlertEventResponse)
def acknowledge_event(
    event_id: int,
    session: Session = Depends(get_db_session),
):
    """Acknowledge an alert event."""
    if not acknowledge_alert(session, event_id):
        raise HTTPException(status_code=404, detail=f"Alert event {event_id} not found")

    event = session.get(AlertEvent, event_id)
    return AlertEventResponse(
        id=event.id,
        rule_id=event.rule_id,
        database_id=event.database_id,
        queryid=str(event.queryid) if event.queryid else None,
        triggered_at=event.triggered_at,
        details=event.details,
        acknowledged=event.acknowledged,
    )


@router.post("/alerts/check", response_model=CheckResult)
def run_check(
    database: str | None = Query(None, description="Check specific database only"),
    notify: bool = Query(True, description="Send webhook notifications"),
    session: Session = Depends(get_db_session),
):
    """Run anomaly detection and generate alerts."""
    database_id = None
    if database:
        db = get_monitored_database_by_name(session, database)
        if not db:
            raise HTTPException(status_code=404, detail=f"Database '{database}' not found")
        database_id = db.id

    events = run_anomaly_check(session, database_id, notify=notify)

    return CheckResult(
        events_created=len(events),
        events=[
            AlertEventResponse(
                id=e.id,
                rule_id=e.rule_id,
                database_id=e.database_id,
                queryid=str(e.queryid) if e.queryid else None,
                triggered_at=e.triggered_at,
                details=e.details,
                acknowledged=e.acknowledged,
            )
            for e in events
        ],
    )

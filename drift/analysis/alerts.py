"""Alert rule evaluation and notification handling."""

import json
import urllib.request
from dataclasses import asdict
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from drift.storage.models import AlertRule, AlertEvent, MonitoredDatabase
from drift.storage.queries import list_monitored_databases
from drift.analysis.trending import (
    Anomaly,
    check_query_for_anomalies,
    calculate_baseline,
    get_recent_metrics,
)


def get_alert_rules(session: Session, enabled_only: bool = True) -> list[AlertRule]:
    """Get all alert rules."""
    stmt = select(AlertRule)
    if enabled_only:
        stmt = stmt.where(AlertRule.enabled == True)  # noqa: E712
    return list(session.scalars(stmt))


def create_alert_rule(
    session: Session,
    name: str,
    rule_type: str,
    threshold: dict,
    notification: dict | None = None,
    enabled: bool = True,
) -> AlertRule:
    """Create a new alert rule.

    Args:
        session: Database session
        name: Human-readable name for the rule
        rule_type: Type of rule (latency_increase, cache_drop, temp_disk, new_query)
        threshold: Rule-specific threshold config
        notification: Optional notification config (webhook_url, etc.)
        enabled: Whether the rule is active

    Returns:
        Created AlertRule
    """
    rule = AlertRule(
        name=name,
        rule_type=rule_type,
        threshold=threshold,
        notification=notification,
        enabled=enabled,
    )
    session.add(rule)
    session.flush()
    return rule


def record_alert_event(
    session: Session,
    rule: AlertRule | None,
    anomaly: Anomaly,
) -> AlertEvent:
    """Record a triggered alert event.

    Args:
        session: Database session
        rule: The rule that triggered (optional)
        anomaly: The detected anomaly

    Returns:
        Created AlertEvent
    """
    event = AlertEvent(
        rule_id=rule.id if rule else None,
        database_id=anomaly.database_id,
        queryid=anomaly.queryid,
        details={
            "anomaly_type": anomaly.anomaly_type,
            "current_value": anomaly.current_value,
            "baseline_value": anomaly.baseline_value,
            "percent_change": anomaly.percent_change if anomaly.percent_change != float("inf") else "inf",
            **anomaly.details,
        },
    )
    session.add(event)
    session.flush()
    return event


def get_alert_events(
    session: Session,
    acknowledged: bool | None = None,
    limit: int = 100,
) -> list[AlertEvent]:
    """Get alert events.

    Args:
        session: Database session
        acknowledged: Filter by acknowledgment status (None = all)
        limit: Maximum number of events to return

    Returns:
        List of AlertEvent objects
    """
    stmt = select(AlertEvent).order_by(AlertEvent.triggered_at.desc()).limit(limit)
    if acknowledged is not None:
        stmt = stmt.where(AlertEvent.acknowledged == acknowledged)
    return list(session.scalars(stmt))


def acknowledge_alert(session: Session, event_id: int) -> bool:
    """Mark an alert event as acknowledged.

    Returns True if event was found and updated, False otherwise.
    """
    event = session.get(AlertEvent, event_id)
    if event:
        event.acknowledged = True
        return True
    return False


def send_webhook_notification(
    webhook_url: str,
    anomaly: Anomaly,
    rule_name: str | None = None,
) -> bool:
    """Send alert notification via webhook.

    Sends a JSON payload to the configured webhook URL.

    Args:
        webhook_url: URL to POST to
        anomaly: The detected anomaly
        rule_name: Name of the rule that triggered

    Returns:
        True if successful, False otherwise
    """
    payload = {
        "alert_type": anomaly.anomaly_type,
        "rule_name": rule_name,
        "queryid": anomaly.queryid,
        "database_id": anomaly.database_id,
        "current_value": anomaly.current_value,
        "baseline_value": anomaly.baseline_value,
        "percent_change": anomaly.percent_change if anomaly.percent_change != float("inf") else "inf",
        "triggered_at": datetime.now(timezone.utc).isoformat(),
        "details": anomaly.details,
    }

    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            webhook_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status == 200
    except Exception:
        return False


def evaluate_rules_for_query(
    session: Session,
    queryid: int,
    database_id: int,
) -> list[tuple[AlertRule, Anomaly]]:
    """Evaluate all enabled rules against a single query.

    Returns list of (rule, anomaly) tuples for triggered rules.
    """
    rules = get_alert_rules(session, enabled_only=True)
    triggered = []

    for rule in rules:
        threshold = rule.threshold or {}

        if rule.rule_type == "latency_increase":
            percent = threshold.get("percent_increase", 50)
            anomalies = check_query_for_anomalies(
                session, queryid, database_id,
                latency_threshold=percent,
                cache_drop_threshold=999,  # Disable
                temp_threshold=9999,  # Disable
            )
            for a in anomalies:
                if a.anomaly_type == "latency_increase":
                    triggered.append((rule, a))

        elif rule.rule_type == "cache_drop":
            drop_points = threshold.get("min_drop_points", 10)
            anomalies = check_query_for_anomalies(
                session, queryid, database_id,
                latency_threshold=9999,  # Disable
                cache_drop_threshold=drop_points,
                temp_threshold=9999,  # Disable
            )
            for a in anomalies:
                if a.anomaly_type == "cache_drop":
                    triggered.append((rule, a))

        elif rule.rule_type == "temp_disk":
            percent = threshold.get("percent_increase", 100)
            anomalies = check_query_for_anomalies(
                session, queryid, database_id,
                latency_threshold=9999,  # Disable
                cache_drop_threshold=999,  # Disable
                temp_threshold=percent,
            )
            for a in anomalies:
                if a.anomaly_type == "temp_disk":
                    triggered.append((rule, a))

    return triggered


def run_anomaly_check(
    session: Session,
    database_id: int | None = None,
    notify: bool = True,
) -> list[AlertEvent]:
    """Run anomaly detection for all queries and create alert events.

    This is the main entry point for scheduled anomaly detection.

    Args:
        session: Database session
        database_id: Optional filter to check only one database
        notify: Whether to send webhook notifications

    Returns:
        List of created AlertEvent objects
    """
    from drift.storage.models import QueryStatsRaw
    from sqlalchemy import distinct

    # Get databases to check
    if database_id:
        databases = [session.get(MonitoredDatabase, database_id)]
        databases = [d for d in databases if d]
    else:
        databases = list_monitored_databases(session, enabled_only=True)

    created_events = []

    for db in databases:
        # Get all queryids with recent activity
        recent_queries = session.execute(
            select(distinct(QueryStatsRaw.queryid))
            .where(QueryStatsRaw.database_id == db.id)
        ).scalars().all()

        for queryid in recent_queries:
            triggered = evaluate_rules_for_query(session, queryid, db.id)

            for rule, anomaly in triggered:
                event = record_alert_event(session, rule, anomaly)
                created_events.append(event)

                # Send notification if configured
                if notify and rule.notification:
                    webhook_url = rule.notification.get("webhook_url")
                    if webhook_url:
                        send_webhook_notification(webhook_url, anomaly, rule.name)

    return created_events

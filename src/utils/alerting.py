"""
Alerting Module
===============

This module provides functions to send alerts.

Why alerting?
- Pipelines run at 3 AM — who's watching?
- Problems need immediate attention
- Right people get right alerts (severity levels)
- Alerts are actionable (include what to do)

Alert Channels:
- Slack: For most alerts (team visibility)
- PagerDuty: For P1 alerts (wakes up on-call)
- Email: For reports and summaries

Severity Levels:
- P1 (Critical): Revenue impact, data loss → Page on-call
- P2 (High): SLA breach, major DQ issue → Slack urgent
- P3 (Medium): Warning, anomaly → Slack normal
- P4 (Low): Informational → Log only

Usage:
    from src.utils.alerting import AlertManager, Severity

    alerter = AlertManager(slack_webhook="https://hooks.slack.com/...")

    alerter.send_alert(
        title="Pipeline Failed",
        message="Silver transactions failed after 3 retries",
        severity=Severity.P1,
        pipeline="silver_transactions",
        runbook_url="https://wiki/runbooks/silver"
    )
"""

import json
import requests
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class Severity(Enum):
    """
    Alert severity levels.

    Why severity levels?
    - P1: Someone needs to wake up RIGHT NOW
    - P2: Needs attention today
    - P3: Should look at this week
    - P4: Nice to know
    """

    P1 = "P1_CRITICAL"
    P2 = "P2_HIGH"
    P3 = "P3_MEDIUM"
    P4 = "P4_LOW"


@dataclass
class Alert:
    """
    Represents an alert to be sent.

    Why a dataclass?
    - Structured way to define alerts
    - Easy to serialize to JSON
    - Required fields are explicit
    """

    title: str
    message: str
    severity: Severity
    pipeline: str
    timestamp: datetime = None
    runbook_url: Optional[str] = None
    additional_context: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "title": self.title,
            "message": self.message,
            "severity": self.severity.value,
            "pipeline": self.pipeline,
            "timestamp": self.timestamp.isoformat(),
            "runbook_url": self.runbook_url,
            "additional_context": self.additional_context,
        }


class AlertManager:
    """
    Manages sending alerts to various channels.

    This class:
    - Sends alerts to Slack
    - Sends P1 alerts to PagerDuty
    - Formats alerts nicely
    - Handles failures gracefully

    Example:
        alerter = AlertManager(
            slack_webhook="https://hooks.slack.com/services/XXX/YYY/ZZZ",
            pagerduty_key="your-routing-key"
        )

        alerter.send_alert(
            title="Pipeline Failed",
            message="Details about the failure...",
            severity=Severity.P1,
            pipeline="silver_transactions"
        )
    """

    # Slack colors for each severity
    SEVERITY_COLORS = {
        Severity.P1: "#FF0000",  # Red
        Severity.P2: "#FFA500",  # Orange
        Severity.P3: "#FFFF00",  # Yellow
        Severity.P4: "#00FF00",  # Green
    }

    # Emoji for each severity
    SEVERITY_EMOJI = {
        Severity.P1: "🚨",
        Severity.P2: "⚠️",
        Severity.P3: "📢",
        Severity.P4: "ℹ️",
    }

    def __init__(
        self,
        slack_webhook: Optional[str] = None,
        pagerduty_key: Optional[str] = None,
        dry_run: bool = False,
    ):
        """
        Initialize the alert manager.

        Args:
            slack_webhook: Slack incoming webhook URL
            pagerduty_key: PagerDuty Events API routing key
            dry_run: If True, print alerts instead of sending
        """
        self.slack_webhook = slack_webhook
        self.pagerduty_key = pagerduty_key
        self.dry_run = dry_run

    def send_alert(
        self,
        title: str,
        message: str,
        severity: Severity,
        pipeline: str,
        runbook_url: Optional[str] = None,
        additional_context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Send an alert to appropriate channels based on severity.

        Args:
            title: Short alert title
            message: Detailed message
            severity: Alert severity level
            pipeline: Name of the pipeline
            runbook_url: Link to runbook for this alert
            additional_context: Extra context to include

        Returns:
            True if alert was sent successfully
        """
        alert = Alert(
            title=title,
            message=message,
            severity=severity,
            pipeline=pipeline,
            runbook_url=runbook_url,
            additional_context=additional_context,
        )

        success = True

        # Always print to console
        self._print_alert(alert)

        if self.dry_run:
            print("[DRY RUN] Alert would be sent (not actually sending)")
            return True

        # Send to Slack for all severities
        if self.slack_webhook:
            success = success and self._send_slack(alert)

        # Send to PagerDuty only for P1
        if severity == Severity.P1 and self.pagerduty_key:
            success = success and self._send_pagerduty(alert)

        return success

    def _print_alert(self, alert: Alert) -> None:
        """Print alert to console."""
        emoji = self.SEVERITY_EMOJI[alert.severity]
        print(f"\n{'='*60}")
        print(f"{emoji} ALERT [{alert.severity.value}]: {alert.title}")
        print(f"{'='*60}")
        print(f"Pipeline: {alert.pipeline}")
        print(f"Time: {alert.timestamp.isoformat()}")
        print(f"Message: {alert.message}")
        if alert.runbook_url:
            print(f"Runbook: {alert.runbook_url}")
        print(f"{'='*60}\n")

    def _send_slack(self, alert: Alert) -> bool:
        """
        Send alert to Slack.

        Uses Slack Block Kit for rich formatting.
        """
        emoji = self.SEVERITY_EMOJI[alert.severity]
        color = self.SEVERITY_COLORS[alert.severity]

        # Build Slack message using Block Kit
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} [{alert.severity.value}] {alert.title}",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": alert.message,
                },
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Pipeline:*\n{alert.pipeline}",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Time:*\n{alert.timestamp.strftime('%Y-%m-%d %H:%M:%S')}",
                    },
                ],
            },
        ]

        # Add runbook link if provided
        if alert.runbook_url:
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"📖 <{alert.runbook_url}|View Runbook>",
                    },
                }
            )

        # Add additional context if provided
        if alert.additional_context:
            context_text = "\n".join(f"• *{k}:* {v}" for k, v in alert.additional_context.items())
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Additional Context:*\n{context_text}",
                    },
                }
            )

        payload = {
            "attachments": [
                {
                    "color": color,
                    "blocks": blocks,
                }
            ]
        }

        try:
            response = requests.post(
                self.slack_webhook,
                json=payload,
                timeout=10,
            )
            return response.status_code == 200
        except Exception as e:
            print(f"Failed to send Slack alert: {e}")
            return False

    def _send_pagerduty(self, alert: Alert) -> bool:
        """
        Send alert to PagerDuty.

        Only used for P1 alerts. Will page on-call engineer.
        """
        payload = {
            "routing_key": self.pagerduty_key,
            "event_action": "trigger",
            "dedup_key": f"{alert.pipeline}_{alert.title}_{alert.timestamp.strftime('%Y%m%d')}",
            "payload": {
                "summary": f"[{alert.severity.value}] {alert.title}",
                "source": alert.pipeline,
                "severity": "critical",
                "timestamp": alert.timestamp.isoformat(),
                "custom_details": {
                    "message": alert.message,
                    "runbook": alert.runbook_url,
                    "additional_context": alert.additional_context,
                },
            },
            "links": [],
        }

        if alert.runbook_url:
            payload["links"].append(
                {
                    "href": alert.runbook_url,
                    "text": "Runbook",
                }
            )

        try:
            response = requests.post(
                "https://events.pagerduty.com/v2/enqueue",
                json=payload,
                timeout=10,
            )
            return response.status_code == 202
        except Exception as e:
            print(f"Failed to send PagerDuty alert: {e}")
            return False


# ============================================================
# CONVENIENCE FUNCTIONS
# ============================================================


def send_pipeline_failure_alert(
    alert_manager: AlertManager,
    pipeline_name: str,
    error_message: str,
    retry_count: int = 0,
    runbook_url: Optional[str] = None,
) -> bool:
    """
    Send a standard pipeline failure alert.

    Args:
        alert_manager: AlertManager instance
        pipeline_name: Name of the failed pipeline
        error_message: Error details
        retry_count: Number of retries attempted
        runbook_url: Link to runbook

    Returns:
        True if alert sent successfully
    """
    return alert_manager.send_alert(
        title=f"{pipeline_name} Pipeline Failed",
        message=f"Pipeline failed after {retry_count} retries.\n\n*Error:*\n```{error_message[:500]}```",
        severity=Severity.P1,
        pipeline=pipeline_name,
        runbook_url=runbook_url,
        additional_context={
            "Retry Count": retry_count,
        },
    )


def send_dq_failure_alert(
    alert_manager: AlertManager,
    pipeline_name: str,
    table_name: str,
    check_name: str,
    failed_records: int,
    total_records: int,
    severity: Severity = Severity.P2,
    runbook_url: Optional[str] = None,
) -> bool:
    """
    Send a data quality failure alert.

    Args:
        alert_manager: AlertManager instance
        pipeline_name: Name of the pipeline
        table_name: Table with DQ issues
        check_name: Name of the failed check
        failed_records: Number of failed records
        total_records: Total records checked
        severity: Alert severity
        runbook_url: Link to runbook

    Returns:
        True if alert sent successfully
    """
    fail_rate = (failed_records / total_records * 100) if total_records > 0 else 0

    return alert_manager.send_alert(
        title=f"Data Quality Check Failed: {check_name}",
        message=f"DQ check `{check_name}` failed on table `{table_name}`.\n\n"
        f"*Failed Records:* {failed_records:,} ({fail_rate:.2f}%)\n"
        f"*Total Records:* {total_records:,}",
        severity=severity,
        pipeline=pipeline_name,
        runbook_url=runbook_url,
        additional_context={
            "Table": table_name,
            "Check": check_name,
            "Fail Rate": f"{fail_rate:.2f}%",
        },
    )


def send_freshness_alert(
    alert_manager: AlertManager,
    table_name: str,
    hours_old: float,
    sla_hours: int,
    runbook_url: Optional[str] = None,
) -> bool:
    """
    Send a data freshness alert.

    Args:
        alert_manager: AlertManager instance
        table_name: Stale table name
        hours_old: How old the data is
        sla_hours: SLA threshold
        runbook_url: Link to runbook

    Returns:
        True if alert sent successfully
    """
    return alert_manager.send_alert(
        title=f"Data Freshness SLA Breach: {table_name}",
        message=f"Table `{table_name}` data is {hours_old:.1f} hours old.\n"
        f"SLA: {sla_hours} hours.\n\n"
        f"Check upstream pipelines.",
        severity=Severity.P2,
        pipeline=f"freshness_check_{table_name}",
        runbook_url=runbook_url,
        additional_context={
            "Table": table_name,
            "Current Age": f"{hours_old:.1f} hours",
            "SLA": f"{sla_hours} hours",
        },
    )

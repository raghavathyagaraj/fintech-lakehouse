"""
Unit Tests for Alerting Module
==============================

These tests verify that alerts are formatted correctly.
We use dry_run=True to avoid actually sending alerts.

How to run:
    pytest tests/unit/test_alerting.py -v
"""

import pytest
from unittest.mock import patch, MagicMock

from src.utils.alerting import (
    AlertManager,
    Severity,
    Alert,
    send_pipeline_failure_alert,
    send_dq_failure_alert,
)


class TestAlertManager:
    """Tests for AlertManager class."""

    def test_dry_run_does_not_send(self):
        """Test that dry_run=True doesn't actually send alerts."""
        # Arrange
        manager = AlertManager(
            slack_webhook="https://fake.slack.com/webhook",
            dry_run=True,  # Don't actually send
        )

        # Act: This should NOT make any HTTP requests
        result = manager.send_alert(
            title="Test Alert",
            message="This is a test",
            severity=Severity.P3,
            pipeline="test_pipeline",
        )

        # Assert: Should return True (simulated success)
        assert result is True

    def test_severity_colors_defined(self):
        """Test that all severities have colors defined."""
        for severity in Severity:
            assert severity in AlertManager.SEVERITY_COLORS

    def test_severity_emojis_defined(self):
        """Test that all severities have emojis defined."""
        for severity in Severity:
            assert severity in AlertManager.SEVERITY_EMOJI

    @patch("src.utils.alerting.requests.post")
    def test_slack_called_for_all_severities(self, mock_post):
        """Test that Slack is called for all severity levels."""
        # Arrange
        mock_post.return_value = MagicMock(status_code=200)
        manager = AlertManager(slack_webhook="https://fake.slack.com/webhook")

        # Act: Send alerts of each severity
        for severity in Severity:
            manager.send_alert(
                title="Test",
                message="Test message",
                severity=severity,
                pipeline="test",
            )

        # Assert: Slack should be called once per severity
        assert mock_post.call_count == len(Severity)

    @patch("src.utils.alerting.requests.post")
    def test_pagerduty_only_called_for_p1(self, mock_post):
        """Test that PagerDuty is only called for P1 alerts."""
        # Arrange
        mock_post.return_value = MagicMock(status_code=200)
        manager = AlertManager(
            slack_webhook="https://fake.slack.com/webhook",
            pagerduty_key="fake-pagerduty-key",
        )

        # Reset mock
        mock_post.reset_mock()

        # Act: Send P2 alert
        manager.send_alert(
            title="P2 Alert",
            message="This is P2",
            severity=Severity.P2,
            pipeline="test",
        )

        # Assert: Only Slack should be called (1 call), not PagerDuty
        assert mock_post.call_count == 1
        # Verify the call was to Slack, not PagerDuty
        call_url = mock_post.call_args[0][0]
        assert "slack" in call_url

        # Reset mock
        mock_post.reset_mock()

        # Act: Send P1 alert
        manager.send_alert(
            title="P1 Alert",
            message="This is P1",
            severity=Severity.P1,
            pipeline="test",
        )

        # Assert: Both Slack and PagerDuty should be called (2 calls)
        assert mock_post.call_count == 2


class TestAlert:
    """Tests for Alert dataclass."""

    def test_alert_to_dict(self):
        """Test that Alert converts to dictionary correctly."""
        # Arrange
        alert = Alert(
            title="Test Title",
            message="Test Message",
            severity=Severity.P2,
            pipeline="test_pipeline",
            runbook_url="https://wiki/runbook",
        )

        # Act
        result = alert.to_dict()

        # Assert
        assert result["title"] == "Test Title"
        assert result["message"] == "Test Message"
        assert result["severity"] == "P2_HIGH"
        assert result["pipeline"] == "test_pipeline"
        assert result["runbook_url"] == "https://wiki/runbook"
        assert "timestamp" in result

    def test_alert_timestamp_auto_set(self):
        """Test that timestamp is automatically set."""
        # Arrange & Act
        alert = Alert(
            title="Test",
            message="Test",
            severity=Severity.P3,
            pipeline="test",
        )

        # Assert
        assert alert.timestamp is not None


class TestConvenienceFunctions:
    """Tests for convenience alert functions."""

    @patch("src.utils.alerting.requests.post")
    def test_send_pipeline_failure_alert(self, mock_post):
        """Test pipeline failure alert function."""
        # Arrange
        mock_post.return_value = MagicMock(status_code=200)
        manager = AlertManager(slack_webhook="https://fake.slack.com/webhook")

        # Act
        result = send_pipeline_failure_alert(
            alert_manager=manager,
            pipeline_name="silver_transactions",
            error_message="FileNotFoundException: file not found",
            retry_count=3,
        )

        # Assert
        assert result is True
        assert mock_post.called

    @patch("src.utils.alerting.requests.post")
    def test_send_dq_failure_alert(self, mock_post):
        """Test DQ failure alert function."""
        # Arrange
        mock_post.return_value = MagicMock(status_code=200)
        manager = AlertManager(slack_webhook="https://fake.slack.com/webhook")

        # Act
        result = send_dq_failure_alert(
            alert_manager=manager,
            pipeline_name="silver_transactions",
            table_name="transactions",
            check_name="null_customer_id",
            failed_records=100,
            total_records=10000,
        )

        # Assert
        assert result is True
        assert mock_post.called

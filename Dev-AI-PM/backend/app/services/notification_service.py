import asyncio
import json
import smtplib
from datetime import datetime
from email.message import EmailMessage
from typing import Optional, List

import httpx
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.core.config import get_settings
from app.models.alarm import Alarm
from app.models.sensor import Sensor
from app.models.email_recipient import EmailRecipient
from app.db.session import AsyncSessionLocal

settings = get_settings()

_email_ready: bool = False
_email_last_error: Optional[str] = None


def email_configured() -> bool:
    """Check whether SMTP settings are present."""
    return bool(settings.email_smtp_host and settings.email_smtp_user and settings.email_smtp_pass)


def email_status() -> tuple[bool, Optional[str]]:
    """Return last known email transport status."""
    return _email_ready, _email_last_error


async def verify_email_transport() -> None:
    """
    Perform a lightweight SMTP connectivity check and cache the result.
    This is used by the /notifications/test-email endpoint.
    """
    global _email_ready, _email_last_error

    if not email_configured():
        _email_ready = False
        _email_last_error = "SMTP credentials not configured"
        return

    try:
        with smtplib.SMTP(settings.email_smtp_host, settings.email_smtp_port, timeout=10) as server:
            server.starttls()
            server.login(settings.email_smtp_user, settings.email_smtp_pass)
        _email_ready = True
        _email_last_error = None
        logger.info("SMTP transport verified successfully")
    except Exception as exc:
        _email_ready = False
        _email_last_error = str(exc)
        logger.warning("SMTP transport verification failed: {}", exc)


async def get_active_email_recipients(session: Optional[AsyncSession] = None) -> List[str]:
    """Get list of active email recipient addresses"""
    if session is None:
        async with AsyncSessionLocal() as session:
            return await get_active_email_recipients(session)
    
    result = await session.execute(
        select(EmailRecipient.email).where(EmailRecipient.is_active == True)
    )
    recipients = result.scalars().all()
    return list(recipients) if recipients else []


async def _send_email(subject: str, body: str, to_override: Optional[str] = None, use_recipients: bool = True) -> None:
    """Send email with improved error handling
    
    Args:
        subject: Email subject
        body: Email body
        to_override: Single email address to send to (overrides recipients list)
        use_recipients: If True, send to all active recipients from database. If False, use default notification_email
    """
    if not email_configured():
        logger.warning("Email not configured, skipping email send")
        raise ValueError("Email SMTP credentials not configured")
    
    # Determine recipients
    recipients: List[str] = []
    if to_override:
        recipients = [to_override]
    elif use_recipients:
        # Get active recipients from database
        async with AsyncSessionLocal() as session:
            recipients = await get_active_email_recipients(session)
        # Fallback to default if no recipients found
        if not recipients:
            recipients = [settings.notification_email] if settings.notification_email else []
    else:
        recipients = [settings.notification_email] if settings.notification_email else []
    
    if not recipients:
        logger.warning("No email recipients configured, skipping email send")
        return
    
    logger.info(f"Sending email to {len(recipients)} recipient(s): {recipients}")
    
    # Get sender email (use email_sender from .env if configured, otherwise fallback to email_smtp_user from .env)
    sender_email = settings.email_sender if settings.email_sender else settings.email_smtp_user
    
    # Send to all recipients
    for recipient_email in recipients:
        try:
            message = EmailMessage()
            message["From"] = sender_email
            message["To"] = recipient_email
            message["Subject"] = subject
            message.set_content(body)

            with smtplib.SMTP(settings.email_smtp_host, settings.email_smtp_port, timeout=10) as server:
                server.starttls()
                server.login(settings.email_smtp_user, settings.email_smtp_pass)
                server.send_message(message)
                logger.info(f"Email notification sent successfully to {recipient_email}")
        except Exception as exc:
            logger.warning(f"Failed to send email to {recipient_email}: {exc}")
            # Continue to next recipient even if one fails


async def _send_slack(body: dict) -> None:
    if not settings.slack_webhook_url:
        logger.info("Slack webhook not configured, skipping notification")
        return
    async with httpx.AsyncClient(timeout=10) as client:
        await client.post(settings.slack_webhook_url, json=body)


async def send_password_reset_email(email: str, reset_link: str) -> None:
    """Send password reset email"""
    subject = "Password Reset Request"
    body = f"""
    You have requested to reset your password.
    
    Click the following link to reset your password:
    {reset_link}
    
    This link will expire in 1 hour.
    
    If you did not request this, please ignore this email.
    """
    await _send_email(subject, body, to_override=email)


async def send_test_email(to_override: Optional[str] = None) -> tuple[bool, Optional[str]]:
    """
    Send a simple test email either to the provided address or to all active recipients.
    Returns (success: bool, error_message: str | None)
    """
    if not email_configured():
        return False, "Email SMTP credentials are not configured. Please configure SMTP settings in backend/.env"
    
    # Get sender email for display in message (from .env configuration)
    sender_email = settings.email_sender if settings.email_sender else settings.email_smtp_user
    
    subject = "Predictive Maintenance – Test Email"
    body = f"""This is a test email from the Predictive Maintenance Platform.

Sent from: {sender_email}
Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}

If you received this email, your email notification system is working correctly."""
    
    try:
        # If to_override is provided, send only to that address
        # Otherwise, send to all active recipients
        await _send_email(subject, body, to_override=to_override, use_recipients=True)
        return True, None
    except ValueError as exc:
        # ValueError contains user-friendly error message
        return False, str(exc)
    except Exception as exc:  # pragma: no cover - defensive
        error_msg = str(exc)
        # Extract meaningful error from exception
        if "535" in error_msg or "BadCredentials" in error_msg:
            return False, (
                "Gmail authentication failed. Please:\n"
                "1. Enable 2-Step Verification on your Google account\n"
                "2. Generate an App Password at https://myaccount.google.com/apppasswords\n"
                "3. Use the App Password (not your regular password) in SMTP configuration"
            )
        logger.warning("Test email failed: {}", exc)
        return False, f"Failed to send email: {error_msg}"


async def send_welcome_email(email: str, full_name: str) -> None:
    """Send welcome email to newly registered user"""
    if not email_configured():
        logger.warning("Email not configured, skipping welcome email")
        return
    
    subject = "Welcome to Predictive Maintenance Platform"
    body = f"""
    Hello {full_name},
    
    Welcome to the Predictive Maintenance Platform!
    
    Your account has been successfully created. You can now:
    - Monitor machine health and sensor data in real-time
    - View AI-powered predictions and anomaly detection
    - Generate reports and manage alarms
    - Access the dashboard at: http://localhost:3000
    
    If you have any questions, please contact your administrator.
    
    Best regards,
    Predictive Maintenance Platform Team
    """
    try:
        await _send_email(subject, body, to_override=email)
        logger.info(f"Welcome email sent to {email}")
    except Exception as exc:
        logger.warning(f"Failed to send welcome email to {email}: {exc}")


async def send_prediction_alert_email(machine_id: str, sensor_id: str, prediction_status: str, score: float, confidence: float) -> None:
    """Send email notification for critical/warning predictions to all active recipients"""
    if not email_configured():
        logger.warning("Email not configured, skipping prediction alert")
        return
    
    severity = "CRITICAL" if prediction_status in ["critical", "anomaly"] or score > 0.8 else "WARNING"
    subject = f"[PM Alert] {severity} Prediction Detected - Machine {machine_id}"
    body = f"""
    Predictive Maintenance Alert
    
    A {prediction_status.upper()} prediction has been detected:
    
    - Machine ID: {machine_id}
    - Sensor ID: {sensor_id}
    - Prediction Status: {prediction_status}
    - Anomaly Score: {score:.2f}
    - Confidence: {confidence:.2f}
    
    Please review the dashboard and take appropriate action.
    
    Dashboard: http://localhost:3000
    
    This is an automated notification from the Predictive Maintenance Platform.
    """
    try:
        # Send to all active recipients from database (uses .env configuration)
        await _send_email(subject, body, to_override=None, use_recipients=True)
        logger.info(f"Prediction alert email sent to active recipients for machine {machine_id}")
    except Exception as exc:
        logger.warning(f"Failed to send prediction alert email: {exc}")


async def send_extruder_high_pressure_email(current_pressure: float) -> None:
    """Send German email when extruder pressure is above 380 bar."""
    if not email_configured():
        logger.warning("Email not configured, skipping high-pressure notification")
        return
    subject = "Extruder Druckwarnung – Druck über Grenzwert"
    body = (
        "Warnung: Der Extruderdruck hat den Grenzwert überschritten.\n"
        "Der aktuelle Druckwert liegt über 380 bar.\n"
        "Bitte Prozess überprüfen.\n\n"
        f"Aktueller Druck: {current_pressure:.1f} bar"
    )
    await _send_email(subject, body, to_override=None, use_recipients=True)


async def send_extruder_low_pressure_email(current_pressure: float) -> None:
    """Send German email when extruder pressure is below 340 bar."""
    if not email_configured():
        logger.warning("Email not configured, skipping low-pressure notification")
        return
    logger.info(f"Attempting to send low-pressure notification (current_pressure={current_pressure:.1f} bar)")
    subject = "Extruder Druckwarnung – Druck unter Grenzwert"
    body = (
        "Warnung: Der Extruderdruck liegt unter dem definierten Bereich.\n"
        "Der aktuelle Druckwert liegt unter 340 bar.\n"
        "Bitte Prozess überprüfen.\n\n"
        f"Aktueller Druck: {current_pressure:.1f} bar"
    )
    try:
        await _send_email(subject, body, to_override=None, use_recipients=True)
        logger.info("Low-pressure notification email sent successfully")
    except Exception as exc:
        logger.error(f"Failed to send low-pressure notification email: {exc}", exc_info=True)


async def send_extruder_started_email() -> None:
    """Send German email when extruder transitions from stopped to production pressure."""
    if not email_configured():
        logger.warning("Email not configured, skipping extruder-start notification")
        return
    subject = "Extruder gestartet"
    body = (
        "Der Extruder wurde gestartet.\n"
        "Der Druck ist vom Stillstand in den Produktionsbereich angestiegen."
    )
    await _send_email(subject, body, to_override=None, use_recipients=True)


async def send_extruder_shutdown_email() -> None:
    """Send German email when extruder transitions from production pressure to near-zero."""
    if not email_configured():
        logger.warning("Email not configured, skipping extruder-shutdown notification")
        return
    subject = "Extruder heruntergefahren"
    body = (
        "Der Extruder wurde heruntergefahren.\n"
        "Der Druck ist vom Produktionsbereich auf niedrige Werte gefallen."
    )
    await _send_email(subject, body, to_override=None, use_recipients=True)


def enqueue_alarm_notification(alarm: Alarm, sensor: Optional[Sensor]) -> None:
    subject = f"[PM] {alarm.severity.upper()} alarm on {sensor.name if sensor else alarm.machine_id}"
    body = json.dumps(
        {
            "machine_id": str(alarm.machine_id),
            "sensor": sensor.name if sensor else None,
            "status": alarm.status,
            "message": alarm.message,
        },
        indent=2,
    )
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.get_event_loop()

    loop.create_task(_send_email(subject, body))
    loop.create_task(
        _send_slack(
            {
                "text": subject,
                "blocks": [
                    {"type": "section", "text": {"type": "mrkdwn", "text": f"*Alarm:* {alarm.message}"}},
                    {
                        "type": "context",
                        "elements": [
                            {"type": "mrkdwn", "text": f"*Severity:* {alarm.severity}"},
                            {"type": "mrkdwn", "text": f"*Machine:* {alarm.machine_id}"},
                        ],
                    },
                ],
            }
        )
    )


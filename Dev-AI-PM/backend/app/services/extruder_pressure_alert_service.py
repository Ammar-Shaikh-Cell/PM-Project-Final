"""
Pressure alert runner using the same data source as GET /dashboard/extruder/latest (TimescaleDB).
Runs on a fixed interval so emails are sent when new data is available.
"""
import asyncio
from datetime import datetime
from typing import Any, Dict, Optional


from loguru import logger

from app.services import notification_service
from app.db.session import AsyncSessionLocal
from app.models.machine import Machine
from app.models.profile import ProfilePressureConfig
from app.services.baseline_learning_service import baseline_learning_service
from sqlalchemy import select

# Cooldowns (seconds)
PRESSURE_EMAIL_COOLDOWN_SECONDS = 1800  # 30 min
START_STOP_EMAIL_COOLDOWN_SECONDS = 600  # 10 min

# Module-level state for cooldowns and start/stop detection
_last_pressure_value: Optional[float] = None
_last_pressure_high_email_at: Optional[datetime] = None
_last_pressure_low_email_at: Optional[datetime] = None
_extruder_running: bool = False
_last_start_email_at: Optional[datetime] = None
_last_shutdown_email_at: Optional[datetime] = None


def _safe_float(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except Exception:
        return 0.0


async def _load_pressure_config() -> Optional[ProfilePressureConfig]:
    """
    Load the active pressure configuration for the extruder profile.
    Uses machine metadata current_material (fallback: 'Material 1').
    """
    async with AsyncSessionLocal() as session:
        machine = (
            (await session.execute(select(Machine).where(Machine.name == "Extruder-SQL"))).scalar_one_or_none()
        )
        if not machine:
            return None
        material_id = (machine.metadata_json or {}).get("current_material", "Material 1")
        profile = await baseline_learning_service.get_active_profile(session, machine.id, str(material_id))
        if not profile:
            return None
        cfg = (
            (
                await session.execute(
                    select(ProfilePressureConfig)
                    .where(ProfilePressureConfig.profile_id == profile.id)
                    .where(ProfilePressureConfig.is_active == True)
                    .where(ProfilePressureConfig.batch_id.is_(None))
                    .order_by(ProfilePressureConfig.created_at.desc())
                )
            )
            .scalars()
            .first()
        )
        return cfg


async def run_pressure_alert_check() -> None:
    """
    Fetch latest extruder row from TimescaleDB (same as /dashboard/extruder/latest),
    then run pressure alert and start/stop detection. Send emails to active recipients with cooldowns.
    """
    global _last_pressure_value, _last_pressure_high_email_at, _last_pressure_low_email_at
    global _extruder_running, _last_start_email_at, _last_shutdown_email_at

    row: Optional[Dict[str, Any]] = None
    try:
        from app.services import tsdb_client
        if tsdb_client.tsdb_configured():
            rows = await tsdb_client.fetch_extruder_latest_from_tsdb(limit=1)
            row = rows[0] if rows else None
    except Exception as e:
        logger.debug("Pressure alert check fetch failed: %s", e)
        return

    if not row:
        return

    current_pressure = _safe_float(row.get("Pressure_bar"))
    current_rpm = _safe_float(row.get("ScrewSpeed_rpm"))
    prev_pressure = _last_pressure_value
    _last_pressure_value = current_pressure

    if not notification_service.email_configured():
        return

    # Gate high/low pressure emails on PRODUCTION state.
    # We use the same practical thresholds as the machine-state detector defaults
    # (seen in logs): rpm_prod ~= 10, p_prod ~= 5. This ensures we don't spam
    # during OFF/IDLE/HEATING/COOLING when pressure is low.
    is_in_production = current_rpm >= 10.0 and current_pressure >= 5.0

    try:
        cfg = await _load_pressure_config()
        warning_thr = float(getattr(cfg, "warning_threshold", 380.0)) if cfg else 380.0
        critical_thr = float(getattr(cfg, "critical_warning_threshold", 395.0)) if cfg else 395.0
        low_thr = float(getattr(cfg, "low_pressure_warning_threshold", 340.0)) if cfg else 340.0
        send_warn = bool(getattr(cfg, "send_email_on_warning", True)) if cfg else True
        send_crit = bool(getattr(cfg, "send_email_on_critical", True)) if cfg else True
        send_start = bool(getattr(cfg, "send_email_on_production_start", False)) if cfg else False
        send_stop = bool(getattr(cfg, "send_email_on_production_stop", False)) if cfg else False

        # High pressure warning/critical (>= configured thresholds)
        if is_in_production and current_pressure >= critical_thr and send_crit:
            should_send = (
                _last_pressure_high_email_at is None
                or (datetime.utcnow() - _last_pressure_high_email_at).total_seconds() > PRESSURE_EMAIL_COOLDOWN_SECONDS
            )
            if should_send:
                await notification_service.send_extruder_high_pressure_email(current_pressure)
                _last_pressure_high_email_at = datetime.utcnow()
                logger.info(
                    "Pressure alert (from latest): CRITICAL pressure email sent (%.1f bar, thr=%.1f)",
                    current_pressure,
                    critical_thr,
                )

        elif is_in_production and current_pressure >= warning_thr and send_warn:
            should_send = (
                _last_pressure_high_email_at is None
                or (datetime.utcnow() - _last_pressure_high_email_at).total_seconds() > PRESSURE_EMAIL_COOLDOWN_SECONDS
            )
            if should_send:
                await notification_service.send_extruder_high_pressure_email(current_pressure)
                _last_pressure_high_email_at = datetime.utcnow()
                logger.info(
                    "Pressure alert (from latest): WARNING pressure email sent (%.1f bar, thr=%.1f)",
                    current_pressure,
                    warning_thr,
                )

        # Low pressure < 340 bar
        if is_in_production and current_pressure < low_thr and send_warn:
            should_send = (
                _last_pressure_low_email_at is None
                or (datetime.utcnow() - _last_pressure_low_email_at).total_seconds() > PRESSURE_EMAIL_COOLDOWN_SECONDS
            )
            if should_send:
                await notification_service.send_extruder_low_pressure_email(current_pressure)
                _last_pressure_low_email_at = datetime.utcnow()
                logger.info(
                    "Pressure alert (from latest): LOW pressure email sent (%.1f bar, thr=%.1f)",
                    current_pressure,
                    low_thr,
                )

        # Start detection: pressure < 50 -> > 300
        if prev_pressure is not None:
            if not _extruder_running and prev_pressure < 50.0 and current_pressure > 300.0:
                should_send = (
                    _last_start_email_at is None
                    or (datetime.utcnow() - _last_start_email_at).total_seconds() > START_STOP_EMAIL_COOLDOWN_SECONDS
                )
                if should_send:
                    if send_start:
                        await notification_service.send_extruder_started_email()
                        _last_start_email_at = datetime.utcnow()
                        logger.info("Pressure alert (from latest): extruder start email sent")
                _extruder_running = True
            elif _extruder_running and prev_pressure > 300.0 and current_pressure < 50.0:
                should_send = (
                    _last_shutdown_email_at is None
                    or (datetime.utcnow() - _last_shutdown_email_at).total_seconds() > START_STOP_EMAIL_COOLDOWN_SECONDS
                )
                if should_send:
                    if send_stop:
                        await notification_service.send_extruder_shutdown_email()
                        _last_shutdown_email_at = datetime.utcnow()
                        logger.info("Pressure alert (from latest): extruder shutdown email sent")
                _extruder_running = False
    except Exception as e:
        logger.error("Pressure alert (from latest) failed: %s", e, exc_info=True)


async def pressure_alert_loop(interval_seconds: int = 60) -> None:
    """Background loop: every interval_seconds, fetch latest from TimescaleDB and run pressure alert check."""
    logger.info("Pressure alert loop started (interval=%ds, data source=TimescaleDB)", interval_seconds)
    while True:
        try:
            await run_pressure_alert_check()
        except asyncio.CancelledError:
            logger.info("Pressure alert loop cancelled")
            break
        except Exception as e:
            logger.warning("Pressure alert loop error: %s", e)
        await asyncio.sleep(interval_seconds)

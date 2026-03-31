from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple, Optional
from functools import lru_cache
import time
import os
import re
import statistics

from fastapi import APIRouter, Depends, Query, HTTPException
from loguru import logger
from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_session, get_current_user, require_viewer
from app.models.user import User
from app.models.machine import Machine
from app.models.sensor import Sensor
from app.models.prediction import Prediction
from app.models.alarm import Alarm
from app.models.sensor_data import SensorData
from app.utils.baseline_formatter import build_standardized_baseline, build_standardized_baseline_from_dict
from app.services import audit_service
from app.schemas.audit_log import AuditLogCreate
from uuid import UUID, uuid4
from sqlalchemy import select as sql_select

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


async def _ensure_extruder_machine_and_sensor(session: AsyncSession) -> Tuple[UUID, UUID]:
    """Ensure machine 'Extruder-SQL' and sensor 'Extruder SQL Snapshot' exist; create if missing. Returns (machine_id, sensor_id)."""
    from app.services import machine_service, sensor_service
    from app.schemas.machine import MachineCreate
    from app.schemas.sensor import SensorCreate

    machine = (await session.scalars(
        sql_select(Machine).where(Machine.name == "Extruder-SQL").limit(1)
    )).first()
    if machine is None:
        created = await machine_service.create_machine(
            session,
            MachineCreate(
                name="Extruder-SQL",
                status="online",
                criticality="high",
                metadata={
                    "source": "mssql",
                    "machine_type": "extruder",
                    "type": "extruder",
                },
            ),
        )
        machine_id = created["id"] if isinstance(created["id"], UUID) else UUID(str(created["id"]))
    else:
        machine_id = machine.id

    sensor = (await session.scalars(
        sql_select(Sensor)
        .where(
            and_(
                Sensor.machine_id == machine_id,
                Sensor.name == "Extruder SQL Snapshot",
            )
        )
        .limit(1)
    )).first()
    if sensor is None:
        created = await sensor_service.create_sensor(
            session,
            SensorCreate(
                machine_id=machine_id,
                name="Extruder SQL Snapshot",
                sensor_type="extruder_sql",
                unit=None,
                metadata={
                    "source": "mssql",
                    "columns": ["Val_4", "Val_6", "Val_7", "Val_8", "Val_9", "Val_10"],
                    "trend_column": "TrendDate",
                },
            ),
        )
        sensor_id = created["id"] if isinstance(created["id"], UUID) else UUID(str(created["id"]))
    else:
        sensor_id = sensor.id

    return (machine_id, sensor_id)


def _diagnose_baseline_learning_issues(
    poller_running: bool,
    mssql_enabled: bool,
    poller_effective_enabled: bool,
    configured: bool,
    machine,
    profile,
    baseline_samples_count: int,
    poller_window_size: int,
) -> List[str]:
    """Diagnose why baseline learning samples are not being collected"""
    issues = []
    
    if not mssql_enabled:
        issues.append("❌ MSSQL_ENABLED environment variable is false - poller will not start")
    
    if not poller_running:
        issues.append("❌ MSSQL poller task is not running - check startup logs")
    
    if not poller_effective_enabled:
        issues.append("❌ MSSQL poller disabled in database (connections.mssql.enabled=false) - enable in Settings → Connections")
    
    if not configured:
        issues.append("❌ MSSQL connection not configured - missing host, username, or password")
    
    if machine is None:
        issues.append("❌ Machine not found - poller cannot collect samples")
    
    if profile is None:
        issues.append("❌ Profile not found - create a profile for the current material")
    elif not profile.baseline_learning:
        issues.append("❌ Profile baseline_learning is False - start baseline learning mode")
    
    if poller_window_size == 0:
        issues.append("⚠️ Poller window is empty - no data fetched from MSSQL yet")
    
    if baseline_samples_count == 0 and poller_running and profile and profile.baseline_learning:
        issues.append("⚠️ No samples collected yet - check if poller is processing data and machine is in PRODUCTION state")
    
    if not issues:
        issues.append("✅ All checks passed - samples should be collecting")
    
    return issues


def apply_decision_hierarchy(
    *,
    rule_based_severity: int,
    stability_severity: Optional[int],
    ml_anomaly_score: Optional[float],
    ml_threshold: float = 0.7,
) -> Tuple[int, bool]:
    """
    Rule vs ML Priority Decision Hierarchy (4-step chain)
    
    STEP 1: Machine state gate (handled by caller - must be PRODUCTION)
    STEP 2: Material rule-based thresholds - IF value outside baseline → return yellow/red
    STEP 3: Stability/trend indicators - IF stability orange/red → override value inside band
    STEP 4: ML signal (Isolation Forest) - IF ml_score > threshold → only add "warning", do NOT set red status
    
    Args:
        rule_based_severity: Severity from rule-based thresholds (0=GREEN, 1=ORANGE, 2=RED, -1=UNKNOWN)
        stability_severity: Stability severity (0=GREEN, 1=ORANGE, 2=RED, None=UNKNOWN)
        ml_anomaly_score: ML anomaly score from Isolation Forest (0.0-1.0, None if not available)
        ml_threshold: Threshold for ML warning (default 0.7)
    
    Returns:
        Tuple of (final_severity: int, ml_warning: bool)
        - final_severity: Final severity after applying hierarchy (0, 1, 2, or -1)
        - ml_warning: True if ML detected anomaly (but did not change status)
    """
    # STEP 2: Material rule-based thresholds
    # IF value outside baseline → return yellow/red
    final_severity = rule_based_severity
    
    # STEP 3: Stability/trend indicators override
    # IF stability orange/red → override value inside band
    if stability_severity is not None and stability_severity >= 1:
        # Stability is orange (1) or red (2)
        # Override: if rule-based severity is GREEN (0) but stability is orange/red, upgrade to at least orange
        if final_severity == 0 and stability_severity >= 1:
            final_severity = stability_severity  # Use stability severity (1=orange or 2=red)
        # If rule-based is already orange/red, keep the higher severity
        elif final_severity >= 0 and stability_severity > final_severity:
            final_severity = stability_severity
    
    # STEP 4: ML signal (Isolation Forest)
    # IF ml_score > threshold → only add "warning", do NOT set red status
    ml_warning = False
    if ml_anomaly_score is not None and ml_anomaly_score > ml_threshold:
        ml_warning = True
        # ML can only add context (warning), never set red status
        # If final_severity is GREEN (0), ML warning does NOT upgrade to orange/red
        # ML warning is informational only - it does not change the final status
    
    return final_severity, ml_warning


_extruder_last_attempt_at: datetime | None = None
_extruder_last_success_at: datetime | None = None
_extruder_last_error_at: datetime | None = None
_extruder_last_error: str | None = None

# Simple in-memory cache (can be replaced with Redis)
_cache: Dict[str, tuple] = {}
CACHE_TTL = 10  # seconds - reduced for faster alarm updates


def get_cached(key: str):
    """Get cached value if not expired"""
    if key in _cache:
        value, timestamp = _cache[key]
        if time.time() - timestamp < CACHE_TTL:
            return value
        del _cache[key]
    return None


def set_cached(key: str, value: Any):
    """Set cached value"""
    _cache[key] = (value, time.time())


@router.get("/overview")
async def get_overview(
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(require_viewer),
):
    """Get dashboard overview statistics"""
    cache_key = "dashboard:overview"
    cached = get_cached(cache_key)
    if cached:
        return cached
    
    # Run all queries in parallel for better performance
    import asyncio
    
    yesterday = datetime.utcnow() - timedelta(days=1)
    
    # Execute all queries concurrently using asyncio.gather
    machine_count, sensor_count, active_alarms, recent_predictions, machines_online = await asyncio.gather(
        session.scalar(select(func.count(Machine.id))),
        session.scalar(select(func.count(Sensor.id))),
        session.scalar(select(func.count(Alarm.id)).where(Alarm.status.in_(["open", "acknowledged"]))),
        session.scalar(select(func.count(Prediction.id)).where(Prediction.timestamp >= yesterday)),
        session.scalar(select(func.count(Machine.id)).where(Machine.status == "online")),
        return_exceptions=True
    )
    
    # Handle any exceptions
    machine_count = machine_count if not isinstance(machine_count, Exception) else 0
    sensor_count = sensor_count if not isinstance(sensor_count, Exception) else 0
    active_alarms = active_alarms if not isinstance(active_alarms, Exception) else 0
    recent_predictions = recent_predictions if not isinstance(recent_predictions, Exception) else 0
    machines_online = machines_online if not isinstance(machines_online, Exception) else 0
    
    result = {
        "machines": {
            "total": machine_count or 0,
            "online": machines_online or 0,
        },
        "sensors": {
            "total": sensor_count or 0,
        },
        "alarms": {
            "active": active_alarms or 0,
        },
        "predictions": {
            "last_24h": recent_predictions or 0,
        },
    }
    
    set_cached(cache_key, result)
    return result


@router.get("/extruder/latest")
async def get_extruder_latest_rows(
    current_user: User = Depends(require_viewer),
    limit: int = Query(200, ge=1, le=5000),
    session: AsyncSession = Depends(get_session),
):
    global _extruder_last_attempt_at, _extruder_last_success_at, _extruder_last_error_at, _extruder_last_error
    _extruder_last_attempt_at = datetime.utcnow()

    host = (os.getenv("MSSQL_HOST") or "").strip()
    port_raw = (os.getenv("MSSQL_PORT") or "1433").strip()
    user = (os.getenv("MSSQL_USER") or "").strip()
    password = os.getenv("MSSQL_PASSWORD")
    database = (os.getenv("MSSQL_DATABASE") or "HISTORISCH").strip()
    table_raw = (os.getenv("MSSQL_TABLE") or "Tab_Actual").strip()
    schema_raw = (os.getenv("MSSQL_SCHEMA") or "dbo").strip()

    try:
        port = int(port_raw)
    except Exception:
        _extruder_last_error = "Invalid MSSQL_PORT"
        _extruder_last_error_at = datetime.utcnow()
        raise HTTPException(status_code=500, detail="Invalid MSSQL_PORT")

    if not host or not user or not password:
        _extruder_last_error = "MSSQL is not configured"
        _extruder_last_error_at = datetime.utcnow()
        raise HTTPException(status_code=500, detail="MSSQL is not configured")

    schema = schema_raw
    table = table_raw
    if "." in table_raw:
        parts = [p for p in table_raw.split(".") if p]
        if len(parts) != 2:
            _extruder_last_error = "Invalid MSSQL table identifier"
            _extruder_last_error_at = datetime.utcnow()
            raise HTTPException(status_code=500, detail="Invalid MSSQL table identifier")
        schema, table = parts[0], parts[1]

    if not re.fullmatch(r"[A-Za-z0-9_]+", schema or "") or not re.fullmatch(r"[A-Za-z0-9_]+", table or ""):
        _extruder_last_error = "Invalid MSSQL schema/table identifier"
        _extruder_last_error_at = datetime.utcnow()
        raise HTTPException(status_code=500, detail="Invalid MSSQL schema/table identifier")

    def _fetch_sync() -> Dict[str, Any]:
        import pymssql

        table_sql = f"[{schema}].[{table}]"
        # MSSQL 2000 does not support parentheses around TOP value
        query = (
            f"SELECT TOP {int(limit)} "
            f"TrendDate, "
            f"Val_4 AS ScrewSpeed_rpm, "
            f"Val_6 AS Pressure_bar, "
            f"Val_7 AS Temp_Zone1_C, "
            f"Val_8 AS Temp_Zone2_C, "
            f"Val_9 AS Temp_Zone3_C, "
            f"Val_10 AS Temp_Zone4_C "
            f"FROM {table_sql} "
            f"ORDER BY TrendDate DESC"
        )

        s = query.strip().lower()
        if not s.startswith("select") or ";" in s:
            raise ValueError("Unsafe SQL blocked")

        conn = pymssql.connect(
            server=host,
            user=user,
            password=password,
            database=database,
            port=port,
            login_timeout=10,
            timeout=10,
        )
        try:
            try:
                conn.autocommit(True)
            except Exception:
                pass

            cur = conn.cursor(as_dict=True)
            try:
                cur.execute("SET NOCOUNT ON")
                cur.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            except Exception:
                pass

            cur.execute(query)
            rows = cur.fetchall() or []
            out = []
            for r in rows:
                td = r.get("TrendDate")
                if isinstance(td, datetime):
                    trend_date = td.isoformat()
                elif td is None:
                    trend_date = None
                else:
                    trend_date = str(td)

                out.append(
                    {
                        "TrendDate": trend_date,
                        "ScrewSpeed_rpm": r.get("ScrewSpeed_rpm"),
                        "Pressure_bar": r.get("Pressure_bar"),
                        "Temp_Zone1_C": r.get("Temp_Zone1_C"),
                        "Temp_Zone2_C": r.get("Temp_Zone2_C"),
                        "Temp_Zone3_C": r.get("Temp_Zone3_C"),
                        "Temp_Zone4_C": r.get("Temp_Zone4_C"),
                    }
                )

            out.reverse()
            return {"rows": out}
        finally:
            try:
                conn.close()
            except Exception:
                pass

    try:
        import asyncio
        result = await asyncio.to_thread(_fetch_sync)
        _extruder_last_success_at = datetime.utcnow()
        _extruder_last_error = None
        _extruder_last_error_at = None

        # Persist each row to sensor_data so /dashboard/extruder/history has data (same logic as latest → history)
        rows = result.get("rows") or []
        if rows:
            try:
                machine_id, sensor_id = await _ensure_extruder_machine_and_sensor(session)
                from app.schemas.sensor_data import SensorDataIn as SensorDataInSchema
                from app.services import sensor_data_service
                from sqlalchemy.exc import IntegrityError

                def _safe_float(v):
                    if v is None:
                        return 0.0
                    try:
                        return float(v)
                    except Exception:
                        return 0.0

                for r in rows:
                    trend_str = r.get("TrendDate")
                    if not trend_str:
                        continue
                    try:
                        ts = datetime.fromisoformat(str(trend_str).replace("Z", "+00:00"))
                    except Exception:
                        continue
                    idempotency_key = f"{sensor_id}_{ts.isoformat()}"
                    payload = SensorDataInSchema(
                        sensor_id=sensor_id,
                        machine_id=machine_id,
                        timestamp=ts,
                        value=_safe_float(r.get("Pressure_bar") or r.get("ScrewSpeed_rpm")),
                        status="normal",
                        metadata={
                            "source": "mssql",
                            "screw_rpm": r.get("ScrewSpeed_rpm"),
                            "pressure_bar": r.get("Pressure_bar"),
                            "temp_zone1_c": r.get("Temp_Zone1_C"),
                            "temp_zone2_c": r.get("Temp_Zone2_C"),
                            "temp_zone3_c": r.get("Temp_Zone3_C"),
                            "temp_zone4_c": r.get("Temp_Zone4_C"),
                        },
                        idempotency_key=idempotency_key,
                    )
                    try:
                        await sensor_data_service.ingest_sensor_data(session, payload)
                    except IntegrityError:
                        pass  # duplicate row, skip
            except Exception as e:
                logger.warning("Failed to persist extruder latest rows to history: %s", e)

        return result
    except HTTPException:
        raise
    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        msg = msg.replace(password or "", "***")
        if len(msg) > 500:
            msg = msg[:500] + "..."

        logger.exception("MSSQL extruder read failed")
        _extruder_last_error = msg
        _extruder_last_error_at = datetime.utcnow()
        raise HTTPException(status_code=502, detail="Failed to read MSSQL extruder data")


@router.get("/extruder/history")
async def get_extruder_history_from_sensor_data(
    current_user: User = Depends(require_viewer),
    days: int = Query(30, ge=1, le=365, description="Number of days of history to return"),
    limit: int = Query(5000, ge=1, le=50000),
    session: AsyncSession = Depends(get_session),
):
    """
    Return historical extruder sensor values from the sensor_data table.

    Shape matches /dashboard/extruder/latest so the frontend can reuse the same chart logic:
    [
      {
        "TrendDate": "...",
        "ScrewSpeed_rpm": ...,
        "Pressure_bar": ...,
        "Temp_Zone1_C": ...,
        "Temp_Zone2_C": ...,
        "Temp_Zone3_C": ...,
        "Temp_Zone4_C": ...
      },
      ...
    ]
    """
    from sqlalchemy import select as sql_select
    from app.models.sensor import Sensor
    from app.models.sensor_data import SensorData
    from app.services import sensor_data_service
    from datetime import datetime, timedelta, timezone
    from uuid import UUID

    # Find the extruder machine and its MSSQL snapshot sensor
    machines = await session.scalars(
        sql_select(Machine).where(Machine.name == "Extruder-SQL").limit(1)
    )
    machine = machines.first()
    if not machine:
        return {"rows": []}

    sensors = await session.scalars(
        sql_select(Sensor)
        .where(
            and_(
                Sensor.machine_id == machine.id,
                Sensor.name == "Extruder SQL Snapshot",
            )
        )
        .limit(1)
    )
    sensor = sensors.first()
    if not sensor:
        return {"rows": []}

    # Build explicit time window: last `days` days up to now (UTC)
    now_utc = datetime.now(timezone.utc)
    start_time = now_utc - timedelta(days=days)

    sid = sensor.id if isinstance(sensor.id, UUID) else UUID(str(sensor.id))
    # Fetch the most recent `limit` rows in the requested window, then reverse
    # so the frontend sees them oldest-first. This ensures 1h/1d always get
    # the latest data points (e.g. up to 17:07) instead of only the earliest
    # rows in the window.
    stmt = (
        sql_select(SensorData)
        .where(
            SensorData.sensor_id == sid,
            SensorData.timestamp >= start_time,
        )
        .order_by(SensorData.timestamp.desc())
        .limit(limit)
    )
    result = await session.execute(stmt)
    rows: list[SensorData] = list(reversed(result.scalars().all()))

    out: list[dict[str, Any]] = []
    for sd in rows:
        meta = sd.metadata_json or {}
        out.append(
            {
                "TrendDate": sd.timestamp.isoformat() if sd.timestamp else None,
                "ScrewSpeed_rpm": meta.get("screw_rpm"),
                "Pressure_bar": meta.get("pressure_bar")
                or (float(sd.value) if sd.value is not None else None),
                "Temp_Zone1_C": meta.get("temp_zone1_c"),
                "Temp_Zone2_C": meta.get("temp_zone2_c"),
                "Temp_Zone3_C": meta.get("temp_zone3_c"),
                "Temp_Zone4_C": meta.get("temp_zone4_c"),
            }
        )

    return {"rows": out}


@router.get("/extruder/history/diagnostic")
async def get_extruder_history_diagnostic(
    current_user: User = Depends(require_viewer),
    session: AsyncSession = Depends(get_session),
):
    """
    Diagnose why /dashboard/extruder/history might be empty.
    Call this to see: machine/sensor lookup, sensor_data counts, poller state, and next steps.
    """
    from sqlalchemy import select as sql_select, func
    from app.models.sensor import Sensor
    from app.models.sensor_data import SensorData
    from app.services.mssql_extruder_poller import mssql_extruder_poller

    # Same lookup as history endpoint
    machines = await session.scalars(
        sql_select(Machine).where(Machine.name == "Extruder-SQL").limit(1)
    )
    machine = machines.first()
    sensor = None
    if machine:
        sensors = await session.scalars(
            sql_select(Sensor)
            .where(
                and_(
                    Sensor.machine_id == machine.id,
                    Sensor.name == "Extruder SQL Snapshot",
                )
            )
            .limit(1)
        )
        sensor = sensors.first()

    # Count sensor_data for this sensor (use UUID for reliable comparison)
    sensor_data_count = 0
    if sensor:
        from uuid import UUID
        sid = sensor.id if isinstance(sensor.id, UUID) else UUID(str(sensor.id))
        r = await session.execute(
            sql_select(func.count(SensorData.id)).where(SensorData.sensor_id == sid)
        )
        sensor_data_count = r.scalar() or 0

    # Total sensor_data rows (any sensor) to see if poller writes at all
    total_r = await session.execute(sql_select(func.count(SensorData.id)))
    sensor_data_total_count = total_r.scalar() or 0

    poller_task = getattr(mssql_extruder_poller, "_task", None)
    poller_running = poller_task is not None and not poller_task.done()
    poller_sensor_id = getattr(mssql_extruder_poller, "_sensor_id", None)
    poller_machine_id = getattr(mssql_extruder_poller, "_machine_id", None)
    poller_effective_enabled = getattr(mssql_extruder_poller, "_effective_enabled", False)
    poller_last_trend_date = getattr(mssql_extruder_poller, "_last_trend_date", None)
    poller_window_size = len(getattr(mssql_extruder_poller, "_window", []))

    ids_match = (
        (sensor is not None and poller_sensor_id is not None)
        and (str(sensor.id) == str(poller_sensor_id))
    )

    issues = []
    if not machine:
        issues.append("History lookup: no machine named 'Extruder-SQL' found.")
    if machine and not sensor:
        issues.append("History lookup: no sensor named 'Extruder SQL Snapshot' for that machine.")
    if sensor_data_count == 0 and sensor:
        issues.append("No sensor_data rows for the extruder sensor — poller may not be writing or has not run yet.")
    if not poller_running:
        issues.append("MSSQL poller task is not running (check MSSQL_ENABLED and startup).")
    if not poller_effective_enabled:
        issues.append("Poller is disabled in DB — enable Settings → Connections → MSSQL.")
    if poller_window_size == 0 and poller_running:
        issues.append("Poller is running but window is empty — no rows fetched from MSSQL yet (check query/timezone).")
    if sensor and poller_sensor_id is not None and not ids_match:
        issues.append("Poller sensor_id does not match history sensor_id — different sensor used.")
    if not issues:
        issues.append("No obvious issue; if history is still empty, check backend logs for 'Failed to persist MSSQL'.")

    return {
        "history_lookup": {
            "machine_found": machine is not None,
            "machine_id": str(machine.id) if machine else None,
            "machine_name": machine.name if machine else None,
            "sensor_found": sensor is not None,
            "sensor_id": str(sensor.id) if sensor else None,
            "sensor_name": sensor.name if sensor else None,
            "sensor_data_count_for_sensor": sensor_data_count,
        },
        "sensor_data_total_count": sensor_data_total_count,
        "poller": {
            "running": poller_running,
            "effective_enabled": poller_effective_enabled,
            "machine_id": str(poller_machine_id) if poller_machine_id else None,
            "sensor_id": str(poller_sensor_id) if poller_sensor_id else None,
            "last_trend_date": poller_last_trend_date.isoformat() if poller_last_trend_date else None,
            "window_size": poller_window_size,
        },
        "ids_match": ids_match,
        "issues": issues,
        "next_steps": [
            "Ensure backend is rebuilt/restarted so poller code that writes sensor_data is running.",
            "Enable MSSQL in Settings → Connections so the poller fetches and stores rows.",
            "Wait at least one poll interval (e.g. 60s) after enabling; then call /dashboard/extruder/history again.",
            "Check backend logs for 'MSSQL poller fetched' and 'Failed to persist MSSQL'.",
        ],
    }


@router.get("/extruder/status")
async def get_extruder_status(
    current_user: User = Depends(require_viewer),
    session: AsyncSession = Depends(get_session),
):
    from app.services.mssql_extruder_poller import mssql_extruder_poller
    from app.services.baseline_learning_service import baseline_learning_service
    from app.models.profile import ProfileBaselineSample, ProfileBaselineStats
    from sqlalchemy import select as sql_select, func
    
    host = (os.getenv("MSSQL_HOST") or "").strip()
    port_raw = (os.getenv("MSSQL_PORT") or "1433").strip()
    user = (os.getenv("MSSQL_USER") or "").strip()
    password = os.getenv("MSSQL_PASSWORD")
    database = (os.getenv("MSSQL_DATABASE") or "HISTORISCH").strip()
    table_raw = (os.getenv("MSSQL_TABLE") or "Tab_Actual").strip()
    schema_raw = (os.getenv("MSSQL_SCHEMA") or "dbo").strip()
    mssql_enabled = os.getenv("MSSQL_ENABLED", "true").lower() in {"1", "true", "yes"}

    schema = schema_raw
    table = table_raw
    if "." in table_raw:
        parts = [p for p in table_raw.split(".") if p]
        if len(parts) == 2:
            schema, table = parts[0], parts[1]

    configured = bool(host and user and password)
    try:
        port = int(port_raw)
    except Exception:
        port = None

    # Check poller status
    poller_running = mssql_extruder_poller._task is not None and not mssql_extruder_poller._task.done()
    poller_enabled = mssql_extruder_poller.enabled
    poller_effective_enabled = mssql_extruder_poller._effective_enabled
    
    # Get machine to check material_id
    machine = None
    material_id = None
    profile = None
    baseline_samples_count = 0
    baseline_stats_count = 0
    
    if mssql_extruder_poller._machine_id:
        machine = await session.get(Machine, mssql_extruder_poller._machine_id)
        if machine:
            material_id = (machine.metadata_json or {}).get("current_material", "Material 1")
            profile = await baseline_learning_service.get_active_profile(
                session, machine.id, material_id
            )
            
            if profile:
                # Count samples
                sample_count_result = await session.execute(
                    sql_select(func.count(ProfileBaselineSample.id))
                    .where(ProfileBaselineSample.profile_id == profile.id)
                )
                baseline_samples_count = sample_count_result.scalar() or 0
                
                # Count stats entries
                stats_count_result = await session.execute(
                    sql_select(func.count(ProfileBaselineStats.id))
                    .where(ProfileBaselineStats.profile_id == profile.id)
                )
                baseline_stats_count = stats_count_result.scalar() or 0

    return {
        "configured": configured,
        "host": host or None,
        "port": port,
        "database": database or None,
        "schema": schema or None,
        "table": table or None,
        "mssql_enabled": mssql_enabled,
        "poller_enabled": poller_enabled,
        "poller_running": poller_running,
        "poller_effective_enabled": poller_effective_enabled,
        "poller_machine_id": str(mssql_extruder_poller._machine_id) if mssql_extruder_poller._machine_id else None,
        "poller_sensor_id": str(mssql_extruder_poller._sensor_id) if mssql_extruder_poller._sensor_id else None,
        "poller_window_size": len(mssql_extruder_poller._window),
        "poller_poll_interval": mssql_extruder_poller.poll_interval_seconds,
        "poller_last_trend_date": mssql_extruder_poller._last_trend_date.isoformat() if mssql_extruder_poller._last_trend_date else None,
        "machine_name": machine.name if machine else None,
        "machine_material_id": material_id,
        "profile_id": str(profile.id) if profile else None,
        "profile_baseline_learning": profile.baseline_learning if profile else None,
        "profile_baseline_ready": profile.baseline_ready if profile else None,
        "baseline_samples_count": baseline_samples_count,
        "baseline_stats_count": baseline_stats_count,
        "diagnostics": {
            "poller_started": poller_running,
            "poller_enabled_env": mssql_enabled,
            "poller_enabled_db": poller_effective_enabled,
            "connection_configured": configured,
            "machine_found": machine is not None,
            "profile_found": profile is not None,
            "profile_learning": profile.baseline_learning if profile else False,
            "has_samples": baseline_samples_count > 0,
            "issues": _diagnose_baseline_learning_issues(
                poller_running=poller_running,
                mssql_enabled=mssql_enabled,
                poller_effective_enabled=poller_effective_enabled,
                configured=configured,
                machine=machine,
                profile=profile,
                baseline_samples_count=baseline_samples_count,
                poller_window_size=len(mssql_extruder_poller._window),
            )
        },
        "last_attempt_at": _extruder_last_attempt_at.isoformat() if _extruder_last_attempt_at else None,
        "last_success_at": _extruder_last_success_at.isoformat() if _extruder_last_success_at else None,
        "last_error_at": _extruder_last_error_at.isoformat() if _extruder_last_error_at else None,
        "last_error": _extruder_last_error,
    }


@router.get("/extruder/derived")
async def get_extruder_derived_kpis(
    current_user: User = Depends(require_viewer),
    window_minutes: int = Query(30, ge=5, le=1440, description="Time window in minutes to analyze"),
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    """
    Step 1–4: Read recent data, compute baseline, derived metrics, and risk indicators.
    
    IMPORTANT: Process evaluation (traffic-light, baseline, risk scores) only runs in PRODUCTION state.
    When machine is OFF/HEATING/IDLE/COOLING, returns empty/neutral values.
    
    Returns:
      - window_minutes: requested window
      - rows: raw rows in the window
      - baseline: per-sensor rolling baseline (mean) and normal range (mean ± 1 std) - only in PRODUCTION
      - derived: Temp_Avg, Temp_Spread, stability flags - only in PRODUCTION
      - risk: per-sensor risk level (green/yellow/red) and overall risk - only in PRODUCTION
    """
    import pymssql
    from datetime import datetime, timedelta

    global _extruder_last_attempt_at, _extruder_last_success_at, _extruder_last_error_at, _extruder_last_error
    _extruder_last_attempt_at = datetime.utcnow()

    # Load config from environment
    host = os.getenv("MSSQL_HOST")
    port_raw = os.getenv("MSSQL_PORT", "1433")
    user = os.getenv("MSSQL_USER")
    password = os.getenv("MSSQL_PASSWORD")
    database = (os.getenv("MSSQL_DATABASE") or "HISTORISCH").strip()
    table_raw = (os.getenv("MSSQL_TABLE") or "Tab_Actual").strip()
    schema_raw = (os.getenv("MSSQL_SCHEMA") or "dbo").strip()

    schema = schema_raw
    table = table_raw
    if "." in table_raw:
        parts = [p for p in table_raw.split(".") if p]
        if len(parts) == 2:
            schema, table = parts[0], parts[1]

    # Validate config
    try:
        port = int(port_raw)
    except Exception:
        _extruder_last_error_at = datetime.utcnow()
        _extruder_last_error = "Invalid MSSQL_PORT"
        raise HTTPException(status_code=500, detail="Invalid MSSQL_PORT")

    if not (host and user and password):
        _extruder_last_error_at = datetime.utcnow()
        _extruder_last_error = "Missing MSSQL connection config"
        raise HTTPException(status_code=500, detail="Missing MSSQL connection config")

    # Step 1: Read latest data within time window
    cutoff = datetime.utcnow() - timedelta(minutes=window_minutes)
    conn = None
    try:
        conn = pymssql.connect(
            server=host,
            port=port,
            user=user,
            password=password,
            database=database,
            as_dict=True,
            login_timeout=10,
        )
        cursor = conn.cursor()
        # Use SQL 2000 compatible syntax
        sql = f"""
        SELECT TOP 200
            TrendDate,
            Val_4 AS ScrewSpeed_rpm,
            Val_6 AS Pressure_bar,
            Val_7 AS Temp_Zone1_C,
            Val_8 AS Temp_Zone2_C,
            Val_9 AS Temp_Zone3_C,
            Val_10 AS Temp_Zone4_C
        FROM [{schema}].[{table}]
        WHERE TrendDate >= DATEADD(minute, -{window_minutes}, GETDATE())
        ORDER BY TrendDate DESC
        """
        cursor.execute(sql)
        rows_raw = cursor.fetchall()
        # Ensure TrendDate is datetime
        rows = []
        for r in rows_raw:
            td = r.get("TrendDate")
            if isinstance(td, datetime):
                rows.append(r)
        # Reverse to chronological order (oldest first)
        rows = list(reversed(rows))
        _extruder_last_success_at = datetime.utcnow()
        _extruder_last_error = None
        _extruder_last_error_at = None
    except pymssql.exceptions.OperationalError as e:
        _extruder_last_error_at = datetime.utcnow()
        _extruder_last_error = f"MSSQL connection failed: {str(e)[:200]}"
        logger.error(f"MSSQL connection error in /extruder/derived: {e}")
        # Return empty data instead of raising exception when MSSQL is unavailable
        rows = []
    except Exception as e:
        _extruder_last_error_at = datetime.utcnow()
        _extruder_last_error = str(e)
        logger.error(f"MSSQL extruder/derived error: {e}")
        # Return empty data instead of raising exception
        rows = []
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

    # Check machine state - only calculate baselines/risk in PRODUCTION
    from app.services.machine_state_manager import MachineStateService
    from sqlalchemy import select as sql_select  # Explicit import to avoid UnboundLocalError
    state_service = MachineStateService(session)
    
    # Get the extruder machine (assuming single machine for now)
    machines = await session.scalars(sql_select(Machine).where(Machine.name == "Extruder-SQL").limit(1))
    machine = machines.first()
    
    is_in_production = False
    current_state = None
    machine_state_str = "UNKNOWN"
    machine_id = None
    if machine:
        machine_id = machine.id
        current_state = await state_service.get_current_state(str(machine.id))
        if current_state:
            machine_state_str = current_state.state.value
            is_in_production = (machine_state_str == "PRODUCTION")
    
    # STEP 1: Machine state gate
    # IF machine_state != PRODUCTION → return "no evaluation"
    if not is_in_production:
        return {
            "window_minutes": window_minutes,
            "rows": rows,
            "baseline": {},
            "baselines_standardized": {},
            "derived": {},
            "risk": {"overall": "unknown", "sensors": {}},
            "risk_score": None,
            "severity": {"overall": -1, "sensors": {}},
            "stability_severity": {},
            "stability_state": "unknown",
            "ml_warning": False,  # No ML warning when not in PRODUCTION
            "risk_components": {},
            "overall_text": f"[{machine_state_str}] Process evaluation disabled - machine not in PRODUCTION state",
            "machine_state": machine_state_str,
            "evaluation_enabled": False,
            "profile_id": None,
            "baseline_ready": False,
        }
    
    # Fetch latest ML predictions for ML warning detection (STEP 4)
    ml_predictions = {}
    ml_warning_overall = False
    if machine_id:
        try:
            # Get latest predictions for this machine (within last 30 minutes)
            cutoff_time = datetime.utcnow() - timedelta(minutes=30)
            predictions_result = await session.execute(
                sql_select(Prediction)
                .where(
                    and_(
                        Prediction.machine_id == machine_id,
                        Prediction.timestamp >= cutoff_time
                    )
                )
                .order_by(Prediction.timestamp.desc())
                .limit(10)
            )
            latest_predictions = predictions_result.scalars().all()
            
            # Extract ML anomaly scores per sensor/metric
            for pred in latest_predictions:
                # Use sensor name or metric from metadata to map to our sensor keys
                sensor_name = None
                if pred.sensor_id:
                    sensor_result = await session.execute(
                        sql_select(Sensor).where(Sensor.id == pred.sensor_id)
                    )
                    sensor = sensor_result.scalar_one_or_none()
                    if sensor:
                        sensor_name = sensor.name
                
                # Get anomaly score (from score field or metadata)
                anomaly_score = float(pred.score) if pred.score else 0.0
                if pred.metadata_json and isinstance(pred.metadata_json, dict):
                    # Check metadata for anomaly_score
                    meta_score = pred.metadata_json.get("anomaly_score")
                    if meta_score is not None:
                        try:
                            anomaly_score = float(meta_score)
                        except (ValueError, TypeError):
                            pass
                
                # Map sensor to our metric keys (e.g., "Pressure" -> "Pressure_bar")
                if sensor_name:
                    # Try to match sensor name to our metric keys
                    for metric_key in ["Pressure_bar", "ScrewSpeed_rpm", "Temp_Zone1_C", "Temp_Zone2_C", "Temp_Zone3_C", "Temp_Zone4_C"]:
                        if sensor_name.lower().replace("_", "").replace("-", "") in metric_key.lower().replace("_", ""):
                            if metric_key not in ml_predictions or anomaly_score > ml_predictions[metric_key]:
                                ml_predictions[metric_key] = anomaly_score
                                break
                
                # Also check overall ML warning (any prediction with high score)
                if anomaly_score > 0.7:  # ML threshold
                    ml_warning_overall = True
        except Exception as e:
            logger.debug(f"Failed to fetch ML predictions for ML warning: {e}")
            # Non-blocking: continue without ML warnings if fetch fails
    
    if not rows:
        return {
            "window_minutes": window_minutes,
            "rows": [],
            "baseline": {},
            "derived": {},
            "risk": {"overall": "unknown", "sensors": {}},
            "risk_score": None,  # N/A when no data
            "machine_state": machine_state_str,
            "evaluation_enabled": is_in_production,
        }
    
    # Helper to extract numeric values safely
    def as_float(val):
        try:
            return float(val) if val is not None else None
        except Exception:
            return None

    # Step 2: Baseline calculation per sensor, operating-point aware
    sensor_keys = ["ScrewSpeed_rpm", "Pressure_bar", "Temp_Zone1_C", "Temp_Zone2_C", "Temp_Zone3_C", "Temp_Zone4_C"]
    
    # Calculate Temp_Avg and Temp_Spread for ALL states (even when not in PRODUCTION)
    temp_keys = ["Temp_Zone1_C", "Temp_Zone2_C", "Temp_Zone3_C", "Temp_Zone4_C"]
    for r in rows:
        temps = [as_float(r.get(k)) for k in temp_keys if as_float(r.get(k)) is not None]
        if temps and len(temps) >= 2:
            r["Temp_Avg"] = round(statistics.mean(temps), 1)
            r["Temp_Spread"] = round(max(temps) - min(temps), 1)
        else:
            r["Temp_Avg"] = None
            r["Temp_Spread"] = None
    
    # Get current Temp_Avg and Temp_Spread from latest row
    current_temp_avg = None
    current_temp_spread = None
    if rows:
        latest_row = rows[-1]
        current_temp_avg = latest_row.get("Temp_Avg")
        current_temp_spread = latest_row.get("Temp_Spread")
    
    # If not in PRODUCTION, return raw data but skip baseline/risk calculations
    if not is_in_production:
        # Text Engine: Neutral status text for non-PRODUCTION states
        neutral_explanations = {}
        state_display_names = {
            "OFF": "Machine is off",
            "HEATING": "Machine is heating up",
            "IDLE": "Machine is idle (warm and ready)",
            "COOLING": "Machine is cooling down",
        }
        state_message = state_display_names.get(machine_state_str, f"Machine is in {machine_state_str} state")
        
        # Generate neutral explanations for each sensor
        for key in sensor_keys:
            neutral_explanations[key] = f"{key}: {state_message}. No process evaluation in this state."
        
        # Add explanations for derived metrics
        neutral_explanations["Temp_Avg"] = f"Temp_Avg: {state_message}. No process evaluation in this state."
        neutral_explanations["Temp_Spread"] = f"Temp_Spread: {state_message}. No process evaluation in this state."
        
        # Overall neutral text
        overall_text = f"{state_message}. Process evaluation is disabled. Evaluation only runs during PRODUCTION."
        
        return {
            "window_minutes": window_minutes,
            "rows": rows,
            "baseline": {},  # Empty - no baseline calculation when not in PRODUCTION
            "derived": {
                "Temp_Avg": {"current": current_temp_avg},  # Show calculated value
                "Temp_Spread": {"current": current_temp_spread},  # Show calculated value
                "explanations": neutral_explanations,  # Neutral status text
            },
            "risk": {"overall": "unknown", "sensors": {}},  # No risk scores when not in PRODUCTION
            "risk_score": None,  # N/A when not in PRODUCTION
            "overall_text": overall_text,  # Overall neutral text
            "machine_state": machine_state_str,
            "evaluation_enabled": False,
            "message": f"Process evaluation disabled - machine is in {machine_state_str} state. Evaluation only runs in PRODUCTION.",
        }

    # Step 2: Baseline calculation per sensor, operating-point aware
    baseline = {}
    # Determine operating point by ScrewSpeed_rpm buckets (simple 2-rpm bins)
    screw_speeds = [as_float(r.get("ScrewSpeed_rpm")) for r in rows if as_float(r.get("ScrewSpeed_rpm")) is not None]
    if screw_speeds:
        current_speed = screw_speeds[-1]
        # Create bucket: round to nearest 2 rpm
        speed_bucket = round(current_speed / 2) * 2
        # Filter rows within this operating point (±2 rpm)
        op_rows = [r for r in rows if as_float(r.get("ScrewSpeed_rpm")) is not None and abs(as_float(r.get("ScrewSpeed_rpm")) - speed_bucket) <= 2]
    else:
        op_rows = rows

    for key in sensor_keys:
        values = [as_float(r.get(key)) for r in op_rows if as_float(r.get(key)) is not None]
        if values:
            mean_val = statistics.mean(values)
            std_val = statistics.stdev(values) if len(values) > 1 else 0.0
            baseline[key] = {
                "mean": round(mean_val, 3),
                "std": round(std_val, 3),
                "min_normal": round(mean_val - std_val, 3),
                "max_normal": round(mean_val + std_val, 3),
                "count": len(values),
                "op_bucket": speed_bucket if key == "ScrewSpeed_rpm" else None,
            }
        else:
            baseline[key] = {"mean": None, "std": None, "min_normal": None, "max_normal": None, "count": 0, "op_bucket": None}

    # Step 3: Derived metrics
    derived = {}
    # Temperature averages per row
    temp_keys = ["Temp_Zone1_C", "Temp_Zone2_C", "Temp_Zone3_C", "Temp_Zone4_C"]
    for r in rows:
        temps = [as_float(r.get(k)) for k in temp_keys if as_float(r.get(k)) is not None]
        if temps:
            r["Temp_Avg"] = round(statistics.mean(temps), 3)
            r["Temp_Spread"] = round(max(temps) - min(temps), 3)
        else:
            r["Temp_Avg"] = None
            r["Temp_Spread"] = None
    # Overall derived aggregates
    all_temp_avg = [r["Temp_Avg"] for r in rows if r.get("Temp_Avg") is not None]
    all_temp_spread = [r["Temp_Spread"] for r in rows if r.get("Temp_Spread") is not None]
    derived["Temp_Avg"] = {
        "current": rows[-1].get("Temp_Avg") if rows else None,
        "mean": round(statistics.mean(all_temp_avg), 3) if all_temp_avg else None,
        "std": round(statistics.stdev(all_temp_avg), 3) if len(all_temp_avg) > 1 else None,
    }
    derived["Temp_Spread"] = {
        "current": rows[-1].get("Temp_Spread") if rows else None,
        "mean": round(statistics.mean(all_temp_spread), 3) if all_temp_spread else None,
        "std": round(statistics.stdev(all_temp_spread), 3) if len(all_temp_spread) > 1 else None,
    }
    # Stability indicators: % of points within normal range
    stability = {}
    for key in sensor_keys:
        vals = [as_float(r.get(key)) for r in rows if as_float(r.get(key)) is not None]
        base = baseline.get(key, {})
        min_n = base.get("min_normal")
        max_n = base.get("max_normal")
        if min_n is not None and max_n is not None and vals:
            stable_count = sum(1 for v in vals if min_n <= v <= max_n)
            stability[key] = round(100 * stable_count / len(vals), 1)
        else:
            stability[key] = None
    derived["stability_percent"] = stability

    # Per-sensor time spread (stability) within window
    per_sensor_spread = {}
    for key in sensor_keys:
        vals = [as_float(r.get(key)) for r in rows if as_float(r.get(key)) is not None]
        if len(vals) >= 2:
            spread = max(vals) - min(vals)
            per_sensor_spread[key] = round(spread, 3)
        else:
            per_sensor_spread[key] = None
    derived["per_sensor_spread"] = per_sensor_spread

    # Step 3.5: Load Profile Data BEFORE Stability Evaluation (needed for baseline_std)
    # Get active profile for scoring bands and baselines
    from app.services.baseline_learning_service import baseline_learning_service, BaselineLearningService
    from app.models.profile import ProfileScoringBand, ProfileBaselineStats
    # Note: select and and_ are already imported at the top of the file
    
    # Get machine and material for profile lookup
    active_profile = None
    scoring_bands = {}
    profile_baselines = {}
    
    try:
        # machine was already fetched earlier in the function
        if machine:
            material_id = (machine.metadata_json or {}).get("current_material", "Material 1")
            active_profile = await baseline_learning_service.get_active_profile(
                session, machine.id, material_id
            )
            
            if active_profile and active_profile.baseline_ready:
                # Load scoring bands for this profile
                bands_result = await session.execute(
                    sql_select(ProfileScoringBand)
                    .where(ProfileScoringBand.profile_id == active_profile.id)
                )
                bands = bands_result.scalars().all()
                for band in bands:
                    scoring_bands[band.metric_name] = {
                        "mode": band.mode,  # "ABS" or "REL"
                        "green_limit": band.green_limit,
                        "orange_limit": band.orange_limit,
                    }
                
                # Load baseline stats from finalized baseline
                baseline_stats_result = await session.execute(
                    sql_select(ProfileBaselineStats)
                    .where(ProfileBaselineStats.profile_id == active_profile.id)
                )
                baseline_stats = baseline_stats_result.scalars().all()
                profile_baseline_stats_dict = {}  # Store for standardized baseline
                for bs in baseline_stats:
                    profile_baselines[bs.metric_name] = {
                        "mean": bs.baseline_mean,
                        "std": bs.baseline_std,
                    }
                    profile_baseline_stats_dict[bs.metric_name] = bs  # Store for standardized baseline
    except Exception as e:
        logger.error(f"Error loading profile in /extruder/derived: {e}")
        # Continue without profile - will use fallback baselines
        active_profile = None
        profile_baseline_stats_dict = {}  # Initialize empty dict if profile loading fails
    else:
        # Initialize if not set in try block
        if 'profile_baseline_stats_dict' not in locals():
            profile_baseline_stats_dict = {}

    # Step 3.6: Stability Evaluation (std dev vs baseline std dev)
    # Stability = current_std / baseline_std
    # Window: last 10 minutes (sliding window)
    # GREEN: ratio ≤ 1.2, ORANGE: 1.2 < ratio ≤ 1.6, RED: ratio > 1.6
    stability_evaluation = {}
    stability_severity = {}
    
    # Metrics to evaluate for stability (all baseline-supported sensors + derived Temp_Avg)
    stability_metrics = {
        "ScrewSpeed_rpm": "RPM stability",
        "Pressure_bar": "Pressure stability",
        "Temp_Zone1_C": "Temp Zone 1 stability",
        "Temp_Zone2_C": "Temp Zone 2 stability",
        "Temp_Zone3_C": "Temp Zone 3 stability",
        "Temp_Zone4_C": "Temp Zone 4 stability",
        "Temp_Avg": "Temperature stability",  # Derived average
    }
    
    # Define 10-minute sliding window for stability evaluation
    now_dt = datetime.utcnow()
    ten_min_ago = now_dt - timedelta(minutes=10)
    
    for metric_key, metric_label in stability_metrics.items():
        # Get current window std dev
        # Only use data from the last 10 minutes
        if metric_key == "Temp_Avg":
            # Use Temp_Avg values from rows
            current_vals = [
                r.get("Temp_Avg")
                for r in rows
                if r.get("Temp_Avg") is not None and r.get("TrendDate") and r.get("TrendDate") >= ten_min_ago
            ]
        else:
            current_vals = [
                as_float(r.get(metric_key))
                for r in rows
                if as_float(r.get(metric_key)) is not None
                and r.get("TrendDate")
                and r.get("TrendDate") >= ten_min_ago
            ]
        
        if len(current_vals) < 2:
            stability_evaluation[metric_key] = {
                "current_std": None,
                "baseline_std": None,
                "ratio": None,
                "severity": -1,
                "label": metric_label,
            }
            stability_severity[metric_key] = -1
            continue
        
        current_std = statistics.stdev(current_vals) if len(current_vals) > 1 else 0.0
        
        # Get baseline std dev (prefer profile baseline, fallback to rolling baseline)
        baseline_std = None
        if metric_key in profile_baselines:
            baseline_std = profile_baselines[metric_key].get("std")
        
        if baseline_std is None:
            # Fallback to rolling baseline
            base = baseline.get(metric_key, {})
            baseline_std = base.get("std")
        
        if baseline_std is None or baseline_std == 0:
            stability_evaluation[metric_key] = {
                "current_std": round(current_std, 3),
                "baseline_std": None,
                "ratio": None,
                "severity": -1,
                "label": metric_label,
            }
            stability_severity[metric_key] = -1
            continue
        
        # Calculate ratio: current_std / baseline_std
        ratio = current_std / baseline_std
        
        # Determine severity: 0 = GREEN, 1 = ORANGE, 2 = RED
        if ratio <= 1.2:
            severity = 0  # GREEN / stable
        elif ratio <= 1.6:
            severity = 1  # ORANGE / fluctuating
        else:
            severity = 2  # RED / unstable
        
        stability_evaluation[metric_key] = {
            "current_std": round(current_std, 3),
            "baseline_std": round(baseline_std, 3),
            "ratio": round(ratio, 3),
            "severity": severity,
            "label": metric_label,
        }
        stability_severity[metric_key] = severity
    
    derived["stability_evaluation"] = stability_evaluation

    # Step 4: Scoring Engine (Only in PRODUCTION) - using ProfileScoringBand
    
    def calculate_severity(value: Optional[float], metric_name: str, baseline_mean: Optional[float]) -> int:
        """
        Calculate severity score using ProfileScoringBand.
        
        Returns:
            0 = GREEN
            1 = ORANGE
            2 = RED
            -1 = UNKNOWN (if no scoring band or baseline)
        """
        if value is None or baseline_mean is None:
            return -1  # UNKNOWN
        
        # Get scoring band for this metric
        band = scoring_bands.get(metric_name)
        if not band:
            # No scoring band configured - fallback to Z-score (backward compatibility)
            # Use rolling baseline from current window
            base = baseline.get(metric_name, {})
            mean = base.get("mean")
            std = base.get("std", 0)
            if mean is None:
                return -1
            if std == 0:
                return 0  # GREEN
            z = abs(value - mean) / std
            if z <= 1:
                return 0  # GREEN
            elif z <= 2:
                return 1  # ORANGE
            else:
                return 2  # RED
        
        # Use profile baseline mean if available, otherwise fallback to rolling baseline
        mean = profile_baselines.get(metric_name, {}).get("mean") or baseline.get(metric_name, {}).get("mean")
        if mean is None:
            return -1
        
        mode = band["mode"]
        green_limit = band["green_limit"]
        orange_limit = band["orange_limit"]
        
        if mode == "ABS":
            # ABS mode: compare absolute difference
            abs_diff = abs(value - mean)
            if green_limit is not None and abs_diff <= green_limit:
                return 0  # GREEN
            elif orange_limit is not None and abs_diff <= orange_limit:
                return 1  # ORANGE
            else:
                return 2  # RED
        elif mode == "REL":
            # REL mode: compare % deviation from baseline
            if mean == 0:
                return -1  # Cannot calculate percentage deviation
            pct_deviation = abs((value - mean) / mean) * 100.0  # Percentage
            
            # Convert limits from percentage to absolute if needed
            # For REL mode, limits are typically in percentage (e.g., 5.0 = 5%)
            if green_limit is not None and pct_deviation <= green_limit:
                return 0  # GREEN
            elif orange_limit is not None and pct_deviation <= orange_limit:
                return 1  # ORANGE
            else:
                return 2  # RED
        else:
            # Unknown mode - fallback
            return -1
    
    # Calculate severity for each sensor using Decision Hierarchy
    risk_sensors = {}
    severity_sensors = {}  # Numeric severity (0, 1, 2) - after applying decision hierarchy
    severity_sensors_rule_based = {}  # Rule-based severity before hierarchy (for debugging)
    ml_warnings_per_sensor = {}  # ML warning per sensor
    current_row = rows[-1] if rows else {}
    for key in sensor_keys:
        val = as_float(current_row.get(key))
        base = baseline.get(key, {})
        mean = base.get("mean")
        
        # Use profile baseline if available
        if key in profile_baselines:
            mean = profile_baselines[key]["mean"]
        
        # STEP 2: Calculate rule-based severity (material thresholds)
        rule_based_severity = calculate_severity(val, key, mean)
        severity_sensors_rule_based[key] = rule_based_severity
        
        # Get stability severity for this sensor
        stability_sev = stability_severity.get(key, None)
        
        # Get ML anomaly score for this sensor
        ml_score = ml_predictions.get(key, None)
        
        # Apply Decision Hierarchy (STEP 2, 3, 4)
        final_severity, ml_warning = apply_decision_hierarchy(
            rule_based_severity=rule_based_severity,
            stability_severity=stability_sev,
            ml_anomaly_score=ml_score,
            ml_threshold=0.7,
        )
        
        severity_sensors[key] = final_severity
        ml_warnings_per_sensor[key] = ml_warning
        
        # Convert to string for backward compatibility
        if final_severity == 0:
            risk_sensors[key] = "green"
        elif final_severity == 1:
            risk_sensors[key] = "orange"
        elif final_severity == 2:
            risk_sensors[key] = "red"
        else:
            risk_sensors[key] = "unknown"
    
    # Calculate severity for derived metrics (Temp_Avg, Temp_Spread) using Decision Hierarchy
    # Temp_Avg severity
    temp_avg_val = current_row.get("Temp_Avg")
    if temp_avg_val is not None:
        temp_avg_base = derived.get("Temp_Avg", {})
        temp_avg_mean = temp_avg_base.get("mean")
        if temp_avg_mean is None and "Temp_Avg" in profile_baselines:
            temp_avg_mean = profile_baselines["Temp_Avg"]["mean"]
        rule_based_temp_avg = calculate_severity(temp_avg_val, "Temp_Avg", temp_avg_mean)
        severity_sensors_rule_based["Temp_Avg"] = rule_based_temp_avg
        
        # Apply Decision Hierarchy
        temp_avg_stability = stability_severity.get("Temp_Avg", None)
        temp_avg_ml = ml_predictions.get("Temp_Avg", None)
        temp_avg_final, temp_avg_ml_warn = apply_decision_hierarchy(
            rule_based_severity=rule_based_temp_avg,
            stability_severity=temp_avg_stability,
            ml_anomaly_score=temp_avg_ml,
            ml_threshold=0.7,
        )
        severity_sensors["Temp_Avg"] = temp_avg_final
        ml_warnings_per_sensor["Temp_Avg"] = temp_avg_ml_warn
    
    # Temp_Spread severity
    temp_spread_val = current_row.get("Temp_Spread")
    if temp_spread_val is not None:
        temp_spread_base = derived.get("Temp_Spread", {})
        temp_spread_mean = temp_spread_base.get("mean")
        if temp_spread_mean is None:
            # Calculate from temp zones if not available
            temp_keys = ["Temp_Zone1_C", "Temp_Zone2_C", "Temp_Zone3_C", "Temp_Zone4_C"]
            all_spreads = []
            for r in rows:
                temps = [as_float(r.get(k)) for k in temp_keys if as_float(r.get(k)) is not None]
                if len(temps) >= 2:
                    all_spreads.append(max(temps) - min(temps))
            if all_spreads:
                temp_spread_mean = statistics.mean(all_spreads)
        rule_based_temp_spread = calculate_severity(temp_spread_val, "Temp_Spread", temp_spread_mean)
        severity_sensors_rule_based["Temp_Spread"] = rule_based_temp_spread
        
        # Apply Decision Hierarchy
        temp_spread_stability = stability_severity.get("Temp_Spread", None)
        temp_spread_ml = ml_predictions.get("Temp_Spread", None)
        temp_spread_final, temp_spread_ml_warn = apply_decision_hierarchy(
            rule_based_severity=rule_based_temp_spread,
            stability_severity=temp_spread_stability,
            ml_anomaly_score=temp_spread_ml,
            ml_threshold=0.7,
        )
        severity_sensors["Temp_Spread"] = temp_spread_final
        ml_warnings_per_sensor["Temp_Spread"] = temp_spread_ml_warn
    
    # Overall Risk Calculation: Weighted Risk Score
    # risk_score = 25 * pressure_severity + 25 * temp_spread_severity + 25 * stability_severity + 25 * temp_avg_severity
    # Range: 0-100
    # 0-33 → GREEN, 34-66 → ORANGE, 67-100 → RED
    
    # Get individual severity scores for weighted risk calculation
    pressure_severity = severity_sensors.get("Pressure_bar", -1)
    temp_avg_severity = severity_sensors.get("Temp_Avg", -1)
    temp_spread_severity = severity_sensors.get("Temp_Spread", -1)
    
    # Get stability_severity (use Pressure stability as primary, or average if multiple)
    stability_severity_val = -1
    if "Pressure_bar" in stability_severity:
        stability_severity_val = stability_severity["Pressure_bar"]
    elif stability_severity:
        # Average of all stability severities
        valid_stability = [v for v in stability_severity.values() if v >= 0]
        if valid_stability:
            stability_severity_val = round(sum(valid_stability) / len(valid_stability))
    
    # Calculate weighted risk score (0-100)
    # Only calculate if all components are available (severity >= 0)
    risk_score = None
    if all(s >= 0 for s in [pressure_severity, temp_spread_severity, stability_severity_val, temp_avg_severity]):
        risk_score = (
            25 * pressure_severity +
            25 * temp_spread_severity +
            25 * stability_severity_val +
            25 * temp_avg_severity
        )
        # Ensure range is 0-100
        risk_score = max(0, min(100, risk_score))
    
    # Determine overall risk color from risk_score
    # Process Status: Worst sensor status = process status (ML warnings do NOT change status)
    if risk_score is not None:
        if risk_score <= 33:
            overall_risk = "green"
            overall_severity = 0
            process_status = "green"
            process_status_text = "Process stable"
        elif risk_score <= 66:
            overall_risk = "orange"
            overall_severity = 1
            process_status = "orange"
            process_status_text = "Process drifting from baseline"
        else:
            overall_risk = "red"
            overall_severity = 2
            process_status = "red"
            process_status_text = "High risk of instability or scrap"
    else:
        # Fallback to worst sensor risk if weighted calculation not possible
        overall_severity = max(severity_sensors.values()) if severity_sensors else -1
        if overall_severity == 0:
            overall_risk = "green"
            process_status = "green"
            process_status_text = "Process stable"
        elif overall_severity == 1:
            overall_risk = "orange"
            process_status = "orange"
            process_status_text = "Process drifting from baseline"
        elif overall_severity == 2:
            overall_risk = "red"
            process_status = "red"
            process_status_text = "High risk of instability or scrap"
        else:
            overall_risk = "unknown"
            process_status = "unknown"
            process_status_text = "System status unknown"

    # Explanations per sensor (using ProfileMessageTemplate if available)
    from app.models.profile import ProfileMessageTemplate
    # Note: Optional is already imported at the top of the file
    
    explanations = {}
    message_templates = {}
    if active_profile:
        templates_result = await session.execute(
            select(ProfileMessageTemplate)
            .where(ProfileMessageTemplate.profile_id == active_profile.id)
        )
        templates = templates_result.scalars().all()
        for template in templates:
            key = f"{template.metric_name}_{template.severity}"
            message_templates[key] = template.text
    
    for key in sensor_keys:
        val = as_float(current_row.get(key))
        base = baseline.get(key, {})
        mean = base.get("mean")
        std = base.get("std")
        severity = severity_sensors.get(key, -1)
        
        # Try to use profile message template
        severity_str = ["GREEN", "ORANGE", "RED"][severity] if 0 <= severity <= 2 else "UNKNOWN"
        template_key = f"{key}_{severity_str}"
        
        if template_key in message_templates:
            explanations[key] = message_templates[template_key]
        else:
            # Fallback to default messages
            if severity == 2:  # RED
                explanations[key] = f"{key} critically deviates from normal ({mean:.1f}±{std:.1f})"
            elif severity == 1:  # ORANGE
                explanations[key] = f"{key} drifting from normal ({mean:.1f}±{std:.1f})"
            elif severity == 0:  # GREEN
                explanations[key] = f"{key} stable"
            else:
                explanations[key] = f"{key} unknown"
    derived["explanations"] = explanations
    derived["severity"] = severity_sensors  # Include numeric severity scores
    
    # Text Engine: Overall text derived from highest severity metric
    # Find metric with highest severity
    highest_severity = -1
    highest_severity_metric = None
    highest_severity_text = None
    
    for metric, severity in severity_sensors.items():
        if severity > highest_severity:
            highest_severity = severity
            highest_severity_metric = metric
            highest_severity_text = explanations.get(metric, f"{metric} status unknown")
    
    # If no severity found, use overall risk
    if highest_severity < 0:
        if overall_severity == 0:
            overall_text = "All systems operating normally"
        elif overall_severity == 1:
            overall_text = "Some metrics require attention"
        elif overall_severity == 2:
            overall_text = "Critical issues detected - immediate action required"
        else:
            overall_text = "System status unknown"
    else:
        # Use text from highest severity metric
        overall_text = highest_severity_text
    
    # Add state context to overall text
    overall_text = f"[PRODUCTION] {overall_text}"
    
    # Map overall stability severity (based on pressure stability or average) to human-readable state
    if stability_severity_val == 0:
        stability_state = "green"
    elif stability_severity_val == 1:
        stability_state = "orange"
    elif stability_severity_val == 2:
        stability_state = "red"
    else:
        stability_state = "unknown"
    
    # Build standardized baseline structures for all sensors
    standardized_baselines = {}
    # Note: Temp_Spread is NOT included - it uses fixed thresholds (5°C, 8°C), not baseline
    sensor_keys_for_baseline = ["ScrewSpeed_rpm", "Pressure_bar", "Temp_Zone1_C", "Temp_Zone2_C", "Temp_Zone3_C", "Temp_Zone4_C", "Temp_Avg"]
    for sensor_key in sensor_keys_for_baseline:
        baseline_stat = profile_baseline_stats_dict.get(sensor_key) if profile_baseline_stats_dict else None
        if baseline_stat and active_profile:
            # Use ProfileBaselineStats if available
            standardized_baselines[sensor_key] = build_standardized_baseline(
                baseline_stat=baseline_stat,
                profile=active_profile,
            )
        elif sensor_key in baseline and baseline[sensor_key].get("mean") is not None:
            # Fallback: Use rolling baseline data
            standardized_baselines[sensor_key] = build_standardized_baseline_from_dict(
                metric_name=sensor_key,
                baseline_data=baseline[sensor_key],
                material_id=active_profile.material_id if active_profile else None,
                confidence=0.8 if baseline[sensor_key].get("count", 0) >= 30 else 0.6,  # Lower confidence for rolling baseline
            )

    # Determine overall ML warning (any sensor has ML warning)
    ml_warning_overall = any(ml_warnings_per_sensor.values()) or ml_warning_overall
    
    # This return is only reached when is_in_production is True (early return above handles non-PRODUCTION)
    return {
        "window_minutes": window_minutes,
        "rows": rows,
        "baseline": baseline,
        "baselines_standardized": standardized_baselines,  # Add standardized baseline structures
        "derived": derived,
        "risk": {"overall": overall_risk, "sensors": risk_sensors},
        "risk_score": risk_score,  # Weighted risk score (0-100) or None if not in PRODUCTION
        "severity": {"overall": overall_severity, "sensors": severity_sensors},  # Numeric severity (0, 1, 2) - after decision hierarchy
        "severity_rule_based": severity_sensors_rule_based,  # Rule-based severity before hierarchy (for debugging)
        "stability_severity": stability_severity,  # Per-sensor stability severity scores (0, 1, 2)
        "stability_state": stability_state,  # Overall stability state: green | orange | red | unknown
        "ml_warning": ml_warning_overall,  # Overall ML warning flag (True if any ML anomaly detected)
        "ml_warnings": ml_warnings_per_sensor,  # Per-sensor ML warning flags
        "risk_components": {  # Individual components for debugging
            "pressure_severity": pressure_severity,
            "temp_spread_severity": temp_spread_severity,
            "stability_severity": stability_severity_val,
            "temp_avg_severity": temp_avg_severity,
        },
        "overall_text": overall_text,  # Overall text derived from highest severity metric (kept for backward compatibility)
        "process_status": process_status,  # Process status: "green" | "orange" | "red" | "unknown" (worst sensor status, ML warnings do NOT change this)
        "process_status_text": process_status_text,  # Process status text: "Process stable" | "Process drifting from baseline" | "High risk of instability or scrap"
        "machine_state": machine_state_str,
        "evaluation_enabled": True,
        "profile_id": str(active_profile.id) if active_profile else None,
        "baseline_ready": active_profile.baseline_ready if active_profile else False,
    }


@router.get("/current")
async def get_current_dashboard_data(
    material_id: Optional[str] = Query(None, description="Material ID to use for profile lookup. If not provided, uses machine metadata."),
    current_user: User = Depends(require_viewer),
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    """
    Single Source of Truth API for Dashboard
    
    Returns all computed values so frontend doesn't need to recompute logic.
    Includes: machine state, metrics with baselines, severity, risk, explanations.
    
    If material_id is provided, it will be used to load the profile. Otherwise, uses machine metadata.
    """
    from app.services.machine_state_manager import MachineStateService
    from app.services.baseline_learning_service import baseline_learning_service, BaselineLearningService
    from app.models.profile import ProfileBaselineStats, ProfileScoringBand, ProfileBaselineSample
    from sqlalchemy import select as sql_select  # Explicit import to avoid UnboundLocalError
    
    # Get the extruder machine (assuming single machine for now)
    machines = await session.scalars(sql_select(Machine).where(Machine.name == "Extruder-SQL").limit(1))
    machine = machines.first()
    
    if not machine:
        raise HTTPException(status_code=404, detail="Extruder machine not found")
    
    # Query MSSQL for latest data first (needed for state calculation)
    import pymssql
    from datetime import datetime, timedelta
    
    # Load MSSQL config
    host = os.getenv("MSSQL_HOST")
    port_raw = os.getenv("MSSQL_PORT", "1433")
    user = os.getenv("MSSQL_USER")
    password = os.getenv("MSSQL_PASSWORD")
    database = (os.getenv("MSSQL_DATABASE") or "HISTORISCH").strip()
    table_raw = (os.getenv("MSSQL_TABLE") or "Tab_Actual").strip()
    schema_raw = (os.getenv("MSSQL_SCHEMA") or "dbo").strip()
    
    schema = schema_raw
    table = table_raw
    if "." in table_raw:
        parts = [p for p in table_raw.split(".") if p]
        if len(parts) == 2:
            schema, table = parts[0], parts[1]
    
    try:
        port = int(port_raw)
    except ValueError:
        logger.error("Invalid MSSQL_PORT configuration")
        port = 1433
    
    # Query MSSQL for latest data to compute state
    conn = None
    current_row = {}
    latest_timestamp = None
    try:
        if host and user and password:
            conn = pymssql.connect(
                server=host,
                port=port,
                user=user,
                password=password,
                database=database,
                as_dict=True,
                login_timeout=5,
            )
            cursor = conn.cursor()
            sql = f"""
            SELECT TOP 1
                TrendDate,
                Val_4 AS ScrewSpeed_rpm,
                Val_6 AS Pressure_bar,
                Val_7 AS Temp_Zone1_C,
                Val_8 AS Temp_Zone2_C,
                Val_9 AS Temp_Zone3_C,
                Val_10 AS Temp_Zone4_C
            FROM [{schema}].[{table}]
            ORDER BY TrendDate DESC
            """
            cursor.execute(sql)
            rows_raw = cursor.fetchall()
            if rows_raw:
                current_row = rows_raw[0]
                latest_timestamp = current_row.get("TrendDate")
    except Exception as e:
        logger.warning(f"MSSQL connection error in /dashboard/current: {e}")
        # Continue without MSSQL data - will use get_current_state fallback
    finally:
        if conn:
            conn.close()
    
    # Get current machine state - compute from latest MSSQL data if available
    state_service = MachineStateService(session)
    current_state = None
    
    # If we have latest MSSQL data, process it through the state detector
    if current_row and latest_timestamp:
        try:
            from app.services.machine_state_service import SensorReading
            
            # Create SensorReading from latest MSSQL data
            sensor_reading = SensorReading(
                timestamp=latest_timestamp if isinstance(latest_timestamp, datetime) else datetime.utcnow(),
                screw_rpm=current_row.get("ScrewSpeed_rpm"),
                pressure_bar=current_row.get("Pressure_bar"),
                temp_zone_1=current_row.get("Temp_Zone1_C"),
                temp_zone_2=current_row.get("Temp_Zone2_C"),
                temp_zone_3=current_row.get("Temp_Zone3_C"),
                temp_zone_4=current_row.get("Temp_Zone4_C"),
            )
            
            # Process the reading to update state
            current_state = await state_service.process_sensor_reading(str(machine.id), sensor_reading)
        except Exception as e:
            logger.warning(f"Error processing sensor reading for state calculation: {e}")
            # Fallback to get_current_state
            current_state = await state_service.get_current_state(str(machine.id))
    else:
        # No MSSQL data available - use get_current_state (may return OFF if no readings)
        current_state = await state_service.get_current_state(str(machine.id))
    
    if not current_state:
        return {
            "machine_state": "UNKNOWN",
            "state_confidence": 0.0,
            "state_since_ts": None,
            "metrics": {},
            "overall_risk": "unknown",
            "overall_severity": -1,
            "explanation_text": "Machine state not available",
            "baseline_status": "not_available",
            "profile_status": "not_available",
        }
    
    machine_state_str = current_state.state.value
    state_confidence = current_state.confidence
    state_since_ts = current_state.state_since.isoformat() if current_state.state_since else None
    is_in_production = (machine_state_str == "PRODUCTION")
    
    # STEP 1: Machine state gate - only evaluate in PRODUCTION
    # Note: We still return data for non-PRODUCTION states, but with neutral/disabled evaluation
    
    # Fetch latest ML predictions for ML warning detection (STEP 4)
    ml_predictions = {}
    ml_warning_overall = False
    if is_in_production and machine:
        try:
            # Get latest predictions for this machine (within last 30 minutes)
            cutoff_time = datetime.utcnow() - timedelta(minutes=30)
            predictions_result = await session.execute(
                sql_select(Prediction)
                .where(
                    and_(
                        Prediction.machine_id == machine.id,
                        Prediction.timestamp >= cutoff_time
                    )
                )
                .order_by(Prediction.timestamp.desc())
                .limit(10)
            )
            latest_predictions = predictions_result.scalars().all()
            
            # Extract ML anomaly scores per sensor/metric
            for pred in latest_predictions:
                # Use sensor name or metric from metadata to map to our sensor keys
                sensor_name = None
                if pred.sensor_id:
                    sensor_result = await session.execute(
                        sql_select(Sensor).where(Sensor.id == pred.sensor_id)
                    )
                    sensor = sensor_result.scalar_one_or_none()
                    if sensor:
                        sensor_name = sensor.name
                
                # Get anomaly score (from score field or metadata)
                anomaly_score = float(pred.score) if pred.score else 0.0
                if pred.metadata_json and isinstance(pred.metadata_json, dict):
                    # Check metadata for anomaly_score
                    meta_score = pred.metadata_json.get("anomaly_score")
                    if meta_score is not None:
                        try:
                            anomaly_score = float(meta_score)
                        except (ValueError, TypeError):
                            pass
                
                # Map sensor to our metric keys (e.g., "Pressure" -> "Pressure_bar")
                if sensor_name:
                    # Try to match sensor name to our metric keys
                    for metric_key in ["Pressure_bar", "ScrewSpeed_rpm", "Temp_Zone1_C", "Temp_Zone2_C", "Temp_Zone3_C", "Temp_Zone4_C", "Temp_Avg", "Temp_Spread"]:
                        if sensor_name.lower().replace("_", "").replace("-", "") in metric_key.lower().replace("_", ""):
                            if metric_key not in ml_predictions or anomaly_score > ml_predictions[metric_key]:
                                ml_predictions[metric_key] = anomaly_score
                                break
                
                # Also check overall ML warning (any prediction with high score)
                if anomaly_score > 0.7:  # ML threshold
                    ml_warning_overall = True
        except Exception as e:
            logger.debug(f"Failed to fetch ML predictions for ML warning in /current: {e}")
            # Non-blocking: continue without ML warnings if fetch fails
    
    # Get active profile - use material_id from query param or fallback to machine metadata
    if not material_id:
        material_id = (machine.metadata_json or {}).get("current_material", "Material 1")
    active_profile = await baseline_learning_service.get_active_profile(
        session, machine.id, material_id
    )
    
    # Baseline and profile status
    baseline_status = "not_ready"
    profile_status = "not_available"
    baseline_samples_collected = 0
    baseline_samples_required = BaselineLearningService.MIN_SAMPLES_FOR_BASELINE
    baseline_progress_percent = 0.0
    
    if active_profile:
        profile_status = "active"
        if active_profile.baseline_ready:
            baseline_status = "ready"
        elif active_profile.baseline_learning:
            baseline_status = "learning"
            # Get sample count from ProfileBaselineStats
            from sqlalchemy import select as sql_select
            from app.models.profile import ProfileBaselineSample
            stats_result = await session.execute(
                sql_select(ProfileBaselineStats)
                .where(ProfileBaselineStats.profile_id == active_profile.id)
            )
            all_stats = stats_result.scalars().all()
            
            if all_stats:
                # Get minimum sample count across all metrics (we need all metrics to have enough samples)
                # Filter out None values and ensure we have valid counts
                sample_counts = [float(stat.sample_count or 0.0) for stat in all_stats if stat.sample_count is not None]
                if sample_counts:
                    baseline_samples_collected = int(min(sample_counts))
                else:
                    baseline_samples_collected = 0
            else:
                # If no stats exist yet, check ProfileBaselineSample table for raw count
                # This handles the case where learning just started but no stats created yet
                sample_count_result = await session.execute(
                    sql_select(func.count(ProfileBaselineSample.id))
                    .where(ProfileBaselineSample.profile_id == active_profile.id)
                )
                raw_sample_count = sample_count_result.scalar() or 0
                # Estimate samples per metric (assuming all metrics are collected together)
                # We have 7 metrics, so divide by 7 to get approximate samples per metric
                baseline_samples_collected = max(0, int(raw_sample_count / 7)) if raw_sample_count > 0 else 0
            
            # Calculate progress percentage (avoid division by zero)
            if baseline_samples_required > 0:
                baseline_progress_percent = min(100.0, (baseline_samples_collected / baseline_samples_required) * 100.0)
            else:
                baseline_progress_percent = 0.0
        else:
            baseline_status = "not_ready"
    
    # current_row and latest_timestamp are already available from the MSSQL query above
    
    # Helper function
    def as_float(val):
        try:
            return float(val) if val is not None else None
        except Exception:
            return None
    
    # Calculate Temp_Avg and Temp_Spread from current row (even when not in PRODUCTION)
    metrics_response = {}
    if current_row:
        temps = [
            as_float(current_row.get("Temp_Zone1_C")),
            as_float(current_row.get("Temp_Zone2_C")),
            as_float(current_row.get("Temp_Zone3_C")),
            as_float(current_row.get("Temp_Zone4_C")),
        ]
        valid_temps = [t for t in temps if t is not None]
        
        if len(valid_temps) >= 2:
            temp_avg = round(statistics.mean(valid_temps), 1)
            temp_spread = round(max(valid_temps) - min(valid_temps), 1)
            
            # Add basic sensor metrics
            metrics_response["ScrewSpeed_rpm"] = {
                "current_value": as_float(current_row.get("ScrewSpeed_rpm")),
                "baseline_mean": None,
                "green_band": None,
                "deviation": None,
                "severity": -1,
            }
            metrics_response["Pressure_bar"] = {
                "current_value": as_float(current_row.get("Pressure_bar")),
                "baseline_mean": None,
                "green_band": None,
                "deviation": None,
                "severity": -1,
            }
            metrics_response["Temp_Zone1_C"] = {
                "current_value": as_float(current_row.get("Temp_Zone1_C")),
                "baseline_mean": None,
                "green_band": None,
                "deviation": None,
                "severity": -1,
            }
            metrics_response["Temp_Zone2_C"] = {
                "current_value": as_float(current_row.get("Temp_Zone2_C")),
                "baseline_mean": None,
                "green_band": None,
                "deviation": None,
                "severity": -1,
            }
            metrics_response["Temp_Zone3_C"] = {
                "current_value": as_float(current_row.get("Temp_Zone3_C")),
                "baseline_mean": None,
                "green_band": None,
                "deviation": None,
                "severity": -1,
            }
            metrics_response["Temp_Zone4_C"] = {
                "current_value": as_float(current_row.get("Temp_Zone4_C")),
                "baseline_mean": None,
                "green_band": None,
                "deviation": None,
                "severity": -1,
            }
            metrics_response["Temp_Avg"] = {
                "current_value": temp_avg,
                "baseline_mean": None,
                "green_band": None,
                "deviation": None,
                "severity": -1,
            }
            metrics_response["Temp_Spread"] = {
                "current_value": temp_spread,
                "baseline_mean": None,
                "green_band": None,
                "deviation": None,
                "severity": -1,
            }
    
    # If not in PRODUCTION, return data with neutral text
    if not is_in_production:
        state_display_names = {
            "OFF": "Machine is off",
            "HEATING": "Machine is heating up",
            "IDLE": "Machine is idle (warm and ready)",
            "COOLING": "Machine is cooling down",
        }
        state_message = state_display_names.get(machine_state_str, f"Machine is in {machine_state_str} state")
        explanation_text = f"{state_message}. Process evaluation is disabled. Evaluation only runs during PRODUCTION."
        
        # IMPORTANT UI contract:
        # Do NOT show orange/red/green evaluation outside PRODUCTION.
        # We still return current values, but keep evaluation status neutral.
        spread_status = "unknown"

        # Attach a plain-language explanation per metric so the UI can always explain
        # why evaluation is disabled.
        for k, v in metrics_response.items():
            if isinstance(v, dict) and "explanation" not in v:
                v["explanation"] = explanation_text
        
        return {
            "machine_state": machine_state_str,
            "state_confidence": state_confidence,
            "state_since_ts": state_since_ts,
            "metrics": metrics_response,  # Include metrics even when not in PRODUCTION
            "overall_risk": "unknown",
            "overall_severity": -1,
            "explanation_text": explanation_text,
            "baseline_status": baseline_status,
            "profile_status": profile_status,
            "spread_status": spread_status,  # Temp_Spread status for all states
        }
    
    # PRODUCTION state: Get full evaluation data from /extruder/derived
    # We'll call the logic from get_extruder_derived_kpis but format it for /current
    import pymssql
    from datetime import datetime, timedelta
    
    # Load MSSQL config
    host = os.getenv("MSSQL_HOST")
    port_raw = os.getenv("MSSQL_PORT", "1433")
    user = os.getenv("MSSQL_USER")
    password = os.getenv("MSSQL_PASSWORD")
    database = (os.getenv("MSSQL_DATABASE") or "HISTORISCH").strip()
    table_raw = (os.getenv("MSSQL_TABLE") or "Tab_Actual").strip()
    schema_raw = (os.getenv("MSSQL_SCHEMA") or "dbo").strip()
    
    schema = schema_raw
    table = table_raw
    if "." in table_raw:
        parts = [p for p in table_raw.split(".") if p]
        if len(parts) == 2:
            schema, table = parts[0], parts[1]
    
    try:
        port = int(port_raw)
    except ValueError:
        logger.error("Invalid MSSQL_PORT configuration")
        raise HTTPException(status_code=500, detail="Invalid MSSQL_PORT")
    
    if not all([host, user, password]):
        logger.error("MSSQL configuration incomplete")
        raise HTTPException(status_code=500, detail="MSSQL configuration incomplete")
    
    # Query MSSQL for recent data (last 30 minutes)
    window_minutes = 30
    conn = None
    rows = []
    try:
        conn = pymssql.connect(
            server=host,
            port=port,
            user=user,
            password=password,
            database=database,
            as_dict=True,
            login_timeout=5,  # Reduced timeout to fail faster
        )
        cursor = conn.cursor()
        
        since = datetime.utcnow() - timedelta(minutes=window_minutes)
        # Use same query format as get_extruder_derived_kpis
        sql = f"""
        SELECT TOP 200
            TrendDate,
            Val_4 AS ScrewSpeed_rpm,
            Val_6 AS Pressure_bar,
            Val_7 AS Temp_Zone1_C,
            Val_8 AS Temp_Zone2_C,
            Val_9 AS Temp_Zone3_C,
            Val_10 AS Temp_Zone4_C
        FROM [{schema}].[{table}]
        WHERE TrendDate >= DATEADD(minute, -{window_minutes}, GETDATE())
        ORDER BY TrendDate DESC
        """
        cursor.execute(sql)
        rows_raw = cursor.fetchall()
        # Ensure TrendDate is datetime and convert to dict format
        for r in rows_raw:
            td = r.get("TrendDate")
            if isinstance(td, datetime):
                rows.append(r)
        # Reverse to chronological order (oldest first)
        rows = list(reversed(rows))
        
    except pymssql.exceptions.OperationalError as e:
        logger.error(f"MSSQL connection error in /current: {e}")
        # Return empty data instead of raising exception when MSSQL is unavailable
        rows = []
    except Exception as e:
        logger.error(f"MSSQL error in /current: {e}")
        # Return empty data instead of raising exception
        rows = []
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
    
    if not rows:
        return {
            "machine_state": machine_state_str,
            "state_confidence": state_confidence,
            "state_since_ts": state_since_ts,
            "metrics": {},
            "overall_risk": "unknown",
            "overall_severity": -1,
            "explanation_text": "No sensor data available",
            "baseline_status": baseline_status,
            "profile_status": profile_status,
        }
    
    # Helper function
    def as_float(val):
        try:
            return float(val) if val is not None else None
        except Exception:
            return None
    
    # Calculate baseline and metrics (reuse logic from get_extruder_derived_kpis)
    sensor_keys = ["ScrewSpeed_rpm", "Pressure_bar", "Temp_Zone1_C", "Temp_Zone2_C", "Temp_Zone3_C", "Temp_Zone4_C"]
    baseline = {}
    current_row = rows[-1] if rows else {}
    
    # Operating-point aware baseline
    screw_speeds = [as_float(r.get("ScrewSpeed_rpm")) for r in rows if as_float(r.get("ScrewSpeed_rpm")) is not None]
    if screw_speeds:
        current_speed = screw_speeds[-1]
        speed_bucket = round(current_speed / 2) * 2
        op_rows = [r for r in rows if as_float(r.get("ScrewSpeed_rpm")) is not None and abs(as_float(r.get("ScrewSpeed_rpm")) - speed_bucket) <= 2]
    else:
        op_rows = rows
    
    for key in sensor_keys:
        values = [as_float(r.get(key)) for r in op_rows if as_float(r.get(key)) is not None]
        if values:
            mean_val = statistics.mean(values)
            std_val = statistics.stdev(values) if len(values) > 1 else 0.0
            baseline[key] = {
                "mean": mean_val,
                "std": std_val,
                "min_normal": mean_val - std_val,
                "max_normal": mean_val + std_val,
            }
    
    # Load profile baselines and scoring bands
    profile_baselines = {}
    profile_baseline_stats_dict = {}  # Store full ProfileBaselineStats objects for standardized baseline
    scoring_bands = {}
    if active_profile and active_profile.baseline_ready:
        # Use sql_select to avoid UnboundLocalError
        from sqlalchemy import select as sql_select
        baseline_stats_result = await session.execute(
            sql_select(ProfileBaselineStats)
            .where(ProfileBaselineStats.profile_id == active_profile.id)
        )
        for bs in baseline_stats_result.scalars().all():
            profile_baselines[bs.metric_name] = {
                "mean": bs.baseline_mean,
                "std": bs.baseline_std,
            }
            profile_baseline_stats_dict[bs.metric_name] = bs  # Store full object for standardized baseline
        
        bands_result = await session.execute(
            sql_select(ProfileScoringBand)
            .where(ProfileScoringBand.profile_id == active_profile.id)
        )
        for band in bands_result.scalars().all():
            scoring_bands[band.metric_name] = {
                "mode": band.mode,
                "green_limit": band.green_limit,
                "orange_limit": band.orange_limit,
            }
    
    # Calculate severity function with 3-5% rule
    def calculate_severity_with_band(value: Optional[float], metric_name: str, baseline_mean: Optional[float], green_band: Optional[Dict[str, float]]) -> Tuple[int, Optional[float]]:
        """
        Calculate severity using simple 3-5% rule:
        - Inside band → green (0)
        - Within 3–5% outside baseline → orange (1)
        - More than 5% outside baseline → red (2)
        
        Returns: (severity, deviation_percent)
        """
        if value is None or baseline_mean is None or baseline_mean == 0:
            return (-1, None)
        
        # Calculate percentage deviation from baseline mean
        deviation_percent = abs((value - baseline_mean) / baseline_mean) * 100.0
        
        # Check if value is inside the green band
        is_inside_band = False
        if green_band and green_band.get("min") is not None and green_band.get("max") is not None:
            if green_band["min"] <= value <= green_band["max"]:
                is_inside_band = True
        
        # If inside band → green
        if is_inside_band:
            return (0, deviation_percent)  # GREEN - inside band
        
        # Value is outside the band - apply 3-5% rule based on deviation from baseline mean
        if deviation_percent <= 3.0:
            # Less than 3% deviation - still green even if slightly outside band
            return (0, deviation_percent)  # GREEN
        elif deviation_percent <= 5.0:
            return (1, deviation_percent)  # ORANGE - 3-5% outside
        else:
            return (2, deviation_percent)  # RED - >5% outside
    
    # Calculate severity function (reuse from get_extruder_derived_kpis)
    def calculate_severity(value: Optional[float], metric_name: str, baseline_mean: Optional[float]) -> int:
        if value is None or baseline_mean is None:
            return -1
        
        band = scoring_bands.get(metric_name)
        if not band:
            # Fallback to Z-score
            base = baseline.get(metric_name, {})
            mean = base.get("mean")
            std = base.get("std", 0)
            if mean is None or std == 0:
                return -1
            z = abs(value - mean) / std
            if z <= 1:
                return 0
            elif z <= 2:
                return 1
            else:
                return 2
        
        mean = profile_baselines.get(metric_name, {}).get("mean") or baseline.get(metric_name, {}).get("mean")
        if mean is None:
            return -1
        
        mode = band["mode"]
        green_limit = band["green_limit"]
        orange_limit = band["orange_limit"]
        
        if mode == "ABS":
            abs_diff = abs(value - mean)
            if green_limit is not None and abs_diff <= green_limit:
                return 0
            elif orange_limit is not None and abs_diff <= orange_limit:
                return 1
            else:
                return 2
        elif mode == "REL":
            if mean == 0:
                return -1
            pct_deviation = abs((value - mean) / mean) * 100.0
            if green_limit is not None and pct_deviation <= green_limit:
                return 0
            elif orange_limit is not None and pct_deviation <= orange_limit:
                return 1
            else:
                return 2
        return -1
    
    # Calculate derived metrics (Temp_Avg, Temp_Spread) for each row
    for r in rows:
        temps = [
            as_float(r.get("Temp_Zone1_C")),
            as_float(r.get("Temp_Zone2_C")),
            as_float(r.get("Temp_Zone3_C")),
            as_float(r.get("Temp_Zone4_C")),
        ]
        valid_temps = [t for t in temps if t is not None]
        if len(valid_temps) >= 2:
            r["Temp_Avg"] = round(statistics.mean(valid_temps), 3)
            r["Temp_Spread"] = round(max(valid_temps) - min(valid_temps), 3)
        else:
            r["Temp_Avg"] = None
            r["Temp_Spread"] = None
    
    # Calculate baselines for derived metrics
    # Note: Temp_Spread does NOT use baseline - it uses fixed thresholds (5°C, 8°C)
    all_temp_avg = [r.get("Temp_Avg") for r in rows if r.get("Temp_Avg") is not None]
    # Skip baseline calculation for Temp_Spread - it uses fixed thresholds, not baseline
    
    if all_temp_avg:
        baseline["Temp_Avg"] = {
            "mean": statistics.mean(all_temp_avg),
            "std": statistics.stdev(all_temp_avg) if len(all_temp_avg) > 1 else 0.0,
            "min_normal": statistics.mean(all_temp_avg) - (statistics.stdev(all_temp_avg) if len(all_temp_avg) > 1 else 0.0),
            "max_normal": statistics.mean(all_temp_avg) + (statistics.stdev(all_temp_avg) if len(all_temp_avg) > 1 else 0.0),
        }
    
    # Temp_Spread does NOT get a baseline - it uses fixed thresholds: <=5°C green, 5-8°C orange, >8°C red
    
    # Build metrics response
    metrics_response = {}
    # Add all sensor keys plus derived metrics
    all_metric_keys = sensor_keys + ["Temp_Avg", "Temp_Spread"]
    
    # Calculate stability severities for decision hierarchy (if in PRODUCTION)
    stability_severity_dict = {}
    if is_in_production and len(rows) >= 2:
        # Calculate stability for each metric (simplified - use last 10 minutes if available)
        from datetime import datetime, timedelta
        now = datetime.utcnow()
        ten_min_ago = now - timedelta(minutes=10)
        recent_rows = [r for r in rows if r.get("TrendDate") and (isinstance(r.get("TrendDate"), datetime) and r.get("TrendDate") >= ten_min_ago or True)]  # Fallback to all if no timestamps
        
        for key in all_metric_keys:
            recent_values = [as_float(r.get(key)) for r in recent_rows[-20:] if as_float(r.get(key)) is not None]  # Last 20 points
            if len(recent_values) >= 3:
                current_std = statistics.stdev(recent_values) if len(recent_values) > 1 else 0.0
                baseline_std = base.get("std", 0.0) if (base := baseline.get(key, {})) else 0.0
                if baseline_std > 0:
                    ratio = current_std / baseline_std
                    if ratio <= 1.2:
                        stability_severity_dict[key] = 0  # GREEN
                    elif ratio <= 1.6:
                        stability_severity_dict[key] = 1  # ORANGE
                    else:
                        stability_severity_dict[key] = 2  # RED
    
    # Special handling for Temp_Spread: Fixed thresholds, no baseline
    spread_status = None
    
    for key in all_metric_keys:
        current_value = as_float(current_row.get(key))
        
        # SPECIAL HANDLING: Temp_Spread uses fixed thresholds, NOT baseline logic
        if key == "Temp_Spread":
            if current_value is not None:
                # Fixed logic: spread <= 5°C → green, 5 < spread <= 8°C → orange, spread > 8°C → red
                if current_value <= 5.0:
                    rule_based_severity = 0  # GREEN
                    spread_status = "green"
                elif current_value <= 8.0:
                    rule_based_severity = 1  # ORANGE
                    spread_status = "orange"
                else:
                    rule_based_severity = 2  # RED
                    spread_status = "red"
            else:
                rule_based_severity = -1  # UNKNOWN
                spread_status = "unknown"
            
            # Temp_Spread does NOT use stability or ML in decision hierarchy (only fixed thresholds)
            final_severity = rule_based_severity if is_in_production else rule_based_severity
            ml_warning = False
            
            metrics_response[key] = {
                "current_value": current_value,
                "baseline_mean": None,  # No baseline for Temp_Spread
                "green_band": None,  # No baseline band for Temp_Spread
                "deviation": None,  # No deviation calculation for Temp_Spread
                "deviation_percent": None,  # No deviation percent for Temp_Spread
                "severity": final_severity,
                "severity_rule_based": rule_based_severity,
                "ml_warning": False,  # Temp_Spread doesn't use ML warnings
                "baseline": None,  # No baseline structure for Temp_Spread
            }
            continue  # Skip normal processing for Temp_Spread
        
        # Normal processing for all other sensors (Temp_Zone1-4, Temp_Avg, ScrewSpeed, Pressure)
        base = baseline.get(key, {})
        baseline_mean = base.get("mean")
        
        # Use profile baseline if available
        if key in profile_baselines:
            baseline_mean = profile_baselines[key]["mean"]
        
        # Calculate deviation (absolute)
        deviation = None
        if current_value is not None and baseline_mean is not None:
            deviation = current_value - baseline_mean
        
        # Green band (from baseline or scoring band) - calculate BEFORE severity
        green_band = None
        if key in profile_baselines:
            std = profile_baselines[key].get("std", 0)
            if std > 0:
                green_band = {
                    "min": baseline_mean - std,
                    "max": baseline_mean + std,
                }
        elif base.get("std", 0) > 0:
            green_band = {
                "min": base.get("min_normal"),
                "max": base.get("max_normal"),
            }
        
        # STEP 2: Calculate rule-based severity using 3-5% rule
        # Use the new function that implements: inside band → green, 3-5% outside → orange, >5% outside → red
        rule_based_severity, deviation_percent = calculate_severity_with_band(
            current_value, key, baseline_mean, green_band
        )
        
        # Fallback to old calculate_severity if new function returns -1 (no baseline)
        if rule_based_severity == -1:
            rule_based_severity = calculate_severity(current_value, key, baseline_mean)
            # Calculate deviation_percent manually if not calculated
            if deviation_percent is None and current_value is not None and baseline_mean is not None and baseline_mean != 0:
                deviation_percent = abs((current_value - baseline_mean) / baseline_mean) * 100.0
        
        # Get stability severity for this sensor
        stability_sev = stability_severity_dict.get(key, None) if is_in_production else None
        
        # Get ML anomaly score for this sensor
        ml_score = ml_predictions.get(key, None) if is_in_production else None
        
        # Apply Decision Hierarchy (STEP 2, 3, 4) - only in PRODUCTION
        if is_in_production:
            final_severity, ml_warning = apply_decision_hierarchy(
                rule_based_severity=rule_based_severity,
                stability_severity=stability_sev,
                ml_anomaly_score=ml_score,
                ml_threshold=0.7,
            )
            if ml_warning:
                ml_warning_overall = True
        else:
            # Not in PRODUCTION - use rule-based only, no ML warnings
            final_severity = rule_based_severity
            ml_warning = False
        
        severity = final_severity
        
        # Build standardized baseline structure
        standardized_baseline = None
        baseline_stat = profile_baseline_stats_dict.get(key)
        if baseline_stat:
            # Use ProfileBaselineStats if available
            standardized_baseline = build_standardized_baseline(
                baseline_stat=baseline_stat,
                profile=active_profile,
            )
        elif baseline_mean is not None:
            # Fallback: Use rolling baseline data
            standardized_baseline = build_standardized_baseline_from_dict(
                metric_name=key,
                baseline_data=base,
                material_id=active_profile.material_id if active_profile else None,
                confidence=0.8 if base.get("count", 0) >= 30 else 0.6,  # Lower confidence for rolling baseline
            )
        
        # Plain-language explanation for UI contract
        explanation = None
        if standardized_baseline and isinstance(standardized_baseline, dict):
            mat = standardized_baseline.get("baseline_material")
            bmin = standardized_baseline.get("baseline_min")
            bmax = standardized_baseline.get("baseline_max")
        else:
            mat = material_id
            bmin = green_band.get("min") if isinstance(green_band, dict) else None
            bmax = green_band.get("max") if isinstance(green_band, dict) else None

        # Detect whether we effectively have a baseline (from profile or rolling)
        has_baseline_mean = (
            baseline_mean is not None
            or (standardized_baseline and isinstance(standardized_baseline, dict) and standardized_baseline.get("baseline_mean") is not None)
        )
        has_baseline_range = bmin is not None and bmax is not None

        if current_value is None:
            explanation = f"{key}: no current value available"
        elif not has_baseline_mean and not has_baseline_range:
            # Truly no baseline information available
            explanation = f"{key}: baseline not available for material {mat}" if mat else f"{key}: baseline not available"
        else:
            # If stability upgraded severity, say so explicitly (early warning).
            if rule_based_severity == 0 and final_severity >= 1 and stability_state_for_sensor in {"orange", "red"}:
                explanation = (
                    f"{key}: value is within baseline band but process variability increased "
                    f"(stability {stability_state_for_sensor}) for material {mat}"
                )
            elif final_severity == 0:
                if has_baseline_range and mat:
                    explanation = f"{key}: within baseline band [{bmin:.1f}–{bmax:.1f}] for material {mat}"
                elif has_baseline_range:
                    explanation = f"{key}: within baseline band [{bmin:.1f}–{bmax:.1f}]"
                else:
                    explanation = f"{key}: within baseline range for material {mat}" if mat else f"{key}: within baseline range"
            elif final_severity == 1:
                if has_baseline_range and mat:
                    explanation = f"{key}: slightly outside baseline band [{bmin:.1f}–{bmax:.1f}] for material {mat}"
                elif has_baseline_range:
                    explanation = f"{key}: slight deviation from baseline band [{bmin:.1f}–{bmax:.1f}]"
                else:
                    explanation = f"{key}: slight deviation from baseline for material {mat}" if mat else f"{key}: slight deviation from baseline"
            elif final_severity == 2:
                if has_baseline_range and mat:
                    explanation = f"{key}: outside baseline band [{bmin:.1f}–{bmax:.1f}] for material {mat}"
                elif has_baseline_range:
                    explanation = f"{key}: critical deviation from baseline band [{bmin:.1f}–{bmax:.1f}]"
                else:
                    explanation = f"{key}: critical deviation from baseline for material {mat}" if mat else f"{key}: critical deviation from baseline"
            else:
                explanation = f"{key}: evaluation status unknown"

        # Get stability state for this sensor (convert severity to state string)
        stability_state_for_sensor = None
        if key in stability_severity_dict:
            stability_sev = stability_severity_dict[key]
            if stability_sev == 0:
                stability_state_for_sensor = "green"
            elif stability_sev == 1:
                stability_state_for_sensor = "orange"
            elif stability_sev == 2:
                stability_state_for_sensor = "red"
            else:
                stability_state_for_sensor = "unknown"
        
        metrics_response[key] = {
            "current_value": current_value,
            "baseline_mean": baseline_mean,
            "green_band": green_band,
            "deviation": deviation,  # Absolute deviation
            "deviation_percent": round(deviation_percent, 2) if deviation_percent is not None else None,  # Percentage deviation
            "severity": severity,  # Final severity after decision hierarchy
            "severity_rule_based": rule_based_severity,  # Rule-based severity before hierarchy (for debugging)
            "ml_warning": ml_warning if is_in_production else False,  # ML warning flag for this metric
            "baseline": standardized_baseline,  # Add standardized baseline structure
            "stability": stability_state_for_sensor,  # Stability state: "green" | "orange" | "red" | null
            "explanation": explanation,  # Plain-language explanation for UI
        }
    
    # Calculate overall risk and severity (reuse logic)
    # Include all metrics (sensors + derived) in severity calculation
    all_metric_keys_for_severity = sensor_keys + ["Temp_Avg", "Temp_Spread"]
    severity_sensors = {key: metrics_response[key]["severity"] for key in all_metric_keys_for_severity if key in metrics_response and metrics_response[key]["severity"] >= 0}
    overall_severity = max(severity_sensors.values()) if severity_sensors else -1
    
    # Process Status: Worst sensor status = process status (ML warnings do NOT change status)
    if overall_severity == 0:
        overall_risk = "green"
        process_status = "green"
        process_status_text = "Process stable"
    elif overall_severity == 1:
        overall_risk = "orange"
        process_status = "orange"
        process_status_text = "Process drifting from baseline"
    elif overall_severity == 2:
        overall_risk = "red"
        process_status = "red"
        process_status_text = "High risk of instability or scrap"
    else:
        overall_risk = "unknown"
        process_status = "unknown"
        process_status_text = "System status unknown"
    
    # Get explanation text (from highest severity metric) - kept for backward compatibility
    highest_severity = -1
    explanation_text = "System status unknown"
    for metric, severity in severity_sensors.items():
        if severity > highest_severity:
            highest_severity = severity
            metric_data = metrics_response[metric]
            if severity == 2:
                explanation_text = f"{metric} critically deviates from baseline"
            elif severity == 1:
                explanation_text = f"{metric} drifting from baseline"
            elif severity == 0:
                explanation_text = f"{metric} stable"
    
    return {
        "machine_state": machine_state_str,
        "state_confidence": state_confidence,
        "state_since_ts": state_since_ts,
        "metrics": metrics_response,
        "overall_risk": overall_risk,  # Kept for backward compatibility
        "overall_severity": overall_severity,
        "process_status": process_status,  # Process status: "green" | "orange" | "red" | "unknown" (worst sensor status, ML warnings do NOT change this)
        "process_status_text": process_status_text,  # Process status text: "Process stable" | "Process drifting from baseline" | "High risk of instability or scrap"
        "ml_warning": ml_warning_overall if is_in_production else False,  # Overall ML warning flag (informational only, does NOT change process_status)
        "explanation_text": explanation_text,  # Kept for backward compatibility
        "baseline_status": baseline_status,
        "baseline_samples_collected": baseline_samples_collected,  # Number of samples collected during learning
        "baseline_samples_required": baseline_samples_required,  # Required samples (100)
        "baseline_progress_percent": baseline_progress_percent,  # Progress percentage (0-100)
        "profile_status": profile_status,
        "evaluation_enabled": is_in_production,
        "spread_status": spread_status,  # Temp_Spread status: "green" | "orange" | "red" | "unknown"
    }


@router.post("/material/change")
async def log_material_change(
    material_id: str = Query(..., description="New material ID"),
    machine_id: Optional[str] = Query(None, description="Machine ID (optional)"),
    previous_material: Optional[str] = Query(None, description="Previous material ID (optional)"),
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(require_viewer),
) -> Dict[str, Any]:
    """
    Log material change event with timestamp and update machine metadata.
    This endpoint is called when user changes material selection in UI.
    """
    from datetime import datetime
    from sqlalchemy import select as sql_select
    
    try:
        # Update machine metadata with new current_material
        # If machine_id is provided, use it; otherwise find the extruder machine
        machine = None
        if machine_id:
            machine = await session.get(Machine, UUID(machine_id))
        else:
            # Find the extruder machine (default behavior)
            machines = await session.scalars(
                sql_select(Machine).where(Machine.name == "Extruder-SQL").limit(1)
            )
            machine = machines.first()
        
        if machine:
            # Update machine metadata with current_material
            metadata = machine.metadata_json or {}
            old_material = metadata.get("current_material")
            metadata["current_material"] = material_id
            machine.metadata_json = metadata
            session.add(machine)
            await session.commit()
            logger.info(
                f"Updated machine {machine.id} metadata: current_material = {old_material} → {material_id}"
            )
        else:
            logger.warning(f"Machine not found for material change (machine_id={machine_id})")
        
        # Create audit log entry for material change
        audit_data = AuditLogCreate(
            user_id=str(current_user.id) if current_user else None,
            action_type="material_change",
            resource_type="material",
            resource_id=material_id,
            details=f"Material changed to {material_id}" + (f" (from {previous_material})" if previous_material else ""),
            metadata={
                "material_id": material_id,
                "previous_material": previous_material,
                "machine_id": str(machine.id) if machine else machine_id,
                "timestamp": datetime.utcnow().isoformat(),
            },
        )
        
        await audit_service.create_audit_log(session, audit_data)
        
        logger.info(f"Material change logged: {previous_material} → {material_id} (user: {current_user.email if current_user else 'unknown'})")
        
        return {
            "success": True,
            "material_id": material_id,
            "machine_id": str(machine.id) if machine else machine_id,
            "timestamp": datetime.utcnow().isoformat(),
            "message": f"Material change to {material_id} logged and machine metadata updated successfully",
        }
    except Exception as e:
        logger.error(f"Error logging material change: {e}", exc_info=True)
        await session.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to log material change: {str(e)}")


@router.get("/material/changes")
async def get_material_changes(
    machine_id: Optional[str] = Query(None, description="Machine ID (optional filter)"),
    start_date: Optional[datetime] = Query(None, description="Start date for filtering"),
    end_date: Optional[datetime] = Query(None, description="End date for filtering"),
    limit: int = Query(50, ge=1, le=500, description="Maximum number of events to return"),
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(require_viewer),
) -> Dict[str, Any]:
    """
    Get material change events for displaying vertical markers in charts.
    Returns list of material change events with timestamp and material_id.
    """
    try:
        # Get audit logs for material changes
        logs = await audit_service.get_audit_logs(
            session,
            action_type="material_change",
            resource_type="material",
            resource_id=None,  # Get all materials
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            offset=0,
        )
        
        # Format material change events
        material_changes = []
        for log in logs:
            material_id = log.resource_id or (log.metadata_json.get("material_id") if log.metadata_json else None)
            timestamp = log.created_at.isoformat() if log.created_at else None
            
            if material_id and timestamp:
                material_changes.append({
                    "material_id": material_id,
                    "timestamp": timestamp,
                    "previous_material": log.metadata_json.get("previous_material") if log.metadata_json else None,
                })
        
        return {
            "material_changes": material_changes,
            "count": len(material_changes),
        }
    except Exception as e:
        logger.error(f"Error fetching material changes: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch material changes: {str(e)}")


@router.get("/machines/stats")
async def get_machines_stats(
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(require_viewer),
):
    """Get machine statistics"""
    cache_key = "dashboard:machines:stats"
    cached = get_cached(cache_key)
    if cached:
        return cached
    
    # Count by status
    status_counts = {}
    for status in ["online", "offline", "maintenance", "degraded"]:
        count = await session.scalar(
            select(func.count(Machine.id)).where(Machine.status == status)
        )
        status_counts[status] = count or 0
    
    # Count by criticality
    criticality_counts = {}
    for crit in ["low", "medium", "high", "critical"]:
        count = await session.scalar(
            select(func.count(Machine.id)).where(Machine.criticality == crit)
        )
        criticality_counts[crit] = count or 0
    
    result = {
        "by_status": status_counts,
        "by_criticality": criticality_counts,
    }
    
    set_cached(cache_key, result)
    return result


@router.get("/sensors/stats")
async def get_sensors_stats(
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(require_viewer),
):
    """Get sensor statistics"""
    cache_key = "dashboard:sensors:stats"
    cached = get_cached(cache_key)
    if cached:
        return cached
    
    total = await session.scalar(select(func.count(Sensor.id)))
    
    # Count by type (if type is stored)
    # This is a simplified version - adjust based on your sensor type field
    
    result = {
        "total": total or 0,
    }
    
    set_cached(cache_key, result)
    return result


@router.get("/predictions/stats")
async def get_predictions_stats(
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(require_viewer),
    hours: int = Query(24, ge=1, le=168),
):
    """Get prediction statistics for the last N hours"""
    cache_key = f"dashboard:predictions:stats:{hours}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    
    since = datetime.utcnow() - timedelta(hours=hours)
    
    total = await session.scalar(
        select(func.count(Prediction.id)).where(Prediction.created_at >= since)
    )
    
    # Count by status
    status_counts = {}
    for status in ["normal", "warning", "critical"]:
        count = await session.scalar(
            select(func.count(Prediction.id)).where(
                and_(
                    Prediction.timestamp >= since,
                    Prediction.status == status
                )
            )
        )
        status_counts[status] = count or 0
    
    result = {
        "total": total or 0,
        "by_status": status_counts,
        "period_hours": hours,
    }
    
    set_cached(cache_key, result)
    return result



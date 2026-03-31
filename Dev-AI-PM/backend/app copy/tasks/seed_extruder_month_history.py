"""Seed ~1 month of realistic dummy extruder data into sensor_data for charts.

This script:
- Finds the `Extruder-SQL` machine and its key sensors (Pressure_bar, ScrewSpeed_rpm, Temp zones)
- Generates synthetic but plausible values for the last 30 days
- Inserts them into the time-series table via sensor_data_service (with idempotency keys)

Run once from the backend directory:

    python -m app.tasks.seed_extruder_month_history
"""

import asyncio
import math
from datetime import datetime, timedelta, timezone

try:
    from loguru import logger  # type: ignore
except Exception:  # pragma: no cover
    import logging

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("seed_extruder_month_history")
from sqlalchemy import select, func

from app.db.session import AsyncSessionLocal
from app.models.machine import Machine
from app.models.sensor import Sensor
from app.schemas.sensor_data import SensorDataIn
from app.services import sensor_data_service
from app.models.sensor_data import SensorData


PRESSURE_BASE = 370.0  # bar, around your normal range (360–380)
PRESSURE_DAILY_SWING = 8.0
PRESSURE_NOISE = 4.0

RPM_BASE = 10.0  # rpm
RPM_SWING = 3.0

TEMP_BASE = 190.0  # °C
TEMP_SWING = 10.0
TEMP_NOISE = 3.0


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _pressure_profile(t: datetime, idx: int) -> float:
    """Generate a plausible melt pressure around 360–380 bar with some excursions."""
    day_frac = (t.hour * 60 + t.minute) / (24 * 60)
    base = PRESSURE_BASE + PRESSURE_DAILY_SWING * math.sin(2 * math.pi * day_frac)
    noise = PRESSURE_NOISE * math.sin(2 * math.pi * (idx % 97) / 97.0)

    # Occasionally dip into lower tolerance or spike into high-critical region
    if (t.day % 9) == 0 and 8 <= t.hour <= 10:
        # Morning low-tolerance dip 340–360
        return max(335.0, base - 25.0 + noise)
    if (t.day % 11) == 0 and 18 <= t.hour <= 20:
        # Evening high spike > 380
        return min(410.0, base + 30.0 + noise)
    return base + noise


def _rpm_profile(t: datetime, idx: int) -> float:
    """Simple rpm profile roughly in production band."""
    day_frac = (t.hour * 60 + t.minute) / (24 * 60)
    base = RPM_BASE + RPM_SWING * math.sin(2 * math.pi * day_frac)
    if 0 <= t.hour < 5:
        # Nighttime lower speed / quasi idle
        base *= 0.4
    return max(0.0, base)


def _temp_profile(t: datetime, zone: int, idx: int) -> float:
    """Temperature zones around 190 °C with small offsets per zone."""
    day_frac = (t.hour * 60 + t.minute) / (24 * 60)
    offset = (zone - 2.5) * 3.0  # spread zones a bit
    base = TEMP_BASE + offset + TEMP_SWING * math.sin(2 * math.pi * day_frac)
    noise = TEMP_NOISE * math.sin(2 * math.pi * ((idx + zone * 13) % 113) / 113.0)
    return base + noise


async def seed_extruder_month_history() -> None:
    async with AsyncSessionLocal() as session:
        # Find extruder machine
        machines = await session.execute(
            select(Machine).where(Machine.name == "Extruder-SQL").limit(1)
        )
        machine = machines.scalar_one_or_none()
        if not machine:
            logger.error("Extruder-SQL machine not found. Run seed_demo_data first.")
            return

        # Time range boundaries for last 30 days
        end_ts = _now_utc().replace(second=0, microsecond=0)
        start_ts = end_ts - timedelta(days=30)

        # Load sensors and find the MSSQL snapshot sensor used by /dashboard/extruder/history
        sensors_result = await session.execute(
            select(Sensor).where(Sensor.machine_id == machine.id)
        )
        sensors = sensors_result.scalars().all()
        if not sensors:
            logger.error("No sensors found for Extruder-SQL. Run seed_sample_machines first.")
            return

        sensors_by_name = {s.name: s for s in sensors}
        snapshot_sensor = sensors_by_name.get("Extruder SQL Snapshot")
        if not snapshot_sensor:
            logger.error("Snapshot sensor 'Extruder SQL Snapshot' not found for Extruder-SQL.")
            return

        logger.info("Seeding ~1 month of DAILY dummy extruder MSSQL snapshot data for machine {}", machine.id)

        # Time range: last 30 days, one point per day (at noon UTC)
        inserted = 0
        days = 30
        for day_offset in range(days, 0, -1):
            ts = end_ts - timedelta(days=day_offset)
            ts = ts.replace(hour=12, minute=0, second=0, microsecond=0)
            idx = day_offset

            # Build small intra-day samples to derive daily avg/min/max for pressure and rpm
            pressure_samples = []
            rpm_samples = []
            for h in (6, 12, 18):
                t_sample = ts.replace(hour=h)
                pressure_samples.append(_pressure_profile(t_sample, idx + h))
                rpm_samples.append(_rpm_profile(t_sample, idx + h))

            pressure_avg = sum(pressure_samples) / len(pressure_samples)
            pressure_min = min(pressure_samples)
            pressure_max = max(pressure_samples)

            rpm_avg = sum(rpm_samples) / len(rpm_samples)
            rpm_min = min(rpm_samples)
            rpm_max = max(rpm_samples)

            # Temperature stats per zone
            temps_day = []
            for zone in range(1, 5):
                zone_samples = []
                for h in (6, 12, 18):
                    t_sample = ts.replace(hour=h)
                    zone_samples.append(_temp_profile(t_sample, zone, idx + h))
                temps_day.append(
                    {
                        "avg": sum(zone_samples) / len(zone_samples),
                        "min": min(zone_samples),
                        "max": max(zone_samples),
                    }
                )

            # Metadata mimics real MSSQL rows so /dashboard/extruder/history can render
            metadata = {
                "source": "month-seed",
                "screw_rpm": rpm_avg,
                "pressure_bar": pressure_avg,
                "temp_zone1_c": temps_day[0]["avg"],
                "temp_zone2_c": temps_day[1]["avg"],
                "temp_zone3_c": temps_day[2]["avg"],
                "temp_zone4_c": temps_day[3]["avg"],
                "pressure_daily_avg": pressure_avg,
                "pressure_daily_min": pressure_min,
                "pressure_daily_max": pressure_max,
                "rpm_daily_avg": rpm_avg,
                "rpm_daily_min": rpm_min,
                "rpm_daily_max": rpm_max,
                "temp_zones_daily": temps_day,
            }

            # One daily MSSQL snapshot point per day, value holds daily max pressure for chart emphasis
            idempotency_key = f"month_seed_daily_snapshot_{snapshot_sensor.id}_{ts.date().isoformat()}"
            payload = SensorDataIn(
                sensor_id=snapshot_sensor.id,
                machine_id=machine.id,
                timestamp=ts,
                value=float(pressure_max),
                status="normal",
                metadata=metadata,
                idempotency_key=idempotency_key,
            )
            try:
                await sensor_data_service.ingest_sensor_data(session, payload)
                inserted += 1
            except Exception as exc:
                logger.debug(
                    "Skipping duplicate or failed insert for snapshot at {}: {}",
                    ts.isoformat(),
                    exc,
                )

        logger.info(
            "Finished seeding DAILY month history: {} points inserted (or already present).",
            inserted,
        )


async def main() -> None:
    await seed_extruder_month_history()


if __name__ == "__main__":
    asyncio.run(main())


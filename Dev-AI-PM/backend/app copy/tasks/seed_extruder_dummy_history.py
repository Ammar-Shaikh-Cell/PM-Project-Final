"""Seed dummy extruder history data for specific dates so they appear in live charts."""
import asyncio
from datetime import datetime, timezone

from loguru import logger
from sqlalchemy import select

from app.db.session import AsyncSessionLocal
from app.models.machine import Machine
from app.models.sensor import Sensor
from app.schemas.sensor_data import SensorDataIn
from app.services import sensor_data_service


async def seed_dummy_extruder_history() -> None:
    """Insert a few synthetic sensor_data rows for Extruder-SQL on specific dates."""
    async with AsyncSessionLocal() as session:
        # Find the extruder machine
        machines = await session.execute(
            select(Machine).where(Machine.name == "Extruder-SQL").limit(1)
        )
        machine = machines.scalar_one_or_none()
        if not machine:
            logger.error("Extruder-SQL machine not found. Run seed_demo_data first.")
            return

        # Pick one sensor to attach the dummy points to – use Pressure_bar if available, else any sensor
        sensors_result = await session.execute(
            select(Sensor).where(Sensor.machine_id == machine.id)
        )
        sensors = sensors_result.scalars().all()
        if not sensors:
            logger.error("No sensors found for Extruder-SQL. Run seed_sample_machines first.")
            return

        sensor = next((s for s in sensors if s.name == "Pressure_bar"), sensors[0])

        # Target dates (noon UTC) - 07, 08, 09, 10 March 2025
        target_dates = [
            datetime(2025, 3, 7, 12, 0, 0, tzinfo=timezone.utc),
            datetime(2025, 3, 8, 12, 0, 0, tzinfo=timezone.utc),
            datetime(2025, 3, 9, 12, 0, 0, tzinfo=timezone.utc),
            datetime(2025, 3, 10, 12, 0, 0, tzinfo=timezone.utc),
        ]

        for idx, ts in enumerate(target_dates, start=1):
            idempotency_key = f"dummy_extruder_{sensor.id}_{ts.isoformat()}"
            payload = SensorDataIn(
                sensor_id=sensor.id,
                machine_id=machine.id,
                timestamp=ts,
                value=100.0 + idx,  # simple increasing dummy value
                status="normal",
                metadata={
                    "source": "dummy-seed",
                    "note": "Dummy extruder history point for chart dates",
                },
                idempotency_key=idempotency_key,
            )
            try:
                await sensor_data_service.ingest_sensor_data(session, payload)
                logger.info("Inserted dummy point at {}", ts.isoformat())
            except Exception as e:
                logger.warning("Failed to insert dummy point at {}: {}", ts.isoformat(), e)

        logger.info("Dummy extruder history seeding finished.")


async def main() -> None:
    await seed_dummy_extruder_history()


if __name__ == "__main__":
    asyncio.run(main())


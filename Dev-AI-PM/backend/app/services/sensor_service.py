"""
Sensor Service - Using Raw SQL to avoid ORM relationship issues
All operations use direct SQL queries instead of SQLAlchemy ORM
"""
from typing import List, Optional, Dict, Any
from uuid import UUID
import json

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger

from app.schemas.sensor import SensorCreate, SensorUpdate


async def list_sensors(session: AsyncSession, machine_id: Optional[UUID] = None) -> List[Dict[str, Any]]:
    """List sensors using raw SQL"""
    if machine_id:
        result = await session.execute(
            text("""
                SELECT 
                    id, machine_id, name, sensor_type, unit,
                    min_threshold, max_threshold, warning_threshold, critical_threshold,
                    metadata, created_at, updated_at
                FROM sensor
                WHERE machine_id = :machine_id
                ORDER BY created_at DESC
            """),
            {"machine_id": str(machine_id)}
        )
    else:
        result = await session.execute(
            text("""
                SELECT 
                    id, machine_id, name, sensor_type, unit,
                    min_threshold, max_threshold, warning_threshold, critical_threshold,
                    metadata, created_at, updated_at
                FROM sensor
                ORDER BY created_at DESC
            """)
        )
    
    rows = result.fetchall()
    sensors = []
    for row in rows:
        sensors.append({
            "id": row[0],
            "machine_id": row[1],
            "name": row[2],
            "sensor_type": row[3],
            "unit": row[4],
            "min_threshold": float(row[5]) if row[5] is not None else None,
            "max_threshold": float(row[6]) if row[6] is not None else None,
            "warning_threshold": float(row[7]) if row[7] is not None else None,
            "critical_threshold": float(row[8]) if row[8] is not None else None,
            "metadata": row[9] if row[9] else {},
            "created_at": row[10],
            "updated_at": row[11],
        })
    return sensors


async def get_sensor(session: AsyncSession, sensor_id: UUID | str) -> Optional[Dict[str, Any]]:
    """Get sensor by ID using raw SQL"""
    sensor_id_str = str(sensor_id)
    result = await session.execute(
        text("""
            SELECT 
                id, machine_id, name, sensor_type, unit,
                min_threshold, max_threshold, warning_threshold, critical_threshold,
                metadata, created_at, updated_at
            FROM sensor
            WHERE id = CAST(:sensor_id AS UUID)
        """),
        {"sensor_id": sensor_id_str}
    )
    row = result.fetchone()
    if not row:
        return None
    
    return {
        "id": UUID(row[0]) if isinstance(row[0], str) else row[0],
        "machine_id": UUID(row[1]) if isinstance(row[1], str) else row[1],
        "name": row[2],
        "sensor_type": row[3],
        "unit": row[4],
        "min_threshold": float(row[5]) if row[5] is not None else None,
        "max_threshold": float(row[6]) if row[6] is not None else None,
        "warning_threshold": float(row[7]) if row[7] is not None else None,
        "critical_threshold": float(row[8]) if row[8] is not None else None,
        "metadata": row[9] if row[9] else {},
        "created_at": row[10],
        "updated_at": row[11],
    }


async def create_sensor(session: AsyncSession, payload: SensorCreate) -> Dict[str, Any]:
    """Create a new sensor using raw SQL"""
    from uuid import uuid4
    sensor_id = str(uuid4())
    metadata_json = json.dumps(payload.metadata) if payload.metadata else None
    
    await session.execute(
        text("""
            INSERT INTO sensor (
                id, machine_id, name, sensor_type, unit,
                min_threshold, max_threshold, warning_threshold, critical_threshold,
                metadata, created_at, updated_at
            ) VALUES (
                CAST(:id AS UUID), CAST(:machine_id AS UUID), :name, :sensor_type, :unit,
                :min_threshold, :max_threshold, :warning_threshold, :critical_threshold,
                CAST(:metadata AS jsonb), NOW(), NOW()
            )
        """),
        {
            "id": sensor_id,
            "machine_id": str(payload.machine_id),
            "name": payload.name,
            "sensor_type": payload.sensor_type,
            "unit": payload.unit,
            "min_threshold": payload.min_threshold,
            "max_threshold": payload.max_threshold,
            "warning_threshold": payload.warning_threshold,
            "critical_threshold": payload.critical_threshold,
            "metadata": metadata_json if metadata_json else "{}",
        }
    )
    await session.commit()
    
    # Return the created sensor
    return await get_sensor(session, sensor_id)


async def update_sensor(
    session: AsyncSession,
    sensor_id: UUID | str,
    payload: SensorUpdate,
) -> Dict[str, Any]:
    """Update a sensor using raw SQL"""
    sensor_id_str = str(sensor_id)
    update_data = payload.model_dump(exclude_unset=True)
    
    # Build dynamic UPDATE query
    set_clauses = []
    params = {"sensor_id": sensor_id_str}
    
    if "name" in update_data:
        set_clauses.append("name = :name")
        params["name"] = update_data["name"]
    if "sensor_type" in update_data:
        set_clauses.append("sensor_type = :sensor_type")
        params["sensor_type"] = update_data["sensor_type"]
    if "unit" in update_data:
        set_clauses.append("unit = :unit")
        params["unit"] = update_data["unit"]
    if "min_threshold" in update_data:
        set_clauses.append("min_threshold = :min_threshold")
        params["min_threshold"] = update_data["min_threshold"]
    if "max_threshold" in update_data:
        set_clauses.append("max_threshold = :max_threshold")
        params["max_threshold"] = update_data["max_threshold"]
    if "warning_threshold" in update_data:
        set_clauses.append("warning_threshold = :warning_threshold")
        params["warning_threshold"] = update_data["warning_threshold"]
    if "critical_threshold" in update_data:
        set_clauses.append("critical_threshold = :critical_threshold")
        params["critical_threshold"] = update_data["critical_threshold"]
    if "metadata" in update_data:
        set_clauses.append("metadata = CAST(:metadata AS jsonb)")
        params["metadata"] = json.dumps(update_data["metadata"]) if update_data["metadata"] else "{}"
    
    if not set_clauses:
        # No updates, just return the sensor
        return await get_sensor(session, sensor_id)
    
    set_clauses.append("updated_at = NOW()")
    
    query = f"""
        UPDATE sensor
        SET {', '.join(set_clauses)}
        WHERE id = CAST(:sensor_id AS UUID)
    """
    
    await session.execute(text(query), params)
    await session.commit()
    
    return await get_sensor(session, sensor_id)


async def delete_sensor(session: AsyncSession, sensor_id: UUID | str) -> None:
    """Delete a sensor and all related data using raw SQL"""
    sensor_id_str = str(sensor_id)
    
    try:
        # Delete all related data in the correct order (child tables first)
        # Using raw SQL to completely avoid ORM relationship issues
        
        # 1. Delete sensor data
        await session.execute(
            text("DELETE FROM sensor_data WHERE sensor_id = CAST(:sensor_id AS UUID)"),
            {"sensor_id": sensor_id_str}
        )
        
        # 2. Delete predictions
        await session.execute(
            text("DELETE FROM prediction WHERE sensor_id = CAST(:sensor_id AS UUID)"),
            {"sensor_id": sensor_id_str}
        )
        
        # 3. Delete alarms
        await session.execute(
            text("DELETE FROM alarm WHERE sensor_id = CAST(:sensor_id AS UUID)"),
            {"sensor_id": sensor_id_str}
        )
        
        # 4. Finally, delete the sensor itself
        result = await session.execute(
            text("DELETE FROM sensor WHERE id = CAST(:sensor_id AS UUID)"),
            {"sensor_id": sensor_id_str}
        )
        
        await session.commit()
        
        if result.rowcount == 0:
            raise ValueError(f"Sensor {sensor_id_str} not found")
        
        logger.info(f"Successfully deleted sensor {sensor_id_str} and all related data")
        
    except Exception as e:
        await session.rollback()
        logger.error(f"Error deleting sensor {sensor_id_str}: {e}")
        raise

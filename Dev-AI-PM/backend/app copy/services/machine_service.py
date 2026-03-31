"""
Machine Service - Using Raw SQL to avoid ORM relationship issues
All operations use direct SQL queries instead of SQLAlchemy ORM
"""
from typing import List, Optional, Dict, Any
from uuid import UUID
from datetime import datetime
import json

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger

from app.schemas.machine import MachineCreate, MachineUpdate


async def list_machines(session: AsyncSession) -> List[Dict[str, Any]]:
    """List all machines using raw SQL"""
    result = await session.execute(
        text("""
            SELECT 
                id, name, location, description, status, criticality,
                metadata, last_service_date, created_at, updated_at
            FROM machine
            ORDER BY created_at DESC
        """)
    )
    rows = result.fetchall()
    machines = []
    for row in rows:
        machines.append({
            "id": row[0],
            "name": row[1],
            "location": row[2] or "",
            "description": row[3] or "",
            "status": row[4],
            "criticality": row[5],
            "metadata": row[6] if row[6] else {},
            "last_service_date": row[7],
            "created_at": row[8],
            "updated_at": row[9],
        })
    return machines


async def get_machine(session: AsyncSession, machine_id: UUID | str) -> Optional[Dict[str, Any]]:
    """Get machine by ID using raw SQL"""
    machine_id_str = str(machine_id)
    result = await session.execute(
        text("""
            SELECT 
                id, name, location, description, status, criticality,
                metadata, last_service_date, created_at, updated_at
            FROM machine
            WHERE id = CAST(:machine_id AS UUID)
        """),
        {"machine_id": machine_id_str}
    )
    row = result.fetchone()
    if not row:
        return None
    
    return {
        "id": UUID(row[0]) if isinstance(row[0], str) else row[0],
        "name": row[1],
        "location": row[2] or "",
        "description": row[3] or "",
        "status": row[4],
        "criticality": row[5],
        "metadata": row[6] if row[6] else {},
        "last_service_date": row[7],
        "created_at": row[8],
        "updated_at": row[9],
    }


async def create_machine(session: AsyncSession, payload: MachineCreate) -> Dict[str, Any]:
    """Create a new machine using raw SQL"""
    from uuid import uuid4
    machine_id = str(uuid4())
    metadata_json = json.dumps(payload.metadata) if payload.metadata else None
    
    # Build the INSERT statement with proper parameter binding
    # Use jsonb type casting in the SQL
    await session.execute(
        text("""
            INSERT INTO machine (
                id, name, location, description, status, criticality,
                metadata, last_service_date, created_at, updated_at
            ) VALUES (
                CAST(:id AS UUID), :name, :location, :description, :status, :criticality,
                CAST(:metadata AS jsonb), :last_service_date, NOW(), NOW()
            )
        """),
        {
            "id": machine_id,
            "name": payload.name,
            "location": payload.location,
            "description": payload.description,
            "status": payload.status or "online",
            "criticality": payload.criticality or "medium",
            "metadata": metadata_json if metadata_json else "{}",
            "last_service_date": payload.last_service_date,
        }
    )
    await session.commit()
    
    # Return the created machine
    return await get_machine(session, machine_id)


async def update_machine(
    session: AsyncSession,
    machine_id: UUID | str,
    payload: MachineUpdate,
) -> Dict[str, Any]:
    """Update a machine using raw SQL"""
    machine_id_str = str(machine_id)
    update_data = payload.model_dump(exclude_unset=True)
    
    # Build dynamic UPDATE query
    set_clauses = []
    params = {"machine_id": machine_id_str}
    
    if "name" in update_data:
        set_clauses.append("name = :name")
        params["name"] = update_data["name"]
    if "location" in update_data:
        set_clauses.append("location = :location")
        params["location"] = update_data["location"]
    if "description" in update_data:
        set_clauses.append("description = :description")
        params["description"] = update_data["description"]
    if "status" in update_data:
        set_clauses.append("status = :status")
        params["status"] = update_data["status"]
    if "criticality" in update_data:
        set_clauses.append("criticality = :criticality")
        params["criticality"] = update_data["criticality"]
    if "metadata" in update_data:
        set_clauses.append("metadata = CAST(:metadata AS jsonb)")
        params["metadata"] = json.dumps(update_data["metadata"]) if update_data["metadata"] else "{}"
    if "last_service_date" in update_data:
        set_clauses.append("last_service_date = :last_service_date")
        params["last_service_date"] = update_data["last_service_date"]
    
    if not set_clauses:
        # No updates, just return the machine
        return await get_machine(session, machine_id)
    
    set_clauses.append("updated_at = NOW()")
    
    query = f"""
        UPDATE machine
        SET {', '.join(set_clauses)}
        WHERE id = CAST(:machine_id AS UUID)
    """
    
    await session.execute(text(query), params)
    await session.commit()
    
    return await get_machine(session, machine_id)


async def delete_machine(session: AsyncSession, machine_id: UUID | str) -> None:
    """Delete a machine and all related data using raw SQL"""
    machine_id_str = str(machine_id)
    
    try:
        # Delete all related data in the correct order (child tables first)
        # Using raw SQL to completely avoid ORM relationship issues
        
        # 1. Delete machine state related data
        # Use separate parameters for UUID and VARCHAR comparisons to avoid type inference issues
        await session.execute(
            text("DELETE FROM machine_state WHERE machine_uuid = CAST(:machine_id_uuid AS UUID) OR machine_id = CAST(:machine_id_str AS VARCHAR)"),
            {"machine_id_uuid": machine_id_str, "machine_id_str": machine_id_str}
        )
        await session.execute(
            text("DELETE FROM machine_state_thresholds WHERE machine_uuid = CAST(:machine_id_uuid AS UUID) OR machine_id = CAST(:machine_id_str AS VARCHAR)"),
            {"machine_id_uuid": machine_id_str, "machine_id_str": machine_id_str}
        )
        await session.execute(
            text("DELETE FROM machine_state_transition WHERE machine_uuid = CAST(:machine_id_uuid AS UUID) OR machine_id = CAST(:machine_id_str AS VARCHAR)"),
            {"machine_id_uuid": machine_id_str, "machine_id_str": machine_id_str}
        )
        await session.execute(
            text("DELETE FROM machine_state_alert WHERE machine_uuid = CAST(:machine_id_uuid AS UUID) OR machine_id = CAST(:machine_id_str AS VARCHAR)"),
            {"machine_id_uuid": machine_id_str, "machine_id_str": machine_id_str}
        )
        await session.execute(
            text("DELETE FROM machine_process_evaluation WHERE machine_uuid = CAST(:machine_id_uuid AS UUID) OR machine_id = CAST(:machine_id_str AS VARCHAR)"),
            {"machine_id_uuid": machine_id_str, "machine_id_str": machine_id_str}
        )
        
        # 2. Delete sensor data
        await session.execute(
            text("DELETE FROM sensor_data WHERE machine_id = CAST(:machine_id AS UUID)"),
            {"machine_id": machine_id_str}
        )
        
        # 3. Delete predictions
        await session.execute(
            text("DELETE FROM prediction WHERE machine_id = CAST(:machine_id AS UUID)"),
            {"machine_id": machine_id_str}
        )
        
        # 4. Delete alarms
        await session.execute(
            text("DELETE FROM alarm WHERE machine_id = CAST(:machine_id AS UUID)"),
            {"machine_id": machine_id_str}
        )
        
        # 5. Delete tickets
        await session.execute(
            text("DELETE FROM ticket WHERE machine_id = CAST(:machine_id AS UUID)"),
            {"machine_id": machine_id_str}
        )
        
        # 6. Delete sensors (must be after sensor_data)
        await session.execute(
            text("DELETE FROM sensor WHERE machine_id = CAST(:machine_id AS UUID)"),
            {"machine_id": machine_id_str}
        )
        
        # 7. Finally, delete the machine itself
        result = await session.execute(
            text("DELETE FROM machine WHERE id = CAST(:machine_id AS UUID)"),
            {"machine_id": machine_id_str}
        )
        
        await session.commit()
        
        if result.rowcount == 0:
            raise ValueError(f"Machine {machine_id_str} not found")
        
        logger.info(f"Successfully deleted machine {machine_id_str} and all related data")
        
    except Exception as e:
        await session.rollback()
        logger.error(f"Error deleting machine {machine_id_str}: {e}")
        raise

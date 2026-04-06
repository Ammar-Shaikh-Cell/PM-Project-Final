from __future__ import annotations

from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_current_user, get_session, require_engineer
from app.models.material_profile import MaterialProfile
from app.models.user import User

router = APIRouter(prefix="/materials", tags=["materials"])


class MaterialProfilePayload(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)

    temp_min_z1: Optional[float] = None
    temp_max_z1: Optional[float] = None
    temp_min_z2: Optional[float] = None
    temp_max_z2: Optional[float] = None
    temp_min_z3: Optional[float] = None
    temp_max_z3: Optional[float] = None
    temp_min_z4: Optional[float] = None
    temp_max_z4: Optional[float] = None
    temp_min_z5: Optional[float] = None
    temp_max_z5: Optional[float] = None

    pressure_min: Optional[float] = None
    pressure_max: Optional[float] = None
    speed_min: Optional[float] = None
    speed_max: Optional[float] = None


def _normalize_payload(payload: MaterialProfilePayload) -> Dict[str, Any]:
    data = payload.model_dump()
    data["name"] = data["name"].strip()
    if not data["name"]:
        raise HTTPException(status_code=400, detail="Material name is required")
    return data


def _material_to_dict(material: MaterialProfile) -> Dict[str, Any]:
    return {
        "id": str(material.id),
        "name": material.name,
        "temp_min_z1": material.temp_min_z1,
        "temp_max_z1": material.temp_max_z1,
        "temp_min_z2": material.temp_min_z2,
        "temp_max_z2": material.temp_max_z2,
        "temp_min_z3": material.temp_min_z3,
        "temp_max_z3": material.temp_max_z3,
        "temp_min_z4": material.temp_min_z4,
        "temp_max_z4": material.temp_max_z4,
        "temp_min_z5": material.temp_min_z5,
        "temp_max_z5": material.temp_max_z5,
        "pressure_min": material.pressure_min,
        "pressure_max": material.pressure_max,
        "speed_min": material.speed_min,
        "speed_max": material.speed_max,
        "created_at": material.created_at.isoformat() if material.created_at else None,
        "updated_at": material.updated_at.isoformat() if material.updated_at else None,
    }


@router.get("")
async def list_material_profiles(
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> List[Dict[str, Any]]:
    result = await session.execute(
        select(MaterialProfile).order_by(MaterialProfile.created_at.desc())
    )
    materials = result.scalars().all()
    return [_material_to_dict(material) for material in materials]


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_material_profile(
    payload: MaterialProfilePayload,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(require_engineer),
) -> Dict[str, Any]:
    material = MaterialProfile(**_normalize_payload(payload))
    session.add(material)
    await session.commit()
    await session.refresh(material)
    return _material_to_dict(material)


@router.put("/{material_id}")
async def update_material_profile(
    material_id: UUID,
    payload: MaterialProfilePayload,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(require_engineer),
) -> Dict[str, Any]:
    material = await session.get(MaterialProfile, material_id)
    if material is None:
        raise HTTPException(status_code=404, detail="Material profile not found")

    for field_name, value in _normalize_payload(payload).items():
        setattr(material, field_name, value)

    session.add(material)
    await session.commit()
    await session.refresh(material)
    return _material_to_dict(material)

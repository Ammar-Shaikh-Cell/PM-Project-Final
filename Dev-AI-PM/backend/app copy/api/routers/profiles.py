from typing import List, Optional, Dict, Any
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, Body
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_

from app.api.dependencies import get_session, get_current_user, require_engineer, require_admin
from app.models.user import User
from app.models.profile import Profile
from app.models.machine import Machine
from app.services.baseline_learning_service import baseline_learning_service
from loguru import logger
from pydantic import BaseModel

router = APIRouter(prefix="/profiles", tags=["profiles"])


class ProfileCreate(BaseModel):
    """Schema for creating a new profile"""
    machine_id: Optional[UUID] = None  # If None, creates material default profile
    material_id: str  # Required
    version: Optional[str] = "1.0"  # Optional version string


@router.post("", status_code=201)
async def create_profile(
    payload: ProfileCreate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    Create a new profile for a machine/material combination.
    
    - If machine_id is provided, creates a machine-specific profile
    - If machine_id is None, creates a material default profile (usable by all machines)
    - Automatically starts baseline learning mode
    """
    try:
        # Validate machine_id if provided
        if payload.machine_id:
            machine = await session.get(Machine, payload.machine_id)
            if not machine:
                raise HTTPException(status_code=404, detail=f"Machine {payload.machine_id} not found")
        
        # Check if active profile already exists for this combination
        query = select(Profile).where(
            and_(
                Profile.machine_id == (payload.machine_id if payload.machine_id else None),
                Profile.material_id == payload.material_id,
                Profile.is_active == True,
            )
        )
        result = await session.execute(query)
        existing_profile = result.scalar_one_or_none()
        
        if existing_profile:
            raise HTTPException(
                status_code=400,
                detail=f"Active profile already exists for {'machine ' + str(payload.machine_id) + ' and ' if payload.machine_id else ''}material {payload.material_id}"
            )
        
        # Create new profile
        new_profile = Profile(
            id=uuid4(),
            machine_id=payload.machine_id,
            material_id=payload.material_id,
            version=payload.version or "1.0",
            is_active=True,
            baseline_learning=True,  # Start learning mode automatically
            baseline_ready=False,
        )
        
        session.add(new_profile)
        await session.commit()
        await session.refresh(new_profile)
        
        # Explicitly start baseline learning to ensure proper initialization
        await baseline_learning_service.start_baseline_learning(session, new_profile.id)
        await session.commit()
        
        logger.info(
            f"Created profile {new_profile.id} for "
            f"{'machine ' + str(payload.machine_id) + ' and ' if payload.machine_id else ''}"
            f"material {payload.material_id} by user {current_user.email}"
        )
        
        return {
            "id": str(new_profile.id),
            "machine_id": str(new_profile.machine_id) if new_profile.machine_id else None,
            "material_id": new_profile.material_id,
            "is_active": new_profile.is_active,
            "version": new_profile.version,
            "baseline_learning": new_profile.baseline_learning,
            "baseline_ready": new_profile.baseline_ready,
            "created_at": new_profile.created_at.isoformat() if new_profile.created_at else None,
            "message": "Profile created successfully. Baseline learning has been started automatically.",
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating profile: {e}")
        await session.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to create profile: {str(e)}")


@router.get("")
async def list_profiles(
    machine_id: Optional[UUID] = Query(None, description="Filter by machine ID"),
    material_id: Optional[str] = Query(None, description="Filter by material ID"),
    is_active: Optional[bool] = Query(None, description="Filter by active status"),
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> List[Dict[str, Any]]:
    """List profiles with optional filters"""
    query = select(Profile)
    conditions = []
    
    if machine_id:
        conditions.append(Profile.machine_id == machine_id)
    if material_id:
        conditions.append(Profile.material_id == material_id)
    if is_active is not None:
        conditions.append(Profile.is_active == is_active)
    
    if conditions:
        query = query.where(and_(*conditions))
    
    result = await session.execute(query)
    profiles = result.scalars().all()
    
    return [
        {
            "id": str(p.id),
            "machine_id": str(p.machine_id) if p.machine_id else None,
            "material_id": p.material_id,
            "is_active": p.is_active,
            "version": p.version,
            "baseline_learning": p.baseline_learning,
            "baseline_ready": p.baseline_ready,
            "created_at": p.created_at.isoformat() if p.created_at else None,
        }
        for p in profiles
    ]


@router.get("/{profile_id}")
async def get_profile(
    profile_id: UUID,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> Dict[str, Any]:
    """Get a specific profile by ID"""
    result = await session.execute(select(Profile).where(Profile.id == profile_id))
    profile = result.scalar_one_or_none()
    
    if not profile:
        raise HTTPException(status_code=404, detail="Profile not found")
    
    return {
        "id": str(profile.id),
        "machine_id": str(profile.machine_id) if profile.machine_id else None,
        "material_id": profile.material_id,
        "is_active": profile.is_active,
        "version": profile.version,
        "baseline_learning": profile.baseline_learning,
        "baseline_ready": profile.baseline_ready,
        "created_at": profile.created_at.isoformat() if profile.created_at else None,
    }


@router.post("/{profile_id}/rollback")
async def rollback_profile(
    profile_id: UUID,
    target_version: Optional[str] = Query(None, description="Version to rollback to. If not provided, rolls back to previous version."),
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(require_engineer),
) -> Dict[str, Any]:
    """
    Rollback a profile to a previous version.
    
    This creates a new profile entry with the previous version's configuration.
    The old profile is deactivated (is_active = false).
    """
    # Get current profile
    result = await session.execute(select(Profile).where(Profile.id == profile_id))
    current_profile = result.scalar_one_or_none()
    
    if not current_profile:
        raise HTTPException(status_code=404, detail="Profile not found")
    
    # Find previous version
    query = select(Profile).where(
        and_(
            Profile.machine_id == current_profile.machine_id,
            Profile.material_id == current_profile.material_id,
            Profile.id != profile_id,
        )
    ).order_by(Profile.created_at.desc())
    
    result = await session.execute(query)
    previous_profiles = result.scalars().all()
    
    if not previous_profiles:
        raise HTTPException(status_code=400, detail="No previous version found to rollback to")
    
    # Use target_version if specified, otherwise use most recent previous version
    target_profile = None
    if target_version:
        for p in previous_profiles:
            if p.version == target_version:
                target_profile = p
                break
        if not target_profile:
            raise HTTPException(status_code=404, detail=f"Version {target_version} not found")
    else:
        target_profile = previous_profiles[0]
    
    # Deactivate current profile
    current_profile.is_active = False
    session.add(current_profile)
    
    # Activate target profile
    target_profile.is_active = True
    session.add(target_profile)
    
    await session.commit()
    
    logger.info(f"Profile {profile_id} rolled back to version {target_profile.version} by user {current_user.email}")
    
    return {
        "message": f"Profile rolled back to version {target_profile.version}",
        "previous_profile_id": str(profile_id),
        "active_profile_id": str(target_profile.id),
        "version": target_profile.version,
    }


@router.get("/{profile_id}/versions")
async def get_profile_versions(
    profile_id: UUID,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> List[Dict[str, Any]]:
    """Get all versions of a profile (same machine_id and material_id)"""
    result = await session.execute(select(Profile).where(Profile.id == profile_id))
    profile = result.scalar_one_or_none()
    
    if not profile:
        raise HTTPException(status_code=404, detail="Profile not found")
    
    # Find all profiles with same machine_id and material_id
    query = select(Profile).where(
        and_(
            Profile.machine_id == profile.machine_id,
            Profile.material_id == profile.material_id,
        )
    ).order_by(Profile.created_at.desc())
    
    result = await session.execute(query)
    versions = result.scalars().all()
    
    return [
        {
            "id": str(v.id),
            "version": v.version,
            "is_active": v.is_active,
            "baseline_ready": v.baseline_ready,
            "baseline_learning": v.baseline_learning,
            "created_at": v.created_at.isoformat() if v.created_at else None,
        }
        for v in versions
    ]

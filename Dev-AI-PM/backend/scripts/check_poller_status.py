#!/usr/bin/env python3
"""
Quick diagnostic script to check MSSQL poller status.
Run this inside the backend container to diagnose baseline learning issues.
"""
import asyncio
import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services.mssql_extruder_poller import mssql_extruder_poller
from app.db.session import AsyncSessionLocal
from app.models.machine import Machine
from app.services.baseline_learning_service import baseline_learning_service
from sqlalchemy import select

async def check_status():
    print("=" * 60)
    print("MSSQL POLLER DIAGNOSTICS")
    print("=" * 60)
    
    # Check environment
    print("\n1. ENVIRONMENT VARIABLES:")
    print(f"   MSSQL_ENABLED: {os.getenv('MSSQL_ENABLED', 'not set')}")
    print(f"   MSSQL_HOST: {os.getenv('MSSQL_HOST', 'not set')}")
    print(f"   MSSQL_USER: {os.getenv('MSSQL_USER', 'not set')}")
    print(f"   MSSQL_PASSWORD: {'***' if os.getenv('MSSQL_PASSWORD') else 'not set'}")
    print(f"   MSSQL_DATABASE: {os.getenv('MSSQL_DATABASE', 'not set')}")
    print(f"   MSSQL_TABLE: {os.getenv('MSSQL_TABLE', 'not set')}")
    
    # Check poller instance
    print("\n2. POLLER INSTANCE:")
    print(f"   enabled: {mssql_extruder_poller.enabled}")
    print(f"   _effective_enabled: {mssql_extruder_poller._effective_enabled}")
    print(f"   _task exists: {mssql_extruder_poller._task is not None}")
    if mssql_extruder_poller._task:
        print(f"   _task done: {mssql_extruder_poller._task.done()}")
        if mssql_extruder_poller._task.done():
            try:
                exc = mssql_extruder_poller._task.exception()
                if exc:
                    print(f"   _task exception: {exc}")
            except Exception as e:
                print(f"   _task exception check failed: {e}")
    print(f"   host: {mssql_extruder_poller.host or 'NOT SET'}")
    print(f"   username: {mssql_extruder_poller.username or 'NOT SET'}")
    print(f"   password: {'SET' if mssql_extruder_poller.password else 'NOT SET'}")
    print(f"   window_size: {len(mssql_extruder_poller._window)}")
    print(f"   machine_id: {mssql_extruder_poller._machine_id}")
    print(f"   sensor_id: {mssql_extruder_poller._sensor_id}")
    
    # Check database
    print("\n3. DATABASE CHECK:")
    async with AsyncSessionLocal() as session:
        # Check machine
        machine_result = await session.execute(
            select(Machine).where(Machine.name == "Extruder-SQL").limit(1)
        )
        machine = machine_result.scalar_one_or_none()
        
        if machine:
            print(f"   Machine found: {machine.id}")
            print(f"   Machine metadata: {machine.metadata_json}")
            material_id = (machine.metadata_json or {}).get("current_material", "Material 1")
            print(f"   Current material: {material_id}")
            
            # Check profile
            profile = await baseline_learning_service.get_active_profile(
                session, machine.id, material_id
            )
            
            if profile:
                print(f"   Profile found: {profile.id}")
                print(f"   baseline_learning: {profile.baseline_learning}")
                print(f"   baseline_ready: {profile.baseline_ready}")
                
                # Count samples
                from app.models.profile import ProfileBaselineSample
                from sqlalchemy import func, select as sql_select
                sample_count_result = await session.execute(
                    sql_select(func.count(ProfileBaselineSample.id))
                    .where(ProfileBaselineSample.profile_id == profile.id)
                )
                sample_count = sample_count_result.scalar() or 0
                print(f"   Baseline samples in DB: {sample_count}")
            else:
                print(f"   ❌ Profile NOT found for machine_id={machine.id}, material_id={material_id}")
        else:
            print("   ❌ Machine 'Extruder-SQL' NOT found")
    
    # Summary
    print("\n4. SUMMARY:")
    issues = []
    if not mssql_extruder_poller.enabled:
        issues.append("❌ Poller disabled via MSSQL_ENABLED")
    if not mssql_extruder_poller._task or mssql_extruder_poller._task.done():
        issues.append("❌ Poller task not running")
    if not mssql_extruder_poller._effective_enabled:
        issues.append("❌ Poller disabled in database (connections.mssql.enabled=false)")
    if not mssql_extruder_poller.host or not mssql_extruder_poller.username:
        issues.append("❌ Missing connection settings")
    if not machine:
        issues.append("❌ Machine not found")
    if machine and not profile:
        issues.append("❌ Profile not found")
    if profile and not profile.baseline_learning:
        issues.append("❌ Profile not in learning mode")
    
    if issues:
        print("   ISSUES FOUND:")
        for issue in issues:
            print(f"   {issue}")
    else:
        print("   ✅ All checks passed - poller should be working")
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    asyncio.run(check_status())

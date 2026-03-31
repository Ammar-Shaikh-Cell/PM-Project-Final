"""
Baseline Learning Service

Manages the baseline learning lifecycle for profiles:
- Start Learning: Set baseline_learning = true, reset sample_count = 0
- Collect Samples: Only when state == PRODUCTION
- Finalize Baseline: Compute mean/std/percentiles, set baseline_ready = true
- Reset Baseline: Archive old baseline, set baseline_ready = false
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any, List
from uuid import UUID
import statistics

from sqlalchemy import select, update, delete, and_
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger

from app.models.profile import Profile, ProfileBaselineStats, ProfileBaselineSample
from app.models.machine_state import MachineStateEnum

# Use loguru logger for consistency
logger = logger


class BaselineLearningService:
    """Service for managing baseline learning lifecycle"""
    
    # Minimum samples required before finalizing baseline
    MIN_SAMPLES_FOR_BASELINE = 100
    
    # Metrics to collect baselines for
    BASELINE_METRICS = [
        "ScrewSpeed_rpm",
        "Pressure_bar",
        "Temp_Zone1_C",
        "Temp_Zone2_C",
        "Temp_Zone3_C",
        "Temp_Zone4_C",
        "Temp_Avg",
        "Temp_Spread",
    ]
    
    async def start_baseline_learning(
        self,
        session: AsyncSession,
        profile_id: UUID,
    ) -> bool:
        """
        Start baseline learning for a profile.
        
        Sets baseline_learning = true, baseline_ready = false, and clears existing samples.
        """
        try:
            # Get profile
            result = await session.execute(
                select(Profile).where(Profile.id == profile_id)
            )
            profile = result.scalar_one_or_none()
            
            if not profile:
                logger.error(f"Profile {profile_id} not found")
                return False
            
            # Check if already learning
            if profile.baseline_learning:
                logger.warning(f"Profile {profile_id} is already in learning mode")
                return False
            
            # Start learning: set flag, clear ready flag
            await session.execute(
                update(Profile)
                .where(Profile.id == profile_id)
                .values(
                    baseline_learning=True,
                    baseline_ready=False,
                )
            )
            
            # Clear existing baseline stats and samples (archive by deleting)
            await session.execute(
                delete(ProfileBaselineStats)
                .where(ProfileBaselineStats.profile_id == profile_id)
            )
            await session.execute(
                delete(ProfileBaselineSample)
                .where(ProfileBaselineSample.profile_id == profile_id)
            )
            
            await session.commit()
            logger.info(f"Started baseline learning for profile {profile_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error starting baseline learning for profile {profile_id}: {e}")
            await session.rollback()
            return False
    
    async def collect_sample(
        self,
        session: AsyncSession,
        profile_id: UUID,
        metric_name: str,
        value: float,
        machine_state: str,
        timestamp: Optional[datetime] = None,
    ) -> bool:
        """
        Collect a sample for baseline learning.
        
        Only collects samples when:
        - baseline_learning = true
        - machine_state == PRODUCTION
        
        Returns True if sample was collected, False otherwise.
        """
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        try:
            # Get profile and check learning flag
            result = await session.execute(
                select(Profile).where(Profile.id == profile_id)
            )
            profile = result.scalar_one_or_none()
            
            if not profile:
                logger.error(f"Profile {profile_id} not found")
                return False
            
            # Only collect in learning mode
            if not profile.baseline_learning:
                logger.warning(f"⏸️ Profile {profile_id} not in learning mode, skipping sample")
                return False
            
            # Only collect when in PRODUCTION state
            if machine_state != MachineStateEnum.PRODUCTION.value:
                logger.warning(
                    f"⏸️ Machine not in PRODUCTION state (current: {machine_state}), "
                    f"skipping sample for profile {profile_id}"
                )
                return False
            
            # Validate metric name
            if metric_name not in self.BASELINE_METRICS:
                logger.warning(f"Unknown metric {metric_name} for baseline learning")
                return False
            
            # Store sample in ProfileBaselineSample table
            sample = ProfileBaselineSample(
                profile_id=profile_id,
                metric_name=metric_name,
                value=value,
                timestamp=timestamp,
            )
            session.add(sample)
            
            # Update baseline stats entry (for sample count tracking)
            stats_result = await session.execute(
                select(ProfileBaselineStats)
                .where(
                    and_(
                        ProfileBaselineStats.profile_id == profile_id,
                        ProfileBaselineStats.metric_name == metric_name,
                    )
                )
            )
            stats = stats_result.scalar_one_or_none()
            
            if not stats:
                # Create new baseline stats entry
                stats = ProfileBaselineStats(
                    profile_id=profile_id,
                    metric_name=metric_name,
                    baseline_mean=None,
                    baseline_std=None,
                    p05=None,
                    p95=None,
                    sample_count=0.0,
                    last_updated=timestamp,
                )
                session.add(stats)
            
            # Increment sample count
            stats.sample_count = (stats.sample_count or 0.0) + 1.0
            stats.last_updated = timestamp
            
            await session.commit()
            logger.info(
                f"✅ Collected baseline sample: profile={profile_id}, metric={metric_name}, "
                f"value={value:.2f}, total_samples={int(stats.sample_count)}"
            )
            return True
            
        except Exception as e:
            logger.error(
                f"Error collecting sample for profile {profile_id}, metric {metric_name}: {e}"
            )
            await session.rollback()
            return False
    
    async def collect_samples_batch(
        self,
        session: AsyncSession,
        profile_id: UUID,
        samples: Dict[str, float],  # metric_name -> value
        machine_state: str,
        timestamp: Optional[datetime] = None,
    ) -> int:
        """
        Collect multiple samples at once.
        
        Returns number of samples successfully collected.
        """
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        collected = 0
        for metric_name, value in samples.items():
            if await self.collect_sample(
                session, profile_id, metric_name, value, machine_state, timestamp
            ):
                collected += 1
        
        return collected
    
    async def finalize_baseline(
        self,
        session: AsyncSession,
        profile_id: UUID,
    ) -> bool:
        """
        Finalize baseline by computing mean/std/percentiles from collected samples.
        
        Retrieves samples from ProfileBaselineSample table, computes statistics,
        stores in ProfileBaselineStats, and sets baseline_ready = true, baseline_learning = false.
        """
        try:
            # Get profile
            result = await session.execute(
                select(Profile).where(Profile.id == profile_id)
            )
            profile = result.scalar_one_or_none()
            
            if not profile:
                logger.error(f"Profile {profile_id} not found")
                return False
            
            if not profile.baseline_learning:
                logger.warning(f"Profile {profile_id} is not in learning mode")
                return False
            
            # Retrieve all samples from ProfileBaselineSample
            samples_result = await session.execute(
                select(ProfileBaselineSample)
                .where(ProfileBaselineSample.profile_id == profile_id)
            )
            all_samples = samples_result.scalars().all()
            
            if not all_samples:
                logger.warning(f"No samples found for profile {profile_id}")
                return False
            
            # Group samples by metric_name
            samples_by_metric: Dict[str, List[float]] = {}
            for sample in all_samples:
                if sample.metric_name not in samples_by_metric:
                    samples_by_metric[sample.metric_name] = []
                samples_by_metric[sample.metric_name].append(sample.value)
            
            # Check if we have enough samples for each metric
            min_samples = min(len(values) for values in samples_by_metric.values()) if samples_by_metric else 0
            
            if min_samples < self.MIN_SAMPLES_FOR_BASELINE:
                logger.warning(
                    f"Not enough samples for profile {profile_id}: "
                    f"min={min_samples}, required={self.MIN_SAMPLES_FOR_BASELINE}"
                )
                return False
            
            # Compute statistics for each metric
            for metric_name, values in samples_by_metric.items():
                if len(values) < self.MIN_SAMPLES_FOR_BASELINE:
                    logger.warning(
                        f"Not enough samples for metric {metric_name}: "
                        f"{len(values)} < {self.MIN_SAMPLES_FOR_BASELINE}"
                    )
                    continue
                
                # Compute statistics
                mean_val = statistics.mean(values)
                std_val = statistics.stdev(values) if len(values) > 1 else 0.0
                
                # Compute percentiles
                sorted_values = sorted(values)
                p05_idx = int(len(sorted_values) * 0.05)
                p95_idx = int(len(sorted_values) * 0.95)
                p05_val = sorted_values[p05_idx] if p05_idx < len(sorted_values) else sorted_values[0]
                p95_val = sorted_values[p95_idx] if p95_idx < len(sorted_values) else sorted_values[-1]
                
                # Get or create baseline stats entry
                stats_result = await session.execute(
                    select(ProfileBaselineStats)
                    .where(
                        and_(
                            ProfileBaselineStats.profile_id == profile_id,
                            ProfileBaselineStats.metric_name == metric_name,
                        )
                    )
                )
                stats = stats_result.scalar_one_or_none()
                
                if not stats:
                    stats = ProfileBaselineStats(
                        profile_id=profile_id,
                        metric_name=metric_name,
                        baseline_mean=mean_val,
                        baseline_std=std_val,
                        p05=p05_val,
                        p95=p95_val,
                        sample_count=float(len(values)),
                        last_updated=datetime.utcnow(),
                    )
                    session.add(stats)
                else:
                    # Update existing stats
                    stats.baseline_mean = mean_val
                    stats.baseline_std = std_val
                    stats.p05 = p05_val
                    stats.p95 = p95_val
                    stats.sample_count = float(len(values))
                    stats.last_updated = datetime.utcnow()
            
            # Delete samples (they're now in baseline_stats)
            await session.execute(
                delete(ProfileBaselineSample)
                .where(ProfileBaselineSample.profile_id == profile_id)
            )
            
            # Mark baseline as ready and stop learning
            await session.execute(
                update(Profile)
                .where(Profile.id == profile_id)
                .values(
                    baseline_learning=False,
                    baseline_ready=True,
                )
            )
            
            await session.commit()
            logger.info(f"Finalized baseline for profile {profile_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error finalizing baseline for profile {profile_id}: {e}")
            await session.rollback()
            return False
    
    async def reset_baseline(
        self,
        session: AsyncSession,
        profile_id: UUID,
        archive: bool = True,
    ) -> bool:
        """
        Reset baseline by archiving old baseline and clearing flags.
        
        Sets baseline_ready = false, baseline_learning = false.
        If archive=True, old baseline stats are deleted (archived).
        """
        try:
            # Get profile
            result = await session.execute(
                select(Profile).where(Profile.id == profile_id)
            )
            profile = result.scalar_one_or_none()
            
            if not profile:
                logger.error(f"Profile {profile_id} not found")
                return False
            
            # Archive old baseline if requested
            if archive:
                await session.execute(
                    delete(ProfileBaselineStats)
                    .where(ProfileBaselineStats.profile_id == profile_id)
                )
                await session.execute(
                    delete(ProfileBaselineSample)
                    .where(ProfileBaselineSample.profile_id == profile_id)
                )
                logger.info(f"Archived old baseline and samples for profile {profile_id}")
            
            # Reset flags
            await session.execute(
                update(Profile)
                .where(Profile.id == profile_id)
                .values(
                    baseline_learning=False,
                    baseline_ready=False,
                )
            )
            
            await session.commit()
            logger.info(f"Reset baseline for profile {profile_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error resetting baseline for profile {profile_id}: {e}")
            await session.rollback()
            return False
    
    async def get_active_profile(
        self,
        session: AsyncSession,
        machine_id: UUID,
        material_id: str,
    ) -> Optional[Profile]:
        """
        Get active profile for (machine, material) with fallback logic:
        1. Try Machine + Material profile
        2. Try Material Default profile (machine_id IS NULL)
        3. Return None if no profile found
        """
        # Try Machine + Material profile
        result = await session.execute(
            select(Profile)
            .where(
                and_(
                    Profile.machine_id == machine_id,
                    Profile.material_id == material_id,
                    Profile.is_active == True,
                )
            )
        )
        profile = result.scalar_one_or_none()
        
        if profile:
            return profile
        
        # Fallback to Material Default profile
        result = await session.execute(
            select(Profile)
            .where(
                and_(
                    Profile.machine_id.is_(None),
                    Profile.material_id == material_id,
                    Profile.is_active == True,
                )
            )
        )
        profile = result.scalar_one_or_none()
        
        return profile
    
    async def is_learning_mode(
        self,
        session: AsyncSession,
        profile_id: UUID,
    ) -> bool:
        """Check if profile is in baseline learning mode"""
        result = await session.execute(
            select(Profile.baseline_learning)
            .where(Profile.id == profile_id)
        )
        learning = result.scalar_one_or_none()
        return learning is True


# Global service instance
baseline_learning_service = BaselineLearningService()

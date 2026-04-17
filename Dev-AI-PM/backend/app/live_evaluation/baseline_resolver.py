from dataclasses import dataclass
from typing import List, Optional
from uuid import UUID

from loguru import logger
from sqlalchemy import and_, desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.live_evaluation.models import BaselineRegistry


@dataclass
class BaselineResolution:
    baselines: List[BaselineRegistry]
    baseline_source: str  # profile_match | regime_baseline | last_known_valid | none


class BaselineResolver:
    async def resolve(
        self,
        session: AsyncSession,
        machine_id: UUID,
        regime: str,
        profile_id: Optional[UUID] = None,
    ) -> BaselineResolution:
        if profile_id is not None:
            profile_rows = await self._load_rows(
                session=session,
                machine_id=machine_id,
                regime=regime,
                profile_id=profile_id,
            )
            if profile_rows:
                return BaselineResolution(baselines=profile_rows, baseline_source="profile_match")

        regime_rows = await self._load_rows(
            session=session,
            machine_id=machine_id,
            regime=regime,
            profile_id=None,
        )
        if regime_rows:
            return BaselineResolution(baselines=regime_rows, baseline_source="regime_baseline")

        regime_any_rows = await self._load_latest_cohort_for_regime(
            session=session,
            machine_id=machine_id,
            regime=regime,
        )
        if regime_any_rows:
            return BaselineResolution(baselines=regime_any_rows, baseline_source="regime_baseline")

        last_known_rows = await self._load_last_known_for_machine(
            session=session,
            machine_id=machine_id,
        )
        if last_known_rows:
            return BaselineResolution(baselines=last_known_rows, baseline_source="last_known_valid")

        logger.warning(
            "No baseline found for machine_id={}, regime={}, profile_id={}",
            machine_id,
            regime,
            profile_id,
        )
        return BaselineResolution(baselines=[], baseline_source="none")

    async def _load_rows(
        self,
        session: AsyncSession,
        machine_id: UUID,
        regime: str,
        profile_id: Optional[UUID],
    ) -> List[BaselineRegistry]:
        query = (
            select(BaselineRegistry)
            .where(BaselineRegistry.machine_id == machine_id)
            .where(BaselineRegistry.regime == regime)
        )
        if profile_id is None:
            query = query.where(BaselineRegistry.profile_id.is_(None))
        else:
            query = query.where(BaselineRegistry.profile_id == profile_id)

        query = query.order_by(desc(BaselineRegistry.created_at))
        result = await session.execute(query)
        rows = result.scalars().all()
        if not rows:
            return []

        newest_created_at = rows[0].created_at
        return [row for row in rows if row.created_at == newest_created_at]

    async def _load_latest_cohort_for_regime(
        self,
        session: AsyncSession,
        machine_id: UUID,
        regime: str,
    ) -> List[BaselineRegistry]:
        anchor_result = await session.execute(
            select(BaselineRegistry)
            .where(BaselineRegistry.machine_id == machine_id)
            .where(BaselineRegistry.regime == regime)
            .order_by(desc(BaselineRegistry.created_at))
            .limit(1)
        )
        anchor = anchor_result.scalar_one_or_none()
        if anchor is None:
            return []

        cohort_result = await session.execute(
            select(BaselineRegistry).where(
                and_(
                    BaselineRegistry.machine_id == machine_id,
                    BaselineRegistry.regime == regime,
                    BaselineRegistry.created_at == anchor.created_at,
                )
            )
        )
        return cohort_result.scalars().all()

    async def _load_last_known_for_machine(
        self,
        session: AsyncSession,
        machine_id: UUID,
    ) -> List[BaselineRegistry]:
        anchor_result = await session.execute(
            select(BaselineRegistry)
            .where(BaselineRegistry.machine_id == machine_id)
            .order_by(desc(BaselineRegistry.created_at))
            .limit(1)
        )
        anchor = anchor_result.scalar_one_or_none()
        if anchor is None:
            return []

        cohort_result = await session.execute(
            select(BaselineRegistry).where(
                and_(
                    BaselineRegistry.machine_id == machine_id,
                    BaselineRegistry.created_at == anchor.created_at,
                )
            )
        )
        return cohort_result.scalars().all()


baseline_resolver = BaselineResolver()

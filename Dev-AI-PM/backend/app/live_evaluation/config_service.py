from datetime import datetime, timedelta
from typing import Dict, Optional
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.live_evaluation.models import EvaluationConfig


_CACHE_TTL_SECONDS = 15
_config_cache: Dict[UUID, tuple[datetime, Dict[str, str]]] = {}


def clear_evaluation_config_cache(machine_id: Optional[UUID] = None) -> None:
    if machine_id is None:
        _config_cache.clear()
        return
    _config_cache.pop(machine_id, None)


async def get_machine_evaluation_config(
    session: AsyncSession,
    machine_id: UUID,
    use_cache: bool = True,
) -> Dict[str, str]:
    now = datetime.utcnow()

    if use_cache and machine_id in _config_cache:
        expires_at, cached_values = _config_cache[machine_id]
        if now <= expires_at:
            return dict(cached_values)

    result = await session.execute(
        select(EvaluationConfig).where(EvaluationConfig.machine_id == machine_id)
    )
    rows = result.scalars().all()
    values = {row.config_key: row.config_value for row in rows}

    _config_cache[machine_id] = (now + timedelta(seconds=_CACHE_TTL_SECONDS), values)
    return dict(values)


async def get_machine_evaluation_config_value(
    session: AsyncSession,
    machine_id: UUID,
    config_key: str,
    use_cache: bool = True,
) -> str:
    configs = await get_machine_evaluation_config(session, machine_id, use_cache=use_cache)
    if config_key not in configs:
        raise KeyError(f"Missing evaluation config key '{config_key}' for machine {machine_id}")
    return configs[config_key]

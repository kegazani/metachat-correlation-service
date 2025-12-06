from typing import Optional, List
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from datetime import datetime
import uuid

from src.infrastructure.models import Correlation, UserCorrelationCache
from src.infrastructure.database import Database


class CorrelationRepository:
    def __init__(self, db: Database):
        self.db = db
    
    async def save_correlation(
        self, session: AsyncSession, user_id: str, correlation_type: str,
        mood_data: dict, biometric_data: dict, correlation_score: float,
        discovered_at: datetime
    ) -> Correlation:
        correlation = Correlation(
            id=str(uuid.uuid4()),
            user_id=user_id,
            correlation_type=correlation_type,
            mood_data=mood_data,
            biometric_data=biometric_data,
            correlation_score=correlation_score,
            discovered_at=discovered_at
        )
        session.add(correlation)
        await session.commit()
        await session.refresh(correlation)
        return correlation
    
    async def get_correlations_by_user(
        self, session: AsyncSession, user_id: str, limit: int = 100
    ) -> List[Correlation]:
        query = select(Correlation).where(
            Correlation.user_id == user_id
        ).order_by(Correlation.discovered_at.desc()).limit(limit)
        
        result = await session.execute(query)
        return list(result.scalars().all())
    
    async def get_or_create_user_cache(
        self, session: AsyncSession, user_id: str
    ) -> UserCorrelationCache:
        result = await session.execute(
            select(UserCorrelationCache).where(
                UserCorrelationCache.user_id == user_id
            )
        )
        existing = result.scalar_one_or_none()
        
        if existing:
            return existing
        
        new_cache = UserCorrelationCache(
            id=str(uuid.uuid4()),
            user_id=user_id,
            mood_entries=[],
            biometric_entries=[]
        )
        session.add(new_cache)
        await session.commit()
        await session.refresh(new_cache)
        return new_cache
    
    async def update_user_cache(
        self, session: AsyncSession, cache: UserCorrelationCache,
        mood_entries: Optional[List[dict]] = None,
        biometric_entries: Optional[List[dict]] = None,
        last_mood_update: Optional[datetime] = None,
        last_biometric_update: Optional[datetime] = None
    ) -> UserCorrelationCache:
        if mood_entries is not None:
            cache.mood_entries = mood_entries
        if biometric_entries is not None:
            cache.biometric_entries = biometric_entries
        if last_mood_update is not None:
            cache.last_mood_update = last_mood_update
        if last_biometric_update is not None:
            cache.last_biometric_update = last_biometric_update
        
        await session.commit()
        await session.refresh(cache)
        return cache


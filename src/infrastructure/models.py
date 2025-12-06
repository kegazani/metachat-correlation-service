from sqlalchemy import Column, String, Float, Integer, DateTime, JSON, Index
from sqlalchemy.sql import func
from datetime import datetime

from src.infrastructure.database import Base


class Correlation(Base):
    __tablename__ = "correlations"
    
    id = Column(String, primary_key=True)
    user_id = Column(String, nullable=False, index=True)
    correlation_type = Column(String, nullable=False)
    mood_data = Column(JSON, nullable=False)
    biometric_data = Column(JSON, nullable=False)
    correlation_score = Column(Float, nullable=False)
    discovered_at = Column(DateTime(timezone=True), nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    
    __table_args__ = (
        Index("idx_correlation_user_discovered", "user_id", "discovered_at"),
        Index("idx_correlation_type", "correlation_type"),
    )


class UserCorrelationCache(Base):
    __tablename__ = "user_correlation_cache"
    
    id = Column(String, primary_key=True)
    user_id = Column(String, nullable=False, index=True, unique=True)
    mood_entries = Column(JSON, nullable=False)
    biometric_entries = Column(JSON, nullable=False)
    last_mood_update = Column(DateTime(timezone=True), nullable=True)
    last_biometric_update = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


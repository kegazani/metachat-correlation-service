from typing import Dict, Any, Optional
from datetime import datetime, timedelta, timezone
import structlog

from src.infrastructure.repository import CorrelationRepository
from src.infrastructure.database import Database
from src.infrastructure.kafka_client import KafkaProducer
from src.domain.correlation_analyzer import CorrelationAnalyzer
from src.config import Config

logger = structlog.get_logger()


class EventHandler:
    def __init__(self, repository: CorrelationRepository, db: Database, kafka_producer: KafkaProducer, config: Config):
        self.repository = repository
        self.db = db
        self.kafka_producer = kafka_producer
        self.config = config
        self.analyzer = CorrelationAnalyzer(
            min_data_days=config.min_data_days,
            min_entries=config.min_entries,
            min_biometric_coverage=config.min_biometric_coverage
        )
    
    async def handle_message(self, topic: str, data: Dict[str, Any], correlation_id: Optional[str] = None, causation_id: Optional[str] = None):
        try:
            if topic == self.config.mood_analyzed_topic:
                await self._handle_mood_analyzed(data)
            elif topic == self.config.biometric_data_received_topic:
                await self._handle_biometric_data_received(data)
            else:
                logger.warning("Unknown topic", topic=topic)
        except Exception as e:
            logger.error("Error handling message", topic=topic, error=str(e), exc_info=True)
    
    async def _handle_mood_analyzed(self, data: Dict[str, Any]):
        try:
            payload = data.get("payload", data)
            user_id = payload.get("user_id")
            
            if not user_id:
                logger.warning("Missing user_id in mood analyzed event")
                return
            
            async for session in self.db.get_session():
                cache = await self.repository.get_or_create_user_cache(session, user_id)
                
                mood_entry = {
                    "timestamp": data.get("timestamp"),
                    "valence": payload.get("valence"),
                    "arousal": payload.get("arousal"),
                    "emotion_vector": payload.get("emotion_vector"),
                    "dominant_emotion": payload.get("dominant_emotion"),
                    "payload": payload
                }
                
                mood_entries = cache.mood_entries or []
                mood_entries.append(mood_entry)
                
                cutoff_date = datetime.now(timezone.utc) - timedelta(days=self.analyzer.min_data_days)
                mood_entries = [
                    e for e in mood_entries
                    if self._parse_timestamp(e.get("timestamp")) and
                    self._parse_timestamp(e.get("timestamp")) >= cutoff_date
                ]
                
                await self.repository.update_user_cache(
                    session, cache,
                    mood_entries=mood_entries,
                    last_mood_update=datetime.now(timezone.utc)
                )
                
                if cache.biometric_entries and len(cache.biometric_entries) >= self.analyzer.min_entries:
                    await self._check_correlations(session, user_id, cache)
        except Exception as e:
            logger.error("Error handling mood analyzed event", error=str(e), exc_info=True)
    
    async def _handle_biometric_data_received(self, data: Dict[str, Any]):
        try:
            payload = data.get("payload", data)
            user_id = payload.get("user_id")
            
            if not user_id:
                logger.warning("Missing user_id in biometric data event")
                return
            
            async for session in self.db.get_session():
                cache = await self.repository.get_or_create_user_cache(session, user_id)
                
                biometric_entry = {
                    "timestamp": data.get("timestamp") or payload.get("timestamp"),
                    "heart_rate": payload.get("heart_rate"),
                    "sleep_data": payload.get("sleep_data"),
                    "activity": payload.get("activity"),
                    "device_id": payload.get("device_id"),
                    "payload": payload
                }
                
                biometric_entries = cache.biometric_entries or []
                biometric_entries.append(biometric_entry)
                
                cutoff_date = datetime.now(timezone.utc) - timedelta(days=self.analyzer.min_data_days)
                biometric_entries = [
                    e for e in biometric_entries
                    if self._parse_timestamp(e.get("timestamp")) and
                    self._parse_timestamp(e.get("timestamp")) >= cutoff_date
                ]
                
                await self.repository.update_user_cache(
                    session, cache,
                    biometric_entries=biometric_entries,
                    last_biometric_update=datetime.now(timezone.utc)
                )
                
                if cache.mood_entries and len(cache.mood_entries) >= self.analyzer.min_entries:
                    await self._check_correlations(session, user_id, cache)
        except Exception as e:
            logger.error("Error handling biometric data event", error=str(e), exc_info=True)
    
    async def _check_correlations(self, session, user_id: str, cache):
        try:
            correlations = self.analyzer.analyze_correlations(
                cache.mood_entries or [],
                cache.biometric_entries or []
            )
            
            for correlation in correlations:
                correlation_obj = await self.repository.save_correlation(
                    session=session,
                    user_id=user_id,
                    correlation_type=correlation["correlation_type"],
                    mood_data=correlation["mood_data"],
                    biometric_data=correlation["biometric_data"],
                    correlation_score=correlation["correlation_score"],
                    discovered_at=datetime.now(timezone.utc)
                )
                
                self.kafka_producer.publish_correlation_discovered(
                    user_id=user_id,
                    correlation_type=correlation["correlation_type"],
                    mood_data=correlation["mood_data"],
                    biometric_data=correlation["biometric_data"],
                    correlation_score=correlation["correlation_score"],
                    timestamp=datetime.now(timezone.utc)
                )
                
                logger.info(
                    "Correlation discovered",
                    user_id=user_id,
                    correlation_type=correlation["correlation_type"],
                    score=correlation["correlation_score"]
                )
        except Exception as e:
            logger.error("Error checking correlations", error=str(e), exc_info=True)
    
    def _parse_timestamp(self, timestamp_str: Any) -> Optional[datetime]:
        if timestamp_str is None:
            return None
        
        if isinstance(timestamp_str, datetime):
            if timestamp_str.tzinfo is None:
                return timestamp_str.replace(tzinfo=timezone.utc)
            return timestamp_str
        
        if isinstance(timestamp_str, str):
            try:
                if timestamp_str.endswith('Z'):
                    timestamp_str = timestamp_str[:-1] + '+00:00'
                dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                return None
        
        return None


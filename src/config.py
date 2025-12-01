from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", case_sensitive=False)
    
    service_name: str = "correlation-service"
    log_level: str = "INFO"
    http_port: int = 8004
    http_host: str = "0.0.0.0"
    
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/metachat_correlation"
    kafka_brokers: List[str] = ["localhost:9092"]
    mood_analyzed_topic: str = "metachat.mood.analyzed"
    biometric_data_received_topic: str = "metachat.biometric.data.received"
    correlation_discovered_topic: str = "metachat.correlation.discovered"
    
    min_data_days: int = 14
    min_entries: int = 10
    min_biometric_coverage: float = 0.7


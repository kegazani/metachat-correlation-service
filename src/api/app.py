from fastapi import FastAPI
from contextlib import asynccontextmanager
import asyncio
import structlog

from src.config import Config
from src.infrastructure.database import Database
from src.infrastructure.repository import CorrelationRepository
from src.infrastructure.kafka_client import KafkaConsumer, KafkaProducer
from src.application.event_handler import EventHandler
from src.api.state import app_state, consumer_task

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    config = Config()
    
    db = Database(config)
    await db.create_database_if_not_exists()
    await db.create_tables()
    
    repository = CorrelationRepository(db)
    kafka_producer = KafkaProducer(config)
    kafka_producer.start()
    
    event_handler = EventHandler(repository, db, kafka_producer, config)
    kafka_consumer = KafkaConsumer(config, event_handler.handle_message)
    kafka_consumer.start()
    
    app_state["config"] = config
    app_state["db"] = db
    app_state["repository"] = repository
    app_state["kafka_consumer"] = kafka_consumer
    app_state["kafka_producer"] = kafka_producer
    
    import src.api.state as state_module
    state_module.consumer_task = asyncio.create_task(kafka_consumer.consume_loop())
    
    logger.info("Correlation Service started")
    yield
    
    import src.api.state as state_module
    if state_module.consumer_task:
        state_module.consumer_task.cancel()
        try:
            await state_module.consumer_task
        except asyncio.CancelledError:
            pass
    
    kafka_consumer.stop()
    kafka_producer.stop()
    await db.close()
    logger.info("Correlation Service stopped")


app = FastAPI(
    title="Correlation Service",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "correlation-service"}


import structlog
from dotenv import load_dotenv
from src.api.app import app
import uvicorn

load_dotenv()

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer()
    ],
    wrapper_class=structlog.make_filtering_bound_logger(20),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=False,
)

logger = structlog.get_logger()


def main():
    from src.config import Config
    config = Config()
    
    logger.info("Starting Correlation Service", service_name=config.service_name)
    
    uvicorn.run(
        "src.api.app:app",
        host=config.http_host,
        port=config.http_port,
        log_level=config.log_level.lower()
    )


if __name__ == "__main__":
    main()


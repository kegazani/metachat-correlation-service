import structlog
from dotenv import load_dotenv
from fastapi import FastAPI
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

app = FastAPI(title="Correlation Service", version="1.0.0")

@app.get("/health")
async def health():
    return {"status": "healthy"}

def main():
    from src.config import Config
    config = Config()
    uvicorn.run(app, host=config.http_host, port=config.http_port)

if __name__ == "__main__":
    main()


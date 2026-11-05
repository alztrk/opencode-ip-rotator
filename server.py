import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from core.config import CORS_ALLOW_ORIGINS
from core.db import init_db
from routes.chat import router as chat_router
from routes.models import router as models_router
from routes.system import router as system_router
from services.discovery import start_discovery, stop_discovery
from services.proxy_pool import load_proxy_list

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True,
)
log = logging.getLogger("server")


@asynccontextmanager
async def lifespan(application: FastAPI):
    init_db()
    load_proxy_list()
    start_discovery()
    log.info("OpenCode IP Rotator Server started.")
    yield
    stop_discovery()
    log.info("OpenCode IP Rotator Server stopped.")


app = FastAPI(title="OpenCode IP Rotator", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOW_ORIGINS,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(chat_router)
app.include_router(models_router)
app.include_router(system_router)

if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)

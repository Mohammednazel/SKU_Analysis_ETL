# FastAPI main application (middleware, routes, startup)

import os
import sys
import logging
from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text


# Add project root to sys.path so imports work regardless of entrypoint
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# Custom imports
from api.middlewares.timing_middleware import TimingMiddleware
from api.middlewares.error_handler import ErrorHandlerMiddleware
from api.utils_logger import setup_json_logging
from routes.instant import router as instant_router


from dotenv import load_dotenv
load_dotenv()

# -------------------------------------------------
# Environment & Logging Setup
# -------------------------------------------------


# Initialize structured JSON logging
setup_json_logging()
logger = logging.getLogger(__name__)

# -------------------------------------------------
# Application Factory
# -------------------------------------------------
def create_app() -> FastAPI:
    """
    Factory function to create the FastAPI app.
    Handles middleware setup, routing, and startup/shutdown hooks.
    """
    app = FastAPI(
        title="Procurement Instant Analytics API",
        version="1.0.0",
        description="High-performance procurement analytics API (Materialized View powered).",
    )

    # -------------------------------------------------
    # Middleware Setup
    # -------------------------------------------------

    # Compress large responses for performance
    app.add_middleware(GZipMiddleware, minimum_size=500)

    # Custom global error handler (structured, traceable)
    app.add_middleware(ErrorHandlerMiddleware)

    # Timing Middleware for request performance monitoring
    app.add_middleware(TimingMiddleware)

    # Enable CORS (frontend & dashboards)
    origins = os.getenv("CORS_ORIGINS", "*")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[o.strip() for o in origins.split(",")] if origins != "*" else ["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # -------------------------------------------------
    # Routers
    # -------------------------------------------------
    app.include_router(instant_router)

    # -------------------------------------------------
    # Startup Event — DB Connection Verification
    # -------------------------------------------------
    @app.on_event("startup")
    def startup_db_check():
        """Verify that the database is reachable before serving requests."""
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            logger.error("❌ DATABASE_URL not found in environment variables.")
            raise RuntimeError("DATABASE_URL missing in environment.")
        try:
            engine = create_engine(db_url)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("✅ Database connection verified successfully.")
        except Exception as e:
            logger.exception("❌ Database connection failed.")
            raise

    # -------------------------------------------------
    # Shutdown Event — Graceful Cleanup
    # -------------------------------------------------
    @app.on_event("shutdown")
    def shutdown_event():
        logger.info("🛑 Shutting down API gracefully.")

    return app


# -------------------------------------------------
# Uvicorn Entrypoint
# -------------------------------------------------
app = create_app()

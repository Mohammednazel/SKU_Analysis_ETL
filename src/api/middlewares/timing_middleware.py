# Tracks request performance

import time
import logging
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger("api_timing")

class TimingMiddleware(BaseHTTPMiddleware):
    """Logs API endpoint duration for performance monitoring."""

    async def dispatch(self, request, call_next):
        start_time = time.perf_counter()
        response = await call_next(request)
        duration = (time.perf_counter() - start_time) * 1000  # ms
        path = request.url.path
        method = request.method
        status = response.status_code
        logger.info(f"⏱️ {method} {path} → {status} in {duration:.2f} ms")
        response.headers["X-Process-Time-ms"] = str(round(duration, 2))
        return response

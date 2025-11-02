# Custom global error middleware

import traceback
import uuid
from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.status import HTTP_500_INTERNAL_SERVER_ERROR
from sqlalchemy.exc import SQLAlchemyError
import logging

logger = logging.getLogger(__name__)

class ErrorHandlerMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        trace_id = str(uuid.uuid4())[:8]  # short trace for tracking
        try:
            response = await call_next(request)
            return response

        except SQLAlchemyError as e:
            logger.exception(f"💥 Database error [trace_id={trace_id}]: {str(e)}")
            return JSONResponse(
                status_code=500,
                content={
                    "error": "Database Error",
                    "detail": str(e),
                    "trace_id": trace_id,
                },
            )

        except Exception as e:
            logger.exception(f"🔥 Unhandled error [trace_id={trace_id}]: {str(e)}")
            return JSONResponse(
                status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                content={
                    "error": "Internal Server Error",
                    "detail": "Something went wrong while processing your request.",
                    "trace_id": trace_id,
                },
            )

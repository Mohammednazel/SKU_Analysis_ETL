# src/api/limiter.py
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

# Single Limiter instance for the whole app
limiter = Limiter(key_func=get_remote_address)

# Export the error handler and exception for app registration
rate_limit_exceeded_handler = _rate_limit_exceeded_handler
RateLimitExceededException = RateLimitExceeded

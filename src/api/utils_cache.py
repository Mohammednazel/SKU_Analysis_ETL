import hashlib
import json
from typing import Any, Dict

def make_etag(payload: Dict[str, Any]) -> str:
    # stable serialization
    s = json.dumps(payload, sort_keys=True, default=str).encode()
    return hashlib.md5(s).hexdigest()

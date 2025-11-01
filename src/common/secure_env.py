# src/common/secure_env.py
import os
from dotenv import load_dotenv

def load_env() -> None:
    # Load base first, then secrets override
    load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env.base"), override=False)
    load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"), override=False)
    load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env.secrets"), override=True)

def require(vars_: list[str]) -> None:
    missing = [v for v in vars_ if not os.getenv(v)]
    if missing:
        raise RuntimeError(f"Missing required env vars: {missing}")

# OM VIGHNHARTAYE NAMO NAMAH:

import datetime
from typing import Optional
from jose import jwt

from ..core.config import settings


# =====================
# 🔐 JWT CONFIG
# =====================
JWT_SECRET_KEY = settings.JWT_SECRET_KEY
JWT_REFRESH_SECRET_KEY = settings.JWT_REFRESH_SECRET_KEY
ALGORITHM = settings.ALGORITHM
ACCESS_TOKEN_EXPIRE_MINUTES = settings.ACCESS_TOKEN_EXPIRE_MINUTES
REFRESH_TOKEN_EXPIRE_MINUTES = settings.REFRESH_TOKEN_EXPIRE_MINUTES


# =====================
# 🔐 ACCESS TOKEN
# =====================
def create_access_token(
    user_id: int,
    username: str,
    role: str,
    site_id: Optional[int] = None,
    expires_delta: Optional[datetime.timedelta] = None
) -> str:

    expire = datetime.datetime.now(datetime.timezone.utc) + (
        expires_delta or datetime.timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )

    payload = {
        "exp": expire,
        "iat": datetime.datetime.now(datetime.timezone.utc),
        "user_id": user_id,
        "username": username,
        "role": role,
        "site_id": site_id if role == "site" else None
    }

    return jwt.encode(payload, JWT_SECRET_KEY, algorithm=ALGORITHM)


# =====================
# 🔐 REFRESH TOKEN
# =====================
def create_refresh_token(
    user_id: int,
    username: str,
    role: str,
    site_id: Optional[int] = None,
    expires_delta: Optional[datetime.timedelta] = None
) -> str:

    expire = datetime.datetime.now(datetime.timezone.utc) + (
        expires_delta or datetime.timedelta(minutes=REFRESH_TOKEN_EXPIRE_MINUTES)
    )

    payload = {
        "exp": expire,
        "iat": datetime.datetime.now(datetime.timezone.utc),
        "user_id": user_id,
        "username": username,
        "role": role,
        "site_id": site_id if role == "site" else None
    }

    return jwt.encode(payload, JWT_REFRESH_SECRET_KEY, algorithm=ALGORITHM)

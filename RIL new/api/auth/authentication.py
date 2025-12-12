# OM VIGHNHARTAYE NAMO NAMAH :

from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from datetime import datetime
from fastapi.security import OAuth2PasswordRequestForm, OAuth2PasswordBearer

from ...database.session import getdb
from ...utils.tokens import create_access_token, create_refresh_token
from ...utils.utils import verify_password
from ...modals.masters import User, UserRole, Role, SiteUser

from jose import jwt, JWTError
from ...core.config import settings


JWT_SECRET_KEY = settings.JWT_SECRET_KEY
ALGORITHM = settings.ALGORITHM


router = APIRouter()

Oauth2Bearer = OAuth2PasswordBearer(tokenUrl="/api/user/login")


# ============================================================
# 🔐 LOGIN ROUTE — WITH site_id SUPPORT FOR SITE ROLE
# ============================================================
@router.post("/api/user/login", summary="Create access and refresh tokens", tags=["Admin - Auth"])
async def user_login(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
    db: Session = Depends(getdb)
):

    # Validate username
    user = db.query(User).filter(User.username == form_data.username).first()
    if user is None:
        raise HTTPException(status_code=401, detail="User not found.")

    # Validate password
    if not verify_password(form_data.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Incorrect password")

    # Fetch role
    user_role = db.query(UserRole).filter(UserRole.user_id == user.id).first()
    role = db.query(Role).filter(Role.id == user_role.role_id).first()
    role_name = role.role_name if role else "user"

    # Default: no site_id
    site_id = None

    # If user is SITE role → fetch site_id
    if role_name == "site":
        site_user = db.query(SiteUser).filter(SiteUser.user_id == user.id).first()

        if site_user:
            if not site_user.is_active:
                raise HTTPException(status_code=403, detail="User is inactive.")
            site_id = site_user.site_id

    # Create JWT tokens including site_id
    access_token = create_access_token(
        user_id=user.id,
        username=user.username,
        role=role_name,
        site_id=site_id
    )
    refresh_token = create_refresh_token(
        user_id=user.id,
        username=user.username,
        role=role_name,
        site_id=site_id
    )

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer"
    }

# ============================================================
# 🔍 CURRENT USER EXTRACTOR — RETURNS site_id ALSO
# ============================================================
async def get_current_user(token: Annotated[str, Depends(Oauth2Bearer)]):

    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, ALGORITHM)

        user_id = payload.get("user_id")
        username = payload.get("username")
        role = payload.get("role")
        site_id = payload.get("site_id")

        if not all([user_id, username, role]):
            raise HTTPException(status_code=401, detail="Invalid credentials")

        return {
            "user_id": user_id,
            "username": username,
            "role": role,
            "site_id": site_id
        }

    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")


user_dependency = Annotated[dict, Depends(get_current_user)]

#OM VIGHNHARTAYE NAMO NAMAH :

from typing import Annotated
from fastapi import APIRouter, Depends, Request, Body, status, Query, HTTPException
from sqlalchemy.orm import Session , joinedload
from datetime import datetime
from ...utils.utils import *
from ...database.session import getdb
from fastapi.security import OAuth2PasswordRequestForm , OAuth2PasswordBearer
router = APIRouter()


Oauth2Bearer = OAuth2PasswordBearer(tokenUrl='/api/user/login')

@router.post('/api/user/login', summary="Create access and refresh tokens for user",tags=['Admin - Auth'])
async def user_login(form_data : Annotated[OAuth2PasswordRequestForm , Depends()] , db : Session = Depends(getdb)):

    user = db.query(User).filter(User.username == form_data.username).first()

    if user is None:
        raise HTTPException(status_code=401, detail="User not found.")
        
    hashed_pass = user.password_hash
    if not verify_password(form_data.password , hashed_pass):
        raise HTTPException(status_code=401, detail="Incorrect password")
    
    user_role = db.query(UserRole).filter(UserRole.user_id == user.id).first()
    role = db.query(Role).filter(Role.id == user_role.role_id).first()

    site_user = db.query(SiteUser).filter(SiteUser.user_id == user.id).first()
    if site_user and not site_user.is_active:
        raise HTTPException(status_code=403, detail="User is inactive.")

    access_token = create_access_token(user.id, user.username, role.role_name if role else "user")
    refresh_token = create_refresh_token(user.id, user.username, role.role_name if role else "user")
    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer"
        }



async def get_current_user(token : Annotated[str , Depends(Oauth2Bearer)]):
    try:
        payload = jwt.decode(token , JWT_SECRET_KEY , ALGORITHM)
        username = payload.get('username')
        user_id = payload.get('user_id')
        role = payload.get('role')

        if username is None or role is None or user_id is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED , detail='invalid credentials')
        
        return{
            "user_id": user_id,
            "username": username,
            "role": role
        }
    
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='invalid credentials')
    

user_dependency = Annotated[dict , Depends(get_current_user)]
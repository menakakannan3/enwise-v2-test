# OM VIGHNHARTAYE NAMO NAMAH:

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import shutil
from fastapi import UploadFile
from sqlalchemy.orm import Session
from ..modals.masters import *
from ..core.config import settings
from passlib.context import CryptContext
from typing import Optional
from jose import jwt , JWTError

JWT_SECRET_KEY = settings.JWT_SECRET_KEY
JWT_REFRESH_SECRET_KEY = settings.JWT_REFRESH_SECRET_KEY
ALGORITHM = settings.ALGORITHM
ACCESS_TOKEN_EXPIRE_MINUTES = settings.ACCESS_TOKEN_EXPIRE_MINUTES
REFRESH_TOKEN_EXPIRE_MINUTES = settings.REFRESH_TOKEN_EXPIRE_MINUTES

password_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def response_strct(status_code='', detail='', data={}, error=''):
    return {
        "status_code": status_code,
        "detail": detail,
        "data": data,
        "error": error
    }

def get_hashed_password(password: str) -> str:
    return password_context.hash(password)


def create_upload_path(base_dir: str, site_uid: str, doc_type: str) -> str:
    """Create a directory structure for document uploads."""
    upload_path = os.path.join(base_dir, "site_documents", site_uid, doc_type)
    Path(upload_path).mkdir(parents=True, exist_ok=True)
    return upload_path


def save_uploaded_files(files: list[UploadFile], upload_path: str) -> list[str]:
    """Save uploaded documents and return their file paths."""
    saved_paths = []
    for file in files:
        file_ext = file.filename.split(".")[-1]
        if file_ext.lower() not in ["pdf", "doc", "docx", "csv", "xlsx"]:
            raise ValueError("Unsupported file type.")
        
        file_path = os.path.join(upload_path, file.filename)
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        saved_paths.append(file_path)
    return saved_paths


def save_site_documents(db: Session, site_id: int, documents: dict):
    """Save document metadata in the database."""
    for doc_type, files in documents.items():
        for file_path in files:
            document = SiteDocument(
                site_id=site_id,
                document_name=os.path.basename(file_path),
                document_path=file_path,
                document_format=file_path.split(".")[-1],
                document_type=db.query(DocumentType).filter(DocumentType.document_type == doc_type).first().id,
                uploaded_at=datetime.datetime.utcnow(),
                created_at=datetime.datetime.utcnow(),
                created_by=1,  # Change based on authenticated user
                updated_by=1,
            )
            db.add(document)
    db.commit()

def verify_password(password: str, hashed_pass: str) -> bool:
    return password_context.verify(password, hashed_pass)

def create_access_token(user_id: int, username: str, role: str, expires_delta: Optional[timedelta] = None) -> str:
    expires_at = (datetime.datetime.now(timezone.utc) + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)))

    to_encode = {
        "exp": expires_at,
        "iat": datetime.datetime.now(timezone.utc),
        "user_id": user_id,
        "username": username,
        "role": role
    }

    return jwt.encode(to_encode, JWT_SECRET_KEY, algorithm=ALGORITHM)

def create_refresh_token(user_id: int, username: str, role: str, expires_delta: Optional[timedelta] = None) -> str:
    expires_at = (datetime.datetime.now(timezone.utc) + (expires_delta or timedelta(minutes=REFRESH_TOKEN_EXPIRE_MINUTES)))

    to_encode = {
        "exp": expires_at,
        "iat": datetime.datetime.now(timezone.utc),
        "user_id": user_id,
        "username": username,
        "role": role
    }

    return jwt.encode(to_encode, JWT_REFRESH_SECRET_KEY, algorithm=ALGORITHM)


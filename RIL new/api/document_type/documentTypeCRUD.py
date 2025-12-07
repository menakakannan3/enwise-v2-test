#OM VIGHNHARTAYE NAMO NAMAH :

import datetime
from fastapi import APIRouter, Depends, Form, HTTPException, status
from sqlalchemy.orm import Session
from typing import Optional
from ...modals.masters import DocumentType
from ...database.session import getdb
from ..auth.authentication import user_dependency

router = APIRouter()

@router.post("/api/document-type/create", tags=["Document Type"])
async def create_document_type(
    user: user_dependency,
    document_type: str = Form(...),
    mandatory: bool = Form(False),
    db: Session = Depends(getdb),
):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")

    new_type = DocumentType(
        document_type=document_type,
        mandatory=mandatory,
        created_at=datetime.datetime.utcnow(),
        updated_at=datetime.datetime.utcnow(),
        updated_by=1,
    )
    db.add(new_type)
    db.commit()
    db.refresh(new_type)

    return {
        "status_code": status.HTTP_201_CREATED,
        "detail": "Document type created successfully",
        "data": {
            "id": new_type.id,
            "document_type": new_type.document_type,
            "mandatory": new_type.mandatory
        }
    }

@router.get("/api/document-type/list", tags=["Document Type"])
def list_document_types(
    user: user_dependency,
    db: Session = Depends(getdb),
):
    if user is None:
        raise HTTPException(status_code=401, detail="Authentication failed")

    doc_types = db.query(DocumentType).all()
    return {"data": [ 
        {
            "id": dt.id,
            "document_type": dt.document_type,
            "mandatory": dt.mandatory,
            "created_at": dt.created_at,
            "updated_at": dt.updated_at,
        } for dt in doc_types
    ]}

@router.post("/api/document-type/update", tags=["Document Type"])
async def update_document_type(
    user: user_dependency,
    id: int = Form(...),
    document_type: str = Form(...),
    mandatory: bool = Form(...),
    db: Session = Depends(getdb),
):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")

    doc_type = db.query(DocumentType).filter(DocumentType.id == id).first()
    if not doc_type:
        raise HTTPException(status_code=404, detail="Document type not found")

    doc_type.document_type = document_type
    doc_type.mandatory = mandatory
    doc_type.updated_at = datetime.datetime.utcnow()
    doc_type.updated_by = 1

    db.commit()
    db.refresh(doc_type)

    return {
        "detail": "Document type updated successfully",
        "data": {
            "id": doc_type.id,
            "document_type": doc_type.document_type,
            "mandatory": doc_type.mandatory
        }
    }

@router.post("/api/document-type/delete", tags=["Document Type"])
async def delete_document_type(
    user: user_dependency,
    id: int = Form(...),
    db: Session = Depends(getdb),
):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")

    doc_type = db.query(DocumentType).filter(DocumentType.id == id).first()
    if not doc_type:
        raise HTTPException(status_code=404, detail="Document type not found")

    db.delete(doc_type)
    db.commit()
    return {"detail": "Document type deleted successfully"}

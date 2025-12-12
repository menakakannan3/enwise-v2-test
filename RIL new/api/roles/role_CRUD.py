from fastapi import APIRouter, Depends, Form, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime

from ...database.session import getdb
from ...modals.masters import Role  # Import the Role model
from ...utils.utils import response_strct

from ..auth.authentication import user_dependency

router = APIRouter()

@router.post('/api/role/create', summary="Create a new role", tags=['Role'])
async def create_role(
    user : user_dependency,
    role_name: str = Form(... , min_length=3),
    db: Session = Depends(getdb)
):
    
    # if user is None or user['role'] != 'admin':
    #     raise HTTPException(status_code=401, detail="Authentication failed")

    existing_role = db.query(Role).filter(Role.role_name == role_name).first()
    if existing_role:
        raise HTTPException(status_code=400, detail="Role with this name already exists")

    new_role = Role(
        role_name=role_name,
        # created_by=1,  # Hardcoded for now
        created_at=datetime.utcnow()
    )
    
    db.add(new_role)
    db.commit()
    db.refresh(new_role)

    return response_strct(
        status_code=status.HTTP_201_CREATED,
        data={"id": new_role.id, "role_name": new_role.role_name, "created_at": new_role.created_at, "created_by": new_role.created_by},
        detail="Role created successfully"
    )

@router.put('/api/role/update/{role_id}', summary="Update an existing role", tags=['Role'])
async def update_role(
    user: user_dependency,
    role_id: int,
    role_name: Optional[str] = Form(None, min_length=3),
    db: Session = Depends(getdb)
):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    
    role = db.query(Role).filter(Role.id == role_id).first()
    if not role:
        raise HTTPException(status_code=404, detail="Role not found")

    # Check for duplicate role name before updating
    if role_name and role_name != role.role_name:
        existing_role = db.query(Role).filter(Role.role_name == role_name).first()
        if existing_role:
            raise HTTPException(status_code=400, detail="Role with this name already exists")

        role.role_name = role_name
    
    role.updated_by = 1  # Hardcoded for now
    role.updated_at = func.now()
    db.commit()
    db.refresh(role)

    return response_strct(
        status_code=status.HTTP_200_OK,
        data={"id": role.id, "role_name": role.role_name, "updated_at": role.updated_at, "updated_by": role.updated_by},
        detail="Role updated successfully"
    )


@router.get('/api/role/all', summary="Get all roles", tags=['Role'])
async def get_all_roles(
    user : user_dependency,
    db: Session = Depends(getdb)
):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    roles = db.query(Role).all()
    if not roles:
        return response_strct(
            status_code=status.HTTP_200_OK,
            detail="No roles found",
            data=[]
        )
    
    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="Roles fetched successfully",
        data=[
            {"id": role.id, "role_name": role.role_name, "created_at": role.created_at, "created_by": role.created_by, "updated_at": role.updated_at, "updated_by": role.updated_by}
            for role in roles
        ]
    )

@router.get('/api/role/{role_id}', summary="Get Role by ID", tags=['Role'])
async def get_role_by_id(
    user : user_dependency,
    role_id: int,
    db: Session = Depends(getdb)
):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    role = db.query(Role).filter(Role.id == role_id).first()
    if not role:
        raise HTTPException(status_code=404, detail="Role not found")

    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="Role fetched successfully",
        data={"id": role.id, "role_name": role.role_name, "created_at": role.created_at, "created_by": role.created_by, "updated_at": role.updated_at, "updated_by": role.updated_by}
    )


@router.delete('/api/role/delete/{role_id}', summary="Delete a role", tags=['Role'])
async def delete_role(
    user : user_dependency,
    role_id: int,
    db: Session = Depends(getdb)
):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    role = db.query(Role).filter(Role.id == role_id).first()
    if not role:
        raise HTTPException(status_code=404, detail="Role not found")
    
    db.delete(role)
    db.commit()

    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="Role deleted successfully",
        data={}
    )
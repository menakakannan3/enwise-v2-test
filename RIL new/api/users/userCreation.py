from sqlalchemy.orm import Session
from ...modals.masters import *
from ...database.session import getdb
from fastapi import APIRouter, HTTPException, Depends, Form, Body
from ...utils.utils import *
from starlette import status
from sqlalchemy.exc import SQLAlchemyError
from ..auth.authentication import user_dependency

router = APIRouter()


@router.post("/api/user/register", tags=['users'])
def create_user(
    user: user_dependency,
    name: str = Form(..., min_length=3, max_length=20),
    username: str = Form(..., min_length=3, max_length=20),
    password_hash: str = Form(..., min_length=8, max_length=255),
    email: str = Form(..., max_length=255),
    phone: str = Form(..., max_length=10),
    role_name: str = Form(..., min_length=3, max_length=50),
    db: Session = Depends(getdb),
):
    try:
        if db.query(User).filter(User.username == username).first():
            raise HTTPException(status_code=400, detail="Username already exists")
        if db.query(User).filter(User.email == email).first():
            raise HTTPException(status_code=400, detail="Email already exists")
        
        role = db.query(Role).filter(Role.role_name == role_name).first()
        if not role:
            raise HTTPException(status_code=400, detail="Role does not exist")
        
        last_user = db.query(User).order_by(User.id.desc()).first()
        new_id = last_user.id + 1 if last_user else 1
        
        new_user = User(
            id=new_id,
            name=name,
            username=username,
            password_hash=get_hashed_password(password_hash),
            email=email,
            phone=phone,
            created_by=1,
            updated_by=1
        )
        db.add(new_user)
        db.commit()
        db.refresh(new_user)

        user_role = UserRole(user_id=new_user.id, role_id=role.id, updated_by=1)
        db.add(user_role)
        db.commit()

        user_data = {
            "id": new_user.id,
            "name": new_user.name,
            "username": new_user.username,
            "email": new_user.email,
            "phone": new_user.phone,
            "role": role.role_name  # Include role name in response
        }
        
        return response_strct(
            status_code=status.HTTP_201_CREATED,
            detail="User created successfully",
            data=user_data,
            error=""
        )
    except Exception as e:
        return response_strct(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred",
            data={},
            error=''
        )    




@router.get("/api/user/getAll", tags=['users'])
def get_users(user: user_dependency,db: Session = Depends(getdb)):
    users = db.query(User).all()

    # Fetch role details for each user
    users_with_roles = []
    for user in users:
        user_role = db.query(UserRole).filter(UserRole.user_id == user.id).first()
        role_name = None
        if user_role:
            role = db.query(Role).filter(Role.id == user_role.role_id).first()
            role_name = role.role_name if role else None

        users_with_roles.append({
            "id": user.id,
            "name": user.name,
            "username": user.username,
            "email": user.email,
            "phone": user.phone,
            "role": role_name
        })

    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="All users fetched successfully",
        data=users_with_roles,
        error=""
    )


@router.get("/api/user/get/{username}", tags=['users'])
def get_user(user: user_dependency,username: str, db: Session = Depends(getdb)):
    user = db.query(User).filter(User.username == username).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Fetch role details
    user_role = db.query(UserRole).filter(UserRole.user_id == user.id).first()
    role_name = None
    if user_role:
        role = db.query(Role).filter(Role.id == user_role.role_id).first()
        role_name = role.role_name if role else None

    user_data = {
        "id": user.id,
        "name": user.name,
        "username": user.username,
        "email": user.email,
        "phone": user.phone,
        "role": role_name
    }

    return response_strct(
        status_code=status.HTTP_202_ACCEPTED,
        detail=f"User {user.username} fetched successfully",
        data=user_data,
        error=""
    )


@router.put("/api/user/update/{username}", tags=['users'])
def update_user(
    user: user_dependency,
    username: str,
    name: str = Form(None, min_length=3, max_length=20),
    email: str = Form(None, max_length=255),
    phone: str = Form(None, max_length=10),
    password_hash : str = Form(None , min_length=8),
    role_name: str = Form(None, min_length=3, max_length=50),
    db: Session = Depends(getdb),
):
    try:
        existing_user = db.query(User).filter(User.username == username).first()
        if not existing_user:
            return response_strct(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found",
                data={},
                error=""
            )

        
        if email is not None and db.query(User).filter(User.email == email, User.username != username).first():
            raise HTTPException(status_code=400, detail="Email already exists")

        if name is not None:
            existing_user.name = name
        if email is not None:
            existing_user.email = email
        if phone is not None:
            existing_user.phone = phone
        if password_hash is not None:
            existing_user.password_hash = get_hashed_password(password_hash)

        if role_name:
            role = db.query(Role).filter(Role.role_name == role_name).first()
            if not role:
                raise HTTPException(status_code=400, detail="Role does not exist")
            
            user_role = db.query(UserRole).filter(UserRole.user_id == existing_user.id).first()
            if user_role:
                user_role.role_id = role.id
            else:
                new_user_role = UserRole(user_id=existing_user.id, role_id=role.id, updated_by=1)
                db.add(new_user_role)
            updated_role_name = role.role_name

        existing_user.updated_by = 1
        db.commit()
        db.refresh(existing_user)

        user_data = {
            "id": existing_user.id,
            "name": existing_user.name,
            "username": existing_user.username,
            "email": existing_user.email,
            "phone": existing_user.phone,
            "role": updated_role_name  # Include updated role name in response
        }

        return response_strct(
            status_code=status.HTTP_200_OK,
            detail=f"User {username} updated successfully",
            data=user_data,
            error=""
        )

    except SQLAlchemyError as e:
        db.rollback()
        return response_strct(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred",
            data={},
            error=''
        )
    except Exception as e:
        return response_strct(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred",
            data={},
            error=''
        )

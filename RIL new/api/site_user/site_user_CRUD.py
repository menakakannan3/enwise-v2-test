from sqlalchemy.orm import Session
from fastapi import APIRouter, HTTPException, Depends, Path, Body
from starlette import status
from sqlalchemy.exc import SQLAlchemyError
from ...modals.masters import *
from ...database.session import getdb
from ...utils.utils import *
from ..auth.authentication import get_current_user,user_dependency

router = APIRouter()


@router.get("/api/site_user/get_candidates", tags=['site_users'])
def get_candidates(user: user_dependency,db: Session = Depends(getdb)):
    candidates = (
        db.query(User, Role.role_name)
        .join(UserRole, User.id == UserRole.user_id)
        .join(Role, UserRole.role_id == Role.id)
        .filter(Role.role_name != "admin")
        .all()
    )
    
    candidate_list = []

    for user, role_name in candidates:
        # Check if the user has the "site" role and if they are already assigned to a site
        if role_name == "site":
            existing_site_user = db.query(SiteUser).filter(SiteUser.user_id == user.id).first()
            if existing_site_user:
                continue  # Skip this user as they are already assigned to a site

        # Add user to the candidate list if they are not excluded
        candidate_list.append({
            "id": user.id,
            "name": user.name,
            "username": user.username,
            "email": user.email,
            "phone": user.phone,
            "role": role_name
        })

    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="Candidates fetched successfully",
        data=candidate_list,
        error=""
    )


@router.post("/api/site_user/register/{site_id}", tags=['site_users'])
def register_site_users(
    user: user_dependency,
    site_id: int,
    user_ids: list[int] = Body(...),
    db: Session = Depends(getdb)
):
    try:
        added_users = []
        skipped_users = 0
        site_role_user_added = False  # Flag to track if a user with "site" role has been added

        # Check if there's already a "site" user associated with this site
        existing_site_user = db.query(SiteUser).join(User, SiteUser.user_id == User.id).join(UserRole, User.id == UserRole.user_id).join(Role, UserRole.role_id == Role.id).filter(
            SiteUser.site_id == site_id,
            Role.role_name == "site"
        ).first()

        if existing_site_user:
            # If there is already a "site" user assigned to this site, no more "site" users can be added
            site_role_user_added = True

        for user_id in user_ids:
            # Fetch the user role for the given user_id
            user_role = db.query(UserRole).filter(UserRole.user_id == user_id).first()
            if not user_role:
                skipped_users += 1
                continue

            # Fetch the role for the user
            role = db.query(Role).filter(Role.id == user_role.role_id).first()
            if not role:
                skipped_users += 1
                continue

            # If the user has an admin role, skip them
            if role.role_name == "admin":
                skipped_users += 1
                continue

            # Handle "site" role logic
            if role.role_name == "site":
                # If a "site" user is already assigned, skip adding more "site" users to the site
                if site_role_user_added:
                    skipped_users += 1
                    continue
                # Mark that a "site" user has been added
                site_role_user_added = True

            # Check if the user is already assigned to this site
            existing_site_user = db.query(SiteUser).filter(SiteUser.user_id == user_id, SiteUser.site_id == site_id).first()
            if existing_site_user:
                skipped_users += 1
                continue

            # Add the user to the site
            site_user = SiteUser(
                site_id=site_id,
                user_id=user_id,
                updated_by=1
            )
            db.add(site_user)
            added_users.append(user_id)

        db.commit()

        return response_strct(
            status_code=status.HTTP_201_CREATED,
            detail="Users registered successfully",
            data={"registered_user_ids": added_users, "skipped_users": skipped_users},
            error=""
        )
    except Exception as e:
        db.rollback()
        return response_strct(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred",
            data={},
            error=''
        )



@router.get("/api/site_user/get_all", tags=['site_users'])
def get_all_site_users(user: user_dependency,db: Session = Depends(getdb)):
    site_users = (
        db.query(SiteUser, User, Role)
        .join(User, SiteUser.user_id == User.id)
        .join(UserRole, User.id == UserRole.user_id)
        .join(Role, UserRole.role_id == Role.id)
        .all()
    )
    
    user_list = [
        {
            "id": user.id,
            "name": user.name,
            "username": user.username,
            "email": user.email,
            "phone": user.phone,
            "role": role.role_name,
            "site_id": site_user.site_id
        }
        for site_user, user, role in site_users
    ]
    
    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="All site users fetched successfully",
        data=user_list,
        error=""
    )

@router.get("/api/site_user/get_by_site/{site_id}", tags=['site_users'])
def get_users_by_site(user: user_dependency, site_id: int, db: Session = Depends(getdb)):
    # Join Site to get the site name
    site_users = (
        db.query(SiteUser, User, Role, Site)
        .join(User, SiteUser.user_id == User.id)
        .join(UserRole, User.id == UserRole.user_id)
        .join(Role, UserRole.role_id == Role.id)
        .join(Site, SiteUser.site_id == Site.id)
        .filter(SiteUser.site_id == site_id)
        .all()
    )

    user_list = [
        {
            "id": user.id,
            "name": user.name,
            "username": user.username,
            "email": user.email,
            "phone": user.phone,
            "role": role.role_name,
            "is_active": site_user.is_active
        }
        for site_user, user, role, site in site_users
    ]

    detail_list = [
        {"message": "Users for site fetched successfully"}
    ]
    # Add site name to detail
    if site_users:
        detail_list.append({"site_name": site_users[0][3].site_name})

    return response_strct(
        status_code=status.HTTP_200_OK,
        detail=detail_list,
        data=user_list,
        error=""
    )

@router.delete("/api/site_user/delete/{site_id}/{user_id}", tags=['site_users'])
def delete_site_user(user: user_dependency,site_id: int, user_id: int, db: Session = Depends(getdb)):
    try:
        site_user = db.query(SiteUser).filter(SiteUser.site_id == site_id, SiteUser.user_id == user_id).first()
        if not site_user:
            return response_strct(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Site user not found",
                data={},
                error=""
            )
        
        db.delete(site_user)
        db.commit()
        
        return response_strct(
            status_code=status.HTTP_200_OK,
            detail="Site user deleted successfully",
            data={},
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
    
from pydantic import BaseModel

class UpdateUserStatusRequest(BaseModel):
    is_active: bool


@router.put("/api/site_user/update_status/{site_id}/{user_id}", tags=["site_users"])
def update_site_user_status(
    user: user_dependency,
    site_id: int,
    user_id: int,
    status_update: UpdateUserStatusRequest,
    db: Session = Depends(getdb)
):
    try:
        site_user = db.query(SiteUser).filter(
            SiteUser.site_id == site_id,
            SiteUser.user_id == user_id
        ).first()

        if not site_user:
            return response_strct(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Site user not found",
                data={},
                error=""
            )

        # Update is_active status
        site_user.is_active = status_update.is_active
        db.commit()

        return response_strct(
            status_code=status.HTTP_200_OK,
            detail="Site user status updated successfully",
            data={"user_id": user_id, "site_id": site_id, "is_active": site_user.is_active},
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

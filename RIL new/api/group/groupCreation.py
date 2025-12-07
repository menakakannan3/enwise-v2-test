from sqlalchemy.orm import Session
from ...schemas.masterSchema import *
from ...modals.masters import *
from ...database.session import getdb
from fastapi import APIRouter, HTTPException,Depends,Form,Body
from ...utils.utils import *
from starlette import status
from sqlalchemy.exc import SQLAlchemyError

from ..auth.authentication import user_dependency

router = APIRouter()

@router.post("/api/group/register", tags=['groups'])
def create_group(
    user: user_dependency,
    group_name: str = Form(..., min_length=3, max_length=255),
    ind_code: str = Form(...),
    db: Session = Depends(getdb),
):
    try:
        if user is None or user['role'] != 'admin':
            raise HTTPException(status_code=401, detail="Authentication failed")

        
        existing_group = db.query(Group).filter(Group.group_name == group_name).first()
        if existing_group:
            return response_strct(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Group name already exists",
                data={},
                error="Duplicate entry"
            )

        last_group = db.query(Group).order_by(Group.id.desc()).first()
        new_id = last_group.id + 1 if last_group else 1
        new_uuid = f"IND_{new_id}"

        new_group = Group(
            uuid=new_uuid,
            id=new_id,
            group_name=group_name,
            ind_code = ind_code,
            created_by=1,  # Hardcoded for now
            updated_by=1,
        )

        db.add(new_group)
        db.commit()
        db.refresh(new_group)

        return response_strct(
            status_code=status.HTTP_201_CREATED,
            detail="Group created successfully",
            data=new_group,
            error=""
        )

    except Exception as e:
        db.rollback()
        logger.exception("Error while creating group")
        return response_strct(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred",
            data={},
            error=''
        )


# Get all groups
@router.get("/api/group/getAll" , tags=["groups"])
def get_groups(user : user_dependency , db: Session = Depends(getdb)):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    groups = db.query(Group).all()
    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="All groups fetched successfully",
        data=groups,
        error=""
    )

# Get a single group by ID
@router.get("/api/group/get/{uuid}")
def get_group( user : user_dependency , uuid: str, db: Session = Depends(getdb)):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    group = db.query(Group).filter(Group.uuid == uuid).first()
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    
    return response_strct(
        status_code=status.HTTP_200_OK,
        detail=f"Group {group.group_name} fetched successfully",
        data=group,
        error=""
    )

# Update a group
@router.put("/api/group/update/{uuid}" , tags=["groups"])
def update_group(
    user : user_dependency,
    uuid: str,
    group_name: Optional[str] = Form(None, min_length=3, max_length=255),
    ind_code : Optional[str] = Form(None),
    db: Session = Depends(getdb),
):
    try:
        if user is None or user['role'] != 'admin':
            raise HTTPException(status_code=401, detail="Authentication failed")
        existing_group = db.query(Group).filter(Group.uuid == uuid).first()
        if not existing_group:
            return response_strct(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Group not found",
                data={},
                error=""
            )
        
        duplicate_group = db.query(Group).filter(
            Group.group_name == group_name,
            Group.uuid != uuid  # Exclude the current group
        ).first()

        if duplicate_group:
            return response_strct(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Another group with the same name already exists",
                data={},
                error="Duplicate entry"
            )

        if group_name is not None:
            existing_group.group_name = group_name
            existing_group.updated_by = 1  
        if ind_code is not None:
            existing_group.ind_code = ind_code
            existing_group.updated_by = 1 


        db.commit()
        db.refresh(existing_group)

        return response_strct(
            status_code=status.HTTP_200_OK,
            detail=f"Group with uuid: {uuid} updated successfully",
            data=existing_group,
            error=""
        )

    except SQLAlchemyError as e:
        db.rollback()
        logger.exception("Error while creating group")
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
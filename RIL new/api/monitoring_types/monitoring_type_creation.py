from sqlalchemy.orm import Session
# from ...schemas.monitoringTypeSchema import *
from ...modals.masters import *
from ...database.session import getdb
from fastapi import APIRouter, HTTPException, Depends, Form
from ...utils.utils import *
from starlette import status
from sqlalchemy.exc import SQLAlchemyError

from ..auth.authentication import user_dependency

router = APIRouter()

@router.post("/api/monitoring_type/register", tags=['monitoring_types'])
def create_monitoring_type(
    user : user_dependency,
    monitoring_type: str = Form(..., min_length=3, max_length=255),
    db: Session = Depends(getdb),
):
    try:
        if user is None or user['role'] != 'admin':
            raise HTTPException(status_code=401, detail="Authentication failed")
        
        existing_type = db.query(MonitoringType).filter(MonitoringType.monitoring_type == monitoring_type).first()
        if existing_type:
            return response_strct(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Monitoring type already exists",
                data={},
                error="Duplicate entry"
            )
        last_entry = db.query(MonitoringType).order_by(MonitoringType.id.desc()).first()
        new_id = last_entry.id + 1 if last_entry else 1
        
        new_entry = MonitoringType(
            id=new_id,
            monitoring_type=monitoring_type,
            created_by=1,  # Hardcoded for now
            updated_by=1
        )
        db.add(new_entry)
        db.commit()
        db.refresh(new_entry)
        
        return response_strct(
            status_code=status.HTTP_201_CREATED,
            detail="Monitoring type created successfully",
            data=new_entry,
            error=""
        )
    except Exception as e:
        logger.exception("Error while creating monitoring type")
        return response_strct(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred",
            data={},
            error=''
        )

@router.get("/api/monitoring_type/getAll", tags=['monitoring_types'])
def get_monitoring_types(user : user_dependency , db: Session = Depends(getdb)):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    monitoring_types = db.query(MonitoringType).all()
    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="All monitoring types fetched successfully",
        data=monitoring_types,
        error=""
    )


@router.get("/api/monitoring_type/get/{type_id}", tags=['monitoring_types'])
def get_monitoring_type(user : user_dependency , type_id: int, db: Session = Depends(getdb)):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    monitoring_type = db.query(MonitoringType).filter(MonitoringType.id == type_id).first()
    if not monitoring_type:
        raise HTTPException(status_code=404, detail="Monitoring type not found")
    return response_strct(
        status_code=status.HTTP_202_ACCEPTED,
        detail=f"Monitoring type {monitoring_type.monitoring_type} fetched successfully",
        data=monitoring_type,
        error=""
    )


@router.put("/api/monitoring_type/update/{type_id}", tags=['monitoring_types'])
def update_monitoring_type(
    user : user_dependency ,
    type_id: int,
    monitoring_type: str = Form(None, min_length=3, max_length=255),
    db: Session = Depends(getdb),
):
    try:
        if user is None or user['role'] != 'admin':
            raise HTTPException(status_code=401, detail="Authentication failed")
        existing_type = db.query(MonitoringType).filter(MonitoringType.id == type_id).first()
        if not existing_type:
            return response_strct(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Monitoring type not found",
                data={},
                error=""
            )

        duplicate_type = db.query(MonitoringType).filter(
            MonitoringType.monitoring_type == monitoring_type,
            MonitoringType.id != type_id  # Exclude the current monitoring type
        ).first()

        if duplicate_type:
            return response_strct(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Another monitoring type with the same name already exists",
                data={},
                error="Duplicate entry"
            )

        if monitoring_type is not None:
            existing_type.monitoring_type = monitoring_type

        existing_type.updated_by = 1  # Hardcoded for now
        db.commit()
        db.refresh(existing_type)

        return response_strct(
            status_code=status.HTTP_200_OK,
            detail=f"Monitoring type {type_id} updated successfully",
            data=existing_type,
            error=""
        )
    except SQLAlchemyError as e:
        db.rollback()
        logger.exception("Database error in update_monitoring_type")
        return response_strct(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred",
            data={},
            error=''
        )
    except Exception as e:
        logger.exception("Unexpected error in update_monitoring_type")
        return response_strct(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred",
            data={},
            error=''
        )
    

@router.delete("/api/monitoring_type/delete/{type_id}", tags=['monitoring_types'])
def delete_monitoring_type(user : user_dependency , type_id: int, db: Session = Depends(getdb)):
    try:
        if user is None or user['role'] != 'admin':
            raise HTTPException(status_code=401, detail="Authentication failed")
        monitoring_type = db.query(MonitoringType).filter(MonitoringType.id == type_id).first()
        if not monitoring_type:
            return response_strct(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Monitoring type not found",
                data={},
                error=""
            )

        db.delete(monitoring_type)
        db.commit()

        return response_strct(
            status_code=status.HTTP_200_OK,
            detail=f"Monitoring type {type_id} deleted successfully",
            data={},
            error=""
        )
    except SQLAlchemyError as e:
        db.rollback()
        logger.exception("Database error in delete_monitoring_type")
        return response_strct(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred",
            data={},
            error=''
        )
    except Exception as e:
        logger.exception("Error while creating monitoring type")
        return response_strct(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred",
            data={},
            error=''
        )

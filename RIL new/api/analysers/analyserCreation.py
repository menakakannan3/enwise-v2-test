#OM VIGHNHARTAYE NAMO NAMAH :

from sqlalchemy import func, text
from sqlalchemy.orm import Session
from fastapi import APIRouter, HTTPException, Depends, Form, File, UploadFile
from sqlalchemy.exc import SQLAlchemyError
from starlette import status
from io import BytesIO
import pandas as pd
import datetime
from typing import Optional
import logging
from ...schemas.masterSchema import *
from ...modals.masters import Analyser
from ...database.session import getdb
from ...utils.utils import response_strct

from ..auth.authentication import user_dependency

router = APIRouter()

@router.post("/api/analyser/register", tags=['analysers'])
def create_analyser(
    user : user_dependency,
    analyser_name: str = Form(..., min_length=3, max_length=250),
    make: str = Form(..., min_length=1, max_length=255),
    model: str = Form(..., min_length=1, max_length=255),
    description: Optional[str] = Form(None, max_length=255),
    db: Session = Depends(getdb),
):
    try:
        if user is None or user['role'] != 'admin':
            raise HTTPException(status_code=401, detail="Authentication failed")
        
        existing_analyser = (
            db.query(Analyser)
            .filter(
                Analyser.analyser_name == analyser_name,
                Analyser.make == make,
                Analyser.model == model
            )
            .first()
        )

        if existing_analyser:
            return response_strct(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Analyser with the same name, make, and model already exists",
                data={},
                error="Duplicate entry"
            )

        last_analyser = db.query(Analyser).order_by(Analyser.id.desc()).first()
        new_id = last_analyser.id + 1 if last_analyser else 1
        new_uid = f"analyser_{new_id}"
        
        new_analyser = Analyser(
            analyser_name=analyser_name,
            analyser_uid=new_uid,
            model = model,
            make=make,
            description=description,
            created_by=1,
            updated_by=1
        )
        db.add(new_analyser)
        db.commit()
        db.refresh(new_analyser)
        return response_strct(
            status_code=status.HTTP_201_CREATED,
            detail="Analyser created successfully",
            data=new_analyser,
            error=""
        )
    except Exception as e:
        logger.exception("Internal server error in analyser API")
        db.rollback()
        return response_strct(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred",
            data={},
            error=""
        )

@router.get("/api/analyser/getAll", tags=['analysers'])
def get_analysers( user : user_dependency ,db: Session = Depends(getdb)):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    analysers = db.query(Analyser).all()
    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="All analysers fetched successfully",
        data=analysers,
        error=""
    )

@router.get("/api/analyser/get/{analyser_uid}", tags=['analysers'])
def get_analyser(user : user_dependency ,analyser_uid: str, db: Session = Depends(getdb)):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    analyser = db.query(Analyser).filter(Analyser.analyser_uid == analyser_uid).first()
    if not analyser:
        raise HTTPException(status_code=404, detail="Analyser not found")
    return response_strct(
        status_code=status.HTTP_202_ACCEPTED,
        detail=f"Analyser {analyser.analyser_name} fetched successfully",
        data=analyser,
        error=""
    )

@router.put("/api/analyser/update/{analyser_uid}", tags=['analysers'])
def update_analyser(
    user : user_dependency,
    analyser_uid: str,
    analyser_name: Optional[str] = Form(None, min_length=3, max_length=250),
    make: Optional[str] = Form(None, min_length=1, max_length=255),
    model: Optional[str] = Form(None, min_length=1, max_length=255),
    description: Optional[str] = Form(None, max_length=255),
    db: Session = Depends(getdb),
):
    try:
        if user is None or user['role'] != 'admin':
            raise HTTPException(status_code=401, detail="Authentication failed")
        existing_analyser = db.query(Analyser).filter(Analyser.analyser_uid == analyser_uid).first()
        if not existing_analyser:
            return response_strct(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Analyser not found",
                data={},
                error=""
            )
        duplicate_check = db.query(Analyser).filter(
            Analyser.analyser_name == (analyser_name or existing_analyser.analyser_name),
            Analyser.make == (make or existing_analyser.make),
            Analyser.model == (model or existing_analyser.model),
            Analyser.analyser_uid != analyser_uid  # Exclude the current analyser
        ).first()

        if duplicate_check:
            return response_strct(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Another analyser with the same name, make, and model already exists",
                data={},
                error="Duplicate entry"
            )

        if analyser_name is not None:
            existing_analyser.analyser_name = analyser_name
        if make is not None:
            existing_analyser.make = make
        if description is not None:
            existing_analyser.description = description
        if model is not None:
            existing_analyser.model = model
        
        existing_analyser.updated_by = 1
        db.commit()
        db.refresh(existing_analyser)

        return response_strct(
            status_code=status.HTTP_200_OK,
            detail=f"Analyser {analyser_uid} updated successfully",
            data=existing_analyser,
            error=""
        )
    except SQLAlchemyError as e:
        db.rollback()
        return response_strct(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred",
            data={},
            error=""
        )
    except Exception as e:
        return response_strct(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred",
            data={},
            error=""
        )

@router.delete("/api/analyser/delete/{analyser_uid}", tags=['analysers'])
def delete_analyser( user : user_dependency ,analyser_uid: str, db: Session = Depends(getdb)):
    try:
        if user is None or user['role'] != 'admin':
            raise HTTPException(status_code=401, detail="Authentication failed")
        existing_analyser = db.query(Analyser).filter(Analyser.analyser_uid == analyser_uid).first()
        if not existing_analyser:
            return response_strct(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Analyser not found",
                data={},
                error=""
            )
        db.delete(existing_analyser)
        db.commit()
        return response_strct(
            status_code=status.HTTP_200_OK,
            detail=f"Analyser {analyser_uid} deleted successfully",
            data={},
            error=""
        )
    except Exception as e:
        logger.exception("Internal server error in analyser API")
        db.rollback()
        return response_strct(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred",
            data={},
            error=""
        )
    
@router.post("/api/analyser/register_bulk", tags=['analysers'])
def create_analysers_bulk(
    user: user_dependency,
    file: UploadFile = File(...),
    db: Session = Depends(getdb),
):
    try:
        # Authentication check
        if user is None or user['role'] != 'admin':
            raise HTTPException(status_code=401, detail="Authentication failed")

        # Read file content
        content = file.file.read()

        # Detect file type and read as DataFrame
        if file.filename.endswith(".csv"):
            df = pd.read_csv(BytesIO(content))
        elif file.filename.endswith((".xls", ".xlsx")):
            df = pd.read_excel(BytesIO(content))
        else:
            raise HTTPException(status_code=400, detail="Invalid file format. Upload CSV or Excel.")

        # Normalize column names (case insensitive, remove special chars)
        df.columns = df.columns.str.lower().str.strip().str.replace('[^a-z_]', '', regex=True)

        # Check for required data (not specific column names)
        if 'make' not in df.columns or 'model_name' not in df.columns:
            # Try alternative column names
            col_map = {
                'make': ['manufacturer', 'brand', 'company'],
                'model_name': ['model', 'version', 'modelnumber']
            }
            
            for standard_col, alternatives in col_map.items():
                for alt in alternatives:
                    if alt in df.columns:
                        df.rename(columns={alt: standard_col}, inplace=True)
                        break
            
            if 'make' not in df.columns or 'model_name' not in df.columns:
                raise HTTPException(
                    status_code=400,
                    detail="Could not find required columns. Need make/brand/company and model_name/model/version"
                )

        # Clean data
        df = df.dropna(how='all')  # Remove empty rows
        df = df.fillna('')  # Replace NaN with empty strings
        
        # Generate analyser_name (make + model) and check for duplicates
        df['analyser_name'] = df['make'].str.strip() + ' ' + df['model_name'].str.strip()
        existing_names = {a.analyser_name for a in db.query(Analyser.analyser_name).all()}
        last_id = db.query(func.max(Analyser.id)).scalar() or 0

        
        inserted_count = 0
        analysers_to_add = []

        for _, row in df.iterrows():
            # Skip if make or model is empty
            if not str(row["make"]).strip() or not str(row["model_name"]).strip():
                continue
                
            # Skip duplicates
            analyser_name = f"{row['make'].strip()} {row['model_name'].strip()}"
            if analyser_name in existing_names:
                continue

            # Get next analyser ID for UID generation
           
            new_uid = f"analyser_{last_id + 1 + inserted_count}"

            analysers_to_add.append({
                "analyser_name": analyser_name,
                "analyser_uid": new_uid,
                "make": row["make"].strip(),
                "model": row["model_name"].strip()
            })
            inserted_count += 1
            existing_names.add(analyser_name)

        # Bulk insert
        if analysers_to_add:
            db.bulk_insert_mappings(Analyser, analysers_to_add)
            db.commit()
            inserted_count = len(analysers_to_add)

        return response_strct(
            status_code=status.HTTP_201_CREATED,
            detail="Bulk analysers created successfully",
            data={"inserted": inserted_count},
            error=""
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Internal server error in analyser API")
        db.rollback()
        return response_strct(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred",
            data={},
            error=""
        )

@router.delete("/api/analyser/delete_all", tags=['analysers'])
def delete_all_analysers(
    user: user_dependency,
    db: Session = Depends(getdb),
):
    try:
        # Authentication check - only allow admins
        if user is None or user['role'] != 'admin':
            raise HTTPException(status_code=401, detail="Authentication failed")

        # Get count before deletion for response
        count = db.query(Analyser).count()

        # Perform bulk delete
        db.query(Analyser).delete()
        db.commit()

        # Reset auto-increment (id) sequence
        if db.bind.dialect.name == "postgresql":
            db.execute(text("ALTER SEQUENCE analysers_id_seq RESTART WITH 1"))
        elif db.bind.dialect.name == "sqlite":
            db.execute("DELETE FROM sqlite_sequence WHERE name='analysers'")
        elif db.bind.dialect.name == "mysql":
            db.execute("ALTER TABLE analysers AUTO_INCREMENT = 1")

        db.commit()

        return response_strct(
            status_code=status.HTTP_200_OK,
            detail=f"Successfully deleted all {count} analysers",
            data={"deleted_count": count},
            error=""
        )

    except Exception as e:
        logger.exception("Internal server error in analyser API")
        db.rollback()
        return response_strct(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete analysers",
            data={},
            error=""
        )

from sqlalchemy.orm import Session
from ...schemas.masterSchema import *
from ...modals.masters import *
from ...database.session import getdb
from fastapi import APIRouter, HTTPException,Depends,Form,Body
from ...utils.utils import *
from starlette import status
from sqlalchemy.exc import SQLAlchemyError
from fastapi import UploadFile , File
from io import BytesIO
import pandas as pd
from sqlalchemy.orm import joinedload
from typing import Optional
import html


from ..auth.authentication import user_dependency

router = APIRouter()

def sanitize_text(value: Optional[str]) -> Optional[str]:
    """
    Basic XSS protection:
    - Escapes <, >, &, " so they cannot be interpreted as HTML/JS
    - If value is None, returns None
    """
    if value is None:
        return None
    return html.escape(value, quote=True)

@router.post("/api/parameter/register", tags=['parameters'])
def create_parameter(
    user: user_dependency,
    name: str = Form(..., min_length=2, max_length=255),
    label: str = Form(..., min_length=2, max_length=255),
    unit: str = Form(..., max_length=50),
    min_threshold: Optional[float] = Form(None),
    max_threshold: float = Form(...),
    monitoring_type_id: int = Form(..., gt=0),
    db: Session = Depends(getdb),
):
    try:
        # Authentication check
        if user is None or user['role'] != 'admin':
            raise HTTPException(status_code=401, detail="Authentication failed")

        # Sanitize user inputs to prevent XSS
        safe_name = sanitize_text(name)
        safe_label = sanitize_text(label)
        safe_unit = sanitize_text(unit)

        # Check duplicate parameter
        existing_param = db.query(Parameter).filter(
            (Parameter.name == safe_name) &
            (Parameter.monitoring_type_id == monitoring_type_id)
        ).first()

        if existing_param:
            raise HTTPException(
                status_code=400,
                detail="Parameter with this name already exists for this monitoring type ID"
            )

        # Generate new ID + UUID
        last_param = db.query(Parameter).order_by(Parameter.id.desc()).first()
        new_id = last_param.id + 1 if last_param else 1
        new_uuid = f"param_{new_id}"

        # Create new Parameter (using sanitized values)
        new_param = Parameter(
            uuid=new_uuid,
            name=safe_name,
            label=safe_label,
            unit=safe_unit,
            monitoring_type_id=monitoring_type_id,
            created_by=1,  # Hardcoded for now
            updated_by=1,
            min_thershold=min_threshold,
            max_thershold=max_threshold
        )

        db.add(new_param)
        db.commit()
        db.refresh(new_param)

        return response_strct(
            status_code=status.HTTP_201_CREATED,
            detail="parameter created successfully",
            data=new_param,
            error=""
        )

    except Exception as e:
        return response_strct(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred",
            data={},
            error=''
        )

@router.get("/api/parameter/getAll", tags=['parameters'])
def get_parameters(user: user_dependency, db: Session = Depends(getdb)):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")

    # Fetch parameters with joined monitoring_type
    params = (
        db.query(
            Parameter.id,
            Parameter.uuid,
            Parameter.name,
            Parameter.label,
            Parameter.unit,
            Parameter.min_thershold,
            Parameter.max_thershold,
            Parameter.monitoring_type_id,
            MonitoringType.monitoring_type  # Fetch monitoring_type name
        )
        .join(MonitoringType, MonitoringType.id == Parameter.monitoring_type_id)
        .all()
    )

    # Format response as a list of dictionaries
    param_list = [
        {
            "id": param.id,
            "uuid": param.uuid,
            "name": param.name,
            "label": param.label,
            "unit": param.unit,
            "min_threshold": param.min_thershold,
            "max_threshold": param.max_thershold,
            "monitoring_type_id": param.monitoring_type_id,
            "monitoring_type_name": param.monitoring_type  # Include name
        }
        for param in params
    ]

    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="All parameters fetched successfully",
        data=param_list,
        error=""
    )

# Read single Parameter
@router.get("/api/parameter/get/{param_uuid}"  , tags=['parameters'])
def get_parameter(user : user_dependency , param_uuid: str, db: Session = Depends(getdb)):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    param = db.query(Parameter).filter(Parameter.uuid == param_uuid).first()
    if not param:
        raise HTTPException(status_code=404, detail="Parameter not found")
    return response_strct(
        status_code=status.HTTP_202_ACCEPTED,
        detail=f"parameter {param.name} fetched successfully",
        data=param,
        error=""
    )

@router.put("/api/parameter/update/{param_uuid}", tags=['parameters'])
def update_parameter(
    user: user_dependency,
    param_uuid: str,
    name: str = Form(None, min_length=2, max_length=255),
    label: str = Form(None, min_length=2, max_length=255),
    unit: str = Form(None, max_length=50),
    min_threshold: Optional[float] = Form(None),
    max_threshold: float = Form(None, gt=0),
    monitoring_type_id: int = Form(None, gt=0),
    db: Session = Depends(getdb),
):
    try:
        if user is None or user['role'] != 'admin':
            raise HTTPException(status_code=401, detail="Authentication failed")

        existing_param = db.query(Parameter).filter(Parameter.uuid == param_uuid).first()
        if not existing_param:
            return response_strct(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Parameter not found",
                data={},
                error=""
            )

        # Sanitize and update string fields
        if name is not None:
            existing_param.name = sanitize_text(name)
        if label is not None:
            existing_param.label = sanitize_text(label)
        if unit is not None:
            existing_param.unit = sanitize_text(unit)

        # Update numeric fields (safe)
        if min_threshold is not None:
            existing_param.min_thershold = min_threshold
        if max_threshold is not None:
            existing_param.max_thershold = max_threshold
        if monitoring_type_id is not None:
            existing_param.monitoring_type_id = monitoring_type_id

        existing_param.updated_by = 1  # Hardcoded for now

        db.commit()
        db.refresh(existing_param)

        return response_strct(
            status_code=status.HTTP_200_OK,
            detail=f"Parameter {param_uuid} updated successfully",
            data=existing_param,
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


@router.post("/api/parameter/register_bulk", tags=['parameters'])
def create_parameters_bulk(
    user: user_dependency,
    file: UploadFile = File(...),
    db: Session = Depends(getdb),
):
    try:
        if user is None or user['role'] != 'admin':
            raise HTTPException(status_code=401, detail="Authentication failed")

        # Read file content
        content = file.file.read()

        # Detect file type and read it as DataFrame
        if file.filename.endswith(".csv"):
            df = pd.read_csv(BytesIO(content))
        elif file.filename.endswith((".xls", ".xlsx")):
            df = pd.read_excel(BytesIO(content))
        else:
            raise HTTPException(status_code=400, detail="Invalid file format. Upload a CSV or Excel file.")

        # Validate required columns
        required_columns = {"name", "label", "unit", "min_thershold", "max_thershold", "monitoring_type_id"}
        if not required_columns.issubset(df.columns):
            raise HTTPException(status_code=400, detail=f"Missing required columns. Required: {required_columns}")

        # Clean the data
        df = df.dropna(how='all')  # Remove completely empty rows
        df = df.fillna('')  # Replace remaining NaN with empty strings
        
        # Convert monitoring_type_id to integer, drop rows that can't be converted
        df = df[pd.to_numeric(df['monitoring_type_id'], errors='coerce').notna()]
        df['monitoring_type_id'] = df['monitoring_type_id'].astype(int)

        # Validate monitoring_type_id values
        valid_types = {1, 2, 3}
        df = df[df['monitoring_type_id'].isin(valid_types)]

        # Fetch all existing name + monitoring_type_id pairs
        existing_pairs = {
            (param.name, param.monitoring_type_id) 
            for param in db.query(Parameter.name, Parameter.monitoring_type_id).all()
        }

        inserted_count = 0

        # Insert records, skipping duplicates
        for _, row in df.iterrows():
            # Skip if name is empty
            if not row["name"]:
                continue

            # Check if the parameter already exists
            if (row["name"], row["monitoring_type_id"]) in existing_pairs:
                continue

            param = Parameter(
                name=str(row["name"]),
                label=str(row["label"]),
                unit=str(row["unit"]),
                min_thershold=float(row["min_thershold"]),
                max_thershold=float(row["max_thershold"]),
                monitoring_type_id=int(row["monitoring_type_id"]),
                created_by=1,
                updated_by=1
            )

            db.add(param)
            db.flush()
            db.refresh(param)
            param.uuid = f"param_{param.id}"
            inserted_count += 1
            existing_pairs.add((row["name"], row["monitoring_type_id"]))

        db.commit()

        return response_strct(
            status_code=status.HTTP_201_CREATED,
            detail="Bulk parameters created successfully",
            data={"inserted": inserted_count},
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
from sqlalchemy import text

@router.delete("/api/parameter/truncate", tags=['parameters'])
def truncate_parameter_table(user : user_dependency ,db: Session = Depends(getdb)):
    try:
        if user is None or user['role'] != 'admin':
            raise HTTPException(status_code=401, detail="Authentication failed")
        db.execute(text("TRUNCATE TABLE parameters RESTART IDENTITY CASCADE;"))
        db.commit()
        return response_strct(
            status_code=status.HTTP_200_OK,
            detail="Parameter table truncated successfully",
            data={},
            error=""
        )
    except Exception as e:
        db.rollback()
        return response_strct(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while truncating the table",
            data={},
            error=''
        )

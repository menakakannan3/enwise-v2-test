#OM VIGHNHARTAYE NAMO NAMAH :

from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from ...modals.masters import AnalyserParameter, Analyser, Parameter , MonitoringType
from ...database.session import getdb
from starlette import status
from ...utils.utils import *

router = APIRouter()

from ..auth.authentication import user_dependency

@router.post("/api/analyser-parameter/create", summary="Create a new analyser parameter", tags=["analyser_parameter"])
def create_analyser_parameter(
    user : user_dependency,
    analyser_id: int,
    parameter_ids: list[int],  # Accept multiple parameter IDs
    db: Session = Depends(getdb)
):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    analyser = db.query(Analyser).filter(Analyser.id == analyser_id).first()
    if not analyser:
        raise HTTPException(status_code=404, detail="Analyser not found")
    
    created_entries = []
    inserted_count = 0
    for parameter_id in parameter_ids:
        parameter = db.query(Parameter).filter(Parameter.id == parameter_id).first()
        
        inserted_count += 1
        monitoring_type = db.query(MonitoringType).filter(MonitoringType.id == parameter.monitoring_type_id).first()
        
        new_analyser_param = AnalyserParameter(
            analyser_id=analyser_id,
            parameter_id=parameter_id,
            created_by=1,
        )
        db.add(new_analyser_param)
        db.flush()
        created_entries.append({
        "analyser_id": new_analyser_param.analyser_id,
        "analyser_name": analyser.analyser_name,
        "parameter_name": parameter.name,
        "parameter_id": new_analyser_param.parameter_id,
        "monitoring_type_name": monitoring_type.monitoring_type if monitoring_type else None,
        "created_by": new_analyser_param.created_by,
        "id": new_analyser_param.id,
        "created_at": new_analyser_param.created_at,
        "updated_at": new_analyser_param.updated_at
})
    
    db.commit()
    
    return response_strct(
        status_code = status.HTTP_201_CREATED,
        detail = "Analyser parameters added successfully!",
        data = {"inserted_count": inserted_count, "entries": created_entries} 
    )

@router.get("/api/analyser-parameters", summary="Get all analyser parameters", tags=["analyser_parameter"])
def get_all_analyser_parameters( user : user_dependency ,db: Session = Depends(getdb)):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    analyser_parameters = db.query(AnalyserParameter).all()
    return response_strct(
       status_code=status.HTTP_201_CREATED,
       detail= "analyser parameters fetched successfully!",
       data=analyser_parameters
    )

@router.get("/api/analyser-parameters/{analyser_id}")
def get_analyser_parameters(analyser_id: int,user: user_dependency, db: Session = Depends(getdb)):
    
    params = (
        db.query(Parameter)
        .join(AnalyserParameter, Parameter.id == AnalyserParameter.parameter_id)
        .filter(AnalyserParameter.analyser_id == analyser_id)
        .all()
    )
   
    return [
        {
            "parameter_id": param.id,
            "name": param.name,
            "label": param.label,
            "unit": param.unit,
        }
        for param in params
    ]


@router.get("/api/analyser_parameter/{analyser_id}/{parameter_id}", summary="Get Analyser Parameter", tags=['analyser_parameter'])
def get_analyser_parameter(user : user_dependency , analyser_id: int, parameter_id: int, db: Session = Depends(getdb)):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    analyser_param = db.query(AnalyserParameter).filter(
        AnalyserParameter.analyser_id == analyser_id,
        AnalyserParameter.parameter_id == parameter_id
    ).first()
    if not analyser_param:
        raise HTTPException(status_code=404, detail="Analyser Parameter not found")
    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="all analyser parameter pairs fetched successfully",
        data = analyser_param
    )

@router.get("/api/analyser_parameters/{analyser_id}", summary="Get All Parameters for Analyser", tags=['analyser_parameter'])
def get_analyser_parameters( user : user_dependency ,analyser_id: int, db: Session = Depends(getdb)):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    analyser = db.query(Analyser).filter(Analyser.id == analyser_id).first()
    if not analyser:
        raise HTTPException(status_code=404, detail="Analyser not found")
    
    analyser_params = db.query(AnalyserParameter).filter(
        AnalyserParameter.analyser_id == analyser_id
    ).all()
    
    formatted_params = []
    for analyser_param in analyser_params:
        parameter = db.query(Parameter).filter(Parameter.id == analyser_param.parameter_id).first()
        monitoring_type = db.query(MonitoringType).filter(MonitoringType.id == parameter.monitoring_type_id).first() if parameter else None
        
        formatted_params.append({
            "analyser_name" : analyser.analyser_name,
            "parameter_name": parameter.name if parameter else None,
            "parameter_id" : parameter.id,
            "monitoring_type_name": monitoring_type.monitoring_type if monitoring_type else None,
            "created_at": analyser_param.created_at,
            "updated_at": analyser_param.updated_at
        })
    
    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="All parameters for given analyser fetched successfully",
        data=formatted_params
    )

@router.put("/api/analyser_parameter/{analyser_id}/{parameter_id}", summary="Update Analyser Parameter", tags=['analyser_parameter'])
def update_analyser_parameter(user : user_dependency , analyser_id: int, parameter_id: int, db: Session = Depends(getdb)):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    analyser_param = db.query(AnalyserParameter).filter(
        AnalyserParameter.analyser_id == analyser_id,
        AnalyserParameter.parameter_id == parameter_id
    ).first()
    if not analyser_param:
        raise HTTPException(status_code=404, detail="Analyser Parameter not found")
    
    analyser_param.updated_by = 1  # Example: Modify as needed
    db.commit()
    db.refresh(analyser_param)
    return {"detail": "Analyser Parameter updated successfully", "data": analyser_param}

@router.delete("/api/analyser_parameter/{analyser_id}/{parameter_id}", summary="Delete Analyser Parameter", tags=['analyser_parameter'])
def delete_analyser_parameter(user : user_dependency , analyser_id: int, parameter_id: int, db: Session = Depends(getdb)):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    analyser_param = db.query(AnalyserParameter).filter(
        AnalyserParameter.analyser_id == analyser_id,
        AnalyserParameter.parameter_id == parameter_id
    ).first()
    if not analyser_param:
        raise HTTPException(status_code=404, detail="Analyser Parameter not found")
    
    db.delete(analyser_param)
    db.commit()
    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="tahe analyser parameter deleted successfully",
        data="delete successfull"
    )
from fastapi import APIRouter, HTTPException, Depends, Body, Path, status
from sqlalchemy.orm import Session
from ...modals.masters import *
from ...database.session import getdb
from starlette import status
from ...utils.utils import *
from ...schemas.masterSchema import StationParameterUpdateRequest
from ..auth.authentication import user_dependency

router = APIRouter()


# -------------------------------
# POST → CREATE STATION PARAMETERS
# -------------------------------
@router.post(
    "/api/station-parameter/create",
    summary="Create new station parameters",
    tags=["station_parameter"],
    status_code=status.HTTP_201_CREATED,
)
def create_station_parameters(
    user: user_dependency,
    station_id: int,
    analyser_id: int,
    parameters: list[dict],
    db: Session = Depends(getdb)
):
    if user is None or user["role"] != "admin":
        raise HTTPException(status_code=401, detail="Authentication failed")

    station = db.query(Station).filter(Station.id == station_id).first()
    if not station:
        raise HTTPException(status_code=404, detail="Station not found")

    created_entries = []
    inserted_count = 0

    for param in parameters:
        parameter_id = param.get("parameter_id")
        pram_lable = param.get("pram_lable")
        para_threshold = param.get("para_threshold")
        para_unit = param.get("para_unit")
        param_interval = param.get("param_interval")  # ✅ NEW FIELD

        if not parameter_id:
            continue

        analyser_param = (
            db.query(AnalyserParameter)
              .filter(
                  AnalyserParameter.analyser_id == analyser_id,
                  AnalyserParameter.parameter_id == parameter_id
              )
              .first()
        )
        if not analyser_param:
            continue

        # Skip if exists
        if db.query(stationParameter).filter_by(
            station_id=station_id,
            analyser_param_id=analyser_param.id
        ).first():
            continue

        # Unique label validation
        if pram_lable:
            if db.query(stationParameter).filter_by(
                station_id=station_id,
                pram_lable=pram_lable
            ).first():
                raise HTTPException(
                    status_code=400,
                    detail=f"Label '{pram_lable}' already exists for this station."
                )

        # Create new record
        new_sp = stationParameter(
            station_id=station_id,
            analyser_param_id=analyser_param.id,
            pram_lable=pram_lable,
            para_threshold=para_threshold,
            para_unit=para_unit,
            param_interval=param_interval,  # ✅ NEW FIELD
            created_by=user["id"] if user and "id" in user else 1,
        )
        db.add(new_sp)
        db.flush()
        inserted_count += 1

        created_entries.append({
            "station_id": station_id,
            "analyser_param_id": analyser_param.id,
            "parameter_id": parameter_id,
            "pram_lable": pram_lable,
            "para_threshold": para_threshold,
            "para_unit": para_unit,
            "param_interval": param_interval,  # ✅ NEW FIELD
        })

    db.commit()

    return response_strct(
        status_code=status.HTTP_201_CREATED,
        detail=f"Station parameters added successfully! (Inserted: {inserted_count})",
        data={
            "inserted_count": inserted_count,
            "entries": created_entries
        }
    )


# -----------------------------------
# GET → FETCH ALL STATION PARAMETERS
# -----------------------------------
@router.get(
    "/api/station-parameters/{station_id}",
    summary="Get all parameters for a station",
    tags=["station_parameter"]
)
def get_station_parameters(
    user: user_dependency,
    station_id: int,
    db: Session = Depends(getdb)
):
    if user is None or user["role"] != "admin":
        raise HTTPException(status_code=401, detail="Authentication failed")

    station_params = (
        db.query(
            stationParameter.id.label("station_param_id"),
            stationParameter.pram_lable.label("param_label"),
            stationParameter.para_unit.label("para_unit"),
            stationParameter.para_threshold.label("para_threshold"),
            stationParameter.param_interval.label("param_interval"),  # ✅ NEW FIELD
            stationParameter.is_editable.label("is_editable"),
            Parameter.id.label("parameter_id"),
            Parameter.name.label("parameter_name"),
            Analyser.id.label("analyser_id"),
            Analyser.analyser_name,
            MonitoringType.id.label("monitoring_type_id"),
            MonitoringType.monitoring_type,
            AnalyserParameter.id.label("analyser_param_id"),
            Station.name.label("station_name"),
            Site.site_name.label("site_name")
        )
        .join(AnalyserParameter, stationParameter.analyser_param_id == AnalyserParameter.id)
        .join(Parameter, AnalyserParameter.parameter_id == Parameter.id)
        .join(Analyser, AnalyserParameter.analyser_id == Analyser.id)
        .join(MonitoringType, Parameter.monitoring_type_id == MonitoringType.id)
        .join(Station, stationParameter.station_id == Station.id)
        .join(Site, Station.site_id == Site.id)
        .filter(stationParameter.station_id == station_id)
        .all()
    )

    if not station_params:
        raise HTTPException(status_code=404, detail="No parameters found for this station")

    station_params_dicts = [
        {
            "station_param_id": p.station_param_id,
            "parameter_id": p.parameter_id,
            "parameter_name": p.parameter_name,
            "param_label": p.param_label,
            "para_unit": p.para_unit,
            "para_threshold": p.para_threshold,
            "param_interval": p.param_interval,  # ✅ NEW FIELD
            "is_editable": p.is_editable,
            "analyser_id": p.analyser_id,
            "analyser_name": p.analyser_name,
            "monitoring_type_id": p.monitoring_type_id,
            "monitoring_type": p.monitoring_type,
            "analyser_parameter_id": p.analyser_param_id,
        }
        for p in station_params
    ]

    site_name = station_params[0].site_name
    station_name = station_params[0].station_name

    return response_strct(
        status_code=status.HTTP_200_OK,
        detail={
            "message": "Station parameters fetched successfully!",
            "site_name": site_name,
            "station_name": station_name
        },
        data=station_params_dicts
    )


# -----------------------------------
# PUT → UPDATE STATION PARAMETER
# -----------------------------------
@router.put(
    "/api/station-parameter/{station_id}/{analyser_id}/{parameter_id}",
    summary="Update Station Parameter",
    tags=['station_parameter']
)
def update_station_parameter(
    user: user_dependency,
    station_id: int,
    analyser_id: int,
    parameter_id: int,
    payload: StationParameterUpdateRequest,
    db: Session = Depends(getdb)
):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")

    analyser_param = db.query(AnalyserParameter).filter(
        AnalyserParameter.analyser_id == analyser_id,
        AnalyserParameter.parameter_id == parameter_id
    ).first()
    if not analyser_param:
        raise HTTPException(status_code=404, detail="Analyser Parameter pair not found")

    station_param = db.query(stationParameter).filter(
        stationParameter.station_id == station_id,
        stationParameter.analyser_param_id == analyser_param.id
    ).first()
    if not station_param:
        raise HTTPException(status_code=404, detail="Station Parameter not found")

    # ✅ Update editable and/or interval fields
    if payload.is_editable is not None:
        station_param.is_editable = payload.is_editable

    if payload.param_interval is not None:
        station_param.param_interval = payload.param_interval  # ✅ NEW FIELD

    station_param.updated_by = user["id"] if user and "id" in user else 1

    db.commit()
    db.refresh(station_param)

    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="Station Parameter updated successfully",
        data={
            "station_id": station_param.station_id,
            "analyser_param_id": station_param.analyser_param_id,
            "pram_lable": station_param.pram_lable,
            "para_threshold": station_param.para_threshold,
            "para_unit": station_param.para_unit,
            "param_interval": station_param.param_interval,  # ✅ NEW FIELD
            "is_editable": station_param.is_editable,
        }
    )


# -----------------------------------
# DELETE → DELETE STATION PARAMETER
# -----------------------------------
@router.delete(
    "/api/station-parameter/{station_id}/{analyser_id}/{parameter_id}",
    summary="Delete Station Parameter",
    tags=['station_parameter']
)
def delete_station_parameter(
    user: user_dependency,
    station_id: int,
    analyser_id: int,
    parameter_id: int,
    db: Session = Depends(getdb)
):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")

    analyser_param = db.query(AnalyserParameter).filter(
        AnalyserParameter.analyser_id == analyser_id,
        AnalyserParameter.parameter_id == parameter_id
    ).first()
    if not analyser_param:
        raise HTTPException(status_code=404, detail="Analyser Parameter pair not found")

    station_param = db.query(stationParameter).filter(
        stationParameter.station_id == station_id,
        stationParameter.analyser_param_id == analyser_param.id
    ).first()
    if not station_param:
        raise HTTPException(status_code=404, detail="Station Parameter not found")

    db.delete(station_param)
    db.commit()

    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="The station parameter deleted successfully",
        data="Delete successful"
    )


# -----------------------------------
# GET → FETCH PARAMETER THRESHOLD
# -----------------------------------
@router.get(
    "/api/station-parameter/threshold/{station_param_id}",
    summary="Get threshold for a station parameter",
    tags=["station_parameter"],
    status_code=status.HTTP_200_OK,
)
def get_station_param_threshold(
    user: user_dependency,
    station_param_id: int,
    db: Session = Depends(getdb),
):
    if user is None:
        raise HTTPException(status_code=401, detail="Authentication failed")

    sp = db.query(stationParameter).filter_by(id=station_param_id).first()
    if not sp:
        raise HTTPException(status_code=404, detail="Station parameter not found")

    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="Threshold fetched successfully",
        data={"station_param_id": sp.id, "para_threshold": sp.para_threshold},
    )


# -----------------------------------
# PUT → UPDATE PARAMETER THRESHOLD
# -----------------------------------
@router.put(
    "/api/station-parameter/threshold/{station_param_id}",
    summary="Update threshold for a station parameter",
    tags=["station_parameter"],
    status_code=status.HTTP_200_OK,
)
def update_station_param_threshold(
    user: user_dependency,
    station_param_id: int,
    para_threshold: float = Body(..., embed=True, description="New threshold value"),
    db: Session = Depends(getdb),
):
    if user is None:
        raise HTTPException(status_code=401, detail="Authentication failed")

    sp = db.query(stationParameter).filter_by(id=station_param_id).first()
    if not sp:
        raise HTTPException(status_code=404, detail="Station parameter not found")

    sp.para_threshold = para_threshold
    sp.updated_by = None
    db.commit()
    db.refresh(sp)

    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="Threshold updated successfully",
        data={"station_param_id": sp.id, "para_threshold": sp.para_threshold},
    )

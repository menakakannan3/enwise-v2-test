from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from datetime import datetime as dt
from datetime import timedelta
from ...modals.masters import *
from ...database.session import getdb
from ..auth.authentication import user_dependency
import logging
from pytz import timezone, UTC

router = APIRouter(
    prefix="/api/camera-parameter",
    tags=["Camera Parameter"],
)

@router.post("/", status_code=status.HTTP_201_CREATED)
def create_camera_parameter(
    user: user_dependency,
    camera_id: int, 
    station_id: int, 
    parameter_id: int, 
    db: Session = Depends(getdb), 
):
    # ✅ Check if the camera exists
    cam = db.query(Camera).filter(Camera.id == camera_id).first()
    if not cam:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Camera does not exist")

    # ✅ Find matching station_param
    station_param = db.query(stationParameter).filter(
        stationParameter.station_id == station_id,
        stationParameter.analyser_param_id.in_(
            db.query(AnalyserParameter.id).filter(AnalyserParameter.parameter_id == parameter_id)
        )
    ).first()

    if not station_param:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Station-Parameter combination does not exist")

    # ✅ Check if camera is already associated with this specific station_param
    existing_mapping = db.query(CameraParameter).filter(
        CameraParameter.camera_id == camera_id,
        CameraParameter.station_parameter_id == station_param.id
    ).first()

    if existing_mapping:
        return {
            "status": status.HTTP_200_OK,
            "message": "Camera is already associated with this parameter. Skipped creating duplicate.",
            "data": {
                "camera_param_id": existing_mapping.id,
                "camera_id": existing_mapping.camera_id,
                "station_parameter_id": existing_mapping.station_parameter_id
            }
        }

    # ✅ Create new mapping
    new_camera_param = CameraParameter(
        camera_id=camera_id,
        station_parameter_id=station_param.id,
        created_at=dt.utcnow(),
        updated_at=dt.utcnow(),
    )

    db.add(new_camera_param)
    db.commit()
    db.refresh(new_camera_param)

    return {"status": status.HTTP_201_CREATED, "data": new_camera_param}

@router.get("/", status_code=status.HTTP_200_OK)
def get_all_camera_parameters(user: user_dependency,db: Session = Depends(getdb)):
    camera_parameters = db.query(CameraParameter).all()
    return {"status": status.HTTP_200_OK, "data": camera_parameters}


@router.get("/{camera_id}", status_code=status.HTTP_200_OK)
def get_camera_parameters_by_camera_id(
    user: user_dependency,
    camera_id: int,
    db: Session = Depends(getdb)
):
    # Query camera parameters along with station, site, and camera make
    camera_params = (
        db.query(
            CameraParameter.id.label("camera_param_id"),
            stationParameter.id.label("station_param_id"),
            AnalyserParameter.parameter_id,
            Parameter.name.label("parameter_name"),
            Analyser.id.label("analyser_id"),
            Analyser.analyser_name.label("analyser_name"),
            MonitoringType.id.label("monitoring_type_id"),
            MonitoringType.monitoring_type.label("monitoring_type"),
            AnalyserParameter.id.label("analyser_param_id"),
            Camera.make.label("camera_make"),
            Station.name.label("station_name"),
            Site.site_name.label("site_name")
        )
        .join(stationParameter, CameraParameter.station_parameter_id == stationParameter.id)
        .join(AnalyserParameter, stationParameter.analyser_param_id == AnalyserParameter.id)
        .join(Parameter, AnalyserParameter.parameter_id == Parameter.id)
        .join(Analyser, AnalyserParameter.analyser_id == Analyser.id)
        .join(MonitoringType, Parameter.monitoring_type_id == MonitoringType.id)
        .join(Camera, CameraParameter.camera_id == Camera.id)
        .join(Station, Camera.station_id == Station.id)
        .join(Site, Station.site_id == Site.id)
        .filter(CameraParameter.camera_id == camera_id)
        .all()
    )

    if not camera_params:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No camera parameters found for this camera"
        )

    # Build response
    response = []
    for param in camera_params:
        response.append({
            "camera_param_id": param.camera_param_id,
            "station_param_id": param.station_param_id,
            "parameter_id": param.parameter_id,
            "parameter_name": param.parameter_name,
            "analyser_id": param.analyser_id,
            "analyser_name": param.analyser_name,
            "monitoring_type_id": param.monitoring_type_id,
            "monitoring_type": param.monitoring_type,
            "analyser_parameter_id": param.analyser_param_id,
            "camera_make": param.camera_make,
            "station_name": param.station_name,
            "site_name": param.site_name
        })

    return {
        "status": status.HTTP_200_OK,
        "data": response,
        "detail": {
            "camera_make": response[0]["camera_make"] if response else "",
            "station_name": response[0]["station_name"] if response else "",
            "site_name": response[0]["site_name"] if response else ""
        }
    }
@router.delete("/{camera_param_id}", status_code=status.HTTP_200_OK)
def delete_camera_parameter(user: user_dependency,camera_param_id: int, db: Session = Depends(getdb)):
    camera_param = db.query(CameraParameter).filter(CameraParameter.id == camera_param_id).first()
    if not camera_param:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Camera-Parameter association not found")

    db.delete(camera_param)
    db.commit()
    return {"status": status.HTTP_200_OK, "message": "Deleted successfully"}

@router.get("/camera/{camera_id}/parameter-hourly-data")
async def get_camera_parameter_hourly_data(user: user_dependency,camera_id: int, db: Session = Depends(getdb)):
    try:
        camera_params = db.query(CameraParameter).filter(CameraParameter.camera_id == camera_id).all()
        
        if not camera_params:
            raise HTTPException(status_code=404, detail="Camera not found or no parameters associated")
        
        tz = timezone('Asia/Kolkata')
        end_time_local = dt.now(tz).replace(minute=0, second=0, microsecond=0)
        start_time_local = end_time_local - timedelta(hours=24)
        
        start_time = start_time_local.astimezone(UTC)
        end_time = end_time_local.astimezone(UTC)
        
        x_axis = [(start_time_local + timedelta(hours=i)).isoformat() for i in range(25)]
        all_parameters_data = []

        for camera_param in camera_params:
            # Fix: Retrieve stationParameter first
            station_param = db.query(stationParameter).filter(
                stationParameter.id == camera_param.station_parameter_id
            ).first()

            if not station_param:
                continue

            analyser_param = db.query(AnalyserParameter).filter(
                AnalyserParameter.id == station_param.analyser_param_id
            ).first()

            if not analyser_param:
                continue

            parameter = db.query(Parameter).filter(Parameter.id == analyser_param.parameter_id).first()
            if not parameter:
                continue

            parameter_id = parameter.id
            parameter_name = parameter.name
            parameter_unit = parameter.unit

            query = (
                db.query(
                    func.date_trunc('hour', SensorData.time).label('hour'),
                    func.avg(SensorData.value).label('avg_value')
                )
                .filter(
                    SensorData.parameter_id == parameter_id,
                    SensorData.time >= start_time,
                    SensorData.time <= end_time
                )
                .group_by(func.date_trunc('hour', SensorData.time))
                .order_by(func.date_trunc('hour', SensorData.time))
            )

            hourly_data = query.all()

            if hourly_data:
                data_dict = {
                    row.hour.astimezone(tz).replace(minute=0, second=0, microsecond=0).isoformat(): 
                    float(row.avg_value) if row.avg_value is not None else None
                    for row in hourly_data
                }
            else:
                data_dict = {}

            y_axis = [data_dict.get(ts, None) for ts in x_axis]

            parameter_data = {
                "parameter_name": parameter_name,
                "parameter_unit": parameter_unit,
                "y_axis": y_axis
            }
            all_parameters_data.append(parameter_data)

        if not all_parameters_data:
            raise HTTPException(status_code=404, detail="No data found for any parameters associated with this camera")

        return {
            "graphData": {
                "x_axis": x_axis,
                "parameters": all_parameters_data
            }
        }

    except Exception as e:
        logger.exception("Internal server error")
        raise HTTPException(
        status_code=500,
        detail="Internal server error. Please try again later."
    )
    finally:
        db.close()
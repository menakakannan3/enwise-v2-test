from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from ...database.session import getdb
from ...modals.masters import *
from datetime import datetime, timedelta
from ...utils.utils import response_strct
from collections import defaultdict
from ..auth.authentication import user_dependency

router = APIRouter()


@router.get('/api/site/{site_id}/cameras', summary="Get Cameras under a Site", tags=['dashboard'])
async def get_cameras_by_site_id(
    user: user_dependency,
    site_id: int,
    db: Session = Depends(getdb)
):
    cameras = (
        db.query(
            Camera,
            Station.name.label("station_name"),
            Parameter.id.label("parameter_id"),
            func.coalesce(Parameter.name, "Unknown").label("parameter_name")
        )
        .join(Station, Camera.station_id == Station.id)
        .outerjoin(CameraParameter, Camera.id == CameraParameter.camera_id)
        .outerjoin(stationParameter, CameraParameter.station_parameter_id == stationParameter.id)
        .outerjoin(AnalyserParameter, stationParameter.analyser_param_id == AnalyserParameter.id)
        .outerjoin(Parameter, AnalyserParameter.parameter_id == Parameter.id)
        .filter(Station.site_id == site_id)
        .all()
    )

    if not cameras:
        return response_strct(
            status_code=status.HTTP_200_OK,
            detail="No cameras found for this site",
            data=[]
        )

    camera_map = defaultdict(lambda: {
        "camera_id": None,
        "station_id": None,
        "station_name": None,
        "parameters": []
    })

    for camera, station_name, parameter_id, parameter_name in cameras:
        cam_data = camera_map[camera.id]
        cam_data["camera_id"] = camera.id
        cam_data["station_id"] = camera.station_id
        cam_data["station_name"] = station_name
        if parameter_id:  # Avoid appending null parameters
            cam_data["parameters"].append({
                "parameter_id": parameter_id,
                "parameter_name": parameter_name
            })

    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="Cameras fetched successfully",
        data=list(camera_map.values())
    )


@router.get("/api/site/{site_id}", tags=['site'] , summary="Get site by UID")
def get_site_by_uid( user: user_dependency,site_id: int, db: Session = Depends(getdb)):
    
    site = db.query(Site).filter(Site.siteuid == site_id).first()
    if not site:
        raise HTTPException(status_code=404, detail="Site not found")
    return site


@router.get('/api/camera/{camera_id}/cameras', summary="Get Cameras under a Station", tags=['dashboard'])
async def get_cameras_by_site_id(
    user: user_dependency,
    camera_id: int,
    db: Session = Depends(getdb)
):
    camera = db.query(Camera).filter(Camera.id == camera_id).first()
    if not camera:
        return {
            "status_code": status.HTTP_200_OK,
            "detail": "No cameras found",
            "data": []
        }

    station = db.query(Station).filter(Station.id == camera.station_id).first()
    site = db.query(Site).filter(Site.id == station.site_id).first() if station else None
    # **New: fetch group name**
    group = None
    if site and site.group_id:
        group = db.query(Group).filter(Group.id == site.group_id).first()

    now = datetime.utcnow()
    start_time = now - timedelta(hours=24)

    # Fetch all parameter IDs for the camera
    param_ids = db.query(CameraParameter.station_parameter_id).filter(CameraParameter.camera_id == camera.id).all()
    param_ids = [p[0] for p in param_ids if p[0] is not None]

    if not param_ids:
        spark_list = []
        spark_list_time = []
    else:
        sensor_data = (
            db.query(
                func.date_trunc('hour', SensorData.time).label("hour_time"),
                func.avg(SensorData.value).label("avg_value")
            )
            .filter(
                SensorData.station_id == camera.station_id,
                SensorData.parameter_id.in_(param_ids),
                SensorData.time >= start_time,
                SensorData.time <= now
            )
            .group_by(func.date_trunc('hour', SensorData.time))
            .order_by(func.date_trunc('hour', SensorData.time))
            .all()
        )

        spark_list = [str(row.avg_value) if row.avg_value is not None else "0" for row in sensor_data]
        spark_list_time = [row.hour_time.strftime("%Y-%m-%d %H:%M:%S") for row in sensor_data]

    return {
        "camersDetails": {
            "camera_id": camera.id,
            "make": camera.make,
            "modal": camera.modal,
            "rtsp_link": camera.rtsp_link,
            "connectivity_type": camera.connectivity_type,
            "location": camera.location,
            "bandwidth": camera.bandwidth,
            "night_vision": camera.night_vision,
            "ptz": camera.ptz,
            "zoom": camera.zoom,
            "ipc_camera": camera.ipc_camera,
            "station": {
                "station_id": station.id if station else None,
                "station_name": station.name if station else "Unknown",
                "latitude": station.latitude if station else None,
                "longitude": station.longitude if station else None
            },
            "site": {
                "site_id": site.id if site else None,
                "site_name": site.site_name if site else "Unknown",
                "address": site.address if site else "Unknown",
                "city": site.city if site else "Unknown",
                "state": site.state if site else "Unknown",
                "group_name": group.group_name if group else None,
                "auth_expiry":  site.auth_expiry.isoformat()     if site and site.auth_expiry else None
            },
            "graphData": {
                "sparkList": spark_list,
                "sparkListTime": spark_list_time
            }
        }
    }
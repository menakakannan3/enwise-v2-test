# OM VIGHNHARTAYE NAMO NAMAH :

from fastapi import APIRouter, Depends, Form, HTTPException, status
from sqlalchemy.orm import Session 
from sqlalchemy.sql import func
from typing import Optional
from datetime import datetime

router = APIRouter()

from ...utils.utils import *
from ...modals.masters import Camera, Station
from ...database.session import getdb 

from ..auth.authentication import user_dependency

from ...schemas.masterSchema import CameraCreation , CameraUpdate
from pydantic import ValidationError

@router.post('/api/camera/create/{station_id}', summary="Register a new camera", tags=['Camera'])
async def create_camera(
    user : user_dependency,
    station_id: int,
    make: str = Form(...),
    modal: str = Form(...),
    rtsp_link: str = Form(...),
    connectivity_type: str = Form(...),
    location: str = Form(...),
    bandwidth: float = Form(...),
    night_vision: bool = Form(...),
    ptz: bool = Form(...),
    zoom: bool = Form(...),
    ipc_camera: bool = Form(...),
    db: Session = Depends(getdb)
):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    existing_station = db.query(Station).filter(Station.id == station_id).first()
    if not existing_station:
        raise HTTPException(status_code=400, detail="Station with this ID does not exist")
    
    try:
        validated_data = CameraCreation(
            station_id=station_id,
            make=make,
            modal=modal,
            rtsp_link=rtsp_link,
            connectivity_type=connectivity_type,
            location=location,
            bandwidth=bandwidth,
            night_vision=night_vision,
            ptz=ptz,
            zoom=zoom,
            ipc_camera=ipc_camera
        )
    except ValidationError as e:
        return {"detail": e.errors()}
    
    new_camera = Camera(
        station_id=station_id,
        make=make,
        modal=modal,
        rtsp_link=rtsp_link,
        connectivity_type=connectivity_type,
        location=location,
        bandwidth=bandwidth,
        night_vision=night_vision,
        ptz=ptz,
        zoom=zoom,
        ipc_camera=ipc_camera
    )
    
    db.add(new_camera)
    db.commit()
    db.refresh(new_camera)

    return response_strct(
        status_code=status.HTTP_201_CREATED,
        data={
            "id": new_camera.id,
            "station_id": new_camera.station_id,
            "make": new_camera.make,
            "modal": new_camera.modal,
            "rtsp_link": new_camera.rtsp_link,
            "connectivity_type": new_camera.connectivity_type,
            "location": new_camera.location,
            "bandwidth": new_camera.bandwidth,
            "night_vision": new_camera.night_vision,
            "ptz": new_camera.ptz,
            "zoom": new_camera.zoom,
            "ipc_camera": new_camera.ipc_camera
        },
        detail="Camera registered successfully"
    )


@router.get('/api/station/camera/{station_id}/cameras', summary="Get Cameras under a Station", tags=['Camera'])
async def get_cameras_by_station_id(
    user: user_dependency,
    station_id: int,
    db: Session = Depends(getdb)
):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")

    # Fetch station + site info first
    station_site = (
        db.query(Station.name.label("station_name"), Site.site_name, Site.id.label("site_id"))
        .join(Site, Station.site_id == Site.id)
        .filter(Station.id == station_id)
        .first()
    )

    if not station_site:
        raise HTTPException(status_code=404, detail="Station not found")

    # Fetch cameras under this station
    cameras = db.query(Camera).filter(Camera.station_id == station_id).all()

    if not cameras:
        return response_strct(
            status_code=status.HTTP_200_OK,
            detail=[
                {"message": "No cameras found for this station"},
                {
                    "site_name": station_site.site_name,
                    "site_id": station_site.site_id,
                    "station_name": station_site.station_name,
                    "station_id": station_id
                }
            ],
            data=[]
        )

    return response_strct(
        status_code=status.HTTP_200_OK,
        detail=[
            {"message": "Cameras fetched successfully"},
            {
                "site_name": station_site.site_name,
                "site_id": station_site.site_id,
                "station_name": station_site.station_name,
                "station_id": station_id
            }
        ],
        data=[
            {
                "camera_id": camera.id,
                "station_id": camera.station_id,
                "make": camera.make,
                "modal": camera.modal,
                "rtsp_link": camera.rtsp_link,
                "connectivity_type": camera.connectivity_type,
                "location": camera.location,
                "bandwidth": camera.bandwidth,
                "night_vision": camera.night_vision,
                "ptz": camera.ptz,
                "zoom": camera.zoom,
                "ipc_camera": camera.ipc_camera
            }
            for camera in cameras
        ]
    )

@router.put('/api/camera/update/{camera_id}', summary="Update an existing camera", tags=['Camera'])
async def update_camera(
    user : user_dependency,
    camera_id: int,
    make: Optional[str] = Form(None),
    modal: Optional[str] = Form(None),
    rtsp_link: Optional[str] = Form(None),
    connectivity_type: Optional[str] = Form(None),
    location: Optional[str] = Form(None),
    bandwidth: Optional[str] = Form(None),
    night_vision: Optional[bool] = Form(None),
    ptz: Optional[bool] = Form(None),
    zoom: Optional[bool] = Form(None),
    ipc_camera: Optional[bool] = Form(None),
    db: Session = Depends(getdb)
):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    camera = db.query(Camera).filter(Camera.id == camera_id).first()
    if not camera:
        raise HTTPException(status_code=404, detail="Camera not found")
    
    try:
        validated_data = CameraUpdate(
            make=make,
            modal=modal,
            rtsp_link=rtsp_link,
            connectivity_type=connectivity_type,
            location=location,
            bandwidth=bandwidth,
            night_vision=night_vision,
            ptz=ptz,
            zoom=zoom,
            ipc_camera=ipc_camera
        )
    except ValidationError as e:
        return {"detail": e.errors()}
    
    if make:
        camera.make = make
    if modal:
        camera.modal = modal
    if rtsp_link:
        camera.rtsp_link = rtsp_link
    if connectivity_type:
        camera.connectivity_type = connectivity_type
    if location:
        camera.location = location
    if bandwidth is not None:
        camera.bandwidth = bandwidth
    if night_vision is not None:
        camera.night_vision = night_vision
    if ptz is not None:
        camera.ptz = ptz
    if zoom is not None:
        camera.zoom = zoom
    if ipc_camera is not None:
        camera.ipc_camera = ipc_camera
    
    db.commit()
    db.refresh(camera)

    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="Camera updated successfully",
        data={
            "id": camera.id,
            "station_id": camera.station_id,
            "make": camera.make,
            "modal": camera.modal,
            "rtsp_link": camera.rtsp_link,
            "connectivity_type": camera.connectivity_type,
            "location": camera.location,
            "bandwidth": camera.bandwidth,
            "night_vision": camera.night_vision,
            "ptz": camera.ptz,
            "zoom": camera.zoom,
            "ipc_camera": camera.ipc_camera
        }
    )


@router.delete('/api/camera/delete/{station_id}/{camera_id}', summary="Delete a specific camera under a station", tags=['Camera'])
async def delete_camera(
    user : user_dependency,
    station_id: int,
    camera_id: int,
    db: Session = Depends(getdb)
):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    camera = db.query(Camera).filter(Camera.id == camera_id, Camera.station_id == station_id).first()
    
    if not camera:
        raise HTTPException(status_code=404, detail="Camera not found for this station")

    db.delete(camera)
    db.commit()

    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="Camera deleted successfully",
        data={}
    )

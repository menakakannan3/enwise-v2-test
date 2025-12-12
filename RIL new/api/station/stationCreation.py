from sqlalchemy.orm import Session
from fastapi import APIRouter, HTTPException, Depends, Form, File, UploadFile
from sqlalchemy.exc import SQLAlchemyError
from starlette import status
from io import BytesIO
import pandas as pd
import datetime
from typing import Optional
from starlette import status
from ...schemas.masterSchema import *
from ...modals.masters import *
from ...database.session import getdb
from ...utils.utils import response_strct

from ..auth.authentication import user_dependency

router = APIRouter()

@router.post("/api/station/create/{site_id}", tags=["stations"])
def create_station(
    user : user_dependency,
    site_id: int ,
    name: str = Form(..., min_length=3, max_length=255),
    latitude: float = Form(... , ge=-90, le=90),
    longitude: float = Form(... , ge=-180, le=180 ),
    calibration_expiry_date: datetime.datetime = Form(...),
    db: Session = Depends(getdb),
):
    
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    existing_site = db.query(Site).filter(Site.id == site_id).first()
    if not existing_site:
        raise HTTPException(status_code=404 , detail="site not found")
    
    existing_station = db.query(Station).filter(
        Station.site_id == site_id, Station.name == name
    ).first()
    if existing_station:
        raise HTTPException(
            status_code=400, detail="A station with the same name already exists for this site"
        )
    last_station = db.query(Station).order_by(Station.id.desc()).first()
    new_station_id = (last_station.id + 1) if last_station else 1
    station_uid = f"EW_STAT_{new_station_id}"

    station = Station(
        name=name,
        latitude=latitude,
        longitude=longitude,
        calibration_expiry_date=calibration_expiry_date,
        site_id=site_id,
        station_uid = station_uid,
        created_by=1,  
        updated_by=1,
    )
    db.add(station)
    db.commit()
    db.refresh(station)

    # Convert station object to dict
    station_data = {key: value for key, value in station.__dict__.items() if not key.startswith("_")}
    return response_strct(
        status_code=status.HTTP_201_CREATED,
        detail="Station created successfully",
        data=station_data,
        error=""
    )


@router.get("/api/station/getStation/{site_id}", tags=['stations'])
def get_station_from_site(
    user: user_dependency,
    site_id: int,
    db: Session = Depends(getdb)
):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    
    site = db.query(Site).filter(Site.id == site_id).first()
    if site is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="site not found")
    
    stations = db.query(Station).filter(Station.site_id == site_id).all()

    if not stations:
        return {
            "status_code": 200,
            "detail": [
                {"message": "No stations found for this site"},
                {"site_name": site.site_name, "site_id": site.id}
            ],
            "data": []
        }

    return {
        "status_code": 200,
        "detail": [
            {"message": f"All stations for the site {site.site_name} fetched successfully"},
            {"site_name": site.site_name, "site_id": site.id}
        ],
        "data": stations
    }


@router.get("/api/station/all", tags=["stations"])
def get_all_stations(user : user_dependency , db: Session = Depends(getdb)):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    stations = db.query(Station).all()
    
    stations_data = [
        {key: value for key, value in station.__dict__.items() if not key.startswith("_")}
        for station in stations
    ]
    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="all sites fetched successfully",
        data=stations_data,
        error=""
    )


@router.get("/api/station/{station_id}", tags=["stations"])
def get_station(user : user_dependency , station_id: int, db: Session = Depends(getdb)):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    station = db.query(Station).filter(Station.id == station_id).first()
    
    if not station:
        raise HTTPException(status_code=404, detail="Station not found")

    station_data = {key: value for key, value in station.__dict__.items() if not key.startswith("_")}
    return response_strct(
         status_code=status.HTTP_200_OK,
        detail=f"station with id {station_id} fetched successfully",
        data=station,
        error=""
    )


@router.put("/api/station/update/{station_id}", tags=["stations"])
def update_station(
    user : user_dependency,
    station_id: int,
    name: Optional[str] = Form(None, min_length=3, max_length=255),
    latitude: Optional[float] = Form(None),
    longitude: Optional[float] = Form(None),
    calibration_expiry_date: Optional[datetime.datetime] = Form(None),
    site_id: Optional[int] = Form(None),
    db: Session = Depends(getdb),
):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    station = db.query(Station).filter(Station.id == station_id).first()

    if not station:
        raise HTTPException(status_code=404, detail="Station not found")
    
    if site_id:
        existing_site = db.query(Site).filter(Site.id == site_id).first()
        if not existing_site:
            raise HTTPException(status_code=404 , detail="site not found")
        
    if name and site_id:
        existing_station = db.query(Station).filter(
            Station.site_id == site_id, Station.name == name, Station.id != station_id
        ).first()
        if existing_station:
            raise HTTPException(
                status_code=400, detail="A station with the same name already exists for this site"
            )

    # Update fields if provided
    if name is not None:
        station.name = name
    if latitude is not None:
        station.latitude = latitude
    if longitude is not None:
        station.longitude = longitude
    if calibration_expiry_date is not None:
        station.calibration_expiry_date = calibration_expiry_date
    if site_id is not None:
        station.site_id = site_id

    station.updated_by = 1  # Hardcoded for now
    station.updated_at = datetime.datetime.utcnow()

    db.commit()
    db.refresh(station)

    station_data = {key: value for key, value in station.__dict__.items() if not key.startswith("_")}

    return response_strct(
         status_code=status.HTTP_200_OK,
        detail=f"station with id {station_id} updated successfully",
        data=station_data,
        error=""
    )


@router.delete("/api/station/delete/{station_id}", tags=["stations"])
def delete_station(user : user_dependency , station_id: int, db: Session = Depends(getdb)):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    station = db.query(Station).filter(Station.id == station_id).first()

    if not station:
        raise HTTPException(status_code=404, detail="Station not found")

    db.delete(station)
    db.commit()

    return response_strct(
         status_code=status.HTTP_200_OK,
        detail=f"station with id {station_id} deleted successfully",
        data="",
        error=""
    )
# OM VIGHNHARTAYE NAMO NAMAH :

from fastapi import APIRouter, Depends, Form, HTTPException, status
from sqlalchemy.orm import Session,joinedload
from sqlalchemy.sql import text , func
from typing import Optional, Annotated,List
from datetime import datetime
from pathlib import Path
from decimal import Decimal
import json
from sqlalchemy import select, join

router = APIRouter()

from ...utils.utils import *
from ...modals.masters import Device, Site  # Import the database models
from ...database.session import getdb 
from ...schemas.masterSchema import DeviceCreation , DeviceUpdate
from pydantic import ValidationError,BaseModel
from ..auth.authentication import user_dependency

@router.post('/api/device/create/{site_id}', summary="Register a new device", tags=['Device'])
async def create_device(
    user: user_dependency,
    site_id: int,
    device_name: str = Form(...),
    device_uid: str = Form(...),
    latitude: float = Form(...),
    longitude: float = Form(...),
    device_type: str = Form(...),
    chip_id: str = Form(...),
    db: Session = Depends(getdb)
):
    # Do imports INSIDE the API to avoid conflicts
    import hashlib
    from sqlalchemy.exc import SQLAlchemyError
    from fastapi import HTTPException, status
    from ...modals.masters import Site, Device
    from ...schemas.masterSchema import DeviceCreation
    from ...utils.utils import response_strct

    # ✅ Basic authentication check
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")

    # ✅ Check site exists
    existing_site = db.query(Site).filter(Site.id == site_id).first()
    if not existing_site:
        raise HTTPException(status_code=400, detail="Site with this ID does not exist")

    # ✅ Check if device UID already exists
    if db.query(Device).filter(Device.device_uid == device_uid).first():
        raise HTTPException(status_code=400, detail="Device with this UID already exists")

    # ✅ Check if chip ID already exists
    if db.query(Device).filter(Device.chip_id == chip_id).first():
        raise HTTPException(status_code=400, detail="Device with this chip ID already exists")

    # ✅ Validate data
    try:
        validated_data = DeviceCreation(
            site_id=site_id,
            device_name=device_name,
            latitude=latitude,
            longitude=longitude,
            device_type=device_type,
            chip_id=chip_id
        )
    except Exception as e:
        raise HTTPException(status_code=422, detail="Validation error")

    try:
        # ✅ Generate the 32-character auth key (device_uid + chip_id)
        raw_string = f"{device_uid}_{chip_id}"
        auth_key = hashlib.md5(raw_string.encode()).hexdigest()  # 32 characters

        # ✅ Create the new device
        new_device = Device(
            device_uid=device_uid,
            device_name=device_name,
            latitude=latitude,
            chip_id=chip_id,
            longitute=longitude,
            device_type=device_type,
            device_authkey=auth_key,
            site_id=site_id,
            created_at=datetime.datetime.now()
        )

        db.add(new_device)
        db.commit()
        db.refresh(new_device)

        # ✅ Return success
        return response_strct(
            status_code=status.HTTP_201_CREATED,
            data={
                "id": new_device.id,
                "device_uid": new_device.device_uid,
                "device_name": new_device.device_name,
                "latitude": new_device.latitude,
                "longitude": new_device.longitute,
                "device_type": new_device.device_type,
                "site_id": new_device.site_id,
                "created_at": new_device.created_at,
                "chip_id": new_device.chip_id,
                "device_authkey": new_device.device_authkey
            },
            detail="Device registered successfully"
        )

    except SQLAlchemyError as db_err:
        db.rollback()
        raise HTTPException(status_code=500, detail="Database error")
    except Exception as e:
        raise HTTPException(status_code=500, detail="Unexpected error")



@router.put('/api/device/update/{device_id}', summary="Update an existing device", tags=['Device'])
async def update_device(
    user : user_dependency,
    device_id: int ,  
    device_name: Optional[str] = Form(None),
    latitude: Optional[str] = Form(None),
    longitude: Optional[str] = Form(None),
    device_type: Optional[str] = Form(None),
    db: Session = Depends(getdb)
):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")

    # Fetch the existing device
    device = db.query(Device).filter(Device.id == device_id).first()
    if not device:
        raise HTTPException(status_code=404, detail="Device not found")
    
    try:
        validated_data = DeviceUpdate(
            device_name=device_name,
            latitude=latitude,
            longitude=longitude,
            device_type=device_type
        )
    except ValidationError as e:
        return {"detail": e.errors()}

    # Update fields if provided
    if device_name:
        device.device_name = device_name
    if latitude:
        try:
            device.latitude = Decimal(latitude)  # Convert to Decimal
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid latitude format")
    if longitude:
        try:
            device.longitute = Decimal(longitude)  # Convert to Decimal
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid longitude format")
    if device_type:
        device.device_type = device_type

    # Update the `updated_at` timestamp
    device.updated_at = func.now()

    # Commit the updates to the database
    db.commit()
    db.refresh(device)

    # Return success response
    return response_strct(
        status_code=status.HTTP_200_OK,
        data={
            "device_id": device.id,
            "device_uid": device.device_uid,
            "device_name": device.device_name,
            "latitude": float(device.latitude), 
            "longitude": float(device.longitute),
            "device_type": device.device_type,
            "updated_at": device.updated_at
        },
        detail="Device updated successfully"
    )

class StationOut(BaseModel):
    id: int
    station_uid: str
    name: str
    site_id: int
    site_uid: str

    class Config:
        orm_mode = True

@router.get("/api/sites/{site_id}/stations/unassigned", response_model=List[StationOut])
def get_unassigned_stations_for_site(site_id: int,user: user_dependency, db: Session = Depends(getdb)):
    """
    Returns all stations under `site_id` that have not been mapped to any device.
    """
    q = (
        db.query(Station, Site.siteuid.label("site_uid"))
        .join(Site, Station.site_id == Site.id)
        .outerjoin(DeviceStation, Station.id == DeviceStation.station_id)
        .filter(Station.site_id == site_id)
        .filter(DeviceStation.station_id == None)
    )
    results = q.all()
    return [
        StationOut(
            id=st.id,
            station_uid=st.station_uid,
            name=st.name,
            site_id=st.site_id,
            site_uid=site_uid
        )
        for st, site_uid in results
    ]


class AssignStationsIn(BaseModel):
    station_ids: list[int]

@router.post(
    "/api/devices/{device_id}/stations",
    status_code=status.HTTP_204_NO_CONTENT,
)
def assign_stations_to_device(
    user: user_dependency,
    device_id: int,
    payload: AssignStationsIn,
    db: Session = Depends(getdb),
):
    # 1) verify device exists
    device = db.query(Device).get(device_id)
    if not device:
        raise HTTPException(404, f"Device {device_id} not found")

    # 2) optionally: validate each station exists
    stations = db.query(Station)\
                 .filter(Station.id.in_(payload.station_ids))\
                 .all()
    if len(stations) != len(payload.station_ids):
        missing = set(payload.station_ids) - {s.id for s in stations}
        raise HTTPException(
            400,
            f"Station IDs not found: {sorted(missing)}"
        )

    # 3) insert mappings (skip ones that already exist)
    for sid in payload.station_ids:
        exists = db.query(DeviceStation)\
                   .filter_by(device_id=device_id, station_id=sid)\
                   .first()
        if not exists:
            db.add(DeviceStation(device_id=device_id, station_id=sid))

    db.commit()
    return 
from sqlalchemy.orm import aliased

# @router.get('/api/device/{site_id}/devices', summary="Get Devices under a Site", tags=['Device'])
# async def get_devices_by_site_id(
#     user: user_dependency,
#     site_id: int,
#     db: Session = Depends(getdb)
# ):
#     # ✅ fetch the site first
#     site = db.query(Site).filter(Site.id == site_id).first()
#     if not site:
#         return response_strct(
#             status_code=status.HTTP_404_NOT_FOUND,
#             detail=[{"message": "Site not found"}],
#             data=[]
#         )

#     # ✅ fetch devices under this site
#     result = (
#         db.query(Device, Site.site_name)
#         .join(Site, Device.site_id == Site.id)
#         .filter(Device.site_id == site_id)
#         .all()
#     )

#     # Case: No devices
#     if not result:
#         return response_strct(
#             status_code=status.HTTP_200_OK,
#             detail=[
#                 {"message": "No devices found for this site"},
#                 {"site_name": site.site_name, "site_id": site.id}
#             ],
#             data=[]
#         )

#     # Case: Devices exist
#     return response_strct(
#         status_code=status.HTTP_200_OK,
#         detail=[
#             {"message": "Devices fetched successfully"},
#             {"site_name": site.site_name, "site_id": site.id}
#         ],
#         data=[
#             {
#                 "device_id": device.id,
#                 "device_uid": device.device_uid,
#                 "device_name": device.device_name,
#                 "latitude": device.latitude,
#                 "longitude": device.longitute,
#                 "device_type": device.device_type,
#                 "created_at": device.created_at,
#                 "updated_at": device.updated_at,
#                 "chip_id": device.chip_id,
#                 "status": device.device_status,
#                 "last_ping": device.last_ping,

#                 "site_name": site_name   # 👈 from join
#             }
#             for device, site_name in result
#         ]
#     )



@router.get('/api/device/{site_id}/devices', summary="Get Devices under a Site", tags=['Device'])
async def get_devices_by_site_id(
    user: user_dependency,
    site_id: int,
    db: Session = Depends(getdb)
):
    # Fetch site
    site = db.query(Site).filter(Site.id == site_id).first()
    if not site:
        return response_strct(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=[{"message": "Site not found"}],
            data=[]
        )

    # Fetch devices under this site
    result = (
        db.query(Device, Site.site_name)
        .join(Site, Device.site_id == Site.id)
        .filter(Device.site_id == site_id)
        .all()
    )

    # No devices found
    if not result:
        return response_strct(
            status_code=status.HTTP_200_OK,
            detail=[
                {"message": "No devices found for this site"},
                {"site_name": site.site_name, "site_id": site.id}
            ],
            data=[]
        )

    # Adjust last_ping: minus 5 hours 30 mins
    offset = timedelta(hours=5, minutes=30)

    # Devices exist
    return response_strct(
        status_code=status.HTTP_200_OK,
        detail=[
            {"message": "Devices fetched successfully"},
            {"site_name": site.site_name, "site_id": site.id}
        ],
        data=[
            {
                "device_id": device.id,
                "device_uid": device.device_uid,
                "device_name": device.device_name,
                "latitude": device.latitude,
                "longitude": device.longitute,
                "device_type": device.device_type,
                "created_at": device.created_at,
                "updated_at": device.updated_at,
                "chip_id": device.chip_id,
                "status": device.device_status,
                "last_ping": (device.last_ping - offset) if device.last_ping else None,
                "site_name": site_name
            }
            for device, site_name in result
        ]
    )




@router.get('/api/device/{site_id}/devices/admin', summary="Get Devices under a Site", tags=['Device'])
async def get_devices_by_site_id(
    user: user_dependency,
    site_id: int,
    db: Session = Depends(getdb)
):
    # ✅ fetch the site first
    site = db.query(Site).filter(Site.id == site_id).first()
    if not site:
        return response_strct(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=[{"message": "Site not found"}],
            data=[]
        )

    # ✅ fetch devices under this site
    result = (
        db.query(Device, Site.site_name)
        .join(Site, Device.site_id == Site.id)
        .filter(Device.site_id == site_id)
        .all()
    )

    # Case: No devices
    if not result:
        return response_strct(
            status_code=status.HTTP_200_OK,
            detail=[
                {"message": "No devices found for this site"},
                {"site_name": site.site_name, "site_id": site.id}
            ],
            data=[]
        )

    # Case: Devices exist
    return response_strct(
        status_code=status.HTTP_200_OK,
        detail=[
            {"message": "Devices fetched successfully"},
            {"site_name": site.site_name, "site_id": site.id}
        ],
        data=[
            {
                "device_id": device.id,
                "device_uid": device.device_uid,
                "device_name": device.device_name,
                "latitude": device.latitude,
                "longitude": device.longitute,
                "device_type": device.device_type,
                "created_at": device.created_at,
                "updated_at": device.updated_at,
                "chip_id": device.chip_id,
                "status": device.device_status,
                "last_ping": device.last_ping,
                "device_authkey": device.device_authkey, 
                "site_name": site_name   # 👈 from join
            }
            for device, site_name in result
        ]
    )





@router.get("/api/devices/{device_id}/stations", response_model=List[StationOut])
def get_stations_for_device(device_id: int,user: user_dependency, db: Session = Depends(getdb)):
    """
    Returns all stations assigned to the given device.
    """
    # 1. verify device exists
    device = db.query(Device).get(device_id)
    if not device:
        raise HTTPException(404, f"Device {device_id} not found")

    # 2. join Station -> DeviceStation -> Site to fetch site_uid
    q = (
        db.query(Station, Site.siteuid.label("site_uid"))
        .join(DeviceStation, Station.id == DeviceStation.station_id)
        .join(Site, Station.site_id == Site.id)
        .filter(DeviceStation.device_id == device_id)
    )

    results = q.all()

    return [
        StationOut(
            id=st.id,
            station_uid=st.station_uid,
            name=st.name,
            site_id=st.site_id,
            site_uid=site_uid
        )
        for st, site_uid in results
    ]
@router.get('/api/device/{device_id}', summary="Get Device by UID", tags=['Device'])
async def get_device_by_id(
    user : user_dependency,
    device_id: int,
    db: Session = Depends(getdb)
):
    if user is None or user['role'] != 'admin':
        raise HTTPException(status_code=401, detail="Authentication failed")
    # Fetch the device with the given device_uid
    device = db.query(Device).filter(Device.id ==device_id ).first()

    # If device not found
    if not device:
        raise HTTPException(status_code=404, detail="Device not found")

    # Return the device details
    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="Device fetched successfully",
        data={
            "device_id": device.id,
            "device_uid": device.device_uid,
            "device_name": device.device_name,
            "latitude": device.latitude,
            "longitude": device.longitute,
            "device_type": device.device_type,
            "created_at": device.created_at,
            "updated_at": device.updated_at,
            "chip_id": device.chip_id,
            "site_id": device.site_id,
            "last_ping": device.last_ping,
            "status": device.status,
            "device_status": device.device_status,
            "device_authkey": device.device_authkey
        }
    )

@router.get('/api/device/{site_id}/stats', summary="Get Device Stats under a Site", tags=['Device'])
async def get_device_stats_by_site_id(
    user: user_dependency,
    site_id: int,
    db: Session = Depends(getdb)
):
    devices = db.query(Device).filter(Device.site_id == site_id).all()

    if not devices:
        return response_strct(
            status_code=status.HTTP_200_OK,
            detail="No devices found for this site",
            data={
                "total": 0,
                "online": 0,
                "offline": 0
            }
        )

    total_devices = len(devices)
    online_devices = sum(1 for d in devices if d.device_status and d.device_status.lower() == "online")
    offline_devices = total_devices - online_devices

    return response_strct(
        status_code=status.HTTP_200_OK,
        detail="Device stats fetched successfully",
        data={
            "total": total_devices,
            "online": online_devices,
            "offline": offline_devices
        }
    )




class GeneratedConfigOut(BaseModel):
    device_id: int
    status: dict


def build_device_config(device_id: int, db: Session) -> dict:
    # [same as before: fetch station_ids, station_rows, site_uid, device]
    rows = db.execute(
        select(DeviceStation.station_id)
        .where(DeviceStation.device_id == device_id)
    ).all()
    station_ids = [r.station_id for r in rows]
    if not station_ids:
        raise HTTPException(400, f"No stations mapped to device {device_id}")

    station_rows = db.execute(
        select(Station.id, Station.station_uid, Station.site_id)
        .where(Station.id.in_(station_ids))
    ).all()
    if not station_rows:
        raise HTTPException(400, "Mapped station IDs not found")

    site_ids = {r.site_id for r in station_rows}
    if len(site_ids) > 1:
        raise HTTPException(400, "Stations span multiple sites")
    site = db.get(Site, site_ids.pop())
    device = db.get(Device, device_id)
    if not device:
        raise HTTPException(404, f"Device {device_id} not found")

    # build JSON payload
    import datetime as dt_mod
    ist     = dt_mod.timezone(dt_mod.timedelta(hours=5, minutes=30))
    now_ist = dt_mod.datetime.now(ist).isoformat()

    cfg = {
        "site_uid":   site.siteuid,
        "chipid":     device.chip_id,
        "device_uid": device.device_uid,
        "timestamp":  now_ist,
        "data":       [],
    }

    for st_id, st_uid, _ in station_rows:
        ap_rows = db.execute(
            select(stationParameter.analyser_param_id)
            .where(stationParameter.station_id == st_id)
        ).all()
        ap_ids = [r.analyser_param_id for r in ap_rows]
        if not ap_ids:
            continue

        j = join(
            AnalyserParameter, Analyser,
            AnalyserParameter.analyser_id == Analyser.id
        ).join(
            Parameter, AnalyserParameter.parameter_id == Parameter.id
        )
        mappings = db.execute(
            select(Analyser.analyser_uid, Parameter.uuid)
            .select_from(j)
            .where(AnalyserParameter.id.in_(ap_ids))
        ).all()

        for analyser_uid, parameter_uuid in mappings:
            cfg["data"].append({
                "station_uid":  st_uid,
                "analyser_id":  analyser_uid,
                "parameter_id": parameter_uuid,
                "value":        0
            })

    return cfg


@router.post(
    "/api/devices/{device_id}/generate-config",
    response_model=GeneratedConfigOut,
    status_code=status.HTTP_200_OK,
)
def generate_device_config(
    device_id: int,
    user: user_dependency,
    db: Session = Depends(getdb),
):
    # only admin
    if user is None or user.get("role") != "admin":
        raise HTTPException(401, "Authentication failed")

    # build the JSON payload
    cfg = build_device_config(device_id, db)

    # save JSON string into device.status
    device: Device = db.get(Device, device_id)
    device.status = json.dumps(cfg)
    db.add(device)
    db.commit()

    return GeneratedConfigOut(device_id=device_id, status=cfg)




class DeviceStatusOut(BaseModel):
    device_id: int
    status: dict | None


@router.get(
    "/api/devices/{device_id}/json",
    response_model=DeviceStatusOut,
    status_code=status.HTTP_200_OK,
)
def get_device_status(
    device_id: int,
    user:user_dependency,
    db: Session = Depends(getdb),
):
    # only admin
    if user is None or user.get("role") != "admin":
        raise HTTPException(status_code=401, detail="Authentication failed")

    device = db.get(Device, device_id)
    if not device:
        raise HTTPException(status_code=404, detail=f"Device {device_id} not found")

    # parse JSON string from the status column, if present
    try:
        status_obj = json.loads(device.status) if device.status else None
    except json.JSONDecodeError:
        raise HTTPException(
            status_code=500,
            detail="Invalid JSON stored in device.status"
        )

    return DeviceStatusOut(device_id=device_id, status=status_obj)
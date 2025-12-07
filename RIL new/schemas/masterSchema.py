# OM VIGHNHARTAYE NAMO NAMAH: 


from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime

class SiteCreation(BaseModel):
    site_name: str = Field(..., min_length=3, max_length=100)
    address: str = Field(..., min_length=5, max_length=255)
    city: str = Field(..., min_length=2, max_length=100)
    state: str = Field(..., min_length=2, max_length=100)
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    group_uuid: Optional[str] = None
    auth_key: str = Field(..., min_length=10, max_length=50)


class SiteUpdate(BaseModel):
    site_name: Optional[str] = Field(None, min_length=3, max_length=100)
    address: Optional[str] = Field(None, min_length=5, max_length=255)
    city: Optional[str] = Field(None, min_length=2, max_length=100)
    state: Optional[str] = Field(None, min_length=2, max_length=100)
    latitude: Optional[float] = Field(None, ge=-90, le=90)
    longitude: Optional[float] = Field(None, ge=-180, le=180)
    group_uuid: Optional[str] = None

class CameraCreation(BaseModel):
    station_id: int = Field(...)
    make: str = Field(..., min_length=2, max_length=100)
    modal: str = Field(..., min_length=2, max_length=100)
    rtsp_link: str = Field(..., min_length=5, max_length=255)
    connectivity_type: str = Field(..., min_length=2, max_length=50)
    location: str = Field(..., min_length=2, max_length=100)
    bandwidth: float = Field(..., ge=0)
    night_vision: bool = Field(...)
    ptz: bool = Field(...)
    zoom: bool = Field(...)
    ipc_camera: bool = Field(...)

class CameraUpdate(BaseModel):
    make: Optional[str] = None
    modal: Optional[str] = None
    rtsp_link: Optional[str] = None
    connectivity_type: Optional[str] = None
    location: Optional[str] = None
    bandwidth: Optional[str] = None
    night_vision: Optional[bool] = None
    ptz: Optional[bool] = None
    zoom: Optional[bool] = None
    ipc_camera: Optional[bool] = None


class DeviceCreation(BaseModel):
    site_id: int = Field(...)
    device_name: str = Field(..., min_length=2, max_length=100)
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    device_type: str = Field(..., min_length=2, max_length=50)
    chip_id: str = Field(..., min_length=5, max_length=50)


class DeviceUpdate(BaseModel):
    device_name: Optional[str] = Field(None, min_length=2, max_length=100)
    latitude: Optional[float] = Field(None, ge=-90, le=90)
    longitude: Optional[float] = Field(None, ge=-180, le=180)
    device_type: Optional[str] = Field(None, min_length=2, max_length=50)


class RawDatum(BaseModel):
    timestamp: str
    value: float

class RawReportResponse(BaseModel):
    raw_data: List[RawDatum]

class StationParameterUpdateRequest(BaseModel):
    is_editable: Optional[bool] = None
    param_interval: Optional[int] = None

class TotLastItem(BaseModel):
    parameter_name: str
    tot_last: float
    tot_time: datetime

class BlockUpdate(BaseModel):
    parameter_name: str = Field(..., alias="Block ID")
    value: float
    time: datetime

    class Config:
        allow_population_by_field_name = True


class UpdateTotaliserPayload(BaseModel):
    blocks: List[BlockUpdate]


class Totaliser6amBlock(BaseModel):
    block_id: str              = Field(..., alias="Block ID")
    value:    Optional[float]
    time:     Optional[datetime]

    class Config:
        # allow using both .block_id and "Block ID" in input
        allow_population_by_field_name = True
        # when serializing, use the alias "Block ID"
        by_alias = True

class Totaliser6amUpdateRequest(BaseModel):
    blocks: List[Totaliser6amBlock]

class Totaliser6amResponse(BaseModel):
    blocks: List[Totaliser6amBlock]

class StationFormulaAlarmOut(BaseModel):
    id: int
    plant_name: str
    station_name: str
    is_alarm: bool

    class Config:
        orm_mode = True

class BulkAlarmUpdateItem(BaseModel):
    id: int
    is_alarm: bool

class BulkAlarmUpdateRequest(BaseModel):
    updates: List[BulkAlarmUpdateItem]

class StationCalibrationUpdate(BaseModel):
    station_id: int
    calib_from_ist: str  # e.g., "2025-11-20 14:30:00"  (IST string)
    calib_to_ist: str    # e.g., "2025-11-25 14:30:00"

    class Config:
        from_attributes = True
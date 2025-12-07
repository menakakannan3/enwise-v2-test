from fastapi import Depends, FastAPI, HTTPException, APIRouter, Form,Query
from sqlalchemy import func, select,literal,text
from sqlalchemy.orm import Session
from pydantic import BaseModel
from datetime import datetime, date, time
from typing import List, Optional , Union
from ...modals.masters import *
from ...database.session import getdb
from ...utils.utils import *
from ..auth.authentication import get_current_user,user_dependency
from starlette import status
from zoneinfo import ZoneInfo
import datetime as dt
from dateutil import parser

router = APIRouter()


IST = ZoneInfo("Asia/Kolkata")

class ReportResponse(BaseModel):
    site_name: str
    parameter_name: str
    station_name: str
    x_axis: List[datetime.datetime]
    y_axis: List[float]

@router.post("/api/sensor-data-report/{site_id}")
async def get_sensor_data_report(
    user: user_dependency,
    site_id: int,
    from_date: str = Form(...),
    to_date:   str = Form(...),
    station_id: Union[int, str]        = Form("all"),
    station_param_id: Union[int, str]  = Form("all"),
    monitoring_type_id: Union[int, str]= Form("all"),
    time_interval: str                 = Form("1hr"),
    db: Session                        = Depends(getdb),
):
    # Parse timezone-aware datetime inputs
    start_date = parser.isoparse(from_date)
    end_date   = parser.isoparse(to_date)

    # Force into IST timezone (to make sure all comparisons are aligned)
    ist = timezone(timedelta(hours=5, minutes=30))
    ist_start = start_date.astimezone(ist)
    ist_end   = end_date.astimezone(ist)

    # For time_interval = "1day", extend end by 1 day
    if time_interval == "1day":
        ist_end += timedelta(days=1)

    # Define valid views and buckets
    VALID_TIME_INTERVALS = ["15min", "1hr", "1day"]
    TIME_INTERVAL_TABLES = {
        "15min": {"table": "sensor_agg_15min", "bucket_col": "bucket"},
        "1hr":   {"table": "sensor_agg_1hr", "bucket_col": "bucket"},
        "1day":  {"table": "sensor_agg_1hr", "bucket_col": "bucket"},
    }

    if time_interval not in VALID_TIME_INTERVALS:
        raise HTTPException(status_code=400, detail=f"Invalid interval. Allowed: {VALID_TIME_INTERVALS}")

    table_info = TIME_INTERVAL_TABLES[time_interval]
    table_name = table_info["table"]
    bucket_col = table_info["bucket_col"]

    filters = [
        "s.id = :site_id",
        f"agg.{bucket_col} >= :ist_start",
        f"agg.{bucket_col} < :ist_end"
    ]
    params = {
        "site_id": site_id,
        "ist_start": ist_start,
        "ist_end": ist_end
    }

    if station_id != "all":
        filters.append("st.id = :station_id")
        params["station_id"] = int(station_id)

    if monitoring_type_id != "all":
        filters.append("mt.id = :monitoring_type_id")
        params["monitoring_type_id"] = int(monitoring_type_id)

    if station_param_id != "all":
        filters.append("agg.station_param_id = :station_param_id")
        params["station_param_id"] = int(station_param_id)

    where_clause = " AND ".join(filters)

    # Adjust bucket expression for 1day grouping
    if time_interval == "1day":
        bucket_expr = f"date_trunc('day', agg.{bucket_col})"
    else:
        bucket_expr = f"agg.{bucket_col}"

    sql = f"""
        SELECT
            {bucket_expr} AS time_interval,
            AVG(agg.avg_value) AS avg_value,
            s.site_name,
            p.name AS parameter_name,
            a.analyser_name,
            st.name AS station_name,
            mt.monitoring_type AS monitoring_type_name,
            CONCAT(s.address, ', ', s.city, ', ', s.state) AS site_address
        FROM {table_name} agg
        JOIN station_parameters sp ON agg.station_param_id = sp.id
        JOIN stations st             ON sp.station_id = st.id
        JOIN site s                  ON st.site_id = s.id
        JOIN analyser_parameter ap   ON sp.analyser_param_id = ap.id
        JOIN analysers a             ON ap.analyser_id = a.id
        JOIN parameters p            ON ap.parameter_id = p.id
        JOIN monitoring_types mt     ON p.monitoring_type_id = mt.id
        WHERE {where_clause}
        GROUP BY {bucket_expr}, s.site_name, p.name,
                 a.analyser_name, mt.monitoring_type, st.name,
                 s.address, s.city, s.state
        ORDER BY mt.monitoring_type, {bucket_expr};
    """

    rows = db.execute(text(sql), params).fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail="No data found")

    group_name = db.execute(
        select(Group.group_name)
        .join(Site, Site.group_id == Group.id)
        .where(Site.id == site_id)
    ).scalar() or "No group"

    response = {}
    for ti, avg, site_nm, param_nm, analyser_nm, station_nm, mon_type, site_addr in rows:
        bucket = response.setdefault(mon_type, {
            "monitoring_type_name": mon_type,
            "site_name": site_nm,
            "site_address": site_addr,
            "data": []
        })
        bucket["data"].append({
            "time_interval": ti.strftime("%Y-%m-%d %H:%M:%S"),
            "avg_value": round(float(avg), 2),
            "group_name": group_name,
            "parameter_name": param_nm,
            "analyser_name": analyser_nm,
            "station_name": station_nm
        })

    return {"sensor_data": list(response.values())}




@router.get("/api/site/summary", tags=['site_users'])
def get_id_at_login(
    user: dict = Depends(get_current_user),
    db: Session = Depends(getdb)
):
    try:
        user_id   = user['user_id']
        userRole  = user['role']

        # ←–– fetch the User record and extract their name
        user_obj  = db.query(User).filter(User.id == user_id).first()    # ← added
        user_name = user_obj.username if user_obj else None                  # ← added

        if userRole == "superAdmin":
            return response_strct(
                status_code=status.HTTP_200_OK,
                detail="user admin authenticated",
                data={
                    "userRole": userRole,
                    "user_name": user_name                              # ← added
                },
                error=""
            )

        site_association = (
            db.query(SiteUser)
              .filter(SiteUser.user_id == user_id)
              .first()
        )
        if not site_association:
            return response_strct(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Not authorised",
                data={
                    "user_name": user_name
                },
                error=""
            )

        return response_strct(
            status_code=status.HTTP_200_OK,
            detail="Sites associated with user fetched successfully",
            data={
                "site_id": site_association.site_id,
                "userRole": userRole,
                "user_name": user_name                                  # ← added
            },
            error=""
        )

    except Exception as e:
        return response_strct(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred",
            data={},
            error=''
        )
    
@router.get("/api/site/{site_id}/has_camera")
def check_site_has_camera(site_id: int,user: user_dependency, db: Session = Depends(getdb)):
    # Subquery to check if any station with this site_id has a camera
    has_camera = (
        db.query(Camera)
        .join(Station, Camera.station_id == Station.id)
        .filter(Station.site_id == site_id)
        .first()
    )
    
    return {"site_id": site_id, "has_camera": bool(has_camera)}



class TotaliserUsagePoint(BaseModel):
    date: dt.date
    usage: Optional[float] = None

class TotaliserUsageResponse(BaseModel):
    site_id: int
    station_id: int
    station_param_id: int
    parameter_name: str  # resolved pram_lable
    data: List[TotaliserUsagePoint]

    class Config:
        from_attributes = True  # pydantic v2


@router.get("/api/totaliser-usage", response_model=TotaliserUsageResponse)
def get_totaliser_usage(
     user: user_dependency,
    site_id: int = Query(..., description="Site ID"),
    station_id: int = Query(..., description="Station ID"),
    station_param_id: int = Query(..., description="Station Parameter ID"),
    from_date: dt.datetime = Query(..., description="Start datetime (e.g. 2025-08-01T00:00:00+05:30)"),
    to_date: dt.datetime = Query(..., description="End datetime (e.g. 2025-08-18T23:59:59+05:30)"),
    db: Session = Depends(getdb),
):
    """
    Resolve pram_lable for the given station_param_id, then fetch rows from
    daily_totaliser_usage where parameter_name == pram_lable and site_id matches,
    for all dates between [from_date, to_date] (inclusive).
    """

    # --- Normalize datetimes to IST and convert to dates (inclusive range) ---
    def _to_ist_date(d: dt.datetime) -> dt.date:
        # If naive, assume IST; else convert to IST
        if d.tzinfo is None:
            d = d.replace(tzinfo=IST)
        return d.astimezone(IST).date()

    start_date = _to_ist_date(from_date)
    end_date = _to_ist_date(to_date)

    if end_date < start_date:
        raise HTTPException(status_code=400, detail="to_date must be on/after from_date")

    # --- Validate station_param_id belongs to station_id and that station to site_id ---
    sp_row = (
        db.query(stationParameter, Station)
        .join(Station, Station.id == stationParameter.station_id)
        .filter(stationParameter.id == station_param_id)
        .first()
    )
    if not sp_row:
        raise HTTPException(status_code=404, detail="station_param_id not found")

    sp, st = sp_row

    if sp.station_id != station_id:
        raise HTTPException(
            status_code=400,
            detail=f"station_param_id {station_param_id} does not belong to station_id {station_id}",
        )
    if st.site_id != site_id:
        raise HTTPException(
            status_code=400,
            detail=f"station_id {station_id} does not belong to site_id {site_id}",
        )

    # Resolve the parameter name to use in daily_totaliser_usage
    # (parameter_name column equals station_parameters.pram_lable)
    param_name = (sp.pram_lable or "").strip()
    if not param_name:
        raise HTTPException(
            status_code=400,
            detail=f"station_param_id {station_param_id} has empty pram_lable",
        )

    # --- Query daily_totaliser_usage for the date range ---
    rows = (
        db.query(DailyTotaliserUsage.date, DailyTotaliserUsage.usage)
        .filter(DailyTotaliserUsage.site_id == site_id)
        .filter(DailyTotaliserUsage.parameter_name == param_name)
        .filter(DailyTotaliserUsage.date >= start_date)
        .filter(DailyTotaliserUsage.date <= end_date)
        .order_by(DailyTotaliserUsage.date.asc())
        .all()
    )

    data = [
        TotaliserUsagePoint(
            date=r[0],
            usage=float(r[1]) if r[1] is not None else None,
        )
        for r in rows
    ]

    return TotaliserUsageResponse(
        site_id=site_id,
        station_id=station_id,
        station_param_id=station_param_id,
        parameter_name=param_name,
        data=data,
    )
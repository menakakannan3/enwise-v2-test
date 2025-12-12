from fastapi import FastAPI, HTTPException , APIRouter , Depends
from sqlalchemy.orm import Session 
from fastapi import APIRouter, HTTPException, Depends, Form, File, UploadFile, Query
from sqlalchemy.exc import SQLAlchemyError 
from starlette import status
from sqlalchemy.sql import text

from datetime import datetime, timedelta, timezone
from typing import Optional
from sqlalchemy import select, func, case,text
from ...schemas.masterSchema import *
from ...modals.masters import *
from ...database.session import getdb
from ...utils.utils import response_strct
from collections import defaultdict
from typing import Dict, List
from ..auth.authentication import user_dependency
from ...utils.permissions import enforce_site_access


 # Assuming you have a database session dependency

router = APIRouter()

@router.get("/api/new/get-metadata/{site_id}", tags=['real-time'])
def get_metadata(user:user_dependency,site_id: int, db: Session = Depends(getdb)):
    enforce_site_access(user, site_id)
    try:
        # 1) Site + group
        site = db.query(Site).filter(Site.id == site_id).first()
        if not site:
            raise HTTPException(status_code=404, detail="Site not found")

        group_name = (
            db.query(Group.group_name)
            .filter(Group.id == site.group_id)
            .scalar()
        )

        # 2) Stations
        stations = (
            db.query(Station.id, Station.name)
            .filter(Station.site_id == site_id)
            .all()
        )
        station_ids = [s.id for s in stations]
        station_name_map = {s.id: s.name for s in stations}

        # 3) Counts via raw SQL
        counts = db.execute(
            text("""
                SELECT
                  COUNT(DISTINCT CASE WHEN p.monitoring_type_id = 1 THEN sp.station_id END) AS ambient,
                  COUNT(DISTINCT CASE WHEN p.monitoring_type_id = 2 THEN sp.station_id END) AS effluent,
                  COUNT(DISTINCT CASE WHEN p.monitoring_type_id = 3 THEN sp.station_id END) AS emission
                FROM analyser_parameter ap
                JOIN parameters p ON p.id = ap.parameter_id
                JOIN station_parameters sp ON sp.analyser_param_id = ap.id
                WHERE sp.station_id = ANY(:station_ids)
            """),
            {"station_ids": station_ids}
        ).fetchone()
        ambient_count  = counts.ambient  or 0
        effluent_count = counts.effluent or 0
        emission_count = counts.emission or 0

        # 4) Build siteDetails
        site_details = {
            "siteId":                   f"site_{site.id}",
            "siteName":                 site.site_name,
            "siteLabel":                site.siteuid or site.site_name.upper(),
            "city":                     site.city,
            "state":                    site.state,
            "address":                  site.address,
            "latitude":                 str(site.latitude) if site.latitude is not None else None,
            "longitude":                str(site.longitude) if site.longitude is not None else None,
            "location":                 site.city or site.address,
            "ambient_stations_count":   ambient_count,
            "effluent_stations_count":  effluent_count,
            "emission_stations_count":  emission_count,
            "groupName":                group_name,
            "authExpiry":               site.auth_expiry.isoformat() if site.auth_expiry else None,
            "siteCurrentStatusEndTime": datetime.datetime.now().isoformat(),
        }

        # 5) (Uncomment to include parametersList)
        # parameters_list = [...]
        # return {"siteDetails": site_details, "parametersList": parameters_list}

        return {"siteDetails": site_details}

    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail="Database error " )
    except Exception as e:
        raise HTTPException(status_code=500, detail="An unexpected error occurred " )
    finally:
        db.close()

# class LastParameterValue(BaseModel):
#     station_param_id: int
#     latest_value: float
#     unit: str
#     timestamp: str  # ISO8601
#     is_editable: bool

class LastParameterValue(BaseModel):
    station_param_id: int
    latest_value:     Optional[float]
    unit:             Optional[str]
    timestamp:        Optional[str]
    is_editable:      bool
    expired:          bool 
@router.get("/site/{site_id}/latest-parameters", tags=['real-time'])
def get_latest_site_parameters(user: user_dependency,site_id: int, db: Session = Depends(getdb)):
    enforce_site_access(user, site_id)
    subquery = (
        select(
            SensorData.parameter_id,
            func.max(SensorData.time).label("latest_time")
        )
        .where(SensorData.site_id == site_id)
        .group_by(SensorData.parameter_id)
        .subquery()
    )

    
    query = (
        select(
            Parameter.name,
            SensorData.value,
            SensorData.time
        )
        .join(AnalyserParameter, AnalyserParameter.parameter_id == Parameter.id)
        .join(stationParameter, stationParameter.analyser_param_id == AnalyserParameter.id)
        .join(Station, Station.id == stationParameter.station_id)
        .join(SensorData, (SensorData.parameter_id == Parameter.id) & (SensorData.site_id == site_id))
        .join(subquery, (SensorData.parameter_id == subquery.c.parameter_id) & (SensorData.time == subquery.c.latest_time))
        .where(Station.site_id == site_id)
        .where(Parameter.monitoring_type_id.isnot(None))
        .group_by(Parameter.id, SensorData.value, SensorData.time)
    )

    results = db.execute(query).fetchall()

    return [
        {
            "parameter_name": name,
            "latest_value": float(value) if value is not None else None,
            "timestamp": timestamp
        }
        for name, value, timestamp in results
    ]



@router.get(
    "/api/site/{site_id}/latest-station-parameters",
    response_model=List[LastParameterValue],
    tags=["real-time"]
)
def get_latest_station_parameters(user: user_dependency,site_id: int, db: Session = Depends(getdb)):
    SD = SensorData
    SP = stationParameter
    ST = Station

    enforce_site_access(user, site_id)
    # 1) latest‑time subquery
    subq = (
        select(
            SD.station_param_id,
            func.max(SD.time).label("latest_time")
        )
        .where(SD.site_id == site_id)
        .group_by(SD.station_param_id)
        .subquery()
    )

    # 2) main query: include an "expired" boolean
    stmt = (
        select(
            SD.station_param_id,
            SD.value.label("latest_value"),
            SP.para_unit.label("unit"),
            SD.time.label("timestamp"),
            SP.is_editable.label("is_editable"),
            # compute expired = (calibration_expiry_date < now())
            (ST.calibration_expiry_date < func.now()).label("expired")
        )
        .select_from(SD)
        .join(subq,
              (SD.station_param_id == subq.c.station_param_id) &
              (SD.time            == subq.c.latest_time)
        )
        .join(SP, SD.station_param_id == SP.id)
        .join(ST, SP.station_id == ST.id)
        .where(SD.site_id == site_id)
        .order_by(SD.station_param_id)
    )

    rows = db.execute(stmt).all()
    if not rows:
        raise HTTPException(status_code=404, detail="No sensor data for this site")

    return [
        LastParameterValue(
            station_param_id = r.station_param_id,
            latest_value     = float(r.latest_value) if not r.expired else None,
            unit             = r.unit     if not r.expired else None,
            timestamp        = r.timestamp.isoformat() if (r.timestamp and not r.expired) else None,
            is_editable      = r.is_editable,
            expired          = bool(r.expired),
        )
        for r in rows
    ]




@router.get("/api/station/getAggregatedAvgLast24hrs", tags=["station-graph"])
async def get_aggregated_avg_last24hrs(
    user: user_dependency,
    station_param_ids: list[int] = Query(...),
    interval: str = Query("1h", description="Aggregation interval: '1h' or '15m'"),
    db: Session = Depends(getdb),
):
    # 0) sanity
    if not station_param_ids:
        raise HTTPException(400, "station_param_ids are required")
    if interval not in ("1h", "15m"):
        raise HTTPException(400, "Invalid interval. Use '1h' or '15m'.")
    from datetime import datetime, timezone,timedelta
    from zoneinfo import ZoneInfo
    # 1) Filter out expired station_param_ids
    now_utc = datetime.now(timezone.utc)
    valid_rows = db.execute(
        select(stationParameter.id)
        .join(Station, stationParameter.station_id == Station.id)
        .where(
            stationParameter.id.in_(station_param_ids),
            Station.calibration_expiry_date >= now_utc
        )
    ).scalars().all()

    if not valid_rows:
        # either none were valid or all expired
        return []  

    # 2) Build the CSV list from only the valid, non‑expired IDs
    ids_csv = ",".join(map(str, valid_rows))

    # 3) Determine bucket for time_bucket()
    bucket = "1 hour" if interval == "1h" else "15 minutes"

    # 4) 24hr window in UTC
    past_24hr_utc = now_utc - timedelta(hours=24)

    # 5) Run your aggregation only for valid_rows
    sql = text(f"""
        SELECT
          sp.id                                      AS station_param_id,
          p.name                                     AS parameter_name,
          a.analyser_name                            AS analyser_name,
          (p.name || '-' || a.analyser_name)         AS display_name,
          time_bucket('{bucket}', sd.time, 'Asia/Kolkata')                AS interval_start_utc,
          time_bucket('{bucket}', sd.time, 'Asia/Kolkata') + INTERVAL '{bucket}' AS interval_end_utc,
          AVG(sd.value)                              AS avg_value,
          COUNT(*)                                   AS total_records,
          MAX(p.unit)                                AS unit
        FROM sensor_data sd
        JOIN station_parameters sp ON sd.station_param_id = sp.id
        JOIN analyser_parameter ap    ON sp.analyser_param_id = ap.id
        JOIN parameters p             ON ap.parameter_id = p.id
        JOIN analysers a              ON ap.analyser_id = a.id
        WHERE sd.station_param_id IN ({ids_csv})
          AND sd.time >= :past_24hr
          AND sd.time <= :now
        GROUP BY sp.id, p.name, a.analyser_name, interval_start_utc
        ORDER BY display_name, interval_start_utc;
    """)

    rows = db.execute(sql, {"past_24hr": past_24hr_utc, "now": now_utc}).fetchall()

    # 6) Assemble response as before...
    ist = ZoneInfo("Asia/Kolkata")
    response: dict[int, dict] = {}
    for r in rows:
        start_ist = r.interval_start_utc.astimezone(ist)
        end_ist   = r.interval_end_utc.astimezone(ist)
        x_axis = start_ist.strftime("%d/%m %H:%M")

        spid = r.station_param_id
        if spid not in response:
            response[spid] = {
                "station_param_id": spid,
                "parameter_name":  r.parameter_name,
                "analyser_name":   r.analyser_name,
                "display_name":    r.display_name,
                "unit":            r.unit,
                "aggregated_data": [],
            }

        response[spid]["aggregated_data"].append({
            "interval_start": start_ist.isoformat(),
            "interval_end":   end_ist.isoformat(),
            "avg_value":      float(r.avg_value) if r.avg_value is not None else None,
            "total_records":  r.total_records,
            "x_axis":         x_axis,
        })

    return list(response.values())

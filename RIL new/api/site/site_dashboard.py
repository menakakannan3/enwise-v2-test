

# OM VIGHNHARTAYE NAMO NAMAH :
from sqlalchemy import TIMESTAMP, cast
from sqlalchemy import func, select,text
from sqlalchemy.orm import joinedload
from sqlalchemy.sql import func
from ...modals.masters import *
from fastapi import APIRouter
from datetime import datetime, timedelta
from typing import List, Dict , Union
from sqlalchemy.sql import text, bindparam
import pytz

router = APIRouter()

# OM VIGHNHARTAYE NAMO NAMAH :

from sqlalchemy.orm import joinedload
from sqlalchemy.sql import func
from ...modals.masters import *
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from ...database.session import getdb
from ..auth.authentication import user_dependency

router = APIRouter()



@router.get("/api/sites/dashboard/{site_id}", tags=['site'], summary="Get site dashboard")
def get_site_dashboard(user: user_dependency,site_id: int, db: Session = Depends(getdb)):
    # 1) Fetch Site details
    site = (
        db.query(
            Site.id, Site.site_name, Site.city, Site.state,
            Site.latitude, Site.longitude, Group.group_name
        )
        .outerjoin(Group, Site.group_id == Group.id)
        .filter(Site.id == site_id)
        .first()
    )
    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    # 2) Monitoring station count
    monitoring_stations = db.query(Station).filter(Station.site_id == site_id).count()

    # 3) All parameter IDs
    parameters = (
        db.query(Parameter.id)
        .join(AnalyserParameter, AnalyserParameter.parameter_id == Parameter.id)
        .join(SiteAnalyser, SiteAnalyser.analyser_id == AnalyserParameter.analyser_id)
        .filter(SiteAnalyser.site_id == site_id)
        .distinct()
        .all()
    )
    parameter_ids = [p.id for p in parameters]
    total_parameters_connected = len(parameter_ids)

    # --- UPDATED DATA AVAILABILITY LOGIC (IST full yesterday) ---
    # Yesterday in IST
    today_ist = datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)
    yesterday_ist = (today_ist.date() - datetime.timedelta(days=1))
    local_start = datetime.datetime.combine(yesterday_ist, datetime.time.min)
    local_end   = datetime.datetime.combine(yesterday_ist, datetime.time.max)

    # 36-second baseline
    EXPECTED_PER_PAIR = 2400

    # 1) fetch relevant station_param_ids
    station_param_ids = [
        sp.id for sp in (
            db.query(StationParameter)
              .join(Station, Station.id == StationParameter.station_id)
              .join(AnalyserParameter, StationParameter.analyser_param_id == AnalyserParameter.id)
              .filter(
                  Station.site_id == site_id,
                  AnalyserParameter.parameter_id.in_(parameter_ids)
              )
              .all()
        )
    ]

    # 2) count every row in IST day window per pair
    avail_values = []
    for sp_id in station_param_ids:
        cnt = db.query(func.count())\
            .filter(
                SensorData.station_param_id == sp_id,
                func.timezone('Asia/Kolkata', SensorData.time) >= local_start,
                func.timezone('Asia/Kolkata', SensorData.time) <= local_end
            )\
            .select_from(SensorData)\
            .scalar() or 0
        avail_values.append(cnt / EXPECTED_PER_PAIR * 100)

    # 3) average
    data_availability = round(sum(avail_values) / len(avail_values), 2) if avail_values else 0
    # --- END DATA AVAILABILITY LOGIC ---

    # 4) Exceeding parameters (yesterday IST)
    exceeding_parameters = (
        db.query(Parameter.name)
        .join(SensorData, SensorData.parameter_id == Parameter.id)
        .join(SiteLevelParameterThreshold, SiteLevelParameterThreshold.parameter_id == Parameter.id)
        .filter(
            SensorData.site_id == site_id,
            func.timezone('Asia/Kolkata', SensorData.time).cast(Date) == yesterday_ist,
            SensorData.value > SiteLevelParameterThreshold.site_level_threshold
        )
        .distinct()
        .all()
    )
    exceeding_parameters = [p[0] for p in exceeding_parameters]

    # 5) Other metrics
    site_status_end_time = db.query(func.max(SensorData.time))\
                             .filter(SensorData.site_id == site_id)\
                             .scalar()
    device_count = db.query(Device).filter(Device.site_id == site_id).count()
    licence_expiry = (
        db.query(
            Station.name,
            func.date(Station.calibration_expiry_date) - func.current_date()
        )
        .filter(Station.site_id == site_id)
        .all()
    )
    licence_expiry_list = [{"MonitoringStation": name, "expiry": days} for name, days in licence_expiry]

    parameters_list = (
        db.query(
            Parameter.name.label("parameterName"),
            MonitoringType.monitoring_type,
            Parameter.id.label("parameterId"),
            Analyser.id.label("analyzerId"),
            SiteLevelParameterThreshold.site_level_threshold.label("Threshold"),
            Parameter.min_thershold.label("MinThreshold"),
            Parameter.unit
        )
        .join(MonitoringType, MonitoringType.id == Parameter.monitoring_type_id)
        .join(AnalyserParameter, AnalyserParameter.parameter_id == Parameter.id)
        .join(SiteAnalyser, SiteAnalyser.analyser_id == AnalyserParameter.analyser_id)
        .outerjoin(SiteLevelParameterThreshold, SiteLevelParameterThreshold.parameter_id == Parameter.id)
        .filter(SiteAnalyser.site_id == site_id)
        .all()
    )
    parameters_list = [
        {
            "parameterName": p.parameterName,
            "monitoringType": p.monitoring_type,
            "parameterId": p.parameterId,
            "analyzerId": p.analyzerId,
            "Threshold": p.Threshold,
            "MinThreshold": p.MinThreshold,
            "unit": p.unit,
        }
        for p in parameters_list
    ]

    # 6) Response
    return {
        "siteId": site_id,
        "siteName": site.site_name,
        "city": site.city,
        "state": site.state,
        "groupName": site.group_name,
        "latitude": str(site.latitude),
        "longitude": str(site.longitude),
        "monitoringStations": monitoring_stations,
        "totalParametersConnected": total_parameters_connected,
        "parametersConnected": total_parameters_connected,
        "exceedingParameters": exceeding_parameters,
        "dataAvailability": data_availability,
        "siteCurrentStatusEndTime": site_status_end_time.strftime("%Y-%m-%dT%H:%M:%S") if site_status_end_time else None,
        "deviceCount": device_count,
        "licenceExpiry": licence_expiry_list,
        "parametersList": parameters_list
    }
  
from dateutil import parser







@router.get("/api/sensor-agg-multi/{site_id}/{station_id}", tags=["sensor aggregation"])
async def get_multi_param_aggregation(
    user: user_dependency,
    site_id: int,
    station_id: int,
    from_date: str,         # Format: YYYY-MM-DD
    to_date: str,           # Format: YYYY-MM-DD
    agg_interval: str,      # Examples: '1 hour', '15 minutes'
    db: Session = Depends(getdb)
):
    try:
        import datetime as dt
        import pytz
        from sqlalchemy import text

        ist = pytz.timezone("Asia/Kolkata")
        now_ist = dt.datetime.now(ist)

        # Parse from_date and to_date in IST timezone
        start_ist = ist.localize(dt.datetime.strptime(from_date, "%Y-%m-%d")).replace(hour=6, minute=0, second=0)
        end_ist = ist.localize(dt.datetime.strptime(to_date, "%Y-%m-%d"))
        end_ist = end_ist.replace(
            hour=now_ist.hour,
            minute=now_ist.minute,
            second=now_ist.second
        )

        start_utc = start_ist.astimezone(pytz.utc)
        end_utc = end_ist.astimezone(pytz.utc)

        query = text(f"""
    WITH bucketed AS (
        SELECT 
            sd.*, 
            time_bucket(:agg_interval, sd.time, :bucket_origin) AS bucket
        FROM sensor_data sd
        WHERE sd.site_id = :site_id
          AND sd.station_id = :station_id
          AND sd.time BETWEEN :start_time AND :end_time
    )
    SELECT 
        bucket AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata' AS time_bucket_ist,
        station_param_id,
        AVG(value) AS avg_value,
        p.name AS parameter_name,
        sp.pram_lable AS parameter_label
    FROM bucketed
    JOIN station_parameters sp ON bucketed.station_param_id = sp.id
    JOIN analyser_parameter ap ON sp.analyser_param_id = ap.id
    JOIN parameters p ON ap.parameter_id = p.id
    GROUP BY bucket, station_param_id, p.name, sp.pram_lable
    ORDER BY bucket;
""")

        bucket_origin_utc = start_ist.astimezone(pytz.utc)


        result = db.execute(query, {
            "site_id": site_id,
            "station_id": station_id,
            "start_time": start_utc,
            "end_time": end_utc,
            "agg_interval": agg_interval,
            "bucket_origin": bucket_origin_utc
        }).fetchall()

        grouped_data = {}
        for row in result:
            param_key = f"{row.parameter_name} "
            if param_key not in grouped_data:
                grouped_data[param_key] = []
            grouped_data[param_key].append({
                "time_bucket": row.time_bucket_ist.strftime("%Y-%m-%d %H:%M:%S"),
               
                "avg_value": row.avg_value,
            
            })

        return {
            "site_id": site_id,
            "station_id": station_id,
            "from_date": from_date,
            "to_date": to_date,
            "aggregation_interval": agg_interval,
            "data": grouped_data
        }

    except Exception as e:
        raise HTTPException(
        status_code=500,
        detail="Internal server error"
    )

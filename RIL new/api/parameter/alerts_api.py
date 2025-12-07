# app/api/alerts.py

import datetime as dt
import pytz
from typing import List, Optional

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text

from ...database.session import getdb
from ..auth.authentication import user_dependency
from ...modals.masters import *

from pydantic import BaseModel

router = APIRouter(prefix="/api/v1", tags=["alerts"])


# -------------------------------
#   RESPONSE MODELS
# -------------------------------
class SiteAlertOut(BaseModel):
    station_name: str
    parameter_name: str
    exceedance_value: Optional[float] = None
    exceedance_time: Optional[dt.datetime] = None
    mail_delivered: bool = False
    delivered_time: Optional[dt.datetime] = None
    mail_status: Optional[str] = None
    bucket_start: dt.datetime

    class Config:
        from_attributes = True


class DeviceOfflineOut(BaseModel):
    station_name: Optional[str] = None
    device_name: Optional[str] = None
    device_uid: Optional[str] = None
    last_ping: Optional[dt.datetime] = None
    minutes_since_last_ping: Optional[int] = None
    is_offline: bool = True
    status: Optional[str] = None
    device_status: Optional[str] = None

    class Config:
        from_attributes = True


class SiteAlertsResponse(BaseModel):
    exceedance_alerts: List[SiteAlertOut]
    device_offline: List[DeviceOfflineOut]


# -------------------------------
#        MAIN API
# -------------------------------
@router.get("/site-alerts/{site_id}", response_model=SiteAlertsResponse)
def get_site_alerts(
    user: user_dependency,
    site_id: int,
    offline_minutes: int = Query(30, ge=5, le=1440),
    db: Session = Depends(getdb),
):
    """
    Returns:
      • device_offline (correct)
      • exceedance_alerts (today-only exceedance using 15-min averages)
    """

    now_utc = dt.datetime.now(dt.timezone.utc)
    cutoff_utc = now_utc - dt.timedelta(minutes=offline_minutes)

    # --------------------------------------------------------------------
    # 1️⃣ DEVICE OFFLINE SECTION
    # --------------------------------------------------------------------
    offline_sql = text("""
        SELECT
            s.name AS station_name,
            d.device_name AS device_name,
            d.device_uid AS device_uid,
            d.last_ping AS last_ping,
            d.status AS status,
            d.device_status AS device_status
        FROM device d
        LEFT JOIN device_station ds ON ds.device_id = d.id
        LEFT JOIN stations s ON s.id = ds.station_id
        WHERE d.site_id = :site_id
        AND (
                d.last_ping IS NULL
             OR d.last_ping < :cutoff
             OR lower(COALESCE(d.status, '')) = 'offline'
             OR lower(COALESCE(d.device_status, '')) = 'offline'
        )
        ORDER BY d.last_ping NULLS FIRST, d.device_name;
    """)

    offline_rows = db.execute(
        offline_sql, {"site_id": site_id, "cutoff": cutoff_utc}
    ).mappings().all()

    offline_output: List[DeviceOfflineOut] = []

    for r in offline_rows:
        lp = r["last_ping"]
        if lp and lp.tzinfo is None:
            lp = lp.replace(tzinfo=dt.timezone.utc)

        minutes_gap = (
            int((now_utc - lp.astimezone(dt.timezone.utc)).total_seconds() // 60)
            if lp else None
        )

        offline_output.append(
            DeviceOfflineOut(
                station_name=r["station_name"],
                device_name=r["device_name"],
                device_uid=r["device_uid"],
                last_ping=lp,
                minutes_since_last_ping=minutes_gap,
                is_offline=True,
                status=r["status"],
                device_status=r["device_status"],
            )
        )

    # --------------------------------------------------------------------
    # 2️⃣ TODAY EXCEEDANCE USING sensor_agg_15min ONLY
    # --------------------------------------------------------------------
    ist = pytz.timezone("Asia/Kolkata")
    now_ist = dt.datetime.now(ist)

    today_start_ist = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end_ist = today_start_ist + dt.timedelta(days=1)

    today_start_utc = today_start_ist.astimezone(dt.timezone.utc)
    today_end_utc = today_end_ist.astimezone(dt.timezone.utc)

    # Fetch all station parameters
    sp_sql = text("""
        SELECT 
            sp.id AS station_param_id,
            sp.para_threshold,
            sp.para_unit,
            p.name AS parameter_name,
            stn.name AS station_name
        FROM station_parameters sp
        JOIN analyser_parameter ap ON sp.analyser_param_id = ap.id
        JOIN parameters p ON ap.parameter_id = p.id
        JOIN stations stn ON sp.station_id = stn.id
        WHERE stn.site_id = :site_id
    """)

    params = db.execute(sp_sql, {"site_id": site_id}).mappings().all()

    exceed_output: List[SiteAlertOut] = []

    # 15-min exceedance with values + bucket_start
    exceed_sql = text("""
        SELECT 
            bucket AS bucket_start,
            avg_value,
            bucket AT TIME ZONE 'UTC' AS bucket_utc
        FROM sensor_agg_15min
        WHERE station_param_id = :sp_id
        AND bucket >= :start_utc AND bucket < :end_utc
        AND avg_value > :threshold * 1.10
        ORDER BY bucket ASC;
    """)

    for p in params:
        rows = db.execute(
            exceed_sql,
            {
                "sp_id": p["station_param_id"],
                "start_utc": today_start_utc,
                "end_utc": today_end_utc,
                "threshold": p["para_threshold"],
            },
        ).mappings().all()

        for r in rows:
            exceed_output.append(
                SiteAlertOut(
                    station_name=p["station_name"],
                    parameter_name=p["parameter_name"],
                    exceedance_value=r["avg_value"],
                    exceedance_time=r["bucket_start"],
                    mail_delivered=False,
                    delivered_time=None,
                    mail_status=None,
                    bucket_start=r["bucket_start"],
                )
            )

    # --------------------------------------------------------------------
    # 3️⃣ FINAL RESPONSE
    # --------------------------------------------------------------------
    return SiteAlertsResponse(
        exceedance_alerts=exceed_output,
        device_offline=offline_output,
    )

from sqlalchemy.orm import joinedload
from sqlalchemy.sql import func, text
from ...modals.masters import *
from fastapi import APIRouter, Depends, HTTPException,Query
from sqlalchemy.orm import Session
from sqlalchemy import select, func
from ...database.session import getdb
from datetime import datetime, timedelta
from ..auth.authentication import user_dependency
from ...modals.masters import LatestSensorData 
from sqlalchemy import MetaData, Table
from ...utils.permissions import enforce_site_access

router = APIRouter(tags=["site-status"])


@router.get("/api/site-details/{site_id}", tags=["site-details"])
async def get_site_table_details(user: user_dependency,site_id: str, db: Session = Depends(getdb)):
    enforce_site_access(user, site_id)
    current_time = datetime.utcnow()
    yesterday_time = current_time - timedelta(hours=24)

    # Get site details
    site = db.query(Site).filter(Site.id == site_id).first()
    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    # Get all stations for the site
    stations = db.query(Station).filter(Station.site_id == site.id).all()
    station_ids = [station.id for station in stations]

    # Build a subquery to fetch the latest sensor value per parameter/station/analyser
    subquery = db.query(
        SensorData.parameter_id,
        SensorData.analyser_id,
        SensorData.station_id,
        SensorData.value,
        SensorData.time,
        func.row_number().over(
            partition_by=[SensorData.parameter_id, SensorData.analyser_id, SensorData.station_id],
            order_by=SensorData.time.desc()
        ).label("row_num")
    ).filter(SensorData.site_id == site.id).subquery()

    # Compute sensor parameter statistics over the last 24 hours
    parameters_stats = (
        db.query(
            Station.name.label("station_name"),
            Parameter.name.label("parameter_name"),
            Parameter.unit,
            Parameter.id.label("parameter_id"),
            Parameter.max_thershold,
            Analyser.analyser_name,
            Analyser.id.label("analyser_id"),
            func.min(SensorData.value).label("min_value"),
            func.max(SensorData.value).label("max_value"),
            func.avg(SensorData.value).label("avg_value"),
            subquery.c.value.label("current_value")  # Latest value from subquery
        )
        .join(Station, SensorData.station_id == Station.id)
        .join(Parameter, SensorData.parameter_id == Parameter.id)
        .join(Analyser, SensorData.analyser_id == Analyser.id)
        .join(
            subquery,
            (subquery.c.parameter_id == SensorData.parameter_id) &
            (subquery.c.analyser_id == SensorData.analyser_id) &
            (subquery.c.station_id == SensorData.station_id) &
            (subquery.c.row_num == 1),
            isouter=True
        )
        .filter(SensorData.site_id == site.id, SensorData.time >= yesterday_time)
        .group_by(
            Station.name,
            Parameter.name,
            Parameter.unit,
            Parameter.id,
            Parameter.max_thershold,
            Analyser.analyser_name,
            Analyser.id,
            subquery.c.value
        )
        .all()
    )

    # Calculate totalExceedingParameters
    response_map = {}
    for row in parameters_stats:
        key = f"{row.station_name}-{row.parameter_name}-analyzer_{row.analyser_id}"
        response_map[key] = row  # temporarily store each row

    total_exceeding = 0
    for row in parameters_stats:
        if row.current_value is not None:
            if row.current_value < row.max_thershold * 0.9 or row.current_value > row.max_thershold:
                total_exceeding += 1

    table_data = [
        {
            "current": float(row.current_value) if row.current_value else 0,
            "name": f"{row.station_name}-{row.parameter_name}",
            "unit": row.unit,
            "id": f"Emission.{row.station_name}.analyzer_{row.analyser_id}.parameter_{row.parameter_id}",
            "max": str(round(float(row.max_value), 2)) if row.max_value else "0.00",
            "min": str(round(float(row.min_value), 2)) if row.min_value else "0.00",
            "avg": str(round(float(row.avg_value), 2)) if row.avg_value else "0.00",
            "Threshold": str(row.max_thershold),
            "enableDynamicLimit": "False"
        }
        for row in parameters_stats
    ]

    site_details = {
        "state": site.state,
        "siteLabel": site.siteuid,
        "location": site.address,
        "siteId": site.siteuid,
        "industry": db.query(Group.group_name).filter(Group.id == site.group_id).scalar(),
        "siteName": site.site_name,
        "isConnected": "Active",
        "city": site.city,
        "totalMonitoringStations": len(stations),
        "totalParametersCount": len(parameters_stats),
        "totalExceedingParameters": total_exceeding,
        "dataAvailablity": None,    # Add calculation if desired
        "deviceAvailablity": None,  # Add calculation if desired
        "lastfetchedTime": current_time.isoformat()
    }

    response = {
        "siteDetails": site_details,
        "data": table_data
    }
    return response
    
@router.get("/api/v2/site-details/{site_id}")
def get_site_latest_values(site_id: int, user: user_dependency, db: Session = Depends(getdb)):

    enforce_site_access(user, site_id)

    now = datetime.utcnow()

    try:
        # 1️⃣ Fetch station parameters for this site
        sp_query = (
            select(
                stationParameter.id.label("station_param_id"),
                stationParameter.station_id,
                stationParameter.para_threshold,
                stationParameter.para_unit,
                Station.name.label("station_name"),
                Station.calibration_expiry_date.label("expiry_date"),
                Parameter.name.label("param_name"),
                MonitoringType.monitoring_type.label("monitoring_type"),
            )
            .join(Station, stationParameter.station_id == Station.id)
            .outerjoin(AnalyserParameter, stationParameter.analyser_param_id == AnalyserParameter.id)
            .outerjoin(Parameter, AnalyserParameter.parameter_id == Parameter.id)
            .outerjoin(MonitoringType, Parameter.monitoring_type_id == MonitoringType.id)
            .where(Station.site_id == site_id)
        )

        sp_rows = db.execute(sp_query).all()
        if not sp_rows:
            return []

        sp_ids = [r.station_param_id for r in sp_rows]

        # 2️⃣ Fetch latest value from site_status_15min
        latest_query = text("""
            SELECT DISTINCT ON (station_param_id)
                station_param_id,
                avg_value AS value,
                bucket_time AS time
            FROM site_status_15min
            WHERE station_param_id = ANY(:sp_ids)
            ORDER BY station_param_id, bucket_time DESC
        """)

        latest_rows = db.execute(latest_query, {"sp_ids": sp_ids}).fetchall()
        latest_map = {row.station_param_id: row for row in latest_rows}

        # 3️⃣ Build response
        response = []
        for r in sp_rows:

            latest = latest_map.get(r.station_param_id)
            val = float(latest.value) if latest and latest.value is not None else 0

            response.append({
                "name": f"{r.station_name} - {r.param_name}" if r.param_name else "Unknown",
                "current": val,
                "unit": r.para_unit or "",
                "station_param_id": r.station_param_id,
                "station_id": r.station_id,
                "is_expired": bool(r.expiry_date and r.expiry_date < now),
                "monitoring_type": r.monitoring_type or "",
                "threshold": r.para_threshold,
                "time": latest.time if latest else None
            })

        return response

    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/site-status/list/{site_id}")
def get_all_site_details(user: user_dependency,site_id : int , db: Session = Depends(getdb)):
    enforce_site_access(user, site_id)
    try:
        query = text("""
            SELECT
                s.id AS site_id,
                s.siteuid,
                s.site_name,
                s.address,
                s.city,
                s.state,
                s.latitude,
                s.longitude,
                s.authkey,
                s.auth_expiry,
                g.group_name,
                g.ind_code,
                s.ganga_basin
            FROM site s
            LEFT JOIN "group" g ON s.group_id = g.id
            where s.id = :site_id
        """)
        result = db.execute(query ,{"site_id": site_id}).fetchall()

        site_list = []
        for row in result:
            site_list.append({
                "siteId": row.site_id,
                "siteUID": row.siteuid,
                "siteName": row.site_name,
                "address": row.address,
                "city": row.city,
                "state": row.state,
                "latitude": float(row.latitude) if row.latitude else None,
                "longitude": float(row.longitude) if row.longitude else None,
                "authKey": row.authkey,
                "authExpiry": row.auth_expiry.isoformat() if row.auth_expiry else None,
                "groupName": row.group_name,
                "industryCode": row.ind_code,
                "gangaBasin": row.ganga_basin,
                # Placeholders if you wish to add additional computed values later:
                "totalMonitoringStations": None,
                "totalParametersCount": None,
                "totalExceedingParameters": None,
                "dataAvailablity": None,
                "deviceAvailablity": None,
                "lastfetchedTime": None
            })
        return site_list
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/site-chart/{site_id}", tags=["site-chart"])
async def get_site_chart_data(user: user_dependency,site_id: str, db: Session = Depends(getdb)):

    enforce_site_access(user, site_id)

    current_time = datetime.utcnow()
    yesterday_time = current_time - timedelta(hours=24)

    # Verify the site exists
    site = db.query(Site).filter(Site.id == site_id).first()
    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    chart_query = text("""
    SELECT 
        s.name AS station_name,
        p.name AS parameter_name,
        p.id AS parameter_id,
        p.unit AS unit,
        a.id AS analyser_id,
        date_trunc('hour', sd.time) AS hour_time,
        AVG(sd.value) AS avg_value
    FROM sensor_data sd
    JOIN stations s ON sd.station_id = s.id
    JOIN parameters p ON sd.parameter_id = p.id
    JOIN analysers a ON sd.analyser_id = a.id
    WHERE sd.site_id = :site_id
      AND sd.time >= :yesterday_time
    GROUP BY 
        p.id,
        s.name,
        p.name,
        p.unit,
        a.id,
        date_trunc('hour', sd.time)
    ORDER BY hour_time ASC
    """)
    chart_data_raw = db.execute(chart_query, {"site_id": site.id, "yesterday_time": yesterday_time}).fetchall()

    chart_data_dict = {}
    for row in chart_data_raw:
        param_key = f"{row.station_name}-{row.parameter_name}-analyzer_{row.analyser_id}"
        if param_key not in chart_data_dict:
            chart_data_dict[param_key] = {
                "id": f"Emission.{row.station_name}.analyzer_{row.analyser_id}.parameter_{row.parameter_id}",
                "name": f"{row.station_name}-{row.parameter_name}",
                "unit": row.unit,
                "sparkList": [],
                "sparkListTime": []
            }
        chart_data_dict[param_key]["sparkList"].append(float(row.avg_value))
        chart_data_dict[param_key]["sparkListTime"].append(row.hour_time.strftime("%Y-%m-%d %H:%M"))

    # Ensure every parameter from the table data exists in chart data.
    # (Optional: if some parameters have no hourly chart data, add them with empty lists.)
    # In this example, we assume chart_data_dict already covers the needed parameters.

    # Add "NA" for incomplete hours if needed (example: if length < 24)
    for chart in chart_data_dict.values():
        if len(chart["sparkList"]) < 24:
            chart["sparkList"].append("NA")
            chart["sparkListTime"].append(current_time.strftime("%Y-%m-%d %H:%M"))

    return {"chartData": list(chart_data_dict.values())}


@router.get("/api/site-chart-detail/{chart_id}", tags=["site-chart-detail"])
async def get_site_chart_detail(
    user: user_dependency,
    chart_id: str,
    site_id: int = Query(..., description="Site ID to filter on"),
    db: Session = Depends(getdb),
):
    """
    chart_id: "Emission.{station_name}.analyzer_{analyser_id}.parameter_{parameter_id}"
    e.g. "Emission.ETP_OUTLET.analyzer_1.parameter_5"
    """
    # 🔽 Timezone imports inside the function
    from datetime import datetime, timedelta, timezone
    from zoneinfo import ZoneInfo

    # 1) Compute the UTC window for the last 24 hours
    now_utc       = datetime.now(timezone.utc)
    past_24hr_utc = now_utc - timedelta(hours=24)

    # 2) Prepare IST tz for later conversion
    ist = ZoneInfo("Asia/Kolkata")
    enforce_site_access(user, site_id)

    # 3) Parse the chart_id
    try:
        parts = chart_id.split(".")
        if len(parts) != 4 or not parts[2].startswith("analyzer_") or not parts[3].startswith("parameter_"):
            raise ValueError()
        station_name  = parts[1]
        analyser_id   = int(parts[2].split("_", 1)[1])
        parameter_id  = int(parts[3].split("_", 1)[1])
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid chart_id format")

    # 4) Verify the site exists
    site = db.query(Site).filter(Site.id == site_id).first()
    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    # 5) Find the station by name and site
    station = (
        db.query(Station)
        .filter(Station.name == station_name, Station.site_id == site_id)
        .first()
    )
    if not station:
        raise HTTPException(status_code=404, detail="Station not found")

    # 6) Find the analyser_parameter entry
    analyser_param = (
        db.query(AnalyserParameter)
        .filter_by(analyser_id=analyser_id, parameter_id=parameter_id)
        .first()
    )
    if not analyser_param:
        raise HTTPException(status_code=404, detail="Analyser‑Parameter combo not found")

    # 7) Find the station_parameter entry
    stp = (
        db.query(stationParameter)
        .filter_by(station_id=station.id, analyser_param_id=analyser_param.id)
        .first()
    )
    if not stp:
        raise HTTPException(status_code=404, detail="Station‑Parameter mapping not found")

    # 8) Query sensor_data for hourly averages over the last 24h
    rows = (
        db.query(
            func.date_trunc("hour", SensorData.time).label("hour_time_utc"),
            func.avg(SensorData.value).label("avg_value"),
        )
        .filter(
            SensorData.site_id == site_id,
            SensorData.station_param_id == stp.id,
            SensorData.time >= past_24hr_utc,
        )
        .group_by("hour_time_utc")
        .order_by("hour_time_utc")
        .all()
    )

    if not rows:
        raise HTTPException(
            status_code=404,
            detail="No chart data found for the specified sensor parameter",
        )

    # 9) Build sparkList and sparkListTime, converting UTC → IST
    sparkList = []
    sparkListTime = []
    for r in rows:
        dt_utc = r.hour_time_utc
        if dt_utc.tzinfo is None:
            dt_utc = dt_utc.replace(tzinfo=timezone.utc)
        dt_ist = dt_utc.astimezone(ist)
        sparkList.append(float(r.avg_value))
        sparkListTime.append(dt_ist.strftime("%Y-%m-%d %H:%M"))

    # 10) Fetch parameter info for unit & display name
    param = db.query(Parameter).get(parameter_id)
    unit = param.unit if param else ""
    display_name = f"{station_name}-{param.name}" if param else chart_id

    

    return {
        "chartData": {
            "id":            chart_id,
            "name":          display_name,
            "unit":          unit,
            "sparkList":     sparkList,
            "sparkListTime": sparkListTime,
        }
    }

@router.get("/api/v2/site-chart-detail/{station_param_id}", tags=["site-chart-detail"])
async def get_chart_detail_v2(
    user: user_dependency,
    station_param_id: int,
    site_id: int = Query(..., description="Site ID for the request"),
    db: Session = Depends(getdb),
):
    from zoneinfo import ZoneInfo

    now_naive = datetime.utcnow()
    start_utc = now_naive - timedelta(hours=24)
    ist = ZoneInfo("Asia/Kolkata")
    enforce_site_access(user, site_id)

    try:
        stp = db.query(stationParameter).filter(stationParameter.id == station_param_id).first()
        if not stp:
            raise HTTPException(status_code=404, detail="StationParameter not found")

        # Check station.site_id match instead of stp.site_id
        station = db.query(Station).filter_by(id=stp.station_id).first()
        if not station or station.site_id != site_id:
            raise HTTPException(status_code=404, detail="Station not found or site_id mismatch")

        analyser_param = db.query(AnalyserParameter).filter_by(id=stp.analyser_param_id).first()
        if not analyser_param:
            raise HTTPException(status_code=404, detail="AnalyserParameter not found")

        parameter = db.query(Parameter).filter_by(id=analyser_param.parameter_id).first()
        if not parameter:
            raise HTTPException(status_code=404, detail="Parameter not found")

        data = (
            db.query(
                func.date_trunc("hour", SensorData.time).label("hour"),
                func.avg(SensorData.value).label("avg")
            )
            .filter(
                SensorData.site_id == site_id,
                SensorData.station_param_id == station_param_id,
                SensorData.time >= start_utc
            )
            .group_by("hour")
            .order_by("hour")
            .all()
        )

        if not data:
            raise HTTPException(status_code=404, detail="No data found")

        sparkList = [float(d.avg) for d in data]
        sparkListTime = [d.hour.replace(tzinfo=ZoneInfo("UTC")).astimezone(ist).isoformat() for d in data]

        def safe_stats(values):
            if not values:
                return 0.0, 0.0, 0.0
            return min(values), max(values), sum(values) / len(values)

        def percentile(values, p):
            values = sorted(values)
            if not values:
                return 0.0
            k = (len(values) - 1) * (p / 100)
            f = int(k)
            c = min(f + 1, len(values) - 1)
            return values[f] + (values[c] - values[f]) * (k - f)

        min_v, max_v, avg_v = safe_stats(sparkList)
        p10 = percentile(sparkList, 10)
        p25 = percentile(sparkList, 25)
        p50 = percentile(sparkList, 50)
        p75 = percentile(sparkList, 75)
        p90 = percentile(sparkList, 90)

        threshold = parameter.max_thershold or 0.0

        above_thresh = len([v for v in sparkList if v > threshold])
        within_thresh = len(sparkList) - above_thresh

        q1 = len([v for v in sparkList if v <= p25])
        q2 = len([v for v in sparkList if p25 < v <= p50])
        q3 = len([v for v in sparkList if p50 < v <= p75])
        q4 = len([v for v in sparkList if v > p75])

        return {
            "chartData": {
                "id": station_param_id,
                "name": f"{station.name}-{parameter.name}",
                "unit": parameter.unit or "",
                "sparkList": sparkList,
                "sparkListTime": sparkListTime,
                "min": min_v,
                "max": max_v,
                "avg": avg_v,
                "threshold": threshold,
                "p10": p10,
                "p25": p25,
                "p50": p50,
                "p75": p75,
                "p90": p90,
                "q1": q1,
                "q2": q2,
                "q3": q3,
                "q4": q4,
                "above_thresh": above_thresh,
                "within_thresh": within_thresh,
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")
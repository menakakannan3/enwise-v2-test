from fastapi import Depends, HTTPException, APIRouter, Form
from fastapi.responses import StreamingResponse
from sqlalchemy import text, select
from sqlalchemy.orm import Session
from datetime import timedelta, timezone
from typing import Union
from ...modals.masters import *
from ...database.session import getdb
from ...utils.utils import *
from dateutil import parser
import io, csv, gzip
import pytz
from datetime import datetime, timedelta, timezone
from fastapi import Query
import math
router = APIRouter()
from ..auth.authentication import user_dependency

@router.post("/api/sensor-data-report/export-csv-gz/{site_id}")
async def export_sensor_data_csv_gz(
     user: user_dependency,
    site_id: int,
    from_date: str = Form(...),
    to_date: str = Form(...),
    station_id: Union[int, str] = Form("all"),
    station_param_id: Union[int, str] = Form("all"),
    monitoring_type_id: Union[int, str] = Form("all"),
    time_interval: str = Form("1hr"),
    db: Session = Depends(getdb),
):
    """
    📦 Exports gzipped CSV with averaged sensor data.
    ✅ Uses processed sensor tables or continuous aggregates.
    ✅ Time window strictly aligned to IST.
    """

    # 🕓 Parse and convert to IST
    try:
        start_date = parser.isoparse(from_date)
        end_date = parser.isoparse(to_date)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid date format")

    ist = timezone(timedelta(hours=5, minutes=30))
    ist_start = start_date.astimezone(ist)
    ist_end = end_date.astimezone(ist)

    if time_interval == "1day":
        ist_end += timedelta(days=1)

    # ✅ Supported intervals & tables
    VALID_TIME_INTERVALS = ["15min", "1hr", "1day"]
    TIME_INTERVAL_TABLES = {
        "15min": {"table": "sensor_agg_15min", "bucket_col": "bucket"},
        "1hr": {"table": "sensor_stddev_1hr", "bucket_col": "bucket_ist"},
        "1day": {"table": "sensor_stddev_1hr", "bucket_col": "bucket_ist"},
    }

    if time_interval not in VALID_TIME_INTERVALS:
        raise HTTPException(status_code=400, detail=f"Invalid interval: {time_interval}")

    table_info = TIME_INTERVAL_TABLES[time_interval]
    table_name = table_info["table"]
    bucket_col = table_info["bucket_col"]

    # 🔍 Filters
    filters = [
        "s.id = :site_id",
        f"agg.{bucket_col} >= :ist_start",
        f"agg.{bucket_col} < :ist_end",
    ]
    params = {"site_id": site_id, "ist_start": ist_start, "ist_end": ist_end}

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
    bucket_expr = f"agg.{bucket_col}"

    # 🧠 SQL query
    if time_interval == "1hr":
        # 1-hour: use pre-aggregated CAGG directly
        sql = f"""
            SELECT
                {bucket_expr} AS time_interval,
                agg.avg_value,
                agg.stddev_value,
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
            ORDER BY mt.monitoring_type, {bucket_expr};
        """
    elif time_interval == "1day":
        # 1-day: aggregate from 1-hour CAGG
        sql = f"""
            SELECT
                date_trunc('day', agg.bucket_ist) AS time_interval,
                AVG(agg.avg_value) AS avg_value,
                sqrt(
                    AVG(agg.stddev_value * agg.stddev_value) + VAR_SAMP(agg.avg_value)
                ) AS stddev_value,
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
            GROUP BY date_trunc('day', agg.bucket_ist),
                     s.site_name, p.name, a.analyser_name,
                     mt.monitoring_type, st.name,
                     s.address, s.city, s.state
            ORDER BY mt.monitoring_type, time_interval;
        """
    else:
        # 15-min: aggregate from raw 15-min table
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

    # 🚀 Execute query
    rows = db.execute(text(sql), params).fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail="No data found")

    # 🏷 Group name lookup
    group_name = (
        db.execute(
            select(Group.group_name)
            .join(Site, Site.group_id == Group.id)
            .where(Site.id == site_id)
        ).scalar()
        or "No group"
    )

    # 🧾 Prepare export rows
    data_rows = [
        {
            "time_interval": row.time_interval,
            "avg_value": row.avg_value,
            "stddev_value": getattr(row, "stddev_value", None),
            "site_name": row.site_name,
            "site_address": row.site_address,
            "parameter_name": row.parameter_name,
            "analyser_name": row.analyser_name,
            "station_name": row.station_name,
            "monitoring_type_name": row.monitoring_type_name,
        }
        for row in rows
    ]

    # 📦 Gzip stream generator
    def gz_stream():
        buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
            text_wrapper = io.TextIOWrapper(gz, encoding="utf-8", newline="")
            writer = csv.writer(text_wrapper)
            headers = [
                "time_interval",
                "avg_value",
                "site_name",
                "site_address",
                "group_name",
                "parameter_name",
                "analyser_name",
                "station_name",
                "monitoring_type_name",
            ]
            if time_interval in ["1hr", "1day"]:
                headers.insert(2, "stddev_value")
            writer.writerow(headers)

            for row_data in data_rows:
                ti_ist = row_data["time_interval"].astimezone(ist)
                row = [
                    ti_ist.strftime("%Y-%m-%d %H:%M:%S"),
                    round(float(row_data["avg_value"] or 0), 2),
                ]
                if time_interval in ["1hr", "1day"]:
                    row.append(round(float(row_data["stddev_value"] or 0), 2))
                row.extend([
                    row_data["site_name"],
                    row_data["site_address"],
                    group_name,
                    row_data["parameter_name"],
                    row_data["analyser_name"],
                    row_data["station_name"],
                    row_data["monitoring_type_name"],
                ])
                writer.writerow(row)
            text_wrapper.flush()
        buffer.seek(0)
        yield buffer.read()

    # 🧾 Filename
    filename = (
        f"sensor_data_site_{site_id}_{ist_start:%Y%m%d_%H%M%S}_"
        f"{ist_end:%Y%m%d_%H%M%S}.csv.gz"
    )

    # ✅ Stream response
    return StreamingResponse(
        gz_stream(),
        media_type="application/gzip",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "X-Accel-Buffering": "no",
        },
    )

@router.get(
    "/api/exceedance-report-daily/{site_id}/{station_id}/{station_param_id}",
    tags=["sensor exceedance report"],
)


async def get_exceedance_report_daily(
    user: user_dependency,
    site_id: int,
    station_id: int,
    station_param_id: int,
    from_date: str,
    to_date: str,
    db: Session = Depends(getdb),
):
    """
    ✅ Daily exceedance report using:
        • Raw `sensor_data`
        • 15-minute averages from `sensor_agg_15min`
    Threshold source: `station_parameters.para_threshold`
    Exceedance rule:
        • Raw exceedance: value > threshold
        • 15-min exceedance: avg_value > threshold * 1.10
    """

    try:
        # ---------- Parse and localize dates ----------
        ist = pytz.timezone("Asia/Kolkata")
        start_date = parser.isoparse(from_date).astimezone(ist)
        end_date = parser.isoparse(to_date).astimezone(ist)

        # ---------- RAW DATA DAILY AGGREGATION ----------
        raw_query = text("""
            SELECT 
                DATE(sd.time AT TIME ZONE 'Asia/Kolkata') AS date_ist,
                p.name AS parameter_name,
                spm.para_threshold AS limit_value,
                spm.para_unit AS unit,
                MIN(sd.value) AS min_value,
                MAX(sd.value) AS max_value,
                AVG(sd.value) AS avg_value,
                STDDEV_POP(sd.value) AS stddev_value,
                SUM(CASE WHEN sd.value > spm.para_threshold THEN 1 ELSE 0 END) AS exceed_count_raw,
                COUNT(*) AS total_records,
                st.site_name,
                stn.name AS station_name
            FROM sensor_data sd
            JOIN station_parameters spm 
                ON sd.station_param_id = spm.id AND sd.station_id = spm.station_id
            JOIN stations stn ON spm.station_id = stn.id
            JOIN site st ON stn.site_id = st.id
            JOIN analyser_parameter ap ON spm.analyser_param_id = ap.id
            JOIN parameters p ON ap.parameter_id = p.id
            WHERE sd.station_param_id = :station_param_id
              AND stn.id = :station_id
              AND st.id = :site_id
              AND sd.time BETWEEN :start_date AND :end_date
            GROUP BY DATE(sd.time AT TIME ZONE 'Asia/Kolkata'),
                     p.name, spm.para_threshold, spm.para_unit, st.site_name, stn.name
            ORDER BY DATE(sd.time AT TIME ZONE 'Asia/Kolkata');
        """)

        raw_result = db.execute(
            raw_query,
            {
                "site_id": site_id,
                "station_id": station_id,
                "station_param_id": station_param_id,
                "start_date": start_date,
                "end_date": end_date,
            },
        ).fetchall()

        # ---------- Graceful Empty Result Handling ----------
        if not raw_result:
            return {
                "debug": {
                    "from_date": start_date.isoformat(),
                    "to_date": end_date.isoformat(),
                    "records_fetched": 0,
                    "source_tables": ["sensor_data", "sensor_agg_15min"],
                    "message": "No raw data found for the given filters"
                },
                "site_id": site_id,
                "station_id": station_id,
                "station_param_id": station_param_id,
                "daily_data": []
            }

        # ---------- 15-MINUTE EXCEEDANCE AGGREGATION ----------
        exceed_15_query = text("""
            SELECT 
                DATE(sa.bucket AT TIME ZONE 'Asia/Kolkata') AS date_ist,
                COUNT(*) AS total_15min_records,
                SUM(CASE WHEN sa.avg_value > spm.para_threshold * 1.10 THEN 1 ELSE 0 END) AS exceed_count_15min
            FROM sensor_agg_15min sa
            JOIN station_parameters spm ON sa.station_param_id = spm.id
            WHERE sa.station_param_id = :station_param_id
              AND sa.bucket BETWEEN :start_date AND :end_date
            GROUP BY DATE(sa.bucket AT TIME ZONE 'Asia/Kolkata')
            ORDER BY DATE(sa.bucket AT TIME ZONE 'Asia/Kolkata');
        """)

        exceed_15_result = db.execute(
            exceed_15_query,
            {
                "station_param_id": station_param_id,
                "start_date": start_date,
                "end_date": end_date,
            },
        ).fetchall()

        # ✅ DATE() returns date, so don't call .date()
        exceed_15_map = {row.date_ist: row for row in exceed_15_result}

        # ---------- GROUP NAME LOOKUP ----------
        group_query = (
            select(Group.group_name)
            .select_from(Site)
            .join(Group, Site.group_id == Group.id, isouter=True)
            .where(Site.id == site_id)
        )
        group_name = db.execute(group_query).scalar() or "Not associated with a group"

        # ---------- BUILD DAILY DATA ----------
        daily_data = []
        for row in raw_result:
            date_key = row.date_ist

            exceed15 = exceed_15_map.get(date_key)
            exceed15_count = exceed15.exceed_count_15min if exceed15 else 0
            exceed15_total = exceed15.total_15min_records if exceed15 else 0

            exceed_percent_raw = round(
                (row.exceed_count_raw / row.total_records * 100)
                if row.total_records else 0, 2
            )
            exceed_percent_15min = round(
                (exceed15_count / exceed15_total * 100)
                if exceed15_total else 0, 2
            )

            daily_data.append({
                "date": row.date_ist.strftime("%Y-%m-%d"),
                "min_value": row.min_value,
                "max_value": row.max_value,
                "avg_value": row.avg_value,
                "std_deviation": row.stddev_value,
                "limit_value": row.limit_value,
                "unit": row.unit,

                # Raw exceedance info
                "exceed_count_raw": row.exceed_count_raw,
                "total_records_raw": row.total_records,
                "exceed_percent_raw": exceed_percent_raw,
                "is_exceed_raw": row.exceed_count_raw > 0,

                # 15-min exceedance info
                "exceed_count_15min": exceed15_count,
                "total_records_15min": exceed15_total,
                "exceed_percent_15min": exceed_percent_15min,
                "is_exceed_15min": exceed15_count > 0,

                # Combined exceedance flag
                "is_exceed_overall": (row.exceed_count_raw > 0) or (exceed15_count > 0)
            })

        # ---------- FINAL RESPONSE ----------
        response = {
            "debug": {
                "from_date": start_date.isoformat(),
                "to_date": end_date.isoformat(),
                "records_fetched": len(daily_data),
                "source_tables": ["sensor_data", "sensor_agg_15min"],
                "threshold_source": "station_parameters.para_threshold",
                "15min_exceed_logic": "avg_value > threshold * 1.10"
            },
            "site_id": site_id,
            "site_name": raw_result[0].site_name,
            "station_id": station_id,
            "station_name": raw_result[0].station_name,
            "station_param_id": station_param_id,
            "parameter_name": raw_result[0].parameter_name,
            "unit": raw_result[0].unit,
            "group_name": group_name,
            "daily_data": daily_data
        }

        return response

    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/api/site-station-parameter-stddev-today/{site_id}",
    tags=["sensor stats"]
)
async def get_site_station_parameter_stddev_today(
    user: user_dependency,
    site_id: int,
    db: Session = Depends(getdb),
):
    """
    Returns TRUE full-day (00:00 to now) weighted standard deviation
    computed from hourly buckets in sensor_stddev_1hr.

    Uses:
        n, sum_x, sum_x2
    to compute:
        stddev = sqrt( (Σx2 / N) - (mean^2) )
    
    Timezone: Asia/Kolkata
    """

    try:
        # ---------- Time window (IST) ----------
        ist = pytz.timezone("Asia/Kolkata")
        now_ist = datetime.now(ist)

        from_date = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)
        to_date = now_ist

        # ---------- SQL QUERY ----------
        # We use SUM(n), SUM(sum_x), SUM(sum_x2) per parameter
        query = text("""
            SELECT
                si.id AS site_id,
                si.site_name,
                st.id AS station_id,
                st.name AS station_name,
                sp.id AS station_param_id,
                p.name AS parameter_name,
                sp.para_unit AS unit,
                p.monitoring_type_id AS monitoring_type_id,
                mt.monitoring_type AS monitoring_type_name,
                
                SUM(sdv.n) AS total_n,
                SUM(sdv.sum_x) AS total_sum_x,
                SUM(sdv.sum_x2) AS total_sum_x2

            FROM sensor_stddev_1hr sdv
            JOIN station_parameters sp ON sp.id = sdv.station_param_id
            JOIN stations st ON st.id = sp.station_id
            JOIN site si ON si.id = st.site_id
            JOIN analyser_parameter ap ON sp.analyser_param_id = ap.id
            JOIN parameters p ON ap.parameter_id = p.id
            JOIN monitoring_types mt ON p.monitoring_type_id = mt.id

            WHERE si.id = :site_id
              AND sdv.bucket_ist BETWEEN :from_date AND :to_date

            GROUP BY
                si.id, si.site_name, st.id, st.name,
                sp.id, p.name, sp.para_unit,
                p.monitoring_type_id, mt.monitoring_type

            ORDER BY st.name, p.name;
        """)

        rows = db.execute(
            query,
            {"site_id": site_id, "from_date": from_date, "to_date": to_date}
        ).fetchall()

        if not rows:
            return {
                "site_id": site_id,
                "site_name": None,
                "records_found": 0,
                "message": "No hourly stddev data found today",
                "data": []
            }

        # ---------- Calculate weighted stddev ----------
        data = []

        for r in rows:
            N = float(r.total_n or 0)
            sum_x = float(r.total_sum_x or 0)
            sum_x2 = float(r.total_sum_x2 or 0)

            if N > 0:
                mean = sum_x / N
                variance = (sum_x2 / N) - (mean * mean)
                stddev_today = math.sqrt(variance) if variance > 0 else 0
            else:
                stddev_today = 0

            data.append({
                "station_id": r.station_id,
                "station_name": r.station_name,
                "station_param_id": r.station_param_id,
                "parameter_name": r.parameter_name,
                "unit": r.unit,
                "monitoring_type_id": r.monitoring_type_id,
                "monitoring_type_name": r.monitoring_type_name,
                "stddev_value": round(stddev_today, 3),
                "total_samples": int(N)
            })

        return {
            "site_id": rows[0].site_id,
            "site_name": rows[0].site_name,
            "from_date": from_date.isoformat(),
            "to_date": to_date.isoformat(),
            "records_found": len(data),
            "data": data
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")



@router.get(
    "/api/site-station-parameter-stddev/{site_id}",
    tags=["sensor stats"]
)
async def get_site_station_parameter_stddev(
    user: user_dependency,
    site_id: int,
    station_id: int = Query(..., description="Station ID to fetch data for"),
    station_param_id: int = Query(..., description="Station Parameter ID to fetch data for"),
    db: Session = Depends(getdb),
):
    """
    Returns last 7 days (today + previous 6 days)
    weighted standard deviation of raw sensor readings,
    computed using hourly buckets from sensor_stddev_1hr.
    """

    try:
        # ---------- SQL Query Using sensor_stddev_1hr ----------
        query = text("""
            SELECT
                si.id AS site_id,
                si.site_name,
                st.id AS station_id,
                st.name AS station_name,
                sp.id AS station_param_id,
                p.name AS parameter_name,
                sp.para_unit AS unit,

                DATE(sdv.bucket_ist) AS reading_date,

                SUM(sdv.n) AS total_n,
                SUM(sdv.sum_x) AS total_sum_x,
                SUM(sdv.sum_x2) AS total_sum_x2

            FROM sensor_stddev_1hr sdv
            JOIN station_parameters sp ON sp.id = sdv.station_param_id
            JOIN stations st ON st.id = sp.station_id
            JOIN site si ON si.id = st.site_id
            JOIN analyser_parameter ap ON sp.analyser_param_id = ap.id
            JOIN parameters p ON ap.parameter_id = p.id

            WHERE si.id = :site_id
              AND st.id = :station_id
              AND sp.id = :station_param_id
              AND DATE(sdv.bucket_ist) >= CURRENT_DATE - INTERVAL '6 days'
              AND DATE(sdv.bucket_ist) <= CURRENT_DATE

            GROUP BY
                si.id, si.site_name, st.id, st.name,
                sp.id, p.name, sp.para_unit,
                DATE(sdv.bucket_ist)

            ORDER BY reading_date;
        """)

        rows = db.execute(
            query,
            {
                "site_id": site_id,
                "station_id": station_id,
                "station_param_id": station_param_id,
            }
        ).fetchall()

        if not rows:
            return {
                "site_id": site_id,
                "station_id": station_id,
                "station_param_id": station_param_id,
                "records_found": 0,
                "message": "No stddev data found in last 7 days",
                "data": []
            }

        # ---------- Compute Weighted Stddev per Day ----------
        data = []

        for r in rows:
            N = float(r.total_n or 0)
            sum_x = float(r.total_sum_x or 0)
            sum_x2 = float(r.total_sum_x2 or 0)

            if N > 0:
                mean = sum_x / N
                variance = (sum_x2 / N) - (mean * mean)
                stddev = math.sqrt(variance) if variance > 0 else 0
            else:
                stddev = 0

            data.append({
                "date": r.reading_date.strftime("%Y-%m-%d"),
                "stddev_value": round(stddev, 3)    # FE expects this name
            })

        return {
            "site_id": rows[0].site_id,
            "site_name": rows[0].site_name,
            "station_id": rows[0].station_id,
            "station_name": rows[0].station_name,
            "station_param_id": rows[0].station_param_id,
            "parameter_name": rows[0].parameter_name,
            "unit": rows[0].unit,
            "records_found": len(data),
            "data": data
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")

# OM VIGHNHARTAYE NAMO NAMAH :
from fastapi import FastAPI, HTTPException , APIRouter, Query
from sqlalchemy.orm import Session 
from fastapi import APIRouter, HTTPException, Depends, Form, File, UploadFile
from sqlalchemy.exc import SQLAlchemyError 
from starlette import status
from sqlalchemy.sql import text
import math
import datetime
from typing import Optional

from ...schemas.masterSchema import *
from ...modals.masters import *
from ...database.session import getdb
from ...utils.utils import response_strct
from collections import defaultdict
from typing import Dict, List
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from ..auth.authentication import user_dependency

router = APIRouter()

class SensorDataResponse(BaseModel):
    time: datetime.datetime
    station_name: str
    parameter_name: str
    site_id: int
    station_id: int
    parameter_id: int
    avg_value: float
    data_points_count: int      

@router.get("/sensor-data/{site_id}", response_model=List[SensorDataResponse], tags=['charts'])
def get_sensor_data(user: user_dependency,site_id: int, db: Session = Depends(getdb)):
    try:
        # Raw SQL query using SQLAlchemy
        query = text("""
        SELECT
            time_bucket('15 minutes', sd.time) AS fifteen_min_interval,
            s.name AS station_name,
            p.name AS parameter_name,
            sd.site_id,
            sd.station_id,
            sd.parameter_id,
            AVG(sd.value) AS avg_value,
            COUNT(*) AS data_points_count
        FROM
            sensor_data sd
        JOIN
            stations s ON sd.station_id = s.id
        JOIN
            parameters p ON sd.parameter_id = p.id
        WHERE
            sd.site_id = :site_id
        GROUP BY
            fifteen_min_interval,
            s.name,
            p.name,
            sd.site_id,
            sd.station_id,
            sd.parameter_id
        ORDER BY
            fifteen_min_interval;
        """)
        result = db.execute(query, {"site_id": site_id}).fetchall()

        # Format the result into the response model
        response = [
            SensorDataResponse(
                time=row.fifteen_min_interval,
                station_name=row.station_name,
                parameter_name=row.parameter_name,
                site_id=row.site_id,
                station_id=row.station_id,
                parameter_id=row.parameter_id,
                avg_value=row.avg_value,
                data_points_count=row.data_points_count,
            )
            for row in result
        ]
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        db.close()



@router.get("/api/site/dashboard/{site_id}", tags=['charts'])
def get_sensor_data(user: user_dependency,site_id: int, db: Session = Depends(getdb)):
    try:
        # Site details query (unchanged)
        site_query = text("""
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
            g.group_name
        FROM
            site s
        LEFT JOIN
            "group" g ON s.group_id = g.id
        WHERE
            s.id = :site_id;
        """)
        site_result = db.execute(site_query, {"site_id": site_id}).fetchone()
        if not site_result:
            raise HTTPException(status_code=404, detail="Site not found")

        # Main sensor data query (unchanged)
        query = text("""
        SELECT
            time_bucket('15 minutes', sd.time) AS fifteen_min_interval,
            s.name AS station_name,
            p.name AS parameter_name,
            p.label AS parameter_label,
            p.unit AS parameter_unit,
            p.min_thershold AS min_threshold,
            p.max_thershold AS max_threshold,
            mt.monitoring_type AS monitoring_type_name,
            a.analyser_name AS analyzer_name,
            sd.site_id,
            sd.station_id,
            sd.parameter_id,
            AVG(sd.value) AS avg_value,
            COUNT(*) AS data_points_count
        FROM
            sensor_data sd
        JOIN
            stations s ON sd.station_id = s.id
        JOIN
            parameters p ON sd.parameter_id = p.id
        JOIN
            analysers a ON sd.analyser_id = a.id
        LEFT JOIN
            monitoring_types mt ON p.monitoring_type_id = mt.id
        WHERE
            sd.site_id = :site_id
            AND sd.time >= NOW() - INTERVAL '24 hours'
        GROUP BY
            fifteen_min_interval,
            s.name,
            p.name,
            p.label,
            p.unit,
            p.min_thershold,
            p.max_thershold,
            mt.monitoring_type,
            a.analyser_name,
            sd.site_id,
            sd.station_id,
            sd.parameter_id
        ORDER BY
            fifteen_min_interval;
        """)
        result = db.execute(query, {"site_id": site_id}).fetchall()
        # Query to fetch the latest value for each parameter
        latest_value_query = text("""
        SELECT
            s.name AS station_name,
            p.name AS parameter_name,
            p.unit AS parameter_unit,
            a.analyser_name AS analyzer_name,
            sd.value AS latest_value,
            sd.time AS latest_time
        FROM
            sensor_data sd
        JOIN
            stations s ON sd.station_id = s.id
        JOIN
            parameters p ON sd.parameter_id = p.id
        JOIN
            analysers a ON sd.analyser_id = a.id
        WHERE
            sd.site_id = :site_id
            AND sd.time = (
                SELECT MAX(time)
                FROM sensor_data sd2
                WHERE sd2.site_id = sd.site_id
                AND sd2.station_id = sd.station_id
                AND sd2.parameter_id = sd.parameter_id
                AND sd2.analyser_id = sd.analyser_id
            );
        """)
        latest_values = db.execute(latest_value_query, {"site_id": site_id}).fetchall()
        # Updated key: use (station_name, parameter_name)
        latest_value_map = {
            (row.station_name, row.parameter_name): { 
                "value": float(row.latest_value), 
                "time": row.latest_time.isoformat() 
            }
            for row in latest_values
        }

        # Transform the result into the desired structure
        response_map = {}
        for row in result:
            # Updated key: remove analyzer_name from the tuple
            key = (row.station_name, row.parameter_name)
            time_interval = row.fifteen_min_interval.isoformat()
            avg_value = float(row.avg_value)

            if key not in response_map:
                response_map[key] = {
                    "stationName": row.station_name,
                    "parameterName": row.station_name+ "-"+row.parameter_label,  # assuming label is desired for display
                    "analyzer": row.analyzer_name,
                    "unit": row.parameter_unit,
                    "parameterLabel": row.parameter_label,
                    "parameterUnit": row.parameter_unit if row.parameter_unit != "nan" else "",
                    "minThreshold": row.min_threshold,
                    "maxThreshold": row.max_threshold,
                    "monitoringType": row.monitoring_type_name,
                    "latestValue": latest_value_map.get(key),  # Updated key lookup
                    "x_axis": [],
                    "y_axis": [],
                }

            response_map[key]["x_axis"].append(time_interval)
            response_map[key]["y_axis"].append(avg_value)

        # Calculate totalExceedingParameters based on whether the latest value exceeds the thresholds.
        total_exceeding = 0
        for param in response_map.values():
            latest = param.get("latestValue")
            if latest:
                if latest["value"] < param["minThreshold"] or latest["value"] > param["maxThreshold"]:
                    total_exceeding += 1

        # Calculate sensor data availability percentage for the last 1 hour.
        one_hour_ago = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)
        sensor_data_count_query = text("""
            SELECT COUNT(*) as total_count 
            FROM sensor_data 
            WHERE site_id = :site_id AND time >= :one_hour_ago
        """)
        sensor_data_count_result = db.execute(sensor_data_count_query, {"site_id": site_id, "one_hour_ago": one_hour_ago}).fetchone()
        actual_count = sensor_data_count_result.total_count if sensor_data_count_result else 0

        # Count distinct parameter combinations using a subquery.
        distinct_params_query = text("""
            SELECT COUNT(*) as param_count FROM (
                SELECT DISTINCT device_id, station_id, parameter_id, analyser_id 
                FROM sensor_data 
                WHERE site_id = :site_id AND time >= :one_hour_ago
            ) as distinct_params
        """)
        distinct_params_result = db.execute(distinct_params_query, {"site_id": site_id, "one_hour_ago": one_hour_ago}).fetchone()
        distinct_params_count = distinct_params_result.param_count if distinct_params_result else 0
        
        expected_count = distinct_params_count * (3600 / 5)
        data_availability = round((actual_count / expected_count) * 100, 2) if expected_count > 0 else 0
        # Cap availability at 100%
        # if data_availability > 100:
        #     data_availability = 100
        # Get the number of devices configured under the site.
        device_count_query = text("SELECT COUNT(*) as device_count FROM device WHERE site_id = :site_id")
        device_count_result = db.execute(device_count_query, {"site_id": site_id}).fetchone()
        device_availablity = device_count_result.device_count if device_count_result else 0

        monitoring_stations = db.query(Station).filter(Station.site_id == site_id).count()
        site_details = {
            "siteId": site_result.site_id,
            "siteUID": site_result.siteuid,
            "siteName": site_result.site_name,
            "address": site_result.address,
            "city": site_result.city,
            "state": site_result.state,
            "latitude": float(site_result.latitude) if site_result.latitude else None,
            "longitude": float(site_result.longitude) if site_result.longitude else None,
            #"authKey": site_result.authkey,
            "authExpiry": site_result.auth_expiry.isoformat() if site_result.auth_expiry else None,
            "groupName": site_result.group_name,
            "totalMonitoringStations": monitoring_stations,
            "totalParametersCount": len(response_map),
            "totalExceedingParameters": total_exceeding,
            "dataAvailablity": data_availability,
            "deviceAvailablity": device_availablity,
            "lastfetchedTime": datetime.datetime.now().isoformat()
        }

        response = {
            "siteDetails": site_details,
            "parameterList": list(response_map.values())
        }

        return response

    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        db.close()

@router.get("/api/site/dashboard/card-details/{site_id}", tags=["dashboard"])
def get_card_details(user: user_dependency,site_id: int, db: Session = Depends(getdb)):
    try:
        query = text("""
        SELECT
            s.name AS station_name,
            p.name AS parameter_name,
            p.label AS parameter_label,
            p.unit AS parameter_unit,
            p.min_thershold AS min_threshold,
            p.max_thershold AS max_threshold,
            mt.monitoring_type AS monitoring_type_name,
            sd.value AS latest_value,
            sd.time AS latest_time
        FROM sensor_data sd
        JOIN stations s ON sd.station_id = s.id
        JOIN parameters p ON sd.parameter_id = p.id
        JOIN analysers a ON sd.analyser_id = a.id
        LEFT JOIN monitoring_types mt ON p.monitoring_type_id = mt.id
        WHERE sd.site_id = :site_id
          AND sd.time = (
              SELECT MAX(sd2.time)
              FROM sensor_data sd2
              WHERE sd2.site_id = sd.site_id
                AND sd2.station_id = sd.station_id
                AND sd2.parameter_id = sd.parameter_id
                AND sd2.analyser_id = sd.analyser_id
          );
        """)
        results = db.execute(query, {"site_id": site_id}).fetchall()

        card_details = []
        for row in results:
            card_details.append({
                "stationName": row.station_name,
                "parameterName": f"{row.station_name}-{row.parameter_label}",
                "unit": row.parameter_unit,
                "parameterLabel": row.parameter_label,
                "parameterUnit": row.parameter_unit if row.parameter_unit != "nan" else "",
                "minThreshold": row.min_threshold,
                "maxThreshold": row.max_threshold,
                "monitoringType": row.monitoring_type_name,
                "latestValue": {
                    "value": float(row.latest_value),
                    "time": row.latest_time.isoformat()
                } if row.latest_value is not None else None
            })

        return {"cardDetails": card_details}
    except Exception as e:
        raise HTTPException(status_code=500, detail='Internal server error')
    finally:
        db.close()


def _sanitize(obj):
    """
    Recursively walk through dicts/lists and replace any
    non-finite floats (NaN, ±Inf) with None.
    """
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    return obj

@router.get("/api/site/dashboard/chart-details/{site_id}", tags=["charts"])
def get_chart_details(
    user: user_dependency,
    site_id: int,
    offset: int = 0,
    limit: int = 15,
    db: Session = Depends(getdb),
):
    try:
        # 1) Get paginated (station_id, parameter_id) pairs for non‑expired stations
        key_query = text("""
            SELECT DISTINCT sd.station_id, sd.parameter_id
            FROM sensor_data sd
            JOIN stations s
              ON sd.station_id = s.id
            WHERE sd.site_id = :site_id
              AND (s.calibration_expiry_date IS NULL OR s.calibration_expiry_date >= NOW())
            ORDER BY sd.station_id, sd.parameter_id
            OFFSET :offset LIMIT :limit
        """)
        key_rows = db.execute(
            key_query,
            {"site_id": site_id, "offset": offset, "limit": limit}
        ).fetchall()
        if not key_rows:
            return JSONResponse({
                "chartDetails": [],
                "offset": offset,
                "limit": limit,
                "hasMore": False
            })

        key_set = [(r.station_id, r.parameter_id) for r in key_rows]
        values_clause = ",".join(f"({sid},{pid})" for sid, pid in key_set)

        # 2) Fetch latest values for those keys
        latest_query = text(f"""
            WITH selected_keys (station_id, parameter_id) AS (
                VALUES {values_clause}
            ),
            latest_details AS (
                SELECT DISTINCT ON (sd.station_param_id)
                    sd.station_id,
                    sd.parameter_id,
                    s.name        AS station_name,
                    p.name        AS parameter_name,
                    p.label       AS parameter_label,
                    sd.value      AS latest_value,
                    sd.time       AS latest_time,
                    sp.para_threshold AS max_threshold,
                    sp.para_unit      AS parameter_unit
                FROM sensor_data sd
                JOIN selected_keys sk
                  ON sd.station_id = sk.station_id
                 AND sd.parameter_id = sk.parameter_id
                JOIN station_parameters sp
                  ON sd.station_param_id = sp.id
                JOIN analyser_parameter ap
                  ON sp.analyser_param_id = ap.id
                JOIN parameters p
                  ON ap.parameter_id = p.id
                JOIN stations s
                  ON sd.station_id = s.id
                WHERE sd.site_id = :site_id
                ORDER BY sd.station_param_id, sd.time DESC
            )
            SELECT * FROM latest_details;
        """)
        latest_rows = db.execute(latest_query, {"site_id": site_id}).fetchall()

        latest_map = {
            (row.station_id, row.parameter_id): {
                "stationName":    row.station_name,
                "parameterName":  row.parameter_name,
                "parameterLabel": row.parameter_label,
                "latestValue": {
                    "value": float(row.latest_value) if row.latest_value is not None else None,
                    "time":  row.latest_time.isoformat() if row.latest_time else None
                } if row.latest_value is not None else None,
                "maxThreshold": float(row.max_threshold) if row.max_threshold is not None else None,
                "unit":         ("" if row.parameter_unit == "nan" else row.parameter_unit)
            }
            for row in latest_rows
        }

        # 3) Fetch 24h time series for those same keys (already filtered by non‑expired stations)
        data_query = text(f"""
            WITH selected_keys (station_id, parameter_id) AS (
                VALUES {values_clause}
            )
            SELECT
                time_bucket('1 hour', sd.time) AS bucket,
                sd.station_id,
                sd.parameter_id,
                s.name AS station_name,
                p.name AS parameter_name,
                AVG(sd.value) AS avg_value
            FROM sensor_data sd
            JOIN selected_keys sk
              ON sd.station_id = sk.station_id
             AND sd.parameter_id = sk.parameter_id
            JOIN stations s
              ON sd.station_id = s.id
            JOIN station_parameters sp
              ON sd.station_param_id = sp.id
            JOIN analyser_parameter ap
              ON sp.analyser_param_id = ap.id
            JOIN parameters p
              ON ap.parameter_id = p.id
            WHERE sd.site_id = :site_id
              AND sd.time >= NOW() - INTERVAL '24 hours'
              AND (s.calibration_expiry_date IS NULL OR s.calibration_expiry_date >= NOW())
            GROUP BY bucket, sd.station_id, sd.parameter_id, s.name, p.name
            ORDER BY sd.station_id, sd.parameter_id, bucket;
        """)
        rows = db.execute(data_query, {"site_id": site_id}).fetchall()

        # 4) Assemble chart blocks
        chart_map: dict[tuple[int, int], dict] = {}
        for r in rows:
            key = (r.station_id, r.parameter_id)
            meta = latest_map.get(key)
            if not meta:
                continue

            block = chart_map.setdefault(key, {
                "stationName":    meta["stationName"],
                "parameterName":  meta["parameterName"],
                "parameterLabel": meta["parameterLabel"],
                "x_axis":         [],
                "y_axis":         [],
                "latestValue":    meta["latestValue"],
                "maxThreshold":   meta["maxThreshold"],
                "unit":           meta["unit"],
            })

            avg_val = (float(r.avg_value)
                       if r.avg_value is not None and math.isfinite(r.avg_value)
                       else None)
            block["x_axis"].append(r.bucket.isoformat())
            block["y_axis"].append(avg_val)

        return JSONResponse({
            "chartDetails": list(chart_map.values()),
            "offset": offset,
            "limit": limit,
            "hasMore": len(key_set) == limit
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail='Internal server error')
    finally:
        db.close()

from sqlalchemy import Table, MetaData
import pytz



@router.get("/api/site/dashboard/details/{site_id}", tags=['dashboard'])
def get_site_details(site_id: int, user: user_dependency, db: Session = Depends(getdb)):
    try:
        # --------------------------------------
        # 1. IST Time Window (last 24 hours)
        # --------------------------------------
        ist = pytz.timezone("Asia/Kolkata")
        now_ist = datetime.datetime.now(ist)
        start_ist = now_ist - datetime.timedelta(hours=24)

        # --------------------------------------
        # 2. Site details
        # --------------------------------------
        site_result = db.execute(text("""
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
                g.group_name
            FROM site s
            LEFT JOIN "group" g ON s.group_id = g.id
            WHERE s.id = :site_id;
        """), {"site_id": site_id}).fetchone()

        if not site_result:
            raise HTTPException(status_code=404, detail="Site not found")

        # --------------------------------------
        # 3. Device count
        # --------------------------------------
        device_count = db.execute(
            text("SELECT COUNT(*) AS device_count FROM device WHERE site_id = :site_id"),
            {"site_id": site_id}
        ).fetchone().device_count

        # --------------------------------------
        # 4. Monitoring stations count
        # --------------------------------------
        monitoring_stations = db.query(Station).filter(Station.site_id == site_id).count()

        # --------------------------------------
        # 5. Total parameters count
        # --------------------------------------
        total_parameters = db.execute(text("""
            SELECT COUNT(*) AS total_parameters
            FROM station_parameters sp
            JOIN stations st ON sp.station_id = st.id
            WHERE st.site_id = :site_id
        """), {"site_id": site_id}).fetchone().total_parameters

        # --------------------------------------
        # 6. PCB parameters
        # --------------------------------------
        pcb_parameter_ct = db.execute(text("""
            SELECT COUNT(*) AS pcb_parameter_ct
            FROM station_parameters sp
            JOIN stations st ON sp.station_id = st.id
            WHERE st.site_id = :site_id AND sp.is_editable = FALSE
        """), {"site_id": site_id}).fetchone().pcb_parameter_ct

        # --------------------------------------
        # 7. NEW DATA AVAILABILITY USING sensor_stddev_1hr
        # --------------------------------------
        availability_result = db.execute(text("""
    WITH time_window AS (
        SELECT 
            CAST(:start_time AS timestamp) AS start_time,
            CAST(:end_time AS timestamp) AS end_time
    ),

    params AS (
        SELECT 
            sp.id AS station_param_id,
            sp.param_interval
        FROM station_parameters sp
        JOIN stations st ON st.id = sp.station_id
        WHERE st.site_id = :site_id
    ),

    expected AS (
        SELECT
            station_param_id,
            CASE 
                WHEN param_interval > 0 THEN CEIL(86400.0 / param_interval)
                ELSE 0
            END AS expected_rows
        FROM params
    ),

    actual AS (
        SELECT
            p.station_param_id,
            COALESCE(SUM(s.n), 0) AS actual_rows
        FROM sensor_stddev_1hr s
        CROSS JOIN time_window tw
        JOIN params p ON p.station_param_id = s.station_param_id
        WHERE s.bucket_ist BETWEEN tw.start_time AND tw.end_time
        GROUP BY p.station_param_id
    ),

    final AS (
        SELECT
            e.station_param_id,
            ROUND(
                (COALESCE(a.actual_rows, 0) / NULLIF(e.expected_rows, 0)::numeric) * 100,
                2
            ) AS availability
        FROM expected e
        LEFT JOIN actual a ON a.station_param_id = e.station_param_id
    )

    SELECT AVG(availability) AS site_availability FROM final;
"""),
{
    "site_id": site_id,
    "start_time": start_ist.strftime("%Y-%m-%d %H:%M:%S"),
    "end_time": now_ist.strftime("%Y-%m-%d %H:%M:%S"),
}).fetchone()


        # Safe float conversion
        site_availability = (
            float(availability_result.site_availability)
            if availability_result and availability_result.site_availability is not None
            else 0.0
        )

        # clamp to 0–100
        site_availability = min(max(site_availability, 0.0), 100.0)

        # --------------------------------------
        # 8. Build Response (same structure)
        # --------------------------------------
        site_details = {
            "siteId": site_result.site_id,
            "siteUID": site_result.siteuid,
            "siteName": site_result.site_name,
            "address": site_result.address,
            "city": site_result.city,
            "state": site_result.state,
            "latitude": float(site_result.latitude) if site_result.latitude else None,
            "longitude": float(site_result.longitude) if site_result.longitude else None,
            "authExpiry": site_result.auth_expiry.isoformat() if site_result.auth_expiry else None,
            "groupName": site_result.group_name,
            "totalMonitoringStations": monitoring_stations,
            "totalParametersCount": total_parameters,
            "pcb_parameter_ct": pcb_parameter_ct,
            "deviceAvailablity": device_count,
            "siteDataAvailability": site_availability,
            "lastfetchedTime": now_ist.isoformat(),
            "availabilityDate": start_ist.strftime("%Y-%m-%d %H:%M"),
        }

        return {"siteDetails": site_details}

    except Exception as e:
        raise HTTPException(status_code=500, detail="Error fetching site details")

    finally:
        db.close()
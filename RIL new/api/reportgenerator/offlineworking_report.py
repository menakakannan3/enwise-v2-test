import logging
from fastapi import APIRouter, Query, Depends
from datetime import datetime, date, time, timedelta
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import Dict, Any
from ..auth.authentication import user_dependency
from ...database.session import getdb

# Configure logging
logging.basicConfig(level=logging.INFO)

router = APIRouter()


@router.get("/offline_report_by_date/", tags=["Date Filtering"])
def get_offline_report_from_site_status(
    user: user_dependency,
    db: Session = Depends(getdb),
    site_id: int = Query(..., description="Site ID to fetch offline report"),
    start_date: date = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: date = Query(..., description="End date (YYYY-MM-DD)"),
) -> Dict[str, Any]:
    """
    Offline report for site_id within date range.
    Calculates offline days, last data received, and Ganga basin status.
    """
    try:
        start_dt = datetime.combine(start_date, time.min)
        end_dt = datetime.combine(end_date, time.max)

        logging.info(f"Generating Offline Report for Site {site_id} from {start_dt} to {end_dt}")

        query = text("""
            WITH total_days AS (
                SELECT GENERATE_SERIES(:start_dt::date, :end_dt::date, interval '1 day')::date AS day
            ),
            online_days AS (
                SELECT 
                    ss.station_param_id,
                    DATE(ss.starttime) AS day,
                    COUNT(*) AS online_entries
                FROM site_status ss
                JOIN station_parameters sp ON ss.station_param_id = sp.id
                JOIN stations st ON sp.station_id = st.id
                WHERE st.site_id = :site_id 
                  AND ss.status = 'Online'
                  AND ss.starttime BETWEEN :start_dt AND :end_dt
                GROUP BY ss.station_param_id, DATE(ss.starttime)
            ),
            last_data_time AS (
                SELECT 
                    sd.site_id,
                    sd.station_id,
                    sd.parameter_id,
                    MAX(sd.time) AS last_seen
                FROM sensor_data sd
                WHERE sd.site_id = :site_id 
                  AND sd.time BETWEEN :start_dt AND :end_dt
                GROUP BY sd.site_id, sd.station_id, sd.parameter_id
            )
            SELECT 
                ROW_NUMBER() OVER () AS s_no,
                s.id AS site_id,
                s.site_name AS industry_name,
                s.address AS full_address,
                s.city,
                s.state,
                st.id AS station_id,
                st.name AS station_name,
                p.id AS parameter_id,
                p.name AS parameter_name,
                p.max_thershold AS parameter_limit,
                COALESCE(ldt.last_seen::text, 'No Data') AS last_data_time,
                (SELECT COUNT(*) FROM total_days) - COUNT(DISTINCT od.day)::int AS offline_days,
                CASE 
                    WHEN s.state IN ('Uttarakhand', 'U.P.', 'M.P.', 'Rajasthan', 'Haryana', 
                                     'Himachal Pradesh', 'Chhattisgarh', 'Jharkhand', 'Bihar', 
                                     'West Bengal', 'Delhi') THEN 'Yes' ELSE 'No' 
                END AS in_ganga_basin
            FROM site s
            JOIN stations st ON st.site_id = s.id
            JOIN station_parameters sp ON sp.station_id = st.id
            JOIN analyser_parameter ap ON sp.analyser_param_id = ap.id
            JOIN parameters p ON ap.parameter_id = p.id
            LEFT JOIN online_days od ON od.station_param_id = sp.id
            LEFT JOIN last_data_time ldt 
                ON ldt.site_id = s.id AND ldt.station_id = st.id AND ldt.parameter_id = p.id
            WHERE s.id = :site_id
            GROUP BY s.id, s.site_name, s.address, s.city, s.state,
                     st.id, st.name,
                     p.id, p.name, p.max_thershold,
                     ldt.last_seen;
        """)

        # Execute safely using bound parameters
        rows = db.execute(query, {
            "start_dt": start_dt,
            "end_dt": end_dt,
            "site_id": site_id
        }).fetchall()

        # Convert results to list of dicts
        columns = list(rows[0]._mapping.keys()) if rows else []
        data = [dict(zip(columns, row)) for row in rows]

        return {"status": "success", "data": data}

    except Exception as e:
        logging.exception("Error while fetching offline report")
        import logging
from fastapi import APIRouter, Query, Depends
from datetime import datetime, date, time, timedelta, timezone
from typing import Union, Dict, Any
from sqlalchemy import text
from sqlalchemy.orm import Session
from ..auth.authentication import user_dependency
from ...database.session import getdb

router = APIRouter()
logging.basicConfig(level=logging.INFO)


@router.get("/sms_alert_report/", tags=['SMS Alert Report'])
def get_sms_alert_report(
    user: user_dependency,
    db: Session = Depends(getdb),
    site_id: int = Query(..., description="Site ID to fetch SMS Alert Report"),
    start_time: Union[datetime, date] = Query(...),
    end_time: Union[datetime, date] = Query(...)
) -> Dict[str, Any]:

    try:
        # ───────────────────────────────
        # Handle IST datetime conversion
        # ───────────────────────────────
        ist = timezone(timedelta(hours=5, minutes=30))
        if isinstance(start_time, date):
            start_time = datetime.combine(start_time, time.min).replace(tzinfo=ist)
        if isinstance(end_time, date):
            end_time = datetime.combine(end_time, time.max).replace(tzinfo=ist)

        # ───────────────────────────────
        # Main query
        # ───────────────────────────────
        query = text("""
            WITH exceedances AS (
                SELECT 
                    st.site_id,
                    st.id AS station_id,
                    p.id AS parameter_id,
                    COUNT(*) AS exceedance_count
                FROM sensor_processed_1hr sp
                JOIN station_parameters spm ON sp.station_param_id = spm.id
                JOIN stations st ON spm.station_id = st.id
                JOIN analyser_parameter ap ON spm.analyser_param_id = ap.id
                JOIN parameters p ON ap.parameter_id = p.id
                WHERE st.site_id = :site_id
                  AND sp.bucket BETWEEN :start_time AND :end_time
                  AND sp.is_exceeded = TRUE
                GROUP BY st.site_id, st.id, p.id
            ),
            sms_alerts AS (
                SELECT 
                    site_id,
                    COUNT(DISTINCT processed_at) AS total_sms_sent
                FROM processed_sms_alerts
                WHERE site_id = :site_id
                  AND time_bucket BETWEEN :start_time AND :end_time
                GROUP BY site_id
            ),
            station_report AS (
                SELECT 
                    st.id AS station_id,
                    st.name AS station_name,
                    JSON_AGG(p.name) AS parameter_names,
                    JSON_AGG(p.max_thershold) AS parameter_limits,
                    JSON_AGG(COALESCE(e.exceedance_count, 0)) AS exceedance_counts
                FROM stations st
                JOIN station_parameters sp ON st.id = sp.station_id
                JOIN analyser_parameter ap ON sp.analyser_param_id = ap.id
                JOIN parameters p ON ap.parameter_id = p.id
                LEFT JOIN exceedances e 
                    ON st.id = e.station_id AND p.id = e.parameter_id
                WHERE st.site_id = :site_id
                GROUP BY st.id, st.name
            ),
            site_report AS (
                SELECT 
                    s.id AS site_id,
                    s.site_name AS industry_name,
                    s.address AS full_address,
                    s.city,
                    s.state,
                    COALESCE(sm.total_sms_sent, 0) AS total_sms_sent,
                    CASE 
                        WHEN s.state IN ('Uttarakhand', 'U.P.', 'M.P.', 'Rajasthan', 'Haryana', 
                                         'Himachal Pradesh', 'Chhattisgarh', 'Jharkhand', 'Bihar', 
                                         'West Bengal', 'Delhi') 
                        THEN 'Yes' ELSE 'No' 
                    END AS in_ganga_basin,
                    COALESCE((
                        SELECT JSON_AGG(JSON_BUILD_OBJECT(
                            'station_id', sr.station_id,
                            'station_name', sr.station_name,
                            'parameters', sr.parameter_names,
                            'parameter_limits', sr.parameter_limits,
                            'exceedance_counts', sr.exceedance_counts
                        ))
                        FROM station_report sr
                    ), '[]'::json) AS stations
                FROM site s
                LEFT JOIN sms_alerts sm ON s.id = sm.site_id
                WHERE s.id = :site_id
                GROUP BY s.id, s.site_name, s.address, s.city, s.state, sm.total_sms_sent
            )
            SELECT * FROM site_report;
        """)

        # ───────────────────────────────
        # Execute query with SQLAlchemy
        # ───────────────────────────────
        rows = db.execute(query, {
            "site_id": site_id,
            "start_time": start_time,
            "end_time": end_time
        }).fetchall()

        columns = list(rows[0]._mapping.keys()) if rows else []
        data = [dict(zip(columns, row)) for row in rows]

        return {
            "status": "success",
            "data": data[0] if data else {}
        }

    except Exception as e:
        logging.exception("Error generating SMS alert report")
        import logging
import re
import calendar
import pytz
from datetime import datetime, date, timedelta
from typing import List, Dict, Any

from fastapi import APIRouter, Query, Form, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..auth.authentication import user_dependency
from ...database.session import getdb

# Configure logging
logging.basicConfig(level=logging.INFO)
router = APIRouter()

# ───────────────────────────────────────────────
# 🧮 KLM REPORT (Optimized)
# ───────────────────────────────────────────────
@router.get("/api/report/klm/all", tags=["KLM Report"])
def get_klm_report_optimized(
    user: user_dependency,
    db: Session = Depends(getdb),
    station_name: str = Query(..., description="Station name"),
    year: int = Query(..., description="Year, e.g. 2025"),
    month: int = Query(..., description="Month (1–12) or 0 for all-months"),
    day: int = Query(None, description="Day (ignored if month=0)"),
    plant_name: str = Query("", description="Plant name, or empty for ALL"),
) -> Dict[str, Any]:
    try:
        today = date.today()

        # 1️⃣ Fetch active formulas
        formulas = db.execute(
            text("""
                SELECT plant_name, formula, cfo_limit_klm
                FROM station_formulas
                WHERE station_name = :station_name AND is_active = TRUE
            """),
            {"station_name": station_name}
        ).fetchall()

        pname = plant_name.strip().lower()
        if pname:
            formulas = [f for f in formulas if f[0].lower() == pname]

        # 2️⃣ Map each Txxx → site_id
        param_re = re.compile(r"T\d+")
        unique_params = {p for _, fm, _ in formulas for p in param_re.findall(fm)}

        site_rows = db.execute(
            text("""
                SELECT parameter_name, site_id
                FROM totaliser_data
                WHERE parameter_name = ANY(:params)
            """),
            {"params": list(unique_params)}
        ).fetchall()
        site_map = {r[0]: r[1] for r in site_rows}

        IST = pytz.timezone('Asia/Kolkata')

        def fetch_bounds_for(params: List[str], start_ist: datetime, end_ist: datetime):
            month_start_6am = datetime(start_ist.year, start_ist.month, 1, 6, 0, 0)
            start_utc = IST.localize(month_start_6am).astimezone(pytz.UTC)
            end_utc = IST.localize(end_ist).astimezone(pytz.UTC)

            sids = [site_map[p] for p in params if p in site_map]
            if not sids:
                return {}, {}, {}, {}

            rows = db.execute(
                text("""
                    WITH md AS (
                      SELECT param_label, value,
                             (time AT TIME ZONE 'Asia/Kolkata') AS ist_time
                        FROM sensor_data
                       WHERE site_id = ANY(:sids)
                         AND param_label = ANY(:params)
                         AND time >= :start_utc
                         AND time <  :end_utc
                    ),
                    firsts AS (
                      SELECT DISTINCT ON(param_label)
                             param_label, ist_time AS first_time, value AS first_value
                        FROM md
                       ORDER BY param_label, ist_time ASC
                    ),
                    lasts AS (
                      SELECT DISTINCT ON(param_label)
                             param_label, ist_time AS last_time, value AS last_value
                        FROM md
                       ORDER BY param_label, ist_time DESC
                    )
                    SELECT f.param_label,
                           f.first_value, f.first_time,
                           l.last_value,  l.last_time
                      FROM firsts f
                      LEFT JOIN lasts l USING(param_label)
                """),
                {"sids": sids, "params": params, "start_utc": start_utc, "end_utc": end_utc}
            ).fetchall()

            fv, ft, lv, lt = {}, {}, {}, {}
            for pl, fval, ftime, lval, ltime in rows:
                fv[pl] = float(fval or 0)
                ft[pl] = ftime.isoformat() if ftime else None
                lv[pl] = float(lval or 0)
                lt[pl] = ltime.isoformat() if ltime else None

            for p in params:
                fv.setdefault(p, 0.0)
                ft.setdefault(p, None)
                lv.setdefault(p, 0.0)
                lt.setdefault(p, None)
            return fv, ft, lv, lt

        results: List[Dict[str, Any]] = []

        def process_period(start_ist, end_ist, y, m, d=None):
            for plant, formula, cfo_limit in formulas:
                params = sorted(param_re.findall(formula))
                fv, ft, lv, lt = fetch_bounds_for(params, start_ist, end_ist)
                if any(ft[p] is None or lt[p] is None for p in params):
                    usage = '-'
                else:
                    usage_map = {p: lv[p] - fv[p] for p in params}
                    try:
                        raw = eval(formula, {}, usage_map)
                        usage = round(abs(raw), 2)
                    except Exception:
                        usage = None
                entry = {
                    "plant": plant,
                    "station": station_name,
                    "year": y,
                    "month": m,
                    "klm_usage": usage,
                    "cfo_limit_klm": float(cfo_limit),
                    "first_values": fv,
                    "first_times": ft,
                    "last_values": lv,
                    "last_times": lt,
                }
                if d is not None:
                    entry["day"] = d
                results.append(entry)

        if pname and month > 0 and day:
            st = datetime(year, month, 1)
            en = datetime(year, month, day, 23, 59, 59)
            process_period(st, en, year, month, day)
            return {"status": "success", "data": results}

        if pname and month == 0:
            for m in range(1, 13):
                st = datetime(year, m, 1)
                en = datetime(year + (m // 12), (m % 12) + 1, 1)
                if year == today.year and m == today.month:
                    en = datetime.combine(today + timedelta(days=1), datetime.min.time())
                process_period(st, en, year, m)
            return {"status": "success", "data": results}

        if month == 0:
            month = today.month
        single = (
            date(year, month, day) if day else
            today if (year == today.year and month == today.month) else
            date(year, month, calendar.monthrange(year, month)[1])
        )
        st = datetime.combine(single, datetime.min.time())
        en = st + timedelta(days=1)
        process_period(st, en, year, month, day)

        return {"status": "success", "data": results}

    except Exception as ex:
        logging.exception("Optimized KLM error")
        return {"status": "error", "message": "Internal server error"}



# ───────────────────────────────────────────────
# 🧾 KLD REPORT
# ───────────────────────────────────────────────
@router.get("/api/report/kld", tags=["KLD Report"])
def get_kld_report(
    user: user_dependency,
    db: Session = Depends(getdb),
    type: str = Query(..., description="Should be 'KLD'"),
    station_name: str = Query(...),
    from_date: date = Query(...),
    to_date: date = Query(...),
    plant_name: str = Query(None)
):
    if type.upper() != "KLD":
        return {"status": "error", "message": "Invalid report type. Must be 'KLD'"}

    try:
        today = date.today()
        if to_date >= today:
            to_date = today - timedelta(days=1)

        logging.info(f"Generating KLD report for {station_name} from {from_date} to {to_date}...")

        formulas = db.execute(
            text("""
                SELECT plant_name, formula, cfo_limit_kld
                FROM station_formulas
                WHERE station_name = :station_name AND is_active = TRUE
                  AND (:plant_name IS NULL OR plant_name = :plant_name)
            """),
            {"station_name": station_name, "plant_name": plant_name}
        ).fetchall()

        results = []
        for single_date in (from_date + timedelta(n) for n in range((to_date - from_date).days + 1)):
            is_today = (single_date == today)
            for plant, formula_str, cfo_limit in formulas:
                params = list(set(re.findall(r"T\d+", formula_str)))
                usage_map = {}
                for param in params:
                    site = db.execute(
                        text("SELECT site_id FROM totaliser_data WHERE parameter_name = :param LIMIT 1"),
                        {"param": param}
                    ).fetchone()
                    if not site:
                        usage_map[param] = 0
                        continue
                    site_id = site[0]

                    if is_today:
                        v6am_row = db.execute(
                            text("""SELECT value_6am FROM daily_totaliser_usage
                                    WHERE site_id=:sid AND parameter_name=:param AND date=:d"""),
                            {"sid": site_id, "param": param, "d": single_date}
                        ).fetchone()
                        v6am = float(v6am_row[0]) if v6am_row and v6am_row[0] else 0.0

                        latest = db.execute(
                            text("""SELECT value FROM sensor_data
                                    WHERE site_id=:sid AND param_label=:param AND time >= date_trunc('day', now())
                                    ORDER BY time DESC LIMIT 1"""),
                            {"sid": site_id, "param": param}
                        ).fetchone()
                        latest_value = float(latest[0]) if latest else 0.0
                        usage_map[param] = latest_value - v6am
                    else:
                        row = db.execute(
                            text("""SELECT usage FROM daily_totaliser_usage
                                    WHERE site_id=:sid AND parameter_name=:param AND date=:d"""),
                            {"sid": site_id, "param": param, "d": single_date}
                        ).fetchone()
                        usage_map[param] = float(row[0]) if row and row[0] else 0.0

                try:
                    result_value = eval(formula_str, {}, usage_map)
                except Exception:
                    result_value = None

                results.append({
                    "date": single_date,
                    "plant": plant,
                    "station": station_name,
                    "kld_usage": abs(round(result_value, 2)) if result_value is not None else None,
                    "cfo_limit_kld": float(cfo_limit) if cfo_limit is not None else None
                })
        return {"status": "success", "data": results}

    except Exception as e:
        logging.exception("Error generating KLD report")
        return {"status": "error", "message": "Internal server error"}


# ───────────────────────────────────────────────
# 🌿 Plant list
# ───────────────────────────────────────────────
@router.get("/api/report/amara/plants", tags=["KLM/KLD Report"])
def get_plants_by_station(
    user: user_dependency,
    db: Session = Depends(getdb),
    station_name: str = Query(...)
):
    try:
        rows = db.execute(
            text("""SELECT DISTINCT plant_name FROM station_formulas
                    WHERE station_name=:sn AND is_active=TRUE"""),
            {"sn": station_name}
        ).fetchall()
        return {
            "status": "success",
            "station": station_name,
            "plants": [r[0] for r in rows]
        }
    except Exception as e:
        logging.exception("Error fetching plant names")
        return {"status": "error", "message": "Internal server error"}


# ───────────────────────────────────────────────
# ⚙️ CFO Limits (get + update)
# ───────────────────────────────────────────────
@router.get("/api/report/amara/cfo-limits", tags=["KLM/KLD Report"])
def get_cfo_limits_for_plant(
    user: user_dependency,
    db: Session = Depends(getdb),
    station_name: str = Query(...),
    plant_name: str = Query(...)
):
    try:
        row = db.execute(
            text("""SELECT cfo_limit_kld, cfo_limit_klm
                    FROM station_formulas
                    WHERE station_name=:sn AND plant_name=:pn AND is_active=TRUE
                    LIMIT 1"""),
            {"sn": station_name, "pn": plant_name}
        ).fetchone()

        if not row:
            return {"status": "error", "message": "No matching record found"}

        return {
            "status": "success",
            "station_name": station_name,
            "plant_name": plant_name,
            "cfo_limit_kld": float(row[0]) if row[0] else None,
            "cfo_limit_klm": float(row[1]) if row[1] else None
        }

    except Exception as e:
        logging.exception("Error fetching CFO limits")
        return {"status": "error", "message": "Internal server error"}


@router.put("/api/report/amara/update-cfo-limits", tags=["KLM/KLD Report"])
def update_cfo_limits_form(
    user: user_dependency,
    db: Session = Depends(getdb),
    station_name: str = Form(...),
    plant_name: str = Form(...),
    cfo_limit_kld: float = Form(None),
    cfo_limit_klm: float = Form(None)
):
    try:
        exists = db.execute(
            text("""SELECT id FROM station_formulas
                    WHERE station_name=:sn AND plant_name=:pn AND is_active=TRUE LIMIT 1"""),
            {"sn": station_name, "pn": plant_name}
        ).fetchone()
        if not exists:
            return {"status": "error", "message": "No active record found"}

        updates = []
        params = {"sn": station_name, "pn": plant_name}
        if cfo_limit_kld is not None:
            updates.append("cfo_limit_kld=:kld")
            params["kld"] = cfo_limit_kld
        if cfo_limit_klm is not None:
            updates.append("cfo_limit_klm=:klm")
            params["klm"] = cfo_limit_klm

        if not updates:
            return {"status": "error", "message": "No values provided to update"}

        db.execute(
            text(f"UPDATE station_formulas SET {', '.join(updates)} "
                 "WHERE station_name=:sn AND plant_name=:pn AND is_active=TRUE"),
            params
        )
        db.commit()
        return {"status": "success", "message": "CFO limits updated successfully"}

    except Exception as e:
        db.rollback()
        logging.exception("Error updating CFO limits")
        return {"status": "error", "message": "Internal server error"}



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

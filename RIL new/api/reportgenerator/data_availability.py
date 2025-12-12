

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from datetime import datetime, time
from pytz import timezone as pytz_timezone

from ...database.session import getdb
from ...modals.masters import stationParameter
from ..auth.authentication import user_dependency
from ...utils.permissions import enforce_site_access

router = APIRouter()

@router.get("/api/data-availability/current", tags=["data availability"])
async def get_current_data_availability(
    user: user_dependency,
    site_id: int = Query(..., description="Site ID"),
    station_id: int = Query(..., description="Station ID"),
    station_param_id: int = Query(..., description="Station Parameter ID"),
    from_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    to_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    db: Session = Depends(getdb)
):
    """
    Returns DAY-WISE data availability (%) for a parameter.
    Based on raw hourly reading counts stored in sensor_processed_1hr.
    """
    enforce_site_access(user, site_id)
    try:
        # 1️⃣ Fetch param_interval (raw logging interval in seconds)
        param_interval = (
            db.query(stationParameter.param_interval)
            .filter(stationParameter.id == station_param_id)
            .scalar()
        )

        if not param_interval or param_interval <= 0:
            raise HTTPException(
                404,
                f"No valid param_interval found for station_param_id={station_param_id}"
            )

        # 2️⃣ Convert input dates → IST → naive timestamps
        IST = pytz_timezone("Asia/Kolkata")

        start_dt = IST.localize(
            datetime.combine(datetime.strptime(from_date, "%Y-%m-%d"), time(0, 0))
        ).replace(tzinfo=None)

        end_dt = IST.localize(
            datetime.combine(datetime.strptime(to_date, "%Y-%m-%d"), time(23, 59, 59))
        ).replace(tzinfo=None)

        # 3️⃣ Query daily ACTUAL readings from sensor_processed_1hr
        sql = text("""
            SELECT
                date(sp.bucket_ist) AS day,
                SUM(sp.data_aval) AS actual_readings
            FROM sensor_processed_1hr sp
            WHERE sp.site_id = :site_id
              AND sp.station_id = :station_id
              AND sp.station_param_id = :station_param_id
              AND sp.bucket_ist BETWEEN :start_dt AND :end_dt
            GROUP BY date(sp.bucket_ist)
            ORDER BY day;
        """)

        rows = db.execute(sql, {
            "site_id": site_id,
            "station_id": station_id,
            "station_param_id": station_param_id,
            "start_dt": start_dt,
            "end_dt": end_dt
        }).fetchall()

        # 4️⃣ Expected readings PER DAY
        expected_per_day = int(24 * (3600 / param_interval))

        # 5️⃣ Build per-day response list
        daily_results = []
        for row in rows:
            actual = int(row.actual_readings or 0)
            pct = round((actual / expected_per_day) * 100, 2) if expected_per_day > 0 else 0
            pct = min(pct, 100.0)

            daily_results.append({
                "date": str(row.day),
                "expected_readings": expected_per_day,
                "actual_readings": actual,
                "availability_percentage": pct,
                "availability_out_of_100": f"{pct}%"
            })

        # 6️⃣ Final response
        return {
            "site_id": site_id,
            "station_id": station_id,
            "station_param_id": station_param_id,
            "param_interval": param_interval,
            "from_date": from_date,
            "to_date": to_date,
            "daily_data_availability": daily_results
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {e}")

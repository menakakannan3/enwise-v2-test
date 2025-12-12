from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from fastapi.responses import JSONResponse
from datetime import datetime
import pytz

from app.database.session import getdb
from ..auth.authentication import user_dependency

router = APIRouter(prefix="/api", tags=["Station Status Report"])

IST = pytz.timezone("Asia/Kolkata")


def clip_window(start_time, end_time, now):
    """
    Ensures:
      - No negative duration
      - Future windows removed
      - Future end_time clipped to NOW
    """

    # 1) If window starts in the future → INVALID → ignore
    if start_time > now:
        return None, None

    # 2) If end_time is in the future → clip
    if end_time > now:
        end_time = now

    # 3) Avoid negative duration
    if end_time < start_time:
        end_time = start_time

    return start_time, end_time


@router.get("/station-status-range")
def station_status_range(
    user: user_dependency,
    station_id: int = Query(...),
    from_date: str = Query(...),
    to_date: str = Query(...),
    db: Session = Depends(getdb)
):
    try:
        # Convert inputs
        from_dt = datetime.strptime(from_date, "%Y-%m-%d").date()
        to_dt = datetime.strptime(to_date, "%Y-%m-%d").date()

        if from_dt > to_dt:
            raise HTTPException(400, "from_date must be <= to_date")

        # Query live view
        q = text("""
            SELECT station_id, station_name, day, start_time, end_time, status, duration_hours
            FROM station_full_status
            WHERE station_id = :sid
              AND day BETWEEN :from_d AND :to_d
            ORDER BY day, start_time;
        """)

        rows = db.execute(q, {
            "sid": station_id,
            "from_d": from_dt,
            "to_d": to_dt
        }).fetchall()

        if not rows:
            raise HTTPException(status_code=404, detail="No data found")

        now = datetime.now(IST)
        daily = {}

        for r in rows:
            day_key = r.day.isoformat()

            start_time = r.start_time.astimezone(IST)
            end_time = r.end_time.astimezone(IST)
            status = r.status

            # ---- FIX: CLIP/FILTER INVALID WINDOWS ----
            start_time, end_time = clip_window(start_time, end_time, now)
            if not start_time:
                continue

            duration = round((end_time - start_time).total_seconds() / 3600.0, 2)

            # Initialize day record
            if day_key not in daily:
                daily[day_key] = {
                    "station_id": r.station_id,
                    "station_name": r.station_name,
                    "day": day_key,
                    "station_status": "Offline",
                    "total_online_hours": 0.0,
                    "total_offline_hours": 0.0,
                    "online_count": 0,
                    "offline_count": 0,
                    "total_transitions": 0,
                    "status_transitions": [],
                    "last_online_time": None
                }

            d = daily[day_key]

            # Track counts and hours
            if status == "Online":
                d["total_online_hours"] += duration
                d["online_count"] += 1
                d["station_status"] = "Online"
                d["last_online_time"] = end_time.isoformat()
            else:
                d["total_offline_hours"] += duration
                d["offline_count"] += 1

            # Merge same-status transitions
            trans = d["status_transitions"]
            if trans and trans[-1]["status"] == status:
                trans[-1]["to"] = end_time.isoformat()
                trans[-1]["duration_hours"] = round(
                    trans[-1]["duration_hours"] + duration, 2
                )
            else:
                trans.append({
                    "status": status,
                    "from": start_time.isoformat(),
                    "to": end_time.isoformat(),
                    "duration_hours": duration
                })

        # Final output
        final = []
        for dk in sorted(daily.keys()):
            d = daily[dk]
            d["total_transitions"] = len(d["status_transitions"])
            d["total_online_hours"] = round(d["total_online_hours"], 2)
            d["total_offline_hours"] = round(d["total_offline_hours"], 2)
            final.append(d)

        return JSONResponse(final)
    except HTTPException as he:
        raise he

    except Exception as e:
        raise HTTPException(
        status_code=500,
        detail="Internal server error"
    )

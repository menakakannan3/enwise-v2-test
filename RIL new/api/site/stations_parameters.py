

from fastapi import APIRouter, Depends, HTTPException,Query
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from typing import List
import datetime as dt
from zoneinfo import ZoneInfo
from ..auth.authentication import user_dependency

from ...database.session import getdb
from ...modals.masters import Site, Station, Parameter, MonitoringType, Analyser, stationParameter, AnalyserParameter

router = APIRouter()


@router.get("/api/sites/{site_id}/stations-parameters", tags=['site-station-parameter'])
async def get_stations_and_parameters(
    user:user_dependency,
    site_id: int,
    db: Session = Depends(getdb)
):
    try:
        # 1) Validate site
        site = db.query(Site).filter(Site.id == site_id).first()
        if not site:
            raise HTTPException(status_code=404, detail="Site not found")

        # 2) Fetch stations
        stations = db.query(Station).filter(Station.site_id == site_id).all()
        if not stations:
            raise HTTPException(status_code=404, detail="No stations found for this site")
        from datetime import datetime, timedelta
        # 3) Time boundaries
        now_utc       = datetime.utcnow()
        past_24hr_utc = now_utc - timedelta(hours=24)
        ist           = ZoneInfo("Asia/Kolkata")

        all_spids          = set()
        station_monitoring = {}
        response_data = {
            "site_id":   site_id,
            "site_name": site.site_name,
            "stations":  []
        }
        spid_expired = {}

        # 4) Build initial shape, init avg=0.00, time="-"
        for station in stations:
            exp = station.calibration_expiry_date
            is_exp = bool(exp and exp < now_utc)

            rows = (
                db.query(Parameter, MonitoringType, Analyser, stationParameter)
                  .join(AnalyserParameter, AnalyserParameter.parameter_id == Parameter.id)
                  .join(stationParameter,  stationParameter.analyser_param_id == AnalyserParameter.id)
                  .join(MonitoringType,    MonitoringType.id == Parameter.monitoring_type_id)
                  .join(Analyser,          Analyser.id == AnalyserParameter.analyser_id)
                  .filter(stationParameter.station_id == station.id)
                  .all()
            )
            if not rows:
                continue

            monitoring_map = {}
            for param, mtype, analyser, sp in rows:
                all_spids.add(sp.id)
                spid_expired[sp.id] = is_exp

                block = monitoring_map.setdefault(mtype.id, {
                    "monitoringLabel": mtype.monitoring_type,
                    "monitoringId":    mtype.id,
                    "parameters":      []
                })
                block["parameters"].append({
                    "id":               param.id,
                    "parameter_name":   param.name,
                    "analyser_name":    analyser.analyser_name,
                    "station_param_id": sp.id,
                    "unit":             sp.para_unit,
                    "is_editable":      sp.is_editable,
                    "15m_avg":          0.00,   # start at 0.00
                    "15m_avg_time":     "-",    # time dash
                    "is_expired":       is_exp
                })

            station_monitoring[station.id] = monitoring_map
            response_data["stations"].append({
                "id":               station.id,
                "station_name":     station.name,
                "is_expired":       is_exp,
                "monitoring_types": []
            })

        # 5) Aggregate real averages
        if all_spids:
            ids_csv = ",".join(map(str, all_spids))
            sql = text(f"""
                SELECT
                  sd.station_param_id,
                  time_bucket('15 minutes', sd.time, 'Asia/Kolkata') AS interval_start,
                  AVG(sd.value) AS avg_value
                FROM sensor_data sd
                WHERE sd.station_param_id IN ({ids_csv})
                  AND sd.time BETWEEN :past_24hr AND :now
                GROUP BY sd.station_param_id, interval_start
                ORDER BY interval_start DESC
            """)
            rows = db.execute(sql, {"past_24hr": past_24hr_utc, "now": now_utc}).fetchall()

            latest_avg = {}
            for r in rows:
                spid = r.station_param_id
                # only take the most recent bucket per spid
                if spid not in latest_avg:
                    latest_avg[spid] = {
                        "15m_avg":      float(r.avg_value),
                        "15m_avg_time": r.interval_start.astimezone(ist).isoformat()
                    }

            # 6) Inject, but leave expired at 0.00
            for st in response_data["stations"]:
                mon_map = station_monitoring.get(st["id"], {})
                for block in mon_map.values():
                    for p in block["parameters"]:
                        spid = p["station_param_id"]
                        if not spid_expired.get(spid, False) and spid in latest_avg:
                            p.update(latest_avg[spid])
                    st["monitoring_types"].append(block)

        return response_data

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")


@router.get("/api/stations/{station_id}/parameter-list", tags=["station-parameter"])
async def get_station_parameter_list(
    user: user_dependency,
    station_id: int,
    db: Session = Depends(getdb)
):
    try:
        # Validate station
        station = db.query(Station).filter(Station.id == station_id).first()
        if not station:
            raise HTTPException(status_code=404, detail="Station not found")

        # Fetch station parameters
        rows = (
            db.query(stationParameter, Parameter)
            .join(AnalyserParameter, AnalyserParameter.id == stationParameter.analyser_param_id)
            .join(Parameter, Parameter.id == AnalyserParameter.parameter_id)
            .filter(stationParameter.station_id == station_id)
            .all()
        )

        result = [
            {
                "station_param_id": sp.id,
                "parameter_name": param.name
            }
            for sp, param in rows
        ]

        return {
            "station_id": station_id,
            "station_name": station.name,
            "parameters": result
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail='Internal server error')

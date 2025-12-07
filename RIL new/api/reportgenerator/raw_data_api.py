
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import ORJSONResponse, StreamingResponse
from fastapi.middleware.gzip import GZipMiddleware
from starlette.background import BackgroundTask
from sqlalchemy import text
from sqlalchemy.orm import Session
import datetime, orjson
from ...database.session import getdb
from fastapi import Query
from ..auth.authentication import user_dependency

router = APIRouter(prefix="/api/raw-data", tags=["Raw Data"])


from time import perf_counter
from fastapi.responses import ORJSONResponse


SQL_TPL = """
SELECT time_bucket(:bucket, time) AS ts,
       AVG(value::double precision) AS avg
FROM sensor_data
WHERE site_id = :site_id
  AND station_param_id = :spid
  AND time >= :start AND time < :end
GROUP BY 1
ORDER BY 1;
"""

def _parse_iso(ts: str, field: str):
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S%z"):
        try:
            return datetime.datetime.strptime(ts, fmt)
        except ValueError:
            pass
    raise HTTPException(400, f"Invalid {field} format. Use ISO8601 with timezone.")

_ALLOWED_BUCKETS = {
    "1 minute","2 minutes","5 minutes","10 minutes","15 minutes",
    "30 minutes","60 minutes","120 minutes","240 minutes","1440 minutes"
}
def _normalize_bucket(b: str | None) -> str:
    if not b:
        return "1 minute"
    if b not in _ALLOWED_BUCKETS:
        raise HTTPException(400, f"Invalid bucket. Allowed: {', '.join(sorted(_ALLOWED_BUCKETS))}")
    return b

@router.get("/{site_id}")
def get_raw_data(
     user: user_dependency, 
    site_id: int,
    station_id: int,                     # kept for compatibility (not used in SQL)
    station_param_id: int,
    from_date: str,
    to_date: str,
    bucket: str | None = Query(None, description="e.g. '1 minute' (default), '5 minutes'"),
    debug: bool = Query(False, description="Return EXPLAIN ANALYZE instead of data"),
    db: Session = Depends(getdb),
):
    start_dt = _parse_iso(from_date, "from_date")
    end_dt   = _parse_iso(to_date,   "to_date")
    bucket   = _normalize_bucket(bucket)

    engine = db.get_bind()
    conn = engine.connect().execution_options(stream_results=True)

    params = {
        "bucket": bucket,
        "site_id": site_id,
        "spid": station_param_id,
        "start": start_dt,
        "end": end_dt,         # end-exclusive in SQL:  time < :end
    }

    if debug:
        plan_sql = "EXPLAIN (ANALYZE, BUFFERS) " + SQL_TPL
        plan_rows = conn.execute(text(plan_sql), params).fetchall()
        conn.close()
        return {"explain": [r[0] for r in plan_rows]}

    result = conn.execute(text(SQL_TPL), params)

    def row_iter():
        try:
            # include meta so you can verify the bucket in the client
            yield b'{"meta":'
            yield orjson.dumps({"bucket": bucket, "from": start_dt.isoformat(), "to": end_dt.isoformat()})
            yield b',"raw_data":['
            first = True
            for ts, avg in result:
                item = {"timestamp": ts.isoformat(), "value": float(avg)}
                if not first: 
                    yield b","
                yield orjson.dumps(item)
                first = False
            yield b"]}"
        finally:
            conn.close()  # ensure the DB connection is released even if client disconnects

    return StreamingResponse(row_iter(), media_type="application/json")
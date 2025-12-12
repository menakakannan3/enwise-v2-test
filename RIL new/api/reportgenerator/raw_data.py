from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from sqlalchemy.orm import Session
import datetime, io, gzip
from ...database.session import getdb
from ..auth.authentication import user_dependency
from ...utils.permissions import enforce_site_access

router = APIRouter(prefix="/api/raw-data", tags=["Raw Data"])

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

_ALLOWED_BUCKETS = {
    "1 minute","2 minutes","5 minutes","10 minutes","15 minutes",
    "30 minutes","60 minutes","120 minutes","240 minutes","1440 minutes"
}

def _parse_iso(ts: str, field: str):
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S%z"):
        try:
            return datetime.datetime.strptime(ts, fmt)
        except ValueError:
            pass
    raise HTTPException(400, f"Invalid {field} format. Use ISO8601 with timezone.")

def _normalize_bucket(b: str | None) -> str:
    if not b:
        return "1 minute"
    if b not in _ALLOWED_BUCKETS:
        raise HTTPException(400, f"Invalid bucket. Allowed: {', '.join(sorted(_ALLOWED_BUCKETS))}")
    return b


@router.get("/export-gz/{site_id}")
def export_raw_data_gz(
     user: user_dependency, 
    site_id: int,
    station_id: int,                     # kept for compatibility
    station_param_id: int,
    from_date: str,
    to_date: str,
    bucket: str | None = Query(None, description="e.g. '1 minute' (default), '5 minutes'"),
    debug: bool = Query(False, description="Return EXPLAIN ANALYZE instead of data"),
    db: Session = Depends(getdb),
):
    enforce_site_access(user, site_id)
    """
    Streams raw sensor data as gzip-compressed CSV.
    Only columns: timestamp,value
    """
    start_dt = _parse_iso(from_date, "from_date")
    end_dt   = _parse_iso(to_date, "to_date")
    bucket   = _normalize_bucket(bucket)

    engine = db.get_bind()
    conn = engine.connect().execution_options(stream_results=True)

    params = {
        "bucket": bucket,
        "site_id": site_id,
        "spid": station_param_id,
        "start": start_dt,
        "end": end_dt,
    }

    if debug:
        plan_sql = "EXPLAIN (ANALYZE, BUFFERS) " + SQL_TPL
        plan_rows = conn.execute(text(plan_sql), params).fetchall()
        conn.close()
        return {"explain": [r[0] for r in plan_rows]}

    result = conn.execute(text(SQL_TPL), params)

    def gz_iter():
        """
        Stream gzip CSV directly from DB cursor.
        """
        try:
            buf = io.BytesIO()
            with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
                # CSV header
                gz.write(b"timestamp,value\n")

                for ts, avg in result:
                    line = f"{ts.isoformat()},{float(avg)}\n"
                    gz.write(line.encode("utf-8"))

                    # Yield chunks periodically (~64KB)
                    if buf.tell() > 65536:
                        gz.flush()
                        buf.seek(0)
                        chunk = buf.read()
                        yield chunk
                        buf.seek(0)
                        buf.truncate(0)

                # Flush any remaining data
                gz.flush()
            buf.seek(0)
            yield buf.read()
        finally:
            conn.close()

    filename = f"site_{site_id}_param_{station_param_id}_{bucket.replace(' ','_')}.csv.gz"

    return StreamingResponse(
        gz_iter(),
        media_type="application/gzip",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "X-Accel-Buffering": "no",
        },
    )

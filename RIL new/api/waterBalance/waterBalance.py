from fastapi import APIRouter, Depends, HTTPException, Query,Body
from sqlalchemy.orm import Session, aliased
from sqlalchemy import func, and_
from ...modals.masters import DashboardPageFormulas, SensorData, TotaliserData,Station,stationParameter
from ...database.session import getdb
import re
import json
from typing import Dict, List, Literal,Any,Optional
from collections import defaultdict
from ...schemas.masterSchema import *
from ..auth.authentication import user_dependency
import pytz
router = APIRouter(
    prefix="/api",
    tags=["WaterBalance"],
)


def evaluate_formula(expression: str, param_map: Dict[str, float]) -> float:
    try:
        for param, value in param_map.items():
            expression = re.sub(rf"\b{re.escape(param)}\b", str(value), expression)
        return round(eval(expression), 2)
    except Exception:
        return None


def get_latest_sensor_values(db: Session, site_id: int, param_labels: List[str]) -> Dict[str, float]:
    subquery = (
        db.query(
            SensorData.param_label,
            func.max(SensorData.time).label("max_time")
        )
        .filter(SensorData.site_id == site_id, SensorData.param_label.in_(param_labels))
        .group_by(SensorData.param_label)
        .subquery()
    )
    s1 = aliased(SensorData)
    s2 = aliased(subquery)

    rows = (
        db.query(s1.param_label, s1.value)
        .join(s2, and_(s1.param_label == s2.c.param_label, s1.time == s2.c.max_time))
        .all()
    )
    return {row.param_label: float(row.value) for row in rows}


def compute_totalizer_deltas(
    db: Session,
    site_id: int,
    t_labels: List[str],
    latest_values: Dict[str, float],
    type: Literal["kld", "klm"]
) -> Dict[str, float]:
    rows = (
        db.query(TotaliserData.parameter_name, TotaliserData.kld_value, TotaliserData.klm_value)
        .filter(TotaliserData.site_id == site_id, TotaliserData.parameter_name.in_(t_labels))
        .all()
    )

    deltas = {}
    for row in rows:
        current_val = latest_values.get(row.parameter_name)
        base_val = row.kld_value if type == "kld" else row.klm_value
        if current_val is not None and base_val is not None:
            deltas[row.parameter_name] = round(current_val - float(base_val), 2)
    return deltas


def evaluate_table_formula(table_formula: dict, param_map: dict) -> dict:
    from collections import defaultdict

    result = defaultdict(dict)

    def resolve(expr: str, current_section: str) -> float:
        if not expr or not isinstance(expr, str):
            return 0.0  # default if expression is missing

        # Replace sensor param values like F84, F6, etc.
        for k, v in param_map.items():
            expr = re.sub(rf"\b{re.escape(k)}\b", str(v if v is not None else 0), expr)

        # Replace section-scoped values (e.g. Domestic.Fresh Water)
        for sec, cols in result.items():
            for label, val in cols.items():
                scoped = f"{sec}.{label}"
                expr = expr.replace(scoped, str(val if val is not None else 0))

        # Replace same-section unscoped labels (e.g. Fresh Water)
        for label, val in result[current_section].items():
            expr = re.sub(rf"\b{re.escape(label)}\b", str(val if val is not None else 0), expr)

        try:
            return round(eval(expr), 2)
        except:
            return 0.0  # fallback to 0 if evaluation fails

    for section, columns in table_formula.items():
        for label, expr in columns.items():
            value = resolve(expr, section)
            result[section][label] = value

    return result


@router.get("/dashboard/formulas/evaluate")
def evaluate_dashboard_blocks(
    user: user_dependency,
    site_id: int = Query(...),
    page_name: str = Query(...),
    type: Literal["kld", "klm"] = Query("kld"),
    db: Session = Depends(getdb)
):
    formula_entry = (
        db.query(DashboardPageFormulas)
        .filter_by(site_id=site_id, page_name=page_name)
        .first()
    )
    if not formula_entry:
        raise HTTPException(status_code=404, detail="Formula not found")

    blocks = formula_entry.formulas
    if isinstance(blocks, str):
        try:
            blocks = json.loads(blocks)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Invalid JSON in formulas: {e}")

    flow_params = set()
    totalizer_params = set()
    for block in blocks:
        fc = block.get("flowCalculation")
        tc = block.get("totalizerCalculation")
        if fc:
            flow_params.update(re.findall(r"\bF-?\d+\b", fc))
        if tc:
            totalizer_params.update(re.findall(r"\bT-?\d+\b", tc))

    all_needed_params = list(flow_params.union(totalizer_params))
    latest_values = get_latest_sensor_values(db, site_id, all_needed_params)
    totalizer_deltas = compute_totalizer_deltas(db, site_id, list(totalizer_params), latest_values, type)

    # for block in blocks:
    #     fc = block.get("flowCalculation")
    #     tc = block.get("totalizerCalculation")
    #     block["flowValue"] = abs(evaluate_formula(fc, latest_values)) if fc else None
    #     block["totalizerValue"] = abs(evaluate_formula(tc, totalizer_deltas)) if tc else None
    for block in blocks:
        fc = block.get("flowCalculation")
        tc = block.get("totalizerCalculation")

        raw_flow      = evaluate_formula(fc, latest_values)      if fc else None
        raw_totalizer = evaluate_formula(tc, totalizer_deltas) if tc else None

        block["flowValue"]      = abs(raw_flow)      if raw_flow is not None      else None
        block["totalizerValue"] = abs(raw_totalizer) if raw_totalizer is not None else None


    return {
        "blocks": blocks
    }

@router.get("/dashboard/layout")
def get_dashboard_layout(
     user: user_dependency, 
    site_id: int = Query(...),
    page_name: str = Query(...),
    db: Session = Depends(getdb),
):
    row = (
        db.query(DashboardPageFormulas)
        .filter_by(site_id=site_id, page_name=page_name)
        .first()
    )
    if not row:
        raise HTTPException(status_code=404, detail="Layout not found")

    import json
    def coerce(value, empty):
        if value is None:
            return empty
        if isinstance(value, (dict, list)):
            return value
        try:
            return json.loads(value)
        except Exception:
            return empty

    # Use getattr so it doesn't blow up if the attribute doesn't exist
    pos_raw  = getattr(row, "positions", None)
    conn_raw = getattr(row, "connections", None)

    return {
        "positions":   coerce(pos_raw,  {}),
        "connections": coerce(conn_raw, []),
    }

@router.get("/dashboard/pages")
def list_dashboard_pages(
    user: user_dependency,
    site_id: int = Query(...),
    db: Session = Depends(getdb),
):
    rows = (
        db.query(DashboardPageFormulas.page_name)
        .filter(DashboardPageFormulas.site_id == site_id)
        .distinct()
        .order_by(DashboardPageFormulas.page_name.asc())
        .all()
    )
    pages = [r[0] for r in rows]
    if not pages:
        # Not fatal; return empty list so UI can show message
        return {"pages": []}
    return {"pages": pages}

def get_latest_sensor_values(db: Session, site_id: int, param_labels: List[str]) -> Dict[str, float]:
    subquery = (
        db.query(
            SensorData.param_label,
            func.max(SensorData.time).label("max_time")
        )
        .filter(SensorData.site_id == site_id, SensorData.param_label.in_(param_labels))
        .group_by(SensorData.param_label)
        .subquery()
    )
    s1 = aliased(SensorData)
    s2 = aliased(subquery)

    rows = (
        db.query(s1.param_label, s1.value)
        .join(s2, and_(s1.param_label == s2.c.param_label, s1.time == s2.c.max_time))
        .all()
    )
    return {row.param_label: float(row.value) for row in rows}

def get_totalizer_deltas(
    db: Session, 
    site_id: int, 
    t_labels: List[str], 
    type: Literal["kld", "klm"]
) -> Dict[str, float]:
    # Get current sensor readings
    sensor_values = get_latest_sensor_values(db, site_id, t_labels)

    # Get baseline values based on type
    rows = (
        db.query(TotaliserData.parameter_name, TotaliserData.kld_value, TotaliserData.klm_value)
        .filter(TotaliserData.site_id == site_id, TotaliserData.parameter_name.in_(t_labels))
        .all()
    )

    result = {}
    for row in rows:
        current_val = sensor_values.get(row.parameter_name, 0.0)
        base_val = row.kld_value if type == "kld" else row.klm_value
        result[row.parameter_name] = round(float(current_val or 0) - float(base_val or 0), 2)

    return result


def evaluate_table_formula(table_formula: dict, param_map: dict) -> dict:
    result = defaultdict(dict)

    def resolve(expr: str, current_section: str) -> float:
        if not expr or not isinstance(expr, str):
            return 0.0

        # Replace sensor param values
        for k, v in param_map.items():
            expr = re.sub(rf"\b{re.escape(k)}\b", str(v if v is not None else 0), expr)

        # Replace section-scoped references
        for sec, cols in result.items():
            for label, val in cols.items():
                expr = expr.replace(f"{sec}.{label}", str(val if val is not None else 0))

        # Replace same-section labels
        for label, val in result[current_section].items():
            expr = re.sub(rf"\b{re.escape(label)}\b", str(val if val is not None else 0), expr)

        try:
            return round(eval(expr), 2)
        except:
            return 0.0

    for section, columns in table_formula.items():
        for label, expr in columns.items():
            value = resolve(expr, section)
            result[section][label] = value

    return result

@router.get(
    "/dashboard/formulas/deltas/manual",
    summary="Fetch per‑parameter manual totaliser deltas",
    response_model=None  # raw dict so you can inspect
)
def get_manual_totaliser_deltas_for_page(
    user: user_dependency,
    site_id: int = Query(..., description="Site ID"),
    page_name: str = Query(..., description="Page name, e.g. 'overall'"),
    type: Literal["kld", "klm"] = Query("kld", description="Which base to subtract"),
    db: Session = Depends(getdb),
) -> Dict[str, float]:
    """
    For every Txx used in this page’s totalizerCalculation,
    compute (tot_last – base) where base = kld_value or klm_value.
    Returns a dict { "T14": 0.17, "T85": 48.13, … }.
    """
    # 1) Load the saved formulas for this page
    entry = (
        db.query(DashboardPageFormulas)
          .filter_by(site_id=site_id, page_name=page_name)
          .first()
    )
    if not entry:
        raise HTTPException(status_code=404, detail="Formula entry not found")

    blocks = entry.formulas
    if isinstance(blocks, str):
        try:
            blocks = json.loads(blocks)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Invalid JSON in formulas: {e}")

    # 2) Extract all T‑labels from totalizerCalculation
    tot_labels = {
        t
        for blk in blocks
        for t in re.findall(r"\bT-?\d+\b", blk.get("totalizerCalculation", ""))
    }
    if not tot_labels:
        return {"deltas": {}}

    # 3) Bulk‑fetch tot_last, kld_value & klm_value
    rows = (
        db.query(
            TotaliserData.parameter_name,
            TotaliserData.tot_last,
            TotaliserData.kld_value,
            TotaliserData.klm_value,
        )
        .filter(
            TotaliserData.site_id == site_id,
            TotaliserData.parameter_name.in_(tot_labels),
        )
        .all()
    )

    # 4) Compute deltas: tot_last − (kld_value or klm_value)
    deltas: Dict[str, float] = {}
    for name, tot_last, kld_val, klm_val in rows:
        base = (kld_val if type == "kld" else klm_val) or 0.0
        delta = float((tot_last or 0.0) - base)
        deltas[name] = round(delta, 2)

    return {"deltas": deltas}
@router.get(
    "/dashboard/formulas/evaluate/manual",
    summary="Evaluate dashboard blocks using totaliser_data.tot_last − base",
    response_model=None,  # raw dict back, no pydantic
)
def evaluate_dashboard_blocks_manual(
    user: user_dependency,
    site_id: int = Query(..., description="Site ID"),
    page_name: str = Query(..., description="Page name, e.g. 'overall'"),
    type: Literal["kld", "klm"] = Query("kld", description="Which base to subtract"),
    db: Session = Depends(getdb),
) -> Dict[str, List[Dict[str, Any]]]:
    # 1) Load saved block definitions
    entry = (
        db.query(DashboardPageFormulas)
          .filter_by(site_id=site_id, page_name=page_name)
          .first()
    )
    if not entry:
        raise HTTPException(status_code=404, detail="Formula not found")

    blocks = entry.formulas
    if isinstance(blocks, str):
        try:
            blocks = json.loads(blocks)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Invalid JSON in formulas: {e}")

    # 2) Pull out every T‑label from totalizerCalculation
    totalizer_params = {
        t
        for blk in blocks
        for t in re.findall(r"\bT-?\d+\b", blk.get("totalizerCalculation", ""))
    }

    # 3) Fetch tot_last, kld_value, klm_value all at once
    rows = (
        db.query(
            TotaliserData.parameter_name,
            TotaliserData.tot_last,
            TotaliserData.kld_value,
            TotaliserData.klm_value,
        )
        .filter(
            TotaliserData.site_id == site_id,
            TotaliserData.parameter_name.in_(totalizer_params),
        )
        .all()
    )

    # 4) Build a map T→(tot_last − base)
    deltas: Dict[str, float] = {}
    for name, tot_last, kld_val, klm_val in rows:
        base = (kld_val if type == "kld" else klm_val) or 0.0
        delta = (float(tot_last or 0.0) - float(base))
        deltas[name] = round(delta, 2)

    # 5) Evaluate each block’s totalizerCalculation inline
    for blk in blocks:
        tc = blk.get("totalizerCalculation") or ""
        if tc:
            expr = tc
            # substitute every Txx in the formula with its delta
            for t_label, dval in deltas.items():
                expr = re.sub(rf"\b{re.escape(t_label)}\b", str(dval), expr)
            try:
                raw = eval(expr)
                blk["totalizerValue"] = round(abs(raw), 2)
            except Exception:
                blk["totalizerValue"] = None
        else:
            blk["totalizerValue"] = None

        # manual always nulls out flowValue
        blk["flowValue"] = None

    return {"blocks": blocks}



@router.get("/dashboard/table/evaluate")
def evaluate_dashboard_table(
    user: user_dependency,
    site_id: int = Query(...),
    page_name: str = Query(...),
    type: Literal["kld", "klm"] = Query("kld"),  # ✅ Add this
    db: Session = Depends(getdb)
):
    formula_entry = (
        db.query(DashboardPageFormulas)
        .filter_by(site_id=site_id, page_name=page_name)
        .first()
    )
    if not formula_entry:
        raise HTTPException(status_code=404, detail="Formula not found")

    table_formula = formula_entry.table_formulae
    if not table_formula:
        raise HTTPException(status_code=400, detail="table_formulae is empty or not defined")

    if isinstance(table_formula, str):
        try:
            table_formula = json.loads(table_formula)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Invalid JSON in table_formulae: {e}")

    # Extract Txx labels
    t_labels = set()
    for section in table_formula.values():
        for formula in section.values():
            if isinstance(formula, str):
                t_labels.update(re.findall(r"\bT-?\d+\b", formula))

    # ✅ Get deltas using type
    latest_deltas = get_totalizer_deltas(db, site_id, list(t_labels), type)

    # Evaluate formulas
    table_result = evaluate_table_formula(table_formula, latest_deltas)

    # Make all values positive
    for section_name, section in table_result.items():
        for col_name, value in section.items():
            if isinstance(value, (int, float)):
                section[col_name] = round(abs(value), 2)

    return {
        "table": table_result
    }



IST = pytz.timezone("Asia/Kolkata")



@router.get(
    "/dashboard/sensors/last",
    summary="Fetch latest raw sensor readings for T1…T90 (with station names)",
    response_model=None,
)
def get_dashboard_sensors_last(
    user: user_dependency,
    site_id: int = Query(..., description="Site ID"),
    db: Session = Depends(getdb),
) -> Dict[str, List[Dict[str, Optional[str]]]]:
    """
    Returns, for each of T1…T90, the most recent
    `value`, exact `time` (in IST) and `stationName`.
    """
    tags = [f"T{i}" for i in range(1, 91)]
    latest = get_latest_sensors_values(db, site_id, tags)

    blocks = [
        {
            "Block ID":   tag,
            "value":      latest[tag]["value"],
            "time":       latest[tag]["time"],
            "stationName": latest[tag]["stationName"],
        }
        for tag in tags
    ]
    return {"blocks": blocks}


def get_latest_sensors_values(
    db: Session,
    site_id: int,
    tags: List[str],
) -> Dict[str, Dict[str, Optional[str]]]:
    """
    For each tag T1…T90, return the most recent reading,
    the exact India‑time `time` column value, and the
    station name (via SensorData.station_id → Station.name).
    """
    out: Dict[str, Dict[str, Optional[str]]] = {}
    for tag in tags:
        # join SensorData → Station to fetch station.name
        row = (
            db.query(SensorData, Station.name.label("station_name"))
              .join(Station, SensorData.station_id == Station.id)
              .filter(
                  SensorData.site_id == site_id,
                  SensorData.param_label == tag,
              )
              .order_by(SensorData.time.desc())
              .limit(1)
              .first()
        )

        if row:
            sensor, station_name = row
            # preserve IST from the original timezone:
            local_dt = sensor.time.astimezone(IST)
            out[tag] = {
                "value":       float(sensor.value),
                "time":        local_dt.isoformat(),         # e.g. "2025-07-28T10:27:54+05:30"
                "stationName": station_name,
            }
        else:
            out[tag] = {"value": None, "time": None, "stationName": None}

    return out

@router.get(
    "/dashboard/totaliser/6am",
    summary="Fetch latest KLD totaliser values (T1…T90)",
    response_model=None, 
)
def get_totaliser_6am(
    user: user_dependency,
    site_id: int = Query(..., description="Site ID"),
    db: Session = Depends(getdb),
) -> Dict[str, List[Dict[str, Optional[float]]]]:
    """
    Returns for each T1…T90 the most recent
    `kld_value`, `kld_time` and `stationName` from totaliser_data.
    """
    # 1) Build full list of tags T1…T90
    tags: List[str] = [f"T{i}" for i in range(1, 91)]

    # 2) Load all matching totaliser_data rows in one go
    rows = (
        db.query(TotaliserData)
          .filter(
              TotaliserData.site_id == site_id,
              TotaliserData.parameter_name.in_(tags),
          )
          .all()
    )

    # 3) Build a map tag→station_name via station_parameters → stations
    sp_rows = (
        db.query(stationParameter.pram_lable, Station.name)
          .join(Station, stationParameter.station_id == Station.id)
          .filter(
              Station.site_id == site_id,
              stationParameter.pram_lable.in_(tags),
          )
          .all()
    )
    station_map = { label: name for label, name in sp_rows }

    # 4) If no totaliser_data at all, return nulls
    if not rows:
        return {
            "blocks": [
                {"Block ID": tag, "value": None, "time": None, "stationName": None}
                for tag in tags
            ]
        }

    # 5) Index by parameter_name for quick lookup
    by_tag = { r.parameter_name: r for r in rows }

    # 6) Assemble ordered response
    blocks: List[Dict[str, Optional[float]]] = []
    for tag in tags:
        row = by_tag.get(tag)
        if row:
            blocks.append({
                "Block ID":    tag,
                "value":       float(row.kld_value) if row.kld_value is not None else None,
                "time":        row.kld_time.isoformat() if row.kld_time is not None else None,
                "stationName": station_map.get(tag),
            })
        else:
            blocks.append({
                "Block ID":    tag,
                "value":       None,
                "time":        None,
                "stationName": None,
            })

    return {"blocks": blocks}
@router.put(
    "/dashboard/totaliser/update-last",
    status_code=204,
    summary="Bulk‑update totaliser_data.tot_last & tot_time for T1–T90",
    response_model=None,
)
def update_totaliser_last_bulk(
    user: user_dependency,
    payload: UpdateTotaliserPayload,
    site_id: int = Query(..., description="Site ID"),
    db: Session = Depends(getdb),
):
    """
    Bulk‑update the `tot_last` and `tot_time` columns in `totaliser_data`
    for each parameter_name (T1–T90) sent in `payload.blocks`.
    """
    # 1) load existing rows for this site
    rows = (
        db.query(TotaliserData)
          .filter_by(site_id=site_id)
          .all()
    )
    if not rows:
        raise HTTPException(404, "No totaliser_data rows found for this site")

    # 2) index them by parameter_name
    row_map = {r.parameter_name: r for r in rows}

    # 3) update each entry
    for blk in payload.blocks:
        row = row_map.get(blk.parameter_name)
        if row:
            row.tot_last = blk.value   # ← write into tot_last
            row.tot_time = blk.time    # ← write into tot_time
            db.add(row)

    db.commit()
    return


@router.put(
    "/dashboard/totaliser/update6am",
    summary="Update (or insert) KLD totaliser 6 AM values (T1…T90)",
    response_model=Totaliser6amResponse,
)
def update_totaliser_6am(
    user: user_dependency,
    site_id: int = Query(..., description="Site ID"),
    request:   Totaliser6amUpdateRequest = Body(...),
    db:        Session                  = Depends(getdb),
) -> Totaliser6amResponse:
    """
    Bulk‐update the `kld_value` and `kld_time` columns in `totaliser_data`
    for each parameter_name (T1–T90) sent in `request.blocks`.
    """
    # 1) load existing rows for this site
    existing_rows = (
        db.query(TotaliserData)
          .filter_by(site_id=site_id)
          .all()
    )
    if not existing_rows:
        raise HTTPException(status_code=404, detail="No totaliser_data rows found for this site")

    # 2) index them by parameter_name
    row_map = { row.parameter_name: row for row in existing_rows }

    saved: List[Totaliser6amBlock] = []

    # 3) upsert each block
    for blk in request.blocks:
        row = row_map.get(blk.block_id)
        if row:
            row.kld_value = blk.value
            row.kld_time  = blk.time
        else:
            row = TotaliserData(
                site_id=site_id,
                parameter_name=blk.block_id,
                kld_value=blk.value,
                kld_time=blk.time,
            )
            db.add(row)
        saved.append(blk)

    # 4) commit
    try:
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Could not save updates: {e}")

    # 5) return exactly the same shape
    return Totaliser6amResponse(blocks=saved)



@router.get(
    "/dashboard/table/evaluate/manual",
    summary="Evaluate table formulas using manual totaliser deltas",
    response_model=None,  # you can plug your Pydantic model here
)
def evaluate_dashboard_table_manual(
    user: user_dependency,
    site_id: int = Query(..., description="Site ID"),
    page_name: str = Query(..., description="Page name, e.g. 'overall'"),
    type: Literal["kld", "klm"] = Query("kld", description="Report type"),
    db: Session = Depends(getdb),
) -> Dict[str, Dict[str, float]]:
    # 1) load table_formulae
    entry = (
        db.query(DashboardPageFormulas)
          .filter_by(site_id=site_id, page_name=page_name)
          .first()
    )
    if not entry:
        raise HTTPException(404, "Formula not found")

    table_formula = entry.table_formulae
    if isinstance(table_formula, str):
        table_formula = json.loads(table_formula)

    # 2) extract all T-labels
    t_labels = {
        t
        for sec in table_formula.values()
        for expr in sec.values() if isinstance(expr, str)
        for t in re.findall(r"\bT-?\d+\b", expr)
    }

    # 3) build a map: Txx → (tot_last, kld_value, klm_value)
    manual_map = {}
    rows = (
        db.query(TotaliserData)
          .filter(
              TotaliserData.site_id == site_id,
              TotaliserData.parameter_name.in_(t_labels),
          )
          .all()
    )
    for r in rows:
        manual_map[r.parameter_name] = {
            "tot_last":  float(r.tot_last or 0),
            "kld_value": float(r.kld_value or 0),
            "klm_value": float(r.klm_value or 0),
        }

    # 4) turn that into “deltas” = tot_last – baseline
    deltas = {
        tag: manual_map.get(tag, {}).get("tot_last", 0)
             - manual_map.get(tag, {}).get(f"{type}_value", 0)
        for tag in t_labels
    }

    # 5) evaluate exactly as your normal evaluator, but feeding `deltas`
    table_result = evaluate_table_formulassss(table_formula, deltas)

    # 6) round & abs
    for sec in table_result.values():
        for col, v in sec.items():
            sec[col] = round(abs(v), 2)

    return {"table": table_result}


def evaluate_table_formulassss(table_formula: dict, param_map: dict) -> dict:
    from collections import defaultdict
    result = defaultdict(dict)

    def resolve(expr: str, section: str) -> float:
        if not expr or not isinstance(expr, str):
            return 0.0
        # substitute every Txx in the expression
        for k, v in param_map.items():
            expr = re.sub(rf"\b{re.escape(k)}\b", str(v), expr)
        # substitute any section‑scoped or same‑section references...
        for sec, cols in result.items():
            for label, val in cols.items():
                expr = expr.replace(f"{sec}.{label}", str(val))
        for label, val in result[section].items():
            expr = re.sub(rf"\b{re.escape(label)}\b", str(val), expr)
        try:
            return float(eval(expr))
        except:
            return 0.0

    for section, cols in table_formula.items():
        for label, formula in cols.items():
            result[section][label] = resolve(formula, section)

    return result
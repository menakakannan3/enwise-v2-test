from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from app.database.session import getdb
from ..auth.authentication import user_dependency
from app.modals.masters import StationFormula
from app.schemas.masterSchema import (
    StationFormulaAlarmOut,
    BulkAlarmUpdateRequest,
    BulkAlarmUpdateItem,
)

router = APIRouter(prefix="/api/station-formulas", tags=["Station Formula"])

# ✅ Get all formulas with plant_name, station_name, is_alarm ordered by ID
@router.get("/alarms", response_model=List[StationFormulaAlarmOut])
def list_station_alarms(user: user_dependency,db: Session = Depends(getdb)):
    return db.query(StationFormula).with_entities(
        StationFormula.id,
        StationFormula.plant_name,
        StationFormula.station_name,
        StationFormula.is_alarm
    ).order_by(StationFormula.id).all()

# ✅ Bulk update is_alarm
@router.put("/alarms/bulk-update")
def bulk_update_alarms(
    user: user_dependency,
    payload: BulkAlarmUpdateRequest,
    db: Session = Depends(getdb)
):
    updated = 0
    for item in payload.updates:
        formula = db.query(StationFormula).filter_by(id=item.id).first()
        if formula:
            formula.is_alarm = item.is_alarm
            updated += 1
    db.commit()
    return {"message": f"{updated} station formulas updated"}

# ✅ Single update is_alarm
@router.put("/alarms/{station_formula_id}", response_model=StationFormulaAlarmOut)
def update_single_alarm(
    user: user_dependency,
    station_formula_id: int,
    payload: BulkAlarmUpdateItem,  # reusing single item schema
    db: Session = Depends(getdb)
):
    formula = db.query(StationFormula).filter_by(id=station_formula_id).first()
    if not formula:
        raise HTTPException(status_code=404, detail="Station formula not found")
    
    formula.is_alarm = payload.is_alarm
    db.commit()
    db.refresh(formula)

    return formula

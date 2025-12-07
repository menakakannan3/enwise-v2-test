from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from datetime import datetime
from zoneinfo import ZoneInfo
import json

from ...database.session import getdb
from ...modals.masters import Station, CalibrationHistory
from ...schemas.masterSchema import StationCalibrationUpdate
from ..auth.authentication import user_dependency

from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from Crypto.Random import get_random_bytes
import paho.mqtt.client as mqtt

router = APIRouter(prefix="/api/stations", tags=["Stations"])

IST_TZ = ZoneInfo("Asia/Kolkata")
UTC_TZ = ZoneInfo("UTC")


def ist_to_utc(ist_str: str) -> datetime:
    try:
        d = datetime.strptime(ist_str.strip(), "%Y-%m-%d %H:%M:%S")
        ist_dt = d.replace(tzinfo=IST_TZ)
        return ist_dt.astimezone(UTC_TZ)
    except Exception as e:
        raise ValueError(f"Invalid datetime format. Use YYYY-MM-DD HH:MM:SS → {e}")


def encrypt_aes(payload: dict, key: str):
    key_bytes = key.encode()[:32].ljust(32, b"0")
    iv = get_random_bytes(16)
    cipher = AES.new(key_bytes, AES.MODE_CBC, iv)
    plaintext = json.dumps(payload).encode()
    ciphertext = cipher.encrypt(pad(plaintext, AES.block_size))
    return iv.hex(), ciphertext.hex()


def publish_mqtt(device_uid: str, auth_key: str, payload: dict):
    iv_hex, ct_hex = encrypt_aes(payload, auth_key)

    message_json = json.dumps({
        "IV": iv_hex,
        "Ciphertext": ct_hex
    })

    topic = f"{device_uid}_IN"

    client = mqtt.Client()
    client.connect("broker.enwise.in", 1883, 60)
    client.publish(topic, message_json)
    client.disconnect()

    print(f"📤 MQTT Published to → {topic}")
    return topic, message_json

@router.post("/update-calibration", tags=["Stations"])
async def update_station_calibration(
    user: user_dependency,         
    payload: StationCalibrationUpdate,
    db: Session = Depends(getdb),
):
    """
    Updates calibration window, stores calibration history,
    and pushes encrypted MQTT message to the device.
    """
    try:
        calib_from_utc = ist_to_utc(payload.calib_from_ist)
        calib_to_utc = ist_to_utc(payload.calib_to_ist)
        now_utc = datetime.now(UTC_TZ)

        station = db.query(Station).filter(Station.id == payload.station_id).first()
        if not station:
            raise HTTPException(status_code=404, detail="station not found")

        # Update calibration window
        station.calib_from_lst = calib_from_utc
        station.calib_to_lst = calib_to_utc

        # Insert history
        history = CalibrationHistory(
            site_id=station.site_id,
            station_id=station.id,
            calib_from=calib_from_utc,
            calib_to=calib_to_utc,
            created_at=now_utc,
        )

        db.add(history)
        db.commit()
        db.refresh(station)

        # Device validation
        if not station.devices:
            raise HTTPException(status_code=500, detail="No device mapped to station")

        device = station.devices[0]

        # MQTT payload (IST timestamp for device)
        mqtt_payload = {
            "station_uid": station.station_uid,
            "calib_from": calib_from_utc.astimezone(IST_TZ).isoformat(),
            "calib_to": calib_to_utc.astimezone(IST_TZ).isoformat(),
        }

        topic, encrypted_msg = publish_mqtt(
            device.device_uid,
            device.device_authkey,
            mqtt_payload,
        )

        return {
            "message": "Calibration updated and pushed to device",
            "mqtt_topic": topic,
            "sent_plain": mqtt_payload,
        }

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail="Unexpected error occurred")

@router.get(
    "/calibration-info/{station_id}",
    tags=["Stations"]
)
async def get_station_calibration_info(
    user: user_dependency,     
    station_id: int,
    db: Session = Depends(getdb),
):
    """
    Returns the latest calibration window + ack status for a station.
    """

    station = db.query(Station).filter(Station.id == station_id).first()

    if not station:
        raise HTTPException(status_code=404, detail="Station not found")

    return {
        "station_id": station.id,
        "station_uid": station.station_uid,
        "calib_from_lst": station.calib_from_lst,
        "calib_to_lst": station.calib_to_lst,
        "calib_ack": station.calib_ack,
    }
@router.get("/calibration-history/{station_id}", tags=["Stations"])
async def get_calibration_history(
    user: user_dependency,         
    station_id: int,
    db: Session = Depends(getdb),
):
    """
    Returns calibration history sorted latest-first.
    """

    station = db.query(Station).filter(Station.id == station_id).first()

    if not station:
        raise HTTPException(status_code=404, detail="Station not found")

    records = (
        db.query(CalibrationHistory)
        .filter(CalibrationHistory.station_id == station_id)
        .order_by(CalibrationHistory.id.desc())
        .all()
    )

    history = [
        {
            "id": h.id,
            "site_id": h.site_id,
            "station_id": h.station_id,
            "calib_from": h.calib_from,
            "calib_to": h.calib_to,
            "created_at": h.created_at,
        }
        for h in records
    ]

    return {
        "station_id": station.id,
        "station_uid": station.station_uid,
        "rows": len(history),
        "history": history,
    }

@router.get("/calibration-history/{station_id}")
async def get_calibration_history(
    station_id: int,
    db: Session = Depends(getdb),
    current_user: dict = Depends(user_dependency)
):
    try:
        station = db.query(Station).filter(Station.id == station_id).first()
        if not station:
            raise HTTPException(status_code=404, detail="Station not found")

        history_records = (
            db.query(CalibrationHistory)
            .filter(CalibrationHistory.station_id == station_id)
            .order_by(CalibrationHistory.id.desc())
            .all()
        )

        result = [
            {
                "id": h.id,
                "site_id": h.site_id,
                "station_id": h.station_id,
                "calib_from": h.calib_from,
                "calib_to": h.calib_to,
                "created_at": h.created_at,
            }
            for h in history_records
        ]

        return {
            "station_id": station_id,
            "station_uid": station.station_uid,
            "rows": len(result),
            "history": result,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail="Unexpected error occurred")

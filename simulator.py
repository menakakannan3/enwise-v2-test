#!/usr/bin/env python3
import json
import random
import time
import threading
from datetime import datetime
from zoneinfo import ZoneInfo
import psycopg2
import paho.mqtt.client as mqtt
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes

# ─── PostgreSQL Config ───────────────────────────────────────────────
DB_PARAMS = {
    "dbname": "enwise_db",
    "user": "enwise_user",
    "password": "enwiseuser",
    "host": "localhost",
    "port": "5432",
}

# ─── MQTT Config ─────────────────────────────────────────────────────
MQTT_BROKER = "116.50.93.126"
MQTT_PORT = 1883

# ─── Timezone ───────────────────────────────────────────────────────
IST = ZoneInfo("Asia/Kolkata")

# Device for which DB insert should happen
TARGET_INSERT_DEVICE = "SA_TEST_01"


# ─── Encryption Helpers ─────────────────────────────────────────────
def zero_pad(b: bytes, block_size: int = 16) -> bytes:
    rem = len(b) % block_size
    if rem == 0:
        return b
    return b + b"\x00" * (block_size - rem)

def encrypt_payload(payload: dict, key_utf8: str):
    key = key_utf8.encode("utf-8")
    if len(key) not in (16, 24, 32):
        raise ValueError(f"Invalid AES key length ({len(key)} bytes) for device_authkey")
    iv = get_random_bytes(16)
    cipher = AES.new(key, AES.MODE_CBC, iv=iv)
    pt = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    pt_padded = zero_pad(pt)
    ct = cipher.encrypt(pt_padded)
    return iv.hex(), ct.hex()


# ─── Build Random Payload ───────────────────────────────────────────
def make_random_payload(status_json: dict) -> dict:
    now = datetime.now(IST)

    timestamp = (
        now.strftime("%Y-%m-%dT%H:%M:%S")
        + "Z"
        + now.strftime("%z")[:3]
        + ":"
        + now.strftime("%z")[3:]
    )

    payload = {
        "site_uid":   status_json.get("site_uid"),
        "chipid":     status_json.get("chipid"),
        "device_uid": status_json.get("device_uid"),
        "timestamp":  timestamp,
        "QualityCode": "U",
        "data": [],
    }

    for item in status_json.get("data", []):
        if not isinstance(item, dict):
            continue

        new_item = dict(item)
        new_item["value"] = random.randint(1, 1000)
        new_item["QualityCode"] = "M"

        payload["data"].append(new_item)

    return payload


# ─── Insert Into sensor_data Table ───────────────────────────────────
def insert_sensor_data(conn, payload):
    cur = conn.cursor()

    for item in payload["data"]:
        cur.execute("""
            INSERT INTO sensor_data
            (time, site_id, station_id, device_id, analyser_id, 
             parameter_id, value, station_param_id, param_label, "qualityCode")
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            payload["timestamp"],              # time
            payload["site_uid"],               # site_id
            item.get("station_id"),            # station_id
            payload["device_uid"],             # device_id
            item.get("analyser_id"),           # analyser_id
            item.get("parameter_id"),          # parameter_id
            item.get("value"),                 # value
            item.get("station_param_id"),      # station_param_id
            item.get("param_label"),           # param_label
            item.get("QualityCode", "U"),      # qualityCode
        ))

    conn.commit()
    print(f"📝 DB INSERT → {payload['device_uid']} ({len(payload['data'])} rows)")


# ─── Per-Device Worker ──────────────────────────────────────────────
def device_worker(device_uid, authkey, status_json, mqtt_client):
    conn = psycopg2.connect(**DB_PARAMS)

    while True:
        try:
            payload = make_random_payload(status_json)

            # Insert only for SA_TEST_01
            if device_uid == TARGET_INSERT_DEVICE:
                insert_sensor_data(conn, payload)

            # MQTT publish (all devices)
            iv_hex, ct_hex = encrypt_payload(payload, authkey)
            envelope = {"IV": iv_hex, "Ciphertext": ct_hex}
            topic = f"{device_uid}_OUT"

            mqtt_client.publish(topic, json.dumps(envelope))
            print(f"📤 MQTT → {device_uid} published ({len(payload['data'])} params)")

        except Exception as e:
            print(f"❌ Error sending {device_uid}: {e}")

        time.sleep(random.randint(10, 30))


# ─── Main Setup ─────────────────────────────────────────────────────
def main():
    mqtt_client = mqtt.Client()
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    mqtt_client.loop_start()

    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    cur.execute("SELECT device_uid, device_authkey, status FROM device WHERE status IS NOT NULL")
    rows = cur.fetchall()
    conn.close()

    print(f"🚀 Loaded {len(rows)} devices from DB")

    for device_uid, authkey, status_raw in rows:
        try:
            status_json = json.loads(status_raw) if isinstance(status_raw, str) else status_raw
            t = threading.Thread(
                target=device_worker,
                args=(device_uid, authkey, status_json, mqtt_client),
                daemon=True
            )
            t.start()

            if device_uid == TARGET_INSERT_DEVICE:
                print(f"🧩 DB INSERT ENABLED → {device_uid}")
            else:
                print(f"🧩 MQTT ONLY → {device_uid}")

        except Exception as e:
            print(f"⚠️ Skipping {device_uid}: {e}")

    print("🎯 All simulators running.")
    while True:
        time.sleep(300)


# ─── Entry Point ─────────────────────────────────────────────────────
if __name__ == "__main__":
    main()

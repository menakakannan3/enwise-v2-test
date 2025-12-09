#!/usr/bin/env python3
import json
import random
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import paho.mqtt.client as mqtt
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes

# ─── MQTT Config ─────────────────────────────────────────────────────
MQTT_BROKER = "broker.enwise.in"
MQTT_PORT = 1883

# ─── Timezone ───────────────────────────────────────────────────────
IST = ZoneInfo("Asia/Kolkata")

# ─── Device Configurations ──────────────────────────────────────────
DEVICES = [
    {
        "auth_key": "9887092c204cc9cd4305840cfbe42f44",  # ASCII KEY (32 chars)
        "payload": {
            "site_uid": "EW_25263",
            "chipid": "SA_TEST_01SA_TEST_01",
            "device_uid": "SA_TEST_01",
            "data": [
                {
                    "station_uid": "EW_STAT_8",
                    "analyser_id": "analyser_1",
                    "parameter_id": "param_3",
                    "value": 0
                }
            ]
        }
    },
    {
        "auth_key": "481f852e88107637b0ac6facf56c35d7",  # ASCII KEY (32 chars)
        "payload": {
            "site_uid": "EW_25264",
            "chipid": "SA_TEST_02SA_TEST_02",
            "device_uid": "SA_TEST_02",
            "data": [
                {
                    "station_uid": "EW_STAT_9",
                    "analyser_id": "analyser_1",
                    "parameter_id": "param_3",
                    "value": 0
                }
            ]
        }
    }
]

# ─── Encryption Helpers ─────────────────────────────────────────────
def zero_pad(b: bytes, block_size: int = 16) -> bytes:
    rem = len(b) % block_size
    return b if rem == 0 else b + b"\x00" * (block_size - rem)

def encrypt_payload(payload: dict, ascii_key: str):
    key = ascii_key.encode("utf-8")      # IMPORTANT FIX
    iv = get_random_bytes(16)

    cipher = AES.new(key, AES.MODE_CBC, iv=iv)

    pt = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    pt_padded = zero_pad(pt)

    ct = cipher.encrypt(pt_padded)
    return iv.hex().upper(), ct.hex().upper()

# ─── Payload Builder ─────────────────────────────────────────────────
def build_payload(base):
    payload = {
        "site_uid": base["site_uid"],
        "chipid": base["chipid"],
        "device_uid": base["device_uid"],
        "QualityCode": "U",
        "timestamp": "",
        "data": []
    }

    for item in base["data"]:
        new_item = dict(item)
        new_item["value"] = f"{random.uniform(1, 5000):.3f}"
        payload["data"].append(new_item)

    return payload

# ─── MQTT Debug Callbacks ───────────────────────────────────────────
def on_connect(client, userdata, flags, rc):
    print("🔗 MQTT CONNECT RC =", rc)

def on_publish(client, userdata, mid):
    print("✔ Published MID:", mid)

# ─── Main Worker ─────────────────────────────────────────────────────
def main():
    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.on_publish = on_publish

    print("Connecting to MQTT...")
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    mqtt_client.loop_start()

    print("🚀 Starting MULTI-DEVICE simulator...")

    while True:
        try:
            for device in DEVICES:
                base = device["payload"]
                ascii_key = device["auth_key"]
                device_uid = base["device_uid"]

                payload = build_payload(base)

                now = datetime.now(IST)
                payload["timestamp"] = now.strftime("%Y-%m-%dT%H:%M:%SZ%z")

                iv_hex, ct_hex = encrypt_payload(payload, ascii_key)

                envelope = {
                    "IV": iv_hex,
                    "Ciphertext": ct_hex
                }

                topic = f"{device_uid}_OUT"
                mqtt_client.publish(topic, json.dumps(envelope))

                # print(f"[Encrypted @ {topic}]: {json.dumps(envelope)}")
                # print(f"[Decrypted]: {json.dumps(payload)}")

        except Exception as e:
            print(f"❌ Error: {e}")

        time.sleep(2)  # change to 60 for once-per-minute


if __name__ == "__main__":
    main()

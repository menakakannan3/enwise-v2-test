from fastapi import APIRouter, Request, Body, HTTPException
import httpx
import paho.mqtt.publish as publish

router = APIRouter()

@router.put("/proxy_ptz")
async def proxy_ptz(request: Request):
    target_url = request.query_params.get("url")
    if not target_url:
        raise HTTPException(status_code=400, detail="Missing 'url' query parameter")

    body = await request.body()

    # ✅ Setup DigestAuth
    username = "admin"
    password = "hbsa@1234"
    digest_auth = httpx.DigestAuth(username, password)

    headers = {
        "Content-Type": "application/xml"
    }

    async with httpx.AsyncClient(verify=False) as client:
        response = await client.put(target_url, data=body, headers=headers, auth=digest_auth)

    return response.text

@router.post("/mqtt/ptz")
def send_ptz_command(
    stream: str = Body(..., embed=True),
    command: str = Body(..., embed=True)
):
    topic = f"{stream}/control"
    try:
        publish.single(topic, command, hostname="broker.enwise.in", port=1883)
        return {"status": "sent", "topic": topic, "command": command}
    except Exception as e:
        raise HTTPException(status_code=500, detail="MQTT publish failed")
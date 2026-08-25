from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager

import asyncio
import json
import threading

import paho.mqtt.client as mqtt 
from datetime import datetime, timezone
from database import db

from models import Package
from database import (
    set_node_armed,
    get_node,
    get_home_by_mac,
    touch_home_last_seen,
    set_armed,
    write_cache,
    analyse_cache,
    upsert_node,
    update_node_warnings,
    start_event,
    update_event,
    close_event,
    get_home,
)

# from notifications import notify_home

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "securifi/master"

loop: asyncio.AbstractEventLoop = None
mqtt_client: mqtt.Client = None


# package processing
async def handle_package(raw: dict):
    try:
        pkg = Package(**raw)
    except Exception as e:
        print(f"[SERVER] invalid package: {e}")
        return

    home = get_home_by_mac(pkg.master_mac)

    if not home:
        print(f"[SERVER] Unknown MAC: {pkg.master_mac}")
        return

    hid = home["hid"]

    # updating individual nodes:
    for node in pkg.nodes:
        upsert_node(
            hid,
            node.node_id,
            node.role
        )

        update_node_warnings(
            hid=hid,
            node_id=node.node_id,
            low_battery=node.warnings.low_battery,
            not_transmitting=node.warnings.not_transmitting,
            signal_weak=node.warnings.signal_weak
        )

    # confirming armed state
    for node in pkg.nodes:
        current_node = get_node(hid, node.node_id)
        if not current_node:
            continue

        requested_armed = current_node.get("requestedArmed")
        if requested_armed is None:
            continue

        if node.armed != requested_armed:
            send_node_arm_command( # TODO
                hid, 
                node.node_id,
                requested_armed
            )

    # writing in the cache
    write_cache(hid, pkg)

    # analysing package history
    analysis = analyse_cache(hid, pkg)

    # event handling:
    home_doc = get_home(hid)
    active_event_id = (
        home_doc.get("activeEventId")
        if home_doc
        else None
    )

    # if analysis["is_threat"] and pkg.armed: # TODO MAI RAMANE PKG.ARMED?
    #     if not active_event_id:
    #         eid = start_event(
    #             hid, 
    #             pkg.intruder_probability
    #         )

    #         print(f"[SERVER] Event opened: {eid}")

    #     else:
    #         update_event(
    #             active_event_id,
    #             pkg.intruder_probability
    #         )
    # elif analysis["should_close_session"] and active_event_id:
    #     close_event(
    #         hid,
    #         active_event_id
    #     )

    #     print(f"[SERVER] Event auto closed: {active_event_id}")



# mqtt
def on_mqtt_message(client, userdata, msg):
    try:
        raw = json.loads(msg.payload.decode("utf-8"))

        asyncio.run_coroutine_threadsafe(handle_package(raw), loop)
    except json.JSONDecodeError as e:
        print(f"[MQTT] JSON parse error: {e}")
    except Exception as e:
        print(f"[MQTT] Unexpected error: {e}")

def on_mqtt_connect(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe(MQTT_TOPIC)
        print(f"[MQTT] Connected, subscribed to {MQTT_TOPIC}")
    else:
        print(f"[MQTT] Connection failed rc={rc}")

def on_mqtt_disconnect(client, userdata, rc):
    if rc != 0:
        print(f"[MQTT] Unexpected disconnect rc={rc}. Paho will reconnect")

def start_mqtt():
    global mqtt_client
    mqtt_client = mqtt.Client()

    mqtt_client.on_message = on_mqtt_message
    mqtt_client.on_connect = on_mqtt_connect
    mqtt_client.on_disconnect = on_mqtt_disconnect

    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    mqtt_client.loop_forever()


# lifespan
@asynccontextmanager
async def lifespan(app: FastAPI):
    global loop
    loop = asyncio.get_event_loop()

    threading.Thread(target=start_mqtt, daemon=True).start()

    print("[SERVER] Securi-Fi MVP4 started")
    yield
    print("[SERVER] Securi-Fi stopped") 


app = FastAPI(lifespan=lifespan)

# esp - server endpoints:
@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}

@app.post("/arm/{hid}")
def request_arm(hid: str, armed: bool):
    from database import set_requested_armed
    try:
        set_requested_armed(hid, armed)
        return {"hid": hid, "requestedArmed": armed}
    except Exception as e:
        print(f"[SERVER] requested_arm error: {e}")
        raise HTTPException(status_code=500, detail="Failed to set requested arm state")
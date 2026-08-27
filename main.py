from fastapi import FastAPI
from contextlib import asynccontextmanager
import asyncio
import json
import threading
import time
import paho.mqtt.client as mqtt
from datetime import datetime, timezone
 
from models import Package, NodeConfigRequest, NodeConfigCommand, NodeConfigConfirmation
from database import (
    get_home_by_mac, get_home, create_home,
    touch_home_last_seen, set_active_event,
    write_cache, analyse_cache,
    upsert_node, update_node_warnings,
    get_node, get_nodes_for_home,
    set_node_armed, set_node_requested_armed,
    start_event, update_event, close_event,
    db,
)
# from notifications import notify_home # NU IN MVP4


# mqtt topics:
MQTT_BROKER = "localhost"
MQTT_PORT = 1883

TOPIC_TELEMETRY = "securifi/master"

TOPIC_CONFIG_REQUEST = "securifi/config/request/#" # pt cand isi cere esp-ul state-ul la inceput
TOPIC_CONFIG_CONFIRM = "securifi/config/confirm/#" # pt cand confirma esp-ul o comanda

TOPIC_CONFIG_COMMAND = "securifi/config/command/{mac}" # server -> esp


# global state:
loop: asyncio.AbstractEventLoop = None
mqtt_client: mqtt.Client = None


# mqtt send:
def send_config_command(master_mac: str, node_id: str, armed: bool):
    if mqtt_client is None:
        print(f"[MQTT] Cannot send command, mqtt client not ready")
        return

    topic = TOPIC_CONFIG_COMMAND.format(mac=master_mac)
    payload = NodeConfigCommand(
        node_id=node_id,
        cmd="arm" if armed else "standby"
    )

    try: 
        mqtt_client.publish(topic, json.dumps(payload.model_dump()))
        print(f"[MQTT] Config command sent: node={node_id} cmd={'arm' if armed else 'standby'} -> {topic}")
    except Exception as e:
        print(f"[MQTT] Failed to send config command: {e}")


# requestedArmed shit
def on_nodes_snapshot(col_snapshot, changes, read_time):
    for change in changes: 
        doc = change.document
        data = doc.to_dict()

        requested = data.get("requestedArmed", False)
        current = data.get("armed", False)

        if requested == current:
            continue

        hid = data.get("hid")
        node_id = data.get("nodeId")

        if not hid or not node_id:
            print(f"[SNAPSHOT] Node doc {doc.id} missing hid or nodeId")
            continue

        home = get_home(hid)
        if not home:
            print(f"[SNAPSHOT] Home {hid} not found for node {node_id}")
            continue

        master_mac = home.get("masterMac")
        if not master_mac:
            print(f"[SNAPSHOT] Home {hid} has no masterMac")
            continue

        print(f"[SNAPSHOT] Node {node_id} mismatch: requestedArmed={requested} armed={current}. Sending command...")
        send_config_command(master_mac, node_id, requested)

def start_firestore_listener():
    db.collection("nodes").on_snapshot(on_nodes_snapshot)
    print("[SNAPSHOT] Nodes collection listener registered")

    while True:
        time.sleep(3600)


# telemetry handler;
async def handle_package(raw: dict):
    # validate:
    try:
        pkg = Package(**raw)
    except Exception as e:
        print(f"[SERVER] Invalid package: {e}")
        return 

    # resolve / auto-create home:
    home = get_home_by_mac(pkg.master_mac)
    if not home:
        print(f"[SERVER] Unknown MAC {pkg.master_mac}, auto-crafting home")

        hid = create_home(pkg.master_mac)
        home = get_home(hid)
        home["hid"] = hid

    hid = home["hid"]

    # upsert nodes + update warnings:
    for node in pkg.nodes:
        upsert_node(hid, node.node_id, node.role)
        update_node_warnings(
            hid=hid,
            node_id=node.node_id,

            low_battery=node.warnings.low_battery,
            not_transmitting=node.warnings.not_transmitting,
            signal_weak=node.warnings.signal_weak
        )

    # disaster detection 
    disaster_type = _get_disaster_type(pkg)
    if disaster_type:
        active_eid = home.get("activeEventId")
        if not active_eid:
            eid = start_event(hid)
            print(f"[SERVER] Disaster event started ({disaster_type}): {eid}")
            # await notify_home(hid, disaster_type, 1.0) # nu in MVP4

        active_eid = home.get("activeEventId") or eid
        update_event(active_eid, [pkg])

        touch_home_last_seen(hid)
        return

    # write cache + in-memory analysis
    analysis = analyse_cache(hid, pkg)
    is_alarm = write_cache(hid, pkg, analysis["current_window"])
    

    # intruder event lifecycle
    active_eid = home.get("activeEventId")

    if analysis["is_threat"]:
        if not active_eid:
            eid = start_event(hid)
            active_eid = eid
            print(f"[SERVER] Intruder event started: {eid}")

        if analysis.get("flushed") and analysis.get("flushed_chunk"):
            update_event(active_eid, analysis["flushed_chunk"])

    elif analysis.get("flushed") and active_eid:
        update_event(active_eid, analysis["flushed_chunk"])
        if analysis["should_close_session"]:
            close_event(hid, active_eid)
            
    elif analysis["should_close_session"] and active_eid:
        close_event(hid, active_eid)


    # log node warning:
    for node in pkg.nodes:
        active = [
            w for w, v in [
                ("low_battery", node.warnings.low_battery),
                ("not_transmitting", node.warnings.not_transmitting),
                ("signal_weak", node.warnings.signal_weak),
            ] if v
        ]
        if active:
            print(f"[SERVER] Node {node.node_id} warnings: {active}")

    # touching last seen: 
    touch_home_last_seen(hid)


# config request handler:
async def handle_config_request(master_mac: str, raw: dict):
    try:
        req = NodeConfigRequest(**raw)
    except Exception as e:
        print(f"[SERVER] Invalid config request: {e}")
        return

    home = get_home_by_mac(master_mac)
    if not home:
        print(f"[SERVER] Config request from unknown MAC {master_mac}")
        return 

    hid = home["hid"]
    node = get_node(hid, req.node_id)

    if not node:
        print(f"[SERVER] Config request for unknown node {req.node_id} in home {hid}. Default to standby")
        send_config_command(master_mac, req.node_id, False)
        return 

    requested = node.get("requestedArmed", False)
    print(f"[SERVER] Config request from {req.node_id}: sending {'arm' if requested else 'standby'}")
    send_config_command(master_mac, req.node_id, requested)


# config confirmation handler
async def handle_config_confirmation(master_mac: str, raw: dict):
    try: 
        conf = NodeConfigConfirmation(**raw)
    except Exception as e:
        print(f"[SERVER] Invalid config confirmation: {e}")
        return

    home = get_home_by_mac(master_mac)
    if not home:
        print(f"[SERVER] Config confirmation from unknown MAC {master_mac}")
        return

    if not conf.success:
        print(f"[SERVER] Node {conf.node_id} FAILED to execute command. Armed state remains unchanged")
        return

    hid = home.get("hid")
    set_node_armed(hid, conf.node_id, conf.armed)
    print(f"[SERVER] Node {conf.node_id} confirmed armed={conf.armed}")


# disaster helper:
def _get_disaster_type(pkg: Package) -> str | None:
    has_flame = any(n.sensors.flame for n in pkg.nodes)
    has_gas = any(n.sensors.gas for n in pkg.nodes)

    if has_flame: return "flame"
    if has_gas: return "gas"
    return None


# mqtt callbacks:
def on_mqtt_message(client, userdata, msg):
    topic = msg.topic
    try:
        raw = json.loads(msg.payload.decode("utf-8"))
    except json.JSONDecodeError as e:
        print(f"[MQTT] JSON parse error on {topic}: {e}")
        return 

    if topic == TOPIC_TELEMETRY:
        asyncio.run_coroutine_threadsafe(handle_package(raw), loop)
    elif topic.startswith("securifi/config/request/"):
        master_mac = topic.split("/", 3)[3]
        asyncio.run_coroutine_threadsafe(handle_config_request(master_mac, raw), loop)
    elif topic.startswith("securifi/config/confirm/"):
        master_mac = topic.split("/", 3)[3]
        asyncio.run_coroutine_threadsafe(handle_config_confirmation(master_mac, raw), loop)
    else:
        print(f"[MQTT] Unhandled topic: {topic}")


def on_mqtt_connect(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe(TOPIC_TELEMETRY)
        client.subscribe(TOPIC_CONFIG_REQUEST)
        client.subscribe(TOPIC_CONFIG_CONFIRM)
        print(f"[MQTT] Connected, subscribed to telemetry + config topics")
    else:
        print(f"[MQTT] Connection failed rc={rc}")
 
 
def on_mqtt_disconnect(client, userdata, rc):
    if rc != 0:
        print(f"[MQTT] Unexpected disconnect rc={rc}, paho will reconnect")

def start_mqtt():
    global mqtt_client
    mqtt_client = mqtt.Client()

    mqtt_client.on_connect = on_mqtt_connect
    mqtt_client.on_message = on_mqtt_message
    mqtt_client.on_disconnect = on_mqtt_disconnect

    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    mqtt_client.loop_forever()


# lifespan:
@asynccontextmanager
async def lifespan(app: FastAPI):
    global loop
    loop = asyncio.get_event_loop()

    threading.Thread(target=start_mqtt, daemon=True).start()
    threading.Thread(target=start_firestore_listener, daemon=True).start()

    print("[SERVER] SecuriFi MVP4 started")
    yield
    print("[SERVER] SecuriFi stopped")
 
 
app = FastAPI(lifespan=lifespan)
 
 
# endpoints
@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}
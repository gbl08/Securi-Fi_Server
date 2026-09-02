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
    touch_home_last_seen,
    upsert_node, update_node_warnings,
    get_node,
    set_node_armed, 
    start_event, update_event, close_event,
    revert_node_requested_armed, clear_node_requested_reboot, clear_node_requested_deep_sleep,
    _node_readings_to_package_reading_and_alarm, _build_cache_entry, append_to_cache,
    send_command_with_timeout, resolve_pending_command,
    resolve_buzzer_command, send_buzzer_to_home, _buzzer_active,
    _pending_commands,
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
def send_config_command(master_mac: str, node_id: str, cmd: str):
    # cmd: "arm" | "disarm" | "reboot" | "deep_sleep"
    if mqtt_client is None:
        print("[MQTT] Cannot send command, mqtt client not ready")
        return

    topic   = TOPIC_CONFIG_COMMAND.format(mac=master_mac)
    payload = NodeConfigCommand(node_id=node_id, cmd=cmd)

    try:
        mqtt_client.publish(topic, json.dumps(payload.model_dump()))
        print(f"[MQTT] Config command: node={node_id} cmd={cmd} → {topic}")
    except Exception as e:
        print(f"[MQTT] Failed to send config command: {e}")

# change in the database
def on_nodes_snapshot(col_snapshot, changes, read_time):
    for change in changes:
        doc = change.document
        data = doc.to_dict()

        hid = data.get("hid")
        node_id = data.get("nodeId")
        if not hid or not node_id:
            continue

        home = get_home(hid)
        if not home:
            continue
        master_mac = home.get("masterMac")
        if not master_mac:
            continue

        requested_armed = data.get("requestedArmed", False)
        current_armed = data.get("armed", False)
        if requested_armed != current_armed:
            cmd = "arm" if requested_armed else "disarm"
            key = (hid, node_id, cmd)
            if key not in _pending_commands:   # ← only send if not already waiting
                print(f"[SNAPSHOT] Node {node_id} {cmd} mismatch, sending command")
                send_command_with_timeout(hid, master_mac, node_id, cmd)
            else:
                print(f"[SNAPSHOT] Node {node_id} {cmd} already pending, skipping")

        if data.get("requestedReboot", False):
            key = (hid, node_id, "reboot")
            if key not in _pending_commands:
                print(f"[SNAPSHOT] Node {node_id} reboot requested")
                send_command_with_timeout(hid, master_mac, node_id, "reboot")

        if data.get("requestedDeepSleep", False):
            key = (hid, node_id, "deep_sleep")
            if key not in _pending_commands:
                print(f"[SNAPSHOT] Node {node_id} deep sleep requested")
                send_command_with_timeout(hid, master_mac, node_id, "deep_sleep")

def start_firestore_listener():
    db.collection("nodes").on_snapshot(on_nodes_snapshot)
    print("[SNAPSHOT] Nodes collection listener registered")

    while True:
        time.sleep(3600)


# telemetry handler;
async def handle_package(raw: dict):
    try:
        pkg = Package(**raw)
    except Exception as e:
        print(f"[SERVER] Invalid package: {e}")
        return

    pkg.timestamp = datetime.now(timezone.utc).isoformat()
    print(f"[SERVER] Package recieved (master_mac={pkg.master_mac}, timestamp={pkg.timestamp}, movement_pct = [{pkg.nodes[0].movement_pct}, {pkg.nodes[1].movement_pct}, {pkg.nodes[2].movement_pct}, {pkg.nodes[3].movement_pct}])")

    home = get_home_by_mac(pkg.master_mac)
    if not home:
        print(f"[SERVER] Unknown MAC {pkg.master_mac}, auto-creating home")
        hid = create_home(pkg.master_mac)
        home = get_home(hid)
        home["hid"] = hid

    hid = home["hid"]

    for node in pkg.nodes:
        upsert_node(hid, node.node_id, node.role)
        update_node_warnings(
            hid=hid,
            node_id=node.node_id,
            low_battery=node.warnings.low_battery,
            not_transmitting=node.warnings.not_transmitting,
            signal_weak=node.warnings.signal_weak,
        )

    active_eid = home.get("activeEventId")

    # disaster detection
    disaster_type = _get_disaster_type(pkg)
    if disaster_type:
        if active_eid:
            close_event(hid, active_eid, send_buzzer_off=False)  # buzzer_off skipped — we're about to resend

        active_eid = start_event(hid, disaster_type)
        send_buzzer_to_home(hid, "buzzer_on_warning")
        _buzzer_active[hid] = True
        print(f"[SERVER] Disaster event started ({disaster_type}): {active_eid}")
        # await notify_home(hid, disaster_type, 1.0)

        movement_pct, _ = _node_readings_to_package_reading_and_alarm(pkg)
        disaster_entry = _build_cache_entry(pkg, movement_pct, True)
        update_event(hid, active_eid, [disaster_entry])
        touch_home_last_seen(hid)
        return

    analysis = append_to_cache(hid, pkg)
    active_eid = home.get("activeEventId") 
    idle_streak = analysis["idle_streak"]

    BUZZER_IDLE_STOP = 10 

    if active_eid:
        buzzer_on = _buzzer_active.get(hid, False)

        if idle_streak >= BUZZER_IDLE_STOP and buzzer_on:
            send_buzzer_to_home(hid, "buzzer_off")
            _buzzer_active[hid] = False

        elif idle_streak == 0 and not buzzer_on:
            send_buzzer_to_home(hid, "buzzer_on_alarm")
            _buzzer_active[hid] = True

    if not analysis["flushed"]:
        if analysis["should_close"] and active_eid:
            close_event(hid, active_eid)
        touch_home_last_seen(hid)
        return

    entries = analysis["entries"]

    if active_eid:
        event_doc = (
            db.collection("home_events").document(hid)
            .collection("events").document(active_eid)
            .get()
        )
        event_data = event_doc.to_dict() if event_doc.exists else {}
        ended_at = event_data.get("endedAt")

        if ended_at:
            update_event(hid, active_eid, entries)
            print(f"[SERVER] Final chunk dumped to user-closed event {active_eid}")
        else:
            update_event(hid, active_eid, entries)
            if analysis["should_close"]:
                close_event(hid, active_eid)
                print(f"[SERVER] Event {active_eid} closed by idle streak")
    else:
        if analysis["is_alarm"]:
            new_eid = start_event(hid, "intruder")
            print(f"[SERVER] Intruder event started: {new_eid}")
            update_event(hid, new_eid, entries)
            send_buzzer_to_home(hid, "buzzer_on_alarm")
            _buzzer_active[hid] = True
            # await notify_home(hid, "intruder", analysis["alarm_count"] / MAX_CACHE)

    for node in pkg.nodes:
        active_warnings = [
            w for w, v in [
                ("low_battery",      node.warnings.low_battery),
                ("not_transmitting", node.warnings.not_transmitting),
                ("signal_weak",      node.warnings.signal_weak),
            ] if v
        ]
        if active_warnings:
            print(f"[SERVER] Node {node.node_id} warnings: {active_warnings}")

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
        print(f"[SERVER] Config request from unknown MAC {master_mac}, auto-creating home")
        hid = create_home(master_mac)
        home = get_home(hid)
        home["hid"] = hid

    hid = home["hid"]

    upsert_node(hid, req.node_id, req.role)

    node = get_node(hid, req.node_id)
    requested_armed = node.get("requestedArmed", False)
    cmd = "arm" if requested_armed else "disarm"
    send_config_command(master_mac, req.node_id, cmd)
    print(f"[SERVER] Config request from {req.node_id}: sending {cmd}")


# config confirmation handler
async def handle_config_confirmation(master_mac: str, raw: dict):
    try:
        conf = NodeConfigConfirmation(**raw)
    except Exception as e:
        print(f"[SERVER] Invalid config confirmation: {e}")
        return

    home = get_home_by_mac(master_mac)
    if not home:
        return
    hid = home["hid"]

    match conf.cmd:
        case "arm":
            if conf.success:
                resolve_pending_command(hid, conf.node_id, "arm")
                set_node_armed(hid, conf.node_id, True)
            else:
                revert_node_requested_armed(hid, conf.node_id)

        case "disarm":
            if conf.success:
                resolve_pending_command(hid, conf.node_id, "disarm")
                set_node_armed(hid, conf.node_id, False)
            else:
                revert_node_requested_armed(hid, conf.node_id)

        case "reboot":
            resolve_pending_command(hid, conf.node_id, "reboot")
            clear_node_requested_reboot(hid, conf.node_id)
            if not conf.success:
                print(f"[SERVER] Node {conf.node_id} reboot FAILED")

        case "deep_sleep":
            resolve_pending_command(hid, conf.node_id, "deep_sleep")
            clear_node_requested_deep_sleep(hid, conf.node_id)
            if not conf.success:
                print(f"[SERVER] Node {conf.node_id} deep sleep FAILED")

        case "buzzer_on_alarm" | "buzzer_on_warning" | "buzzer_off":
            resolve_buzzer_command(hid, conf.node_id, conf.cmd)
            if not conf.success:
                print(f"[SERVER] Buzzer command '{conf.cmd}' FAILED on node {conf.node_id}")
                
        case _:
            print(f"[SERVER] Unknown cmd in confirmation: {conf.cmd}")


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
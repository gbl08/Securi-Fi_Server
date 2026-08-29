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
    revert_node_requested_armed, clear_node_requested_reboot, clear_node_requested_deep_sleep,
    _node_readings_to_package_reading_and_alarm, _build_cache_entry, _recompute_alarm_count, append_to_cache,
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

        # arm / disarm mismatch
        requested_armed = data.get("requestedArmed", False)
        current_armed   = data.get("armed", False)
        if requested_armed != current_armed:
            cmd = "arm" if requested_armed else "disarm"
            print(f"[SNAPSHOT] Node {node_id} {cmd} mismatch, sending command")
            send_config_command(master_mac, node_id, cmd)

        # reboot request
        if data.get("requestedReboot", False):
            print(f"[SNAPSHOT] Node {node_id} reboot requested")
            send_config_command(master_mac, node_id, "reboot")

        # deep sleep request
        if data.get("requestedDeepSleep", False):
            print(f"[SNAPSHOT] Node {node_id} deep sleep requested")
            send_config_command(master_mac, node_id, "deep_sleep")

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

    # disaster detection — bypasses cache entirely
    disaster_type = _get_disaster_type(pkg)
    if disaster_type:
        active_eid = home.get("activeEventId")
        if not active_eid:
            active_eid = start_event(hid)
            print(f"[SERVER] Disaster event started ({disaster_type}): {active_eid}")

        movement_pct, _ = _node_readings_to_package_reading_and_alarm(pkg)
        disaster_entry = _build_cache_entry(pkg, movement_pct, True)
        update_event(hid, active_eid, [disaster_entry])
        touch_home_last_seen(hid)
        return

    # append to cache — flushes internally if full
    analysis = append_to_cache(hid, pkg)

    active_eid = home.get("activeEventId")

    if not analysis["flushed"]:
        # mid-cache: only check idle streak for closing an open event
        if analysis["should_close"] and active_eid:
            close_event(hid, active_eid)
        touch_home_last_seen(hid)
        return

    # --- cache flushed: make all decisions here ---
    entries = analysis["entries"]   # list[CacheEntry], 10 items

    if active_eid:
        # there is an open event — always push this chunk to it
        # but first: filter out entries that happened after the event closed
        # (handles the case where user closed the event mid-cache)
        event_doc = (
            db.collection("home_events").document(hid)
            .collection("events").document(active_eid)
            .get()
        )
        event_data = event_doc.to_dict() if event_doc.exists else {}
        ended_at = event_data.get("endedAt")   # None if still open

        if ended_at:
            # event was closed by user mid-cache: push only packages before endedAt
            entries_before_close = [
                e for e in entries
                if e.timestamp <= ended_at.isoformat()
            ]
            if entries_before_close:
                update_event(hid, active_eid, entries_before_close)
                print(f"[SERVER] Pushed {len(entries_before_close)} pre-close entries to closed event {active_eid}")
            # drop the rest — event is closed, we move on
        else:
            # event still open: push full chunk
            update_event(hid, active_eid, entries)

            if analysis["should_close"]:
                close_event(hid, active_eid)
                print(f"[SERVER] Event {active_eid} closed by idle streak")

    else:
        # no active event
        if analysis["is_alarm"]:
            new_eid = start_event(hid)
            print(f"[SERVER] Intruder event started: {new_eid}")
            update_event(hid, new_eid, entries)
        # else: not a threat, cache just cleared, nothing to do

    for node in pkg.nodes:
        active_warnings = [
            w for w, v in [
                ("low_battery", node.warnings.low_battery),
                ("not_transmitting", node.warnings.not_transmitting),
                ("signal_weak", node.warnings.signal_weak),
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
        print(f"[SERVER] Config request from unknown MAC {master_mac}")
        return 

    hid = home["hid"]
    node = get_node(hid, req.node_id)

    if not node:
        print(f"[SERVER] Config request for unknown node {req.node_id} in home {hid}. Default to standby")
        send_config_command(master_mac, req.node_id, "disarm")
        return 

    requested_armed = node.get("requestedArmed", False)
    cmd = "arm" if requested_armed else "disarm"
    send_config_command(master_mac, req.node_id, cmd)
    print(f"[SERVER] Config request from {req.node_id}: sending {cmd} setup command")


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
                set_node_armed(hid, conf.node_id, True)
                print(f"[SERVER] Node {conf.node_id} armed confirmed")
            else:
                revert_node_requested_armed(hid, conf.node_id)
                print(f"[SERVER] Node {conf.node_id} arm FAILED — requestedArmed reverted")

        case "disarm":
            if conf.success:
                set_node_armed(hid, conf.node_id, False)
                print(f"[SERVER] Node {conf.node_id} disarmed confirmed")
            else:
                revert_node_requested_armed(hid, conf.node_id)
                print(f"[SERVER] Node {conf.node_id} disarm FAILED — requestedArmed reverted")

        case "reboot":
            clear_node_requested_reboot(hid, conf.node_id)
            if not conf.success:
                print(f"[SERVER] Node {conf.node_id} reboot FAILED")

        case "deep_sleep":
            clear_node_requested_deep_sleep(hid, conf.node_id)
            if not conf.success:
                print(f"[SERVER] Node {conf.node_id} deep sleep FAILED")

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
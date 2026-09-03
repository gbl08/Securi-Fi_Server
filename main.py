from fastapi import FastAPI
from contextlib import asynccontextmanager

import asyncio
import json
import threading
import time
import queue

import paho.mqtt.client as mqtt

from datetime import datetime, timezone

from models import (
    Package,
    NodeConfigRequest,
    NodeConfigCommand,
    NodeConfigConfirmation,
)

from database import (
    db,
    get_home, get_home_by_mac, create_home, touch_home_last_seen,
    _upsert_nodes_if_needed,
    get_node, set_node_armed,
    start_event, close_event,
    append_to_cache, update_last_package, append_to_event_buffer,
    _node_readings_to_package_reading_and_alarm,
    send_command_with_timeout, resolve_pending_command,
    resolve_buzzer_command, send_buzzer_to_home,
    _buzzer_active, _pending_commands,
)

# mqtt: 
MQTT_BROKER = "localhost"
MQTT_PORT = 1883

TOPIC_TELEMETRY = "securifi/master"

TOPIC_CONFIG_REQUEST = "securifi/config/request/#"
TOPIC_CONFIG_CONFIRM = "securifi/config/confirm/#"
TOPIC_CONFIG_COMMAND = "securifi/config/command/{mac}"

# global state:
loop: asyncio.AbstractEventLoop = None
mqtt_client: mqtt.Client = None

# telemetry:
_telemetry_queue: queue.Queue = queue.Queue(maxsize=50)

# mqtt command sending
def send_config_command(master_mac: str, node_id: str, cmd: str):
    if mqtt_client is None:
        print("[MQTT] Cannot send command, mqtt client not ready")
        return

    topic = TOPIC_CONFIG_COMMAND.format(mac=master_mac)
    payload = NodeConfigCommand(node_id=node_id, cmd=cmd)

    try:
        mqtt_client.publish(topic, json.dumps(payload.model_dump()))
        print(f"[MQTT] Config command: node={node_id} cmd={cmd} -> {topic}")

    except Exception as e:
        print(f"[MQTT] Failed to send command: {e}")

# firestore node snapshot: 
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

        # arm / disarm:
        requested_armed = data.get("requestedArmed", False)
        current_armed = data.get("armed", False)
        if requested_armed != current_armed:
            cmd = "arm" if requested_armed else "disarm"
            key = (hid, node_id, cmd)
            if key not in _pending_commands:
                print(f"[SNAPSHOT] Node {node_id} {cmd} mismatch, sending command")
                send_command_with_timeout(hid, master_mac, node_id, cmd)
            else:
                print(f"[SNAPSHOT] Node {node_id} {cmd} already pending, skipping")

        # reboot
        if data.get("requestedReboot", False):
            key = (hid, node_id, "reboot")
            if key not in _pending_commands:
                print(f"[SNAPSHOT] Node {node_id} reboot requested")
                send_command_with_timeout(hid, master_mac, node_id, "reboot")

        # deepsleep:
        if data.get("requestedDeepSleep", False):
            key = (hid, node_id, "deep_sleep")
            if key not in _pending_commands:
                print(f"[SNAPSHOT] Node {node_id} deep sleep requested")
                send_command_with_timeout(hid, master_mac, node_id, "deep_sleep")


# home snapshot
def on_homes_snapshot(col_snapshot, changes, read_time):
    for change in changes:
        doc = change.document
        data = doc.to_dict()
        hid = doc.id

        requested_cache = data.get("requestedCache", False)
        if not requested_cache:
            continue

        print(f"[SNAPSHOT] Cache requested for home {hid}")
        try:
            from database import dump_cache_to_firestore
            dump_cache_to_firestore(hid)

            db.collection("homes").document(hid).update({"requestedCache": False})
            print(f"[SNAPSHOT] Cache request completed for {hid}")
        except Exception as e:
            print(f"[SNAPSHOT] Failed to dump cache for {hid}: {e}")

def start_firestore_listener():
    db.collection("nodes").on_snapshot(on_nodes_snapshot)
    db.collection("homes").on_snapshot(on_homes_snapshot)

    print("[SNAPSHOT] Nodes collection listener registered")
    print("[SNAPSHOT] Homes collection listener registered")

    while True:
        time.sleep(3600)


# telemetry processing: 
def handle_package_sync(raw: dict):
    try:
        pkg = Package(**raw)
    except Exception as e:
        print(f"[SERVER] Invalid package: {e}")
        return

    received_at = datetime.now(timezone.utc)
    pkg.timestamp = received_at.isoformat()

    print(f"[SERVER] Package received (master_mac={pkg.master_mac}, timestamp={pkg.timestamp}, nodes={len(pkg.nodes)})")

    home = get_home_by_mac(pkg.master_mac)
    if not home:
        print(f"[SERVER] Unknown MAC {pkg.master_mac}, auto-creating home")

        hid = create_home(pkg.master_mac)
        home = get_home(hid)

        if not home:
            print(f"[SERVER] Failed to load newly created home {hid}")
            return

    hid = home["hid"]
    _upsert_nodes_if_needed(hid, pkg)

    analysis = append_to_cache(hid, pkg)
    latest_entry = analysis["latest_entry"]

    update_last_package(hid, latest_entry)

    disaster_type = _get_disaster_type(pkg)
    if disaster_type:
        current_home = get_home(hid)
        active_eid = current_home.get("activeEventId") if current_home else None

        if active_eid:
            close_event(hid, active_eid, send_buzzer_off=False)

        active_eid = start_event(hid, disaster_type)
        send_buzzer_to_home(hid, "buzzer_on_warning")
        _buzzer_active[hid] = True

        print(f"[SERVER] Disaster event started ({disaster_type}): {active_eid}")

        append_to_event_buffer(hid, active_eid, latest_entry)
        touch_home_last_seen(hid)

        return

    # current active event: 
    current_home = get_home(hid)
    if not current_home:
        print(f"[SERVER] Home {hid} disappeared while processing package")
        return

    active_eid = current_home.get("activeEventId")
    idle_streak = analysis["idle_streak"]

    BUZZER_IDLE_STOP = 10

    if active_eid:
        buzzer_on = _buzzer_active.get(hid, False)
        if (idle_streak >= BUZZER_IDLE_STOP and buzzer_on):
            send_buzzer_to_home(hid, "buzzer_off")
            _buzzer_active[hid] = False
        elif (idle_streak == 0 and not buzzer_on):
            send_buzzer_to_home(hid, "buzzer_on_alarm")
            _buzzer_active[hid] = True

        append_to_event_buffer(hid, active_eid, latest_entry)

        if (analysis["should_close"]):
            close_event(hid, active_eid)

        touch_home_last_seen(hid)
        return

    if analysis["is_alarm"]:
        new_eid = start_event(hid, "intrusion")

        print(f"[SERVER] Intrusion event started: {new_eid}")

        append_to_event_buffer(hid, new_eid, latest_entry)
        send_buzzer_to_home(hid, "buzzer_on_alarm")

        _buzzer_active[hid] = True

    for node in pkg.nodes:
        active_warnings = []
        if node.warnings.low_battery:
            active_warnings.append("low_battery")

        if node.warnings.not_transmitting:
            active_warnings.append("not_transmitting")

        if node.warnings.signal_weak:
            active_warnings.append("signal_weak")

        if active_warnings:
            print(f"[SERVER] Node {node.node_id} warnings: {active_warnings}")

    touch_home_last_seen(hid)


# config request: 
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

        if not home:
            return

    hid = home["hid"]
    node = get_node(hid, req.node_id)
    if not node:
        from database import upsert_node

        upsert_node(hid, req.node_id, req.role)
        node = get_node(hid, req.node_id)

    if not node:
        print(f"[SERVER] Failed to create/read node {req.node_id}")
        return

    requested_armed = node.get("requestedArmed", False)

    cmd = (
        "arm"
        if requested_armed
        else "disarm"
    )

    send_config_command(
        master_mac,
        req.node_id,
        cmd
    )

    print(
        f"[SERVER] Config request "
        f"from {req.node_id}: "
        f"sending {cmd}"
    )


# ============================================================
# CONFIG CONFIRMATION
# ============================================================

async def handle_config_confirmation(
    master_mac: str,
    raw: dict
):
    try:

        conf = NodeConfigConfirmation(
            **raw
        )

    except Exception as e:

        print(
            f"[SERVER] Invalid config "
            f"confirmation: {e}"
        )

        return

    home = get_home_by_mac(
        master_mac
    )

    if not home:
        return

    hid = home["hid"]

    match conf.cmd:

        # ----------------------------------------------------
        # ARM
        # ----------------------------------------------------

        case "arm":

            if conf.success:

                resolve_pending_command(
                    hid,
                    conf.node_id,
                    "arm"
                )

                set_node_armed(
                    hid,
                    conf.node_id,
                    True
                )

            else:

                from database import (
                    revert_node_requested_armed
                )

                revert_node_requested_armed(
                    hid,
                    conf.node_id
                )

        # ----------------------------------------------------
        # DISARM
        # ----------------------------------------------------

        case "disarm":

            if conf.success:

                resolve_pending_command(
                    hid,
                    conf.node_id,
                    "disarm"
                )

                set_node_armed(
                    hid,
                    conf.node_id,
                    False
                )

            else:

                from database import (
                    revert_node_requested_armed
                )

                revert_node_requested_armed(
                    hid,
                    conf.node_id
                )

        # ----------------------------------------------------
        # REBOOT
        # ----------------------------------------------------

        case "reboot":

            resolve_pending_command(
                hid,
                conf.node_id,
                "reboot"
            )

            from database import (
                clear_node_requested_reboot
            )

            clear_node_requested_reboot(
                hid,
                conf.node_id
            )

            if not conf.success:

                print(
                    f"[SERVER] Node "
                    f"{conf.node_id} "
                    f"reboot FAILED"
                )

        # ----------------------------------------------------
        # DEEP SLEEP
        # ----------------------------------------------------

        case "deep_sleep":

            resolve_pending_command(
                hid,
                conf.node_id,
                "deep_sleep"
            )

            from database import (
                clear_node_requested_deep_sleep
            )

            clear_node_requested_deep_sleep(
                hid,
                conf.node_id
            )

            if not conf.success:

                print(
                    f"[SERVER] Node "
                    f"{conf.node_id} "
                    f"deep sleep FAILED"
                )

        # ----------------------------------------------------
        # BUZZER
        # ----------------------------------------------------

        case (
            "buzzer_on_alarm"
            | "buzzer_on_warning"
            | "buzzer_off"
        ):

            resolve_buzzer_command(
                hid,
                conf.node_id,
                conf.cmd
            )

            if not conf.success:

                print(
                    f"[SERVER] Buzzer command "
                    f"'{conf.cmd}' FAILED "
                    f"on node "
                    f"{conf.node_id}"
                )

        # ----------------------------------------------------
        # UNKNOWN
        # ----------------------------------------------------

        case _:

            print(
                f"[SERVER] Unknown cmd "
                f"in confirmation: "
                f"{conf.cmd}"
            )


# ============================================================
# DISASTER DETECTION
# ============================================================

def _get_disaster_type(
    pkg: Package
) -> str | None:

    has_fire = any(
        node.sensors.fire
        for node in pkg.nodes
    )

    has_gas = any(
        node.sensors.gas
        for node in pkg.nodes
    )

    if has_fire:
        return "fire"

    if has_gas:
        return "gas_leak"

    return None


# ============================================================
# TELEMETRY WORKER
# ============================================================

def _telemetry_worker():

    print(
        "[WORKER] Telemetry worker started"
    )

    while True:

        raw = _telemetry_queue.get()

        try:

            if raw is None:

                print(
                    "[WORKER] Telemetry "
                    "worker stopping"
                )

                return

            handle_package_sync(
                raw
            )

        except Exception as e:

            print(
                "[WORKER] Unhandled error "
                f"processing telemetry: {e}"
            )

        finally:

            _telemetry_queue.task_done()


# ============================================================
# MQTT CALLBACK
# ============================================================

def on_mqtt_message(
    client,
    userdata,
    msg
):

    topic = msg.topic

    try:

        raw = json.loads(
            msg.payload.decode(
                "utf-8"
            )
        )

    except json.JSONDecodeError as e:

        print(
            f"[MQTT] JSON parse error "
            f"on {topic}: {e}"
        )

        return

    # --------------------------------------------------------
    # TELEMETRY
    # --------------------------------------------------------

    if topic == TOPIC_TELEMETRY:

        try:

            _telemetry_queue.put_nowait(
                raw
            )

        except queue.Full:

            print(
                "[MQTT] Telemetry queue "
                "full, dropping package"
            )

    # --------------------------------------------------------
    # CONFIG REQUEST
    # --------------------------------------------------------

    elif topic.startswith(
        "securifi/config/request/"
    ):

        master_mac = topic.split(
            "/",
            3
        )[3]

        asyncio.run_coroutine_threadsafe(
            handle_config_request(
                master_mac,
                raw
            ),
            loop
        )

    # --------------------------------------------------------
    # CONFIG CONFIRMATION
    # --------------------------------------------------------

    elif topic.startswith(
        "securifi/config/confirm/"
    ):

        master_mac = topic.split(
            "/",
            3
        )[3]

        asyncio.run_coroutine_threadsafe(
            handle_config_confirmation(
                master_mac,
                raw
            ),
            loop
        )

    else:

        print(
            f"[MQTT] Unhandled topic: "
            f"{topic}"
        )


# ============================================================
# MQTT CONNECTION
# ============================================================

def on_mqtt_connect(
    client,
    userdata,
    flags,
    rc
):

    if rc == 0:

        client.subscribe(
            TOPIC_TELEMETRY
        )

        client.subscribe(
            TOPIC_CONFIG_REQUEST
        )

        client.subscribe(
            TOPIC_CONFIG_CONFIRM
        )

        print(
            "[MQTT] Connected, subscribed "
            "to telemetry + config topics"
        )

    else:

        print(
            f"[MQTT] Connection failed rc={rc}"
        )


def on_mqtt_disconnect(
    client,
    userdata,
    rc
):

    if rc != 0:

        print(
            f"[MQTT] Unexpected disconnect "
            f"rc={rc}, paho will reconnect"
        )


def start_mqtt():

    global mqtt_client

    mqtt_client = mqtt.Client()

    mqtt_client.on_connect = (
        on_mqtt_connect
    )

    mqtt_client.on_message = (
        on_mqtt_message
    )

    mqtt_client.on_disconnect = (
        on_mqtt_disconnect
    )

    mqtt_client.connect(
        MQTT_BROKER,
        MQTT_PORT,
        keepalive=60
    )

    mqtt_client.loop_forever()


# ============================================================
# FASTAPI LIFESPAN
# ============================================================

@asynccontextmanager
async def lifespan(
    app: FastAPI
):

    global loop

    loop = asyncio.get_event_loop()

    # --------------------------------------------------------
    # MQTT
    # --------------------------------------------------------

    threading.Thread(
        target=start_mqtt,
        daemon=True
    ).start()

    # --------------------------------------------------------
    # FIRESTORE LISTENERS
    # --------------------------------------------------------

    threading.Thread(
        target=start_firestore_listener,
        daemon=True
    ).start()

    # --------------------------------------------------------
    # TELEMETRY WORKER
    # --------------------------------------------------------

    threading.Thread(
        target=_telemetry_worker,
        daemon=True
    ).start()

    print(
        "[SERVER] SecuriFi MVP4 started"
    )

    yield

    # --------------------------------------------------------
    # SHUTDOWN
    # --------------------------------------------------------

    _telemetry_queue.put(
        None
    )

    print(
        "[SERVER] SecuriFi stopped"
    )


# ============================================================
# FASTAPI
# ============================================================

app = FastAPI(
    lifespan=lifespan
)


# ============================================================
# ENDPOINTS
# ============================================================

@app.get("/health")
def health():

    return {
        "status": "ok",
        "timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
    }
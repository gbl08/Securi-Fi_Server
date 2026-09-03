import firebase_admin
from firebase_admin import credentials, firestore

from datetime import datetime, timezone
from typing import Optional
import uuid
import threading

from models import (
    Package,
    NodeReading,
    HomeDoc,
    NodeDoc,
    CacheNodeReadingDoc,
    CacheEntry,
    EventDoc,
)


# ============================================================
# FIREBASE SETUP
# ============================================================

cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)

db = firestore.client()

print("[DB] Firebase connected :D")


# ============================================================
# HOMES
# ============================================================

def create_home(master_mac: str) -> str:
    hid = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    home = HomeDoc(
        master_mac=master_mac,
        active_event_id=None,
        requested_cache=False,
        last_seen=now,
        registered_at=now,
    )

    db.collection("homes").document(hid).set(
        home.model_dump(by_alias=True)
    )

    print(f"[DB] Home created: {hid} ({master_mac})")

    return hid


def get_home_by_mac(master_mac: str) -> Optional[dict]:
    query = (
        db.collection("homes")
        .where("masterMac", "==", master_mac)
        .limit(1)
        .stream()
    )

    for doc in query:
        return {
            "hid": doc.id,
            **doc.to_dict(),
        }

    return None


def get_home(hid: str) -> Optional[dict]:
    doc = (
        db.collection("homes")
        .document(hid)
        .get()
    )

    if not doc.exists:
        return None

    return {
        "hid": doc.id,
        **doc.to_dict(),
    }


def touch_home_last_seen(hid: str):
    db.collection("homes").document(hid).update({
        "lastSeen": datetime.now(timezone.utc)
    })


def set_active_event(
    hid: str,
    event_id: Optional[str]
):
    db.collection("homes").document(hid).update({
        "activeEventId": event_id
    })


# ============================================================
# NODES
# ============================================================

def get_node(
    hid: str,
    node_id: str
) -> Optional[dict]:

    doc_id = f"{hid}_{node_id}"

    doc = (
        db.collection("nodes")
        .document(doc_id)
        .get()
    )

    if not doc.exists:
        return None

    return {
        "doc_id": doc.id,
        **doc.to_dict(),
    }


def upsert_node(
    hid: str,
    node_id: str,
    role: str,
    nickname: Optional[str] = None,
):
    doc_id = f"{hid}_{node_id}"
    doc_ref = db.collection("nodes").document(doc_id)

    existing = doc_ref.get()

    existing_data = (
        existing.to_dict()
        if existing.exists
        else {}
    )

    node = NodeDoc(
        hid=hid,
        node_id=node_id,
        nickname=(
            nickname
            or existing_data.get("nickname", node_id)
        ),
        role=role,

        battery_pct=existing_data.get("batteryPct"),
        report_type=existing_data.get("reportType"),
        sensor_reading=existing_data.get(
            "sensorReading",
            0
        ),
        movement_pct=existing_data.get(
            "movementPct",
            0
        ),
        warning_type=existing_data.get(
            "warningType"
        ),

        armed=existing_data.get(
            "armed",
            False
        ),

        requested_armed=existing_data.get(
            "requestedArmed",
            False
        ),
    )

    doc_ref.set(
        node.model_dump(by_alias=True),
        merge=True
    )


def _get_report_type(
    node: NodeReading
) -> Optional[str]:

    if node.warnings.low_battery:
        return "low_battery"

    if node.warnings.not_transmitting:
        return "not_transmitting"

    if node.warnings.signal_weak:
        return "signal_weak"

    return None


def _get_warning_type(
    node: NodeReading
) -> Optional[str]:

    if node.sensors.fire:
        return "fire"

    if node.sensors.gas:
        return "gas_leak"

    return None


def update_node_from_reading(
    hid: str,
    node: NodeReading
):
    """
    Update the persistent Firestore node document with
    the latest telemetry from this node.
    """

    doc_id = f"{hid}_{node.node_id}"

    existing = (
        db.collection("nodes")
        .document(doc_id)
        .get()
    )

    if not existing.exists:
        upsert_node(
            hid,
            node.node_id,
            node.role
        )

    db.collection("nodes").document(doc_id).update({
        "batteryPct": node.sensors.battery_pct,
        "reportType": _get_report_type(node),
        "sensorReading": node.raw_mq2_reading,
        "movementPct": node.movement_pct,
        "warningType": _get_warning_type(node),
    })


def _upsert_nodes_if_needed(
    hid: str,
    pkg: Package
):
    """
    Creates node documents when first seen.

    Telemetry values are also updated here so the nodes
    collection always contains the latest node state.
    """

    for node in pkg.nodes:
        key = f"{hid}_{node.node_id}"

        if key not in _known_nodes:
            upsert_node(
                hid,
                node.node_id,
                node.role
            )

            _known_nodes.add(key)

        update_node_from_reading(
            hid,
            node
        )


def get_nodes_for_home(hid: str) -> list[dict]:
    try:
        docs = (
            db.collection("nodes")
            .where("hid", "==", hid)
            .stream()
        )

        return [
            {
                "doc_id": doc.id,
                **doc.to_dict(),
            }
            for doc in docs
        ]

    except Exception as e:
        print(
            f"[DB] get_nodes_for_home error: {e}"
        )

        return []


# ============================================================
# NODE ARMING
# ============================================================

def set_node_armed(
    hid: str,
    node_id: str,
    armed: bool
):
    """
    Called ONLY after explicit ESP confirmation.

    Telemetry never changes the persistent armed state.
    """

    doc_id = f"{hid}_{node_id}"

    try:
        db.collection("nodes").document(doc_id).update({
            "armed": armed
        })

        print(
            f"[DB] Node {node_id} armed confirmed={armed}"
        )

    except Exception as e:
        print(
            f"[DB] Failed to confirm node armed state "
            f"{doc_id}: {e}"
        )


def set_node_requested_armed(
    hid: str,
    node_id: str,
    requested_armed: bool
):
    """
    Called when the app requests arm/disarm.

    Only requestedArmed changes here.
    """

    doc_id = f"{hid}_{node_id}"

    try:
        db.collection("nodes").document(doc_id).update({
            "requestedArmed": requested_armed
        })

        print(
            f"[DB] Node {node_id} "
            f"requestedArmed={requested_armed}"
        )

    except Exception as e:
        print(
            f"[DB] Failed to set requestedArmed="
            f"{requested_armed} at {doc_id}: {e}"
        )


def revert_node_requested_armed(
    hid: str,
    node_id: str
):
    node = get_node(hid, node_id)

    if not node:
        return

    db.collection("nodes").document(
        f"{hid}_{node_id}"
    ).update({
        "requestedArmed": node.get(
            "armed",
            False
        )
    })


def clear_node_requested_reboot(
    hid: str,
    node_id: str
):
    db.collection("nodes").document(
        f"{hid}_{node_id}"
    ).update({
        "requestedReboot": False
    })


def clear_node_requested_deep_sleep(
    hid: str,
    node_id: str
):
    db.collection("nodes").document(
        f"{hid}_{node_id}"
    ).update({
        "requestedDeepSleep": False
    })


# ============================================================
# USER
# ============================================================

def get_user_profile(
    uid: str
) -> Optional[dict]:

    doc = (
        db.collection("users")
        .document(uid)
        .get()
    )

    return (
        doc.to_dict()
        if doc.exists
        else None
    )


# ============================================================
# LIVE CACHE
# ============================================================

MOVEMENT_THRESHOLD = 140

ALARM_MULTI_COUNT = 2
THREAT_COUNT = 3

# The live cache contains roughly the last minute.
LIVE_CACHE_SIZE = 60

# Event chunks contain 10 packages.
EVENT_CHUNK_SIZE = 10

IDLE_CLOSE_COUNT = 30


# ============================================================
# IN-MEMORY STATE
# ============================================================

# hid -> latest packages
_in_memory_cache: dict[str, list[CacheEntry]] = {}

# eid -> packages waiting to become the next event chunk
_event_buffers: dict[str, list[CacheEntry]] = {}

# eid -> next chunk number
_event_chunk_counters: dict[str, int] = {}

# hid -> consecutive idle package count
_idle_streaks: dict[str, int] = {}

# hid -> whether buzzer is currently considered active
_buzzer_active: dict[str, bool] = {}

# Used so we don't repeatedly create the same node documents
_known_nodes: set[str] = set()


# ============================================================
# PACKAGE ANALYSIS
# ============================================================

def _node_readings_to_package_reading_and_alarm(
    pkg: Package
) -> tuple[int, bool]:

    active_nodes = [
        node
        for node in pkg.nodes
        if not node.warnings.not_transmitting
    ]

    if not active_nodes:
        return 0, False

    readings = sorted(
        (
            node.movement_pct
            for node in active_nodes
        ),
        reverse=True
    )

    if len(readings) == 1:
        package_movement_pct = readings[0]
    else:
        package_movement_pct = (
            readings[0] + readings[1]
        ) // 2

    package_movement_pct = min(
        200,
        max(0, package_movement_pct)
    )

    over_threshold = sum(
        1
        for reading in readings
        if reading >= MOVEMENT_THRESHOLD
    )

    is_alarm = (
        package_movement_pct >= MOVEMENT_THRESHOLD
        or over_threshold >= ALARM_MULTI_COUNT
    )

    return (
        package_movement_pct,
        is_alarm
    )


def _build_cache_entry(
    pkg: Package,
    movement_pct: int,
    is_alarm: bool
) -> CacheEntry:

    return CacheEntry(
        package_pct=movement_pct,
        is_alarm=is_alarm,
        timestamp=datetime.fromisoformat(
            pkg.timestamp
        ),

        nodes={
            node.node_id: CacheNodeReadingDoc(
                battery_pct=node.sensors.battery_pct,
                report_type=_get_report_type(node),
                sensor_reading=node.raw_mq2_reading,
                movement_pct=node.movement_pct,
                warning_type=_get_warning_type(node),
            )
            for node in pkg.nodes
        }
    )


def _recompute_alarm_count(
    entries: list[CacheEntry]
) -> int:

    return sum(
        1
        for entry in entries
        if entry.is_alarm
    )


def _update_idle_streak(
    hid: str,
    new_entry: CacheEntry
) -> int:

    if (
        new_entry.is_alarm
        or any(
            node.warning_type is not None
            for node in new_entry.nodes.values()
        )
        or any(
            node.report_type is not None
            for node in new_entry.nodes.values()
        )
    ):
        _idle_streaks[hid] = 0

    else:
        _idle_streaks[hid] = (
            _idle_streaks.get(hid, 0) + 1
        )

    return _idle_streaks[hid]


# ============================================================
# IN-MEMORY CACHE
# ============================================================

def append_to_cache(
    hid: str,
    pkg: Package
) -> dict:

    movement_pct, is_alarm = (
        _node_readings_to_package_reading_and_alarm(
            pkg
        )
    )

    new_entry = _build_cache_entry(
        pkg,
        movement_pct,
        is_alarm
    )

    entries = _in_memory_cache.setdefault(
        hid,
        []
    )

    entries.append(new_entry)

    # Keep only the latest 60 packages.
    if len(entries) > LIVE_CACHE_SIZE:
        entries.pop(0)

    idle_streak = _update_idle_streak(
        hid,
        new_entry
    )

    alarm_count = _recompute_alarm_count(
        entries
    )

    cache_is_alarm = (
        alarm_count >= THREAT_COUNT
    )

    return {
        "entries": entries.copy(),
        "alarm_count": alarm_count,
        "idle_streak": idle_streak,
        "is_alarm": cache_is_alarm,
        "should_close": (
            idle_streak >= IDLE_CLOSE_COUNT
        ),
        "latest_entry": new_entry,
    }


def dump_cache_to_firestore(hid: str):
    """
    Called ONLY when the app sets requestedCache=true.

    Takes the current RAM cache and creates a snapshot
    in cache/{hid}.
    """

    entries = _in_memory_cache.get(
        hid,
        []
    )

    cache_data = {
        "packages": [
            entry.model_dump(by_alias=True)
            for entry in entries
        ],

        "updatedAt": datetime.now(timezone.utc),
    }

    (
        db.collection("cache")
        .document(hid)
        .set(cache_data)
    )

    print(
        f"[DB] Cache snapshot written for {hid}: "
        f"{len(entries)} packages"
    )


def update_last_package(
    hid: str,
    entry: CacheEntry
):
    """
    Writes ONLY the newest package to homes/{hid}.lastPackage.

    This is the live Firestore update that the app listens to.
    """

    db.collection("homes").document(hid).update({
        "lastPackage": entry.model_dump(
            by_alias=True
        )
    })


# ============================================================
# EVENT CHUNKS
# ============================================================

def update_event(
    hid: str,
    eid: str,
    entries: list[CacheEntry]
):
    if not entries:
        return

    chunk_data = {
        "savedAt": datetime.now(timezone.utc),

        "packages": [
            entry.model_dump(by_alias=True)
            for entry in entries
        ],
    }

    chunk_index = _event_chunk_counters.get(
        eid,
        0
    )

    cid = f"{chunk_index:05d}"

    _event_chunk_counters[eid] = (
        chunk_index + 1
    )

    (
        db.collection("home_events")
        .document(hid)
        .collection("events")
        .document(eid)
        .collection("chunks")
        .document(cid)
        .set(chunk_data)
    )

    print(
        f"[DB] Event {eid}: chunk {cid} saved "
        f"({len(entries)} packages)"
    )


def append_to_event_buffer(
    hid: str,
    eid: str,
    entry: CacheEntry
):
    """
    Add one package to the active event buffer.

    Once EVENT_CHUNK_SIZE is reached, write a chunk
    to Firestore.
    """

    entries = _event_buffers.setdefault(
        eid,
        []
    )

    entries.append(entry)

    if len(entries) >= EVENT_CHUNK_SIZE:
        update_event(
            hid,
            eid,
            entries
        )

        entries.clear()


def _flush_event_buffer(
    hid: str,
    eid: str
):
    entries = _event_buffers.get(
        eid,
        []
    )

    if not entries:
        return

    update_event(
        hid,
        eid,
        entries
    )

    entries.clear()


# ============================================================
# EVENTS
# ============================================================

def start_event(
    hid: str,
    event_type: str
) -> str:

    eid = str(uuid.uuid4())

    now = datetime.now(timezone.utc)

    event = EventDoc(
        hid=hid,
        event_type=event_type,
        started_at=now,
        ended_at=None,
        false_alarm=False,
        false_alarm_description=None,
    )

    (
        db.collection("home_events")
        .document(hid)
        .collection("events")
        .document(eid)
        .set(
            event.model_dump(
                by_alias=True
            )
        )
    )

    _event_buffers[eid] = []
    _event_chunk_counters[eid] = 0

    set_active_event(
        hid,
        eid
    )

    _idle_streaks[hid] = 0

    print(
        f"[DB] Event started "
        f"({event_type}): {eid}"
    )

    return eid


def close_event(
    hid: str,
    eid: str,
    send_buzzer_off: bool = True
):
    # Never lose a partial final chunk.
    _flush_event_buffer(
        hid,
        eid
    )

    (
        db.collection("home_events")
        .document(hid)
        .collection("events")
        .document(eid)
        .update({
            "endedAt": datetime.now(timezone.utc)
        })
    )

    set_active_event(
        hid,
        None
    )

    _event_buffers.pop(
        eid,
        None
    )

    _event_chunk_counters.pop(
        eid,
        None
    )

    _idle_streaks[hid] = 0

    _buzzer_active[hid] = False

    if send_buzzer_off:
        send_buzzer_to_home(
            hid,
            "buzzer_off"
        )

    print(
        f"[DB] Event closed: {eid}"
    )


# ============================================================
# BUZZER
# ============================================================

COMMAND_TIMEOUT_SECONDS = 15

_pending_commands: dict[
    tuple,
    threading.Timer
] = {}


def send_buzzer_to_home(
    hid: str,
    cmd: str
):
    """
    Sends a buzzer command to all nodes of a home.

    cmd:
        buzzer_on_alarm
        buzzer_on_warning
        buzzer_off
    """

    from main import send_config_command

    home = get_home(hid)

    if not home:
        return

    master_mac = home.get(
        "masterMac"
    )

    if not master_mac:
        return

    nodes = get_nodes_for_home(
        hid
    )

    for node in nodes:

        node_id = node.get(
            "nodeId"
        )

        if not node_id:
            continue

        send_config_command(
            master_mac,
            node_id,
            cmd
        )

        key = (
            hid,
            node_id,
            cmd
        )

        existing = _pending_commands.get(
            key
        )

        if existing:
            existing.cancel()

        timer = threading.Timer(
            COMMAND_TIMEOUT_SECONDS,
            _on_buzzer_timeout,
            args=(
                hid,
                node_id,
                cmd,
            )
        )

        _pending_commands[key] = timer

        timer.start()

    print(
        f"[DB] Buzzer command '{cmd}' "
        f"sent to all nodes of home {hid}"
    )


def _on_buzzer_timeout(
    hid: str,
    node_id: str,
    cmd: str
):
    key = (
        hid,
        node_id,
        cmd
    )

    _pending_commands.pop(
        key,
        None
    )

    print(
        f"[DB] Buzzer command '{cmd}' "
        f"to node {node_id} timed out "
        f"(best-effort, no revert)"
    )


def resolve_buzzer_command(
    hid: str,
    node_id: str,
    cmd: str
):
    timer = _pending_commands.pop(
        (
            hid,
            node_id,
            cmd
        ),
        None
    )

    if timer:
        timer.cancel()


# ============================================================
# COMMAND TIMEOUTS
# ============================================================

def _on_command_timeout(
    hid: str,
    node_id: str,
    cmd: str
):
    key = (
        hid,
        node_id,
        cmd
    )

    if key not in _pending_commands:
        return

    _pending_commands.pop(
        key,
        None
    )

    print(
        f"[DB] Command '{cmd}' "
        f"to node {node_id} timed out, reverting"
    )

    match cmd:

        case "arm" | "disarm":
            revert_node_requested_armed(
                hid,
                node_id
            )

        case "reboot":
            clear_node_requested_reboot(
                hid,
                node_id
            )

        case "deep_sleep":
            clear_node_requested_deep_sleep(
                hid,
                node_id
            )


def send_command_with_timeout(
    hid: str,
    master_mac: str,
    node_id: str,
    cmd: str
):
    from main import send_config_command

    send_config_command(
        master_mac,
        node_id,
        cmd
    )

    key = (
        hid,
        node_id,
        cmd
    )

    existing = _pending_commands.get(
        key
    )

    if existing:
        existing.cancel()

    timer = threading.Timer(
        COMMAND_TIMEOUT_SECONDS,
        _on_command_timeout,
        args=(
            hid,
            node_id,
            cmd,
        )
    )

    _pending_commands[key] = timer

    timer.start()


def resolve_pending_command(
    hid: str,
    node_id: str,
    cmd: str
):
    timer = _pending_commands.pop(
        (
            hid,
            node_id,
            cmd
        ),
        None
    )

    if timer:
        timer.cancel()
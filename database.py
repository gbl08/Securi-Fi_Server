import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timezone
from typing import Optional
import uuid

from models import (
    Package,
    HomeDoc,
    NodeDoc,
    NodeWarningDoc,
    CacheDoc,
    CacheNodeReadingDoc,
    CacheSensorsDoc,
    EventDoc,
    CacheEntry
)

# setup: 
cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

print("[DB] Firebase connected :D")


# homes:
def create_home(master_mac: str) -> str:
    hid = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    home = HomeDoc(
        master_mac=master_mac,
        active_event_id=None,
        last_seen=now,
        registered_at=now,
    )
    db.collection("homes").document(hid).set(home.model_dump(by_alias=True))
    print(f"[DB] Home created: {hid} ({master_mac})")

    return hid


def get_home_by_mac(master_mac: str) -> Optional[dict]:
    query = db.collection("homes").where("masterMac", "==", master_mac).limit(1).stream()
    for doc in query:
        return {"hid": doc.id, **doc.to_dict()}
    
    return None


def get_home(hid: str) -> Optional[dict]:
    doc = db.collection("homes").document(hid).get()
    return doc.to_dict() if doc.exists else None


def touch_home_last_seen(hid: str):
    db.collection("homes").document(hid).update({
        "lastSeen": datetime.now(timezone.utc)
    })


def set_active_event(hid: str, event_id: Optional[str]):
    db.collection("homes").document(hid).update({"activeEventId": event_id})


# nodes: 
def get_node(hid: str, node_id: str) -> Optional[dict]:
    doc_id = f"{hid}_{node_id}"
    doc = db.collection("nodes").document(doc_id).get()

    if not doc.exists:
        return None

    return doc.to_dict()

def upsert_node(hid: str, node_id: str, role: str, nickname: Optional[str] = None):
    doc_id = f"{hid}_{node_id}"
    doc_ref = db.collection("nodes").document(doc_id)
    existing = doc_ref.get()

    existing_data = existing.to_dict() if existing.exists else {}

    node = NodeDoc(
        hid=hid,
        node_id=node_id,

        nickname=nickname or existing_data.get("nickname", node_id),
        role=role,

        warnings=NodeWarningDoc(
            low_battery=False,
            not_transmitting=False,
            signal_weak=False,
        ),
        armed=existing_data.get("armed", False),
        requested_armed=existing_data.get("requestedArmed", False),
    )

    doc_ref.set(node.model_dump(by_alias=True), merge=True)


def update_node_warnings(hid: str, node_id: str, low_battery: bool, not_transmitting: bool, signal_weak: bool):
    doc_id = f"{hid}_{node_id}"

    warnings = NodeWarningDoc(
        low_battery=low_battery,
        not_transmitting=not_transmitting,
        signal_weak=signal_weak,
    )

    db.collection("nodes").document(doc_id).update({
        "warnings": warnings.model_dump(by_alias=True)
    })

def set_node_armed(hid: str, node_id: str, armed: bool):
    """
    Called ONLY when ESP sends explicit NodeConfigConfirmation.
    Never called from telemetry.
    """

    doc_id = f"{hid}_{node_id}"

    try:
        db.collection("nodes").document(doc_id).update({"armed": armed})
        print(f"[DB] Node {node_id} armed confirmed={armed}")
    except Exception as e:
        print(f"[DB] Failed to confirm node armed state {doc_id}: {e}")

def set_node_requested_armed(hid: str, node_id: str, requested_armed: bool):
    """
    Called when user requests arm/disarm.
    Does NOT update armed — only requestedArmed.
    The Firestore snapshot listener detects the mismatch and sends MQTT command.
    """

    doc_id = f"{hid}_{node_id}"

    try:
        db.collection("nodes").document(doc_id).update({
            "requestedArmed": requested_armed
        })

        print(f"[DB] Node {node_id} requestedArmed={requested_armed}")
    except Exception as e:
        print(f"[DB] Failed to set requestedArmed={requested_armed} at {doc_id}: {e}")

def get_user_profile(uid: str) -> Optional[dict]:
    doc = db.collection("users").document(uid).get()
    return doc.to_dict() if doc.exists else None

def get_nodes_for_home(hid: str) -> list[dict]:
    try:
        docs = db.collection("nodes").where("hid", "==", hid).stream()

        return [{"doc_id": d.id, **d.to_dict()} for d in docs]
    except Exception as e:
        print(f"[DB] get_nodes_for_home error: {e}")

        return []


# cache & threat analysis
#constants:
MOVEMENT_THRESHOLD = 140
ALARM_MULTI_COUNT = 2
THREAT_COUNT = 3
MAX_CACHE = 10
IDLE_CLOSE_COUNT = 30 #TODO cum pot ajunge la 30 daca in cahce sunt numai 10??

_event_chunk_counters: dict[str, int] = {}

def node_readings_to_package_reading_and_alarm(pkg: Package) -> tuple[int, bool]:
    active_nodes = [n for n in pkg.nodes if not n.warnings.not_transmitting]
    if not active_nodes:
        return (0, False)

    readings = sorted((n.movement_pct for n in active_nodes), reverse=True)

    if len(readings) == 1:
        package_movement_pct = readings[0]
    else:
        package_movement_pct = (readings[0] + readings[1]) // 2

    package_movement_pct = min(200, max(0, package_movement_pct))
    over_threshold = sum(1 for r in readings if r >= MOVEMENT_THRESHOLD)

    is_alarm = (
        package_movement_pct >= MOVEMENT_THRESHOLD
        or over_threshold >= ALARM_MULTI_COUNT
    )

    return (package_movement_pct, is_alarm)

def _build_cache_entry(pkg: Package, movement_pct: int, is_alarm: bool) -> CacheEntry:
    return CacheEntry(
        timestamp=pkg.timestamp,
        warning_type=pkg.warning_type,
        package_movement_pct=movement_pct,
        is_alarm=is_alarm,
        nodes=[
            CacheNodeReadingDoc(
                state=node.state,
                movement_pct=node.movement_pct,
                raw_mq2_reading=node.raw_mq2_reading,
                is_alarm=node.movement_pct >= MOVEMENT_THRESHOLD,
                sensors=CacheSensorsDoc(
                    flame=node.sensors.flame,
                    gas=node.sensors.gas,
                    battery_pct=node.sensors.battery_pct,
                )
            )
            for node in pkg.nodes
        ]
    )

def _recompute_counters(entries: list[CacheEntry]) -> tuple[int, int]: # (alarm_count, idle_streak)
    alarm_count = sum(1 for e in entries if e.is_alarm)
    idle_streak = 0
    for e in reversed(entries):
        if not e.is_alarm and e.warning_type is None:
            idle_streak += 1
        else:
            break
    return alarm_count, idle_streak

def analyse_and_flush_cache(hid: str, entries: list[CacheEntry]) -> dict:
    alarm_count, idle_streak = _recompute_counters(entries)
    is_alarm = alarm_count >= THREAT_COUNT
    should_close = idle_streak >= IDLE_CLOSE_COUNT

    db.collection("cache").document(hid).set({
        "packages": [],
        "alarmCount": 0,
        "idleStreak": 0,
        "isAlarm": False,
        "nodeReadings": {},
        "updatedAt": datetime.now(timezone.utc),
    })

    return {
        "flushed": True,
        "entries": entries,          # the 10 tagged CacheEntry objects
        "alarm_count": alarm_count,
        "idle_streak": idle_streak,
        "is_alarm": is_alarm,
        "should_close": should_close,
        "latest_entry": entries[-1],
    }

def append_to_cache(hid: str, pkg: Package) -> dict:
    """
    Tags the incoming package, appends it to Firestore cache, 
    recomputes counters. Returns current cache state.
    If the cache hits MAX_CACHE, triggers analyse_and_flush_cache instead.
    """
    movement_pct, is_alarm = node_readings_to_package_reading_and_alarm(pkg)
    new_entry = _build_cache_entry(pkg, movement_pct, is_alarm)

    cache_ref = db.collection("cache").document(hid)
    cache_doc = cache_ref.get()
    cache_data = cache_doc.to_dict() if cache_doc.exists else {}

    raw_entries = cache_data.get("packages", [])
    entries: list[CacheEntry] = [CacheEntry(**e) for e in raw_entries]
    entries.append(new_entry)

    if len(entries) >= MAX_CACHE:
        # hand off to flush, it will write to Firestore itself
        return analyse_and_flush_cache(hid, entries)

    alarm_count, idle_streak = _recompute_counters(entries)

    now = datetime.now(timezone.utc)
    latest_pkg_nodes = {
        node.node_id: CacheNodeReadingDoc(
            state=node.state,
            movement_pct=node.movement_pct,
            raw_mq2_reading=node.raw_mq2_reading,
            is_alarm=node.movement_pct >= MOVEMENT_THRESHOLD,
            sensors=CacheSensorsDoc(
                flame=node.sensors.flame,
                gas=node.sensors.gas,
                battery_pct=node.sensors.battery_pct,
            )
        )
        for node in pkg.nodes
    }

    cache_ref.set({
        "packages": [e.model_dump(by_alias=True) for e in entries],
        "alarmCount": alarm_count,
        "idleStreak": idle_streak,
        "isAlarm": alarm_count >= THREAT_COUNT,
        "nodeReadings": {k: v.model_dump(by_alias=True) for k, v in latest_pkg_nodes.items()},
        "updatedAt": now,
    })

    return {
        "flushed": False,
        "entries": entries,
        "alarm_count": alarm_count,
        "idle_streak": idle_streak,
        "is_alarm": alarm_count >= THREAT_COUNT,
        "should_close": idle_streak >= IDLE_CLOSE_COUNT,
        "latest_entry": new_entry,
    }

def write_cache(hid: str, entry: CacheEntry, analysis: dict):
    cache = CacheDoc(
        alarm_count=analysis["alarm_count"],
        is_alarm=analysis["is_alarm"],
        window_size=analysis["cache_size"],
        node_readings={
            node.node_id: CacheNodeReadingDoc(
                state=node.state,
                movement_pct=node.movement_pct,
                raw_mq2_reading=node.raw_mq2_reading,
                is_alarm=node.movement_pct >= MOVEMENT_THRESHOLD,
                sensors=CacheSensorsDoc(
                    flame=node.sensors.flame,
                    gas=node.sensors.gas,
                    battery_pct=node.sensors.battery_pct,
                )
            )
            for node in entry.package.nodes
        },
        updated_at=datetime.now(timezone.utc),
    )
    db.collection("cache").document(hid).set(cache.model_dump(by_alias=True))


def analyse_cache(hid: str, package: Package) -> dict: # TODO RESCRIS PE NOUA STRUCTURA
    window = _package_windows.setdefault(hid, [])
    window.append(package)

    flushed = False
    flushed_chunk = None

    if len(window) > MAX_CACHE:
        flushed = True
        flushed_chunk = list(window)
        window.clear()

    current_window = flushed_chunk if flushed else window
    alarm_count = 0
    for p in current_window:
        m_pct, is_a = node_readings_to_package_reading_and_alarm(p)
        if is_a:
            alarm_count += 1

    is_alarm = alarm_count >= THREAT_COUNT

    idle_streak = 0
    for p in reversed(window):
        _, is_alarm = node_readings_to_package_reading_and_alarm(p) # TODO IN LOC SA RECALCULAM LA FIECARE ITERATIE SA CONTINA P O VARIABILA IS_ARMED PE CARE O SETAM SI REFOLOSIM
        if not is_alarm and p.warning_type is None:
            idle_streak 
        else:
            break

    return {
        "is_alarm": alarm_count >= 3,    
        "alarm_count": alarm_count,
        "should_close_session": idle_streak >= IDLE_CLOSE_COUNT,
        "window_size": len(window),
        "current_window": window,

        "flushed": flushed,
        "flushed_chunk": flushed_chunk
    }


# events
def start_event(hid: str) -> str:
    eid = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    event = EventDoc(hid=hid, started_at=now, ended_at=None, dismissed_by_user=False, false_alarm=None)

    (
        db.collection("home_events").document(hid)
        .collection("events").document(eid)
        .set(event.model_dump(by_alias=True))
    )
    set_active_event(hid, eid)

    return eid

def update_event(hid: str, eid: str, entries: list[CacheEntry]): 
    if not entries:
        return

    chunk_data = {
        "savedAt": datetime.now(timezone.utc).isoformat(),
        "packages": [
            {
                "timestamp": e.package.timestamp,
                "warning_type": e.package.warning_type,
                "package_movement_pct": e.package_movement_pct,
                "is_alarm": e.is_alarm,
                "nodes": [
                    {
                        "node_id": n.node_id,
                        "state": n.state,
                        "movement_pct": n.movement_pct,
                        "raw_mq2_reading": n.raw_mq2_reading,
                        "is_alarm": n.movement_pct >= MOVEMENT_THRESHOLD,
                        "warnings": {
                            "low_battery": n.warnings.low_battery,
                            "not_transmitting": n.warnings.not_transmitting,
                            "signal_weak": n.warnings.signal_weak,
                        },
                        "sensors": {
                            "flame": n.sensors.flame,
                            "gas": n.sensors.gas,
                            "battery_pct": n.sensors.battery_pct,
                        },
                    }
                    for n in e.package.nodes
                ],
            }
            for e in entries
        ],
    }

    chunk_index = _event_chunk_counters.get(eid, 0)
    cid = f"{chunk_index:05d}"
    _event_chunk_counters[eid] = chunk_index + 1

    (
        db.collection("home_events")
        .document(hid)
        .collection("events")
        .document(eid)
        .collection("chunks")
        .document(cid)
        .set(chunk_data)
    )   
    print(f"[DB] Event {eid}: chunks saved ({len(entries)} packages)")


def close_event(hid: str, eid: str):
    (
        db.collection("home_events").document(hid)
        .collection("events").document(eid)
        .update({"endedAt": datetime.now(timezone.utc)})
    )
    set_active_event(hid, None)
    _event_chunk_counters.pop(eid, None)   

    print(f"[DB] Event closed: {eid}")


def revert_node_requested_armed(hid: str, node_id: str):
    node = get_node(hid, node_id)
    if not node:
        return
    
    db.collection("nodes").document(f"{hid}_{node_id}").update({
        "requestedArmed": node.get("armed", False)
    })

def clear_node_requested_reboot(hid: str, node_id: str):
    db.collection("nodes").document(f"{hid}_{node_id}").update({"requestedReboot": False})

def clear_node_requested_deep_sleep(hid: str, node_id: str):
    db.collection("nodes").document(f"{hid}_{node_id}").update({"requestedDeepSleep": False})

def set_active_event(hid: str, event_id: Optional[str]):
    db.collection("homes").document(hid).update({
        "activeEventId": event_id,
    })

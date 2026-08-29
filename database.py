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


# cache
#constants:
MOVEMENT_THRESHOLD = 140
ALARM_MULTI_COUNT = 2
THREAT_COUNT = 3
MAX_CACHE = 10
IDLE_CLOSE_COUNT = 30 # se ia si dupa flush

# in-memory state
_event_chunk_counters: dict[str, int] = {}
_idle_streaks: dict[str, int] = {}

# helpers
def _node_readings_to_package_reading_and_alarm(pkg: Package) -> tuple[int, bool]:
    active_nodes = [n for n in pkg.nodes if not n.warnings.not_transmitting]
    if not active_nodes:
        return (0, False)

    readings = sorted((n.movement_pct for n in active_nodes), reverse=True)

    package_movement_pct = readings[0] if len(readings) == 1 else (readings[0] + readings[1]) // 2
    package_movement_pct = min(200, max(0, package_movement_pct))
    over_threshold = sum(1 for r in readings if r >= MOVEMENT_THRESHOLD)

    is_alarm = (
        package_movement_pct >= MOVEMENT_THRESHOLD
        or over_threshold >= ALARM_MULTI_COUNT
    )
    return (package_movement_pct, is_alarm)


def _build_cache_entry(pkg: Package, movement_pct: int, is_alarm: bool) -> CacheEntry:
    # pkg.timestamp is already overwritten with server UTC before this is called
    return CacheEntry(
        timestamp=pkg.timestamp,
        warning_type=pkg.warning_type,
        package_movement_pct=movement_pct,
        is_alarm=is_alarm,
        nodes=[
            CacheNodeReadingDoc(
                node_id=node.node_id,
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

def _recompute_alarm_count(entries: list[CacheEntry]) -> int:
    return sum(1 for e in entries if e.is_alarm)

def _update_idle_streak(hid: str, new_entry: CacheEntry) -> int:
    if new_entry.is_alarm or new_entry.warning_type is not None:
        _idle_streaks[hid] = 0
    else:
        _idle_streaks[hid] = _idle_streaks.get(hid, 0) + 1
    return _idle_streaks[hid]


# cache
def _analyse_and_flush_cache(hid: str, entries: list[CacheEntry], idle_streak: int, cache_ref,) -> dict:
    alarm_count = _recompute_alarm_count(entries)
    is_alarm    = alarm_count >= THREAT_COUNT
    should_close = idle_streak >= IDLE_CLOSE_COUNT

    # reset Firestore cache
    cache_ref.set({
        "packages":    [],
        "alarmCount":  0,
        "idleStreak":  idle_streak,   # keep the running streak visible to app
        "isAlarm":     False,
        "nodeReadings": {},
        "updatedAt":   datetime.now(timezone.utc),
    })

    return {
        "flushed":      True,
        "entries":      entries,
        "alarm_count":  alarm_count,
        "idle_streak":  idle_streak,
        "is_alarm":     is_alarm,
        "should_close": should_close,
        "latest_entry": entries[-1],
    }

def append_to_cache(hid: str, pkg: Package) -> dict:
    """
    Tags the incoming package, appends it to the Firestore cache doc
    (so the app always has a live view). Flushes and analyses when full.
    Returns a dict describing current state for handle_package to act on.
    """
    movement_pct, is_alarm = _node_readings_to_package_reading_and_alarm(pkg)
    new_entry = _build_cache_entry(pkg, movement_pct, is_alarm)

    idle_streak = _update_idle_streak(hid, new_entry)

    cache_ref = db.collection("cache").document(hid)
    cache_data = (cache_ref.get().to_dict()) or {}
    raw_entries = cache_data.get("packages", [])
    entries: list[CacheEntry] = [CacheEntry(**e) for e in raw_entries]
    entries.append(new_entry)

    if len(entries) >= MAX_CACHE:
        return _analyse_and_flush_cache(hid, entries, idle_streak, cache_ref)

    alarm_count = _recompute_alarm_count(entries)

    node_readings = {
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
        "packages":    [e.model_dump(by_alias=True) for e in entries],
        "alarmCount":  alarm_count,
        "idleStreak":  idle_streak,
        "isAlarm":     alarm_count >= THREAT_COUNT,
        "nodeReadings": {k: v.model_dump(by_alias=True) for k, v in node_readings.items()},
        "updatedAt":   datetime.now(timezone.utc),
    })

    return {
        "flushed":      False,
        "entries":      entries,
        "alarm_count":  alarm_count,
        "idle_streak":  idle_streak,
        "is_alarm":     alarm_count >= THREAT_COUNT,
        "should_close": idle_streak >= IDLE_CLOSE_COUNT,
        "latest_entry": new_entry,
    }


# home helpers
def set_active_event(hid: str, event_id: Optional[str]):
    db.collection("homes").document(hid).update({
        "activeEventId": event_id,
    })

# events
def start_event(hid: str) -> str:
    eid = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    event = EventDoc(
        hid=hid,
        started_at=now,
        ended_at=None,
        dismissed_by_user=False,
        false_alarm=None,
    )
    (
        db.collection("home_events").document(hid)
        .collection("events").document(eid)
        .set(event.model_dump(by_alias=True))
    )
    set_active_event(hid, eid)
    _idle_streaks[hid] = 0 
    print(f"[DB] Event started: {eid}")

    return eid


def update_event(hid: str, eid: str, entries: list[CacheEntry]): 
    if not entries:
        return

    chunk_data = {
        "savedAt": datetime.now(timezone.utc).isoformat(),
        "packages": [
            {
                "timestamp":          e.timestamp,
                "warning_type":       e.warning_type,
                "package_movement_pct": e.package_movement_pct,
                "is_alarm":           e.is_alarm,
                "nodes": [
                    {
                        "node_id":         n.node_id if hasattr(n, "node_id") else None,
                        "state":           n.state,
                        "movement_pct":    n.movement_pct,
                        "raw_mq2_reading": n.raw_mq2_reading,
                        "is_alarm":        n.is_alarm,
                        "sensors": {
                            "flame":       n.sensors.flame,
                            "gas":         n.sensors.gas,
                            "battery_pct": n.sensors.battery_pct,
                        },
                    }
                    for n in e.nodes
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
    _idle_streaks[hid] = 0

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

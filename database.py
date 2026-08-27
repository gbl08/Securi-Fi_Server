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
)

# setup: 
cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

MAX_CACHE = 10          # how many recent packages to keep per home's sliding window
IDLE_CLOSE_COUNT = 30   # consecutive idle packages needed to auto-close an open session

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
MOVEMENT_THRESHOLD = 140
THREAT_COUNT = 3
_package_windows: dict[str, list[Package]] = {}
_idle_streaks: dict[str, int] = {}

def node_readings_to_package_reading_and_alarm(pkg: Package) -> tuple[int, bool]: # TODO DE RESCRIS LOGICA
    # de modificat logica, deocamndata daca 2 node readings sunt peste THRESHOLD este considerat warning, nush ce formula sa bagam ca sa scoatem din 4 movement_pct 1

    over_THRESHOLD = sum(
        1
        for node in pkg.nodes
        if node.movement_pct >= MOVEMENT_THRESHOLD
        and not node.warnings.not_transmitting)

    is_alarm = over_THRESHOLD > 1
    new_movement_pct = 0 # idfk

    return (new_movement_pct, is_alarm)


def write_cache(hid: str, package: Package, window: list[Package]):
    package_movement_pct, is_alarm = node_readings_to_package_reading_and_alarm(package) 

    cache = CacheDoc(
        is_alarm=is_alarm,
        window_size=len(window),
        node_readings={
            node.node_id: CacheNodeReadingDoc(
                state=node.state,
                movement_pct=node.movement_pct,
                raw_mq2_reading=node.raw_mq2_reading,
                is_alarm=node.movement_pct >= MOVEMENT_THRESHOLD,
                sensors=node.sensors
            )
            for node in package.nodes
        },
        updated_at=datetime.now(timezone.utc),
    )
    db.collection("cache").document(hid).set(cache.model_dump(by_alias=True))

    return is_alarm


def analyse_cache(hid: str, package: Package) -> dict:
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

    is_threat = alarm_count >= THREAT_COUNT

    idle_streak = 0
    for p in reversed(window):
        _, is_alarm = node_readings_to_package_reading_and_alarm(p) # TODO IN LOC SA RECALCULAM LA FIECARE ITERATIE SA CONTINA P O VARIABILA IS_ARMED PE CARE O SETAM SI REFOLOSIM
        if not is_alarm and p.warning_type is None:
            idle_streak += 1
        else:
            break

    return {
        "is_threat": alarm_count >= 3,    
        "above_threshold": alarm_count,
        "should_close_session": idle_streak >= IDLE_CLOSE_COUNT,
        "window_size": len(window),
        "window": window,

        "flushed": flushed,
        "flushed_chunk": flushed_chunk
    }


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
    db.collection("events").document(eid).set(event.model_dump(by_alias=True))
    set_active_event(hid, eid)
    print(f"[DB] Event started: {eid} for home {hid}")

    return eid

def update_event(eid: str, chunk: list[Package]):
    if not chunk:
        return

    chunk_data = {
        "savedAt": datetime.now(timezone.utc).isoformat(),
        "packages": [
            {
                "timestamp":   p.timestamp,
                "warning_type": p.warning_type,
                "nodes": [
                    {
                        "node_id":         n.node_id,
                        "state":           n.state,
                        "movement_pct":    n.movement_pct,
                        "raw_mq2_reading": n.raw_mq2_reading,
                        "is_warning":      n.movement_pct >= MOVEMENT_THRESHOLD,
                        "warnings": {
                            "low_battery":      n.warnings.low_battery,
                            "not_transmitting": n.warnings.not_transmitting,
                            "signal_weak":      n.warnings.signal_weak,
                        },
                        "sensors": {
                            "flame":       n.sensors.flame,
                            "gas":         n.sensors.gas,
                            "battery_pct": n.sensors.battery_pct,
                        },
                    }
                    for n in p.nodes
                ],
            }
            for p in chunk
        ],
    }

    # TODO de dat add in db corect


def close_event(hid: str, eid: str):
    db.collection("events").document(eid).update({
        "endedAt": datetime.now(timezone.utc)
    })
    set_active_event(hid, None)
    print(f"[DB] Event closed: {eid}")
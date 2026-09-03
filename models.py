from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List
from datetime import datetime


# ============================================================
# ESP -> SERVER TELEMETRY MODELS
# ============================================================

class NodeWarning(BaseModel):
    low_battery: bool = False
    not_transmitting: bool = False
    signal_weak: bool = False


class NodeSensors(BaseModel):
    fire: bool = False
    gas: bool = False
    battery_pct: Optional[int] = None


class NodeReading(BaseModel):
    node_id: str
    role: str
    state: str
    armed: Optional[bool] = None

    movement_pct: int
    raw_mq2_reading: int

    warnings: NodeWarning
    sensors: NodeSensors


class Package(BaseModel):
    """
    Raw package received from the ESP/master.

    timestamp is replaced by the server with the server receive
    timestamp before the package is processed.
    """

    master_mac: str
    timestamp: str

    warning_type: Optional[str] = None
    nodes: List[NodeReading]


# ============================================================
# CONFIG PROTOCOL MODELS
# ============================================================

class NodeConfigRequest(BaseModel):
    node_id: str
    master_mac: str

    role: str  # "master" | "slave"


class NodeConfigCommand(BaseModel):
    node_id: str

    cmd: Optional[str] = None
    # "arm"
    # "disarm"
    # "reboot"
    # "deep_sleep"
    # "buzzer_on_alarm"
    # "buzzer_on_warning"
    # "buzzer_off"


class NodeConfigConfirmation(BaseModel):
    node_id: str
    master_mac: str

    cmd: Optional[str] = None

    success: bool


# ============================================================
# FIRESTORE: HOMES
# ============================================================

class HomeDoc(BaseModel):
    master_mac: str = Field(alias="masterMac")

    active_event_id: Optional[str] = Field(
        default=None,
        alias="activeEventId"
    )

    requested_cache: bool = Field(
        default=False,
        alias="requestedCache"
    )

    last_seen: datetime = Field(alias="lastSeen")
    registered_at: datetime = Field(alias="registeredAt")

    model_config = ConfigDict(populate_by_name=True)


# ============================================================
# FIRESTORE: NODES
# ============================================================

class NodeDoc(BaseModel):
    hid: str

    node_id: str = Field(alias="nodeId")
    nickname: str
    role: str

    # Current node telemetry
    battery_pct: Optional[int] = Field(
        default=None,
        alias="batteryPct"
    )

    report_type: Optional[str] = Field(
        default=None,
        alias="reportType"
    )

    sensor_reading: int = Field(
        default=0,
        alias="sensorReading"
    )

    movement_pct: int = Field(
        default=0,
        alias="movementPct"
    )

    warning_type: Optional[str] = Field(
        default=None,
        alias="warningType"
    )

    # Configuration state
    armed: bool = False

    requested_armed: bool = Field(
        default=False,
        alias="requestedArmed"
    )

    model_config = ConfigDict(populate_by_name=True)


# ============================================================
# FIRESTORE: LIVE PACKAGE / CACHE
# ============================================================

class CacheNodeReadingDoc(BaseModel):
    battery_pct: Optional[int] = Field(
        default=None,
        alias="batteryPct"
    )

    report_type: Optional[str] = Field(
        default=None,
        alias="reportType"
    )

    sensor_reading: int = Field(
        alias="sensorReading"
    )

    movement_pct: int = Field(
        alias="movementPct"
    )

    warning_type: Optional[str] = Field(
        default=None,
        alias="warningType"
    )

    model_config = ConfigDict(populate_by_name=True)


class CacheEntry(BaseModel):
    """
    One package in the in-memory rolling cache.

    This is also the representation written to cache/{hid}
    when the app explicitly requests a cache snapshot.
    """

    package_pct: int = Field(alias="packagePct")
    is_alarm: bool = Field(alias="isAlarm")
    timestamp: datetime

    nodes: dict[str, CacheNodeReadingDoc]

    model_config = ConfigDict(populate_by_name=True)


# ============================================================
# FIRESTORE: EVENTS
# ============================================================

class EventDoc(BaseModel):
    hid: str

    event_type: str = Field(alias="eventType")

    started_at: datetime = Field(alias="startedAt")

    ended_at: Optional[datetime] = Field(
        default=None,
        alias="endedAt"
    )

    false_alarm: bool = Field(
        default=False,
        alias="falseAlarm"
    )

    false_alarm_description: Optional[str] = Field(
        default=None,
        alias="falseAlarmDescription"
    )

    model_config = ConfigDict(populate_by_name=True)
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List
from datetime import datetime

# ============================================================
# Incoming hardware payload models — validate raw MQTT packages
# exactly as the ESP32 master sends them (snake_case, matches
# the wire format 1:1 — no renaming here).
# ============================================================

class NodeWarning(BaseModel):
    low_battery: bool = False
    not_transmitting: bool = False
    signal_weak: bool = False


class NodeSensors(BaseModel):
    flame: bool = False
    water: bool = False
    gas: bool = False


class NodeReading(BaseModel):
    node_id: str
    role: str
    state: str
    movement_pct: int
    probability: float = Field(ge=0.0, le=1.0)
    raw_mq2_reading: int  # was missing in the original model — see note below

    warnings: NodeWarning
    sensors: NodeSensors


class Package(BaseModel):
    master_mac: str
    timestamp: str

    armed: bool
    intruder_probability: float = Field(ge=0.0, le=1.0)

    warning_type: Optional[str] = None
    nodes: List[NodeReading]


# ============================================================
# Firestore document models — what actually gets written to the
# database, per the agreed schema. Field names are camelCase to
# match the mobile app's existing Firestore conventions; the
# `alias` on each field handles that translation automatically.
#
# Usage: build with snake_case kwargs as usual, then call
# `.model_dump(by_alias=True)` right before writing to Firestore
# to get camelCase keys out.
# ============================================================

class HomeDoc(BaseModel):
    master_mac: str = Field(alias="masterMac")
    requested_armed: bool = Field(alias="requestedArmed")
    active_event_id: Optional[str] = Field(default=None, alias="activeEventId")
    last_seen: datetime = Field(alias="lastSeen")
    registered_at: datetime = Field(alias="registeredAt")

    model_config = ConfigDict(populate_by_name=True)


class NodeWarningDoc(BaseModel):
    low_battery: bool = Field(alias="lowBattery")
    not_transmitting: bool = Field(alias="notTransmitting")
    signal_weak: bool = Field(alias="signalWeak")

    model_config = ConfigDict(populate_by_name=True)


class NodeDoc(BaseModel):
    hid: str
    node_id: str = Field(alias="nodeId")
    nickname: str
    role: str  # "master" | "slave"
    warnings: NodeWarningDoc
    armed: bool

    model_config = ConfigDict(populate_by_name=True)


class CacheSensorsDoc(BaseModel):
    flame: bool
    gas: bool

    model_config = ConfigDict(populate_by_name=True)


class CacheReadingDoc(BaseModel):
    probability: float = Field(ge=0.0, le=1.0)
    state: str
    sensors: CacheSensorsDoc
    raw_mq2_reading: int = Field(alias="rawMq2Reading")
    movement_pct: int = Field(alias="movementPct")

    model_config = ConfigDict(populate_by_name=True)


class CacheDoc(BaseModel):
    overall_reading: float = Field(alias="overallReading", ge=0.0, le=1.0)
    node_readings: dict[str, CacheReadingDoc] = Field(alias="nodeReadings")
    updated_at: datetime = Field(alias="updatedAt")

    model_config = ConfigDict(populate_by_name=True)


class EventDoc(BaseModel):
    hid: str
    started_at: datetime = Field(alias="startedAt")
    ended_at: Optional[datetime] = Field(default=None, alias="endedAt")
    peak_probability: float = Field(alias="peakProbability", ge=0.0, le=1.0)
    avg_probability: float = Field(alias="avgProbability", ge=0.0, le=1.0)
    dismissed_by_user: bool = Field(default=False, alias="dismissedByUser")
    false_alarm: Optional[str] = Field(default=None, alias="falseAlarm")

    model_config = ConfigDict(populate_by_name=True)
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List
from datetime import datetime
from dataclasses import dataclass


# node & package stuff: 
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

    movement_pct: int # 0 - 200
    raw_mq2_reading: int

    warnings: NodeWarning
    sensors: NodeSensors

class Package(BaseModel):
    master_mac: str
    timestamp: str # overwritten on the server bc claude said so

    warning_type: Optional[str] = None
    nodes: List[NodeReading]



# config protocol models: 
class NodeConfigRequest(BaseModel):
    node_id: str
    master_mac: str

    role: str # "master" | "slave"

    

class NodeConfigCommand(BaseModel):
    node_id: str

    cmd: Optional[str] = None # "arm" | "disarm" | "reboot" | "deep_sleep" | "buzzer_on_alarm" | "buzzer_on_warning" | "buzzer_off" 

class NodeConfigConfirmation(BaseModel):
    node_id: str
    master_mac: str

    cmd: Optional[str] = None # "arm" | "disarm" | "reboot" | "deep_sleep"

    success: bool

# docs: 
class HomeDoc(BaseModel):
    master_mac: str = Field(alias="masterMac")

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

    armed: bool = False
    requested_armed: bool = Field(default=False, alias="requestedArmed")

    model_config = ConfigDict(populate_by_name=True)


# cache
class CacheSensorsDoc(BaseModel):
    fire: bool
    gas: bool
    battery_pct: Optional[int] = Field(default=None, alias="batteryPct")

    model_config = ConfigDict(populate_by_name=True)


class CacheNodeReadingDoc(BaseModel):
    node_id: str = Field(alias="nodeId")
    state: str

    raw_mq2_reading: int = Field(alias="rawMq2Reading")
    movement_pct: int = Field(alias="movementPct")
    is_alarm: bool = Field(alias="isAlarm")

    sensors: CacheSensorsDoc

    model_config = ConfigDict(populate_by_name=True)

class CacheEntry(BaseModel):
    timestamp: str
    warning_type: Optional[str] = Field(default=None, alias="warningType")
    is_alarm: bool = Field(alias="isAlarm")
    package_movement_pct: int = Field(alias="packageMovementPct")
    nodes: List[CacheNodeReadingDoc]

    model_config = ConfigDict(populate_by_name=True)

class CacheDoc(BaseModel):
    packages: List[CacheEntry] = Field(default_factory=list)

    alarm_count: int = Field(default=0, alias="alarmCount")
    idle_streak: int = Field(default=0, alias="idleStreak")
    is_alarm: bool = Field(default=False, alias="isAlarm")   # true daca cache-ul ar trebui sa dea trigger la un event

    node_readings: dict[str, CacheNodeReadingDoc] = Field(alias="nodeReadings")

    updated_at: datetime = Field(alias="updatedAt")

    model_config = ConfigDict(populate_by_name=True)


class EventDoc(BaseModel):
    hid: str
    event_type: str = Field(alias="eventType") # "intrusion" | "fire" | "gas_leak"

    started_at: datetime = Field(alias="startedAt")
    ended_at: Optional[datetime] = Field(default=None, alias="endedAt")
    
    dismissed_by_user: bool = Field(default=False, alias="dismissedByUser")
    false_alarm: Optional[str] = Field(default=None, alias="falseAlarm")

    model_config = ConfigDict(populate_by_name=True)
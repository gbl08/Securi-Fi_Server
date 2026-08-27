from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List
from datetime import datetime


# node & package stuff: 
class NodeWarning(BaseModel):
    low_battery: bool = False
    not_transmitting: bool = False
    signal_weak: bool = False

class NodeSensors(BaseModel):
    flame: bool = False
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
    timestamp: str

    warning_type: Optional[str] = None
    nodes: List[NodeReading]

    package_movement_pct: int # cv formula magica ca sa scoatem un overall movement_pct? 



# config protocol models: 
class NodeConfigRequest(BaseModel):
    node_id: str
    master_mac: str

class NodeConfigCommand(BaseModel):
    node_id: str
    cmd: str

class NodeConfigConfirmation(BaseModel):
    node_id: str
    master_mac: str

    armed: bool
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


class CacheSensorsDoc(BaseModel):
    flame: bool
    gas: bool
    battery_pct: Optional[int] = None

    model_config = ConfigDict(populate_by_name=True)


class CacheNodeReadingDoc(BaseModel):
    state: str

    raw_mq2_reading: int = Field(alias="rawMq2Reading")
    movement_pct: int = Field(alias="movementPct")
    is_warning: bool = Field(alias="isWarning") # inca nush daca asta ramane sau nu

    sensors: CacheSensorsDoc

    model_config = ConfigDict(populate_by_name=True)


class CacheDoc(BaseModel):
    above_treshold: int = Field(alias="aboveTreshold")# asta folosesti drept "probability"
    is_alarm: bool = Field(alias="isAlarm") # daca cache-ul in sine ar trebui sa inceapa un nou event sau nu

    window_size: int = Field(alias="windowSize")

    node_readings: dict[str, CacheNodeReadingDoc] = Field(alias="nodeReadings")
    updated_at: datetime = Field(alias="updatedAt")

    model_config = ConfigDict(populate_by_name=True)


class EventDoc(BaseModel):
    hid: str

    started_at: datetime = Field(alias="startedAt")
    ended_at: Optional[datetime] = Field(default=None, alias="endedAt")
    
    dismissed_by_user: bool = Field(default=False, alias="dismissedByUser")
    false_alarm: Optional[str] = Field(default=None, alias="falseAlarm")

    model_config = ConfigDict(populate_by_name=True)


# in database in events va fi gen:
# hid, startedAt, endedAt, dismissedByUser
# si un subcollection packages/{auto_id} unde sa dea push cache la chunk-uri de package-uri cand e plin


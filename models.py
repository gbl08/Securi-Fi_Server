from pydantic import BaseModel # pentru a da validate la parametrii, da bypass la constructori
from typing import Optional, List # optional ca sa poata fi string numai cand trebuie, List ca sa se asigure ca toate lucrurile din list sunt la fel, gen acelasi data type / class
from datetime import datetime

# node-uri: 
class node_warning(BaseModel):
    low_battery: bool = False
    not_transmitting: bool = False
    signal_weak: bool = False

class node_sensors(BaseModel):
    flame: bool = False
    water: bool = False
    gas: bool = False

class node_reading(BaseModel):
    node_id: str
    role: str
    
    state: str
    movement_pct: int
    probability: float

    warnings: node_warning
    sensors: node_sensors


# package: 
class package(BaseModel):
    master_mac: str
    timestamp: str

    armed: bool
    intruder_probability: float 

    warning_type: Optional[str] = None 
    nodes: List[node_reading]
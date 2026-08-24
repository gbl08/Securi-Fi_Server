from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager

import asyncio
import json
import threading

import paho.mqtt.client as mqtt 
from datetime import datetime, timezone

from models import Package
from database import (
    get_home_by_mac,
    touch_home_last_seen,
    set_armed,
    write_cache,
    analyse_cache,
    upsert_node,
    update_node_warnings,
    start_event,
    update_event,
    close_event,
    get_home,
)
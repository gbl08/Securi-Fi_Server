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

from notifications import notify_home

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "securifi/master"

loop: asyncio.AbstractEventLoop = None
mqtt_client: mqtt.Client = None


# package processing
async def handle_package(raw: dict):
    try:
        pkg = Package(**raw)
    except Exception as e:
        print(f"[SERVER] invalid package: {e}")
        return

    home = get_home_by_mac(pkg.master_mac)
    if not home:
        print(f"[SERVER] Unknown MAC: {pkg.master_mac}")
        return

    hid = home["hid"]

    for node in pkg.nodes:
        upsert_node(hid, node.node_id, node.role)
        update_node_warnings(
            hid= hid,
            node_id= node.node_id,

            low_battery= node.warnings.low_battery,
            not_transmitting= node.warnings.not_transmitting,
            signal_weak= node.warnings.signal_weak
        )
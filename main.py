from fastapi import FastAPI, WebSocket, WebSocketDisconnect # web socket e ca sa luam package-urile din mosquitto de la esp master
from contextlib import asynccontextmanager # intreaba-l pe Neamt daca iti trebuie, e legat de startup si shutdown

import asyncio # dai run la chiestii in "paralel" fara multi threading, foarte smecher daca ma intrebi pe mine :)
import json
import threading

from paho.mqtt.client import mqtt
from datetime import datetime

from server.auxiliary.models import package
from server.auxiliary.database import init_db, set_event, set_armed, set_fcm_token, get_history, get_armed, get_fcm_token
from server.auxiliary.notifications import send_fcm

# Setup:
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
#!/usr/bin/env python3
"""
Latency-measuring subscriber.
Prints "sendTimestampUs,latencyMs" to stdout; node_agent reads it.
Env vars:
  BROKER, TOPIC, QOS  (same defaults as pub.py)
"""
import os, time, sys
import paho.mqtt.client as mqtt

broker = os.getenv("BROKER", "localhost")
if ":" in broker:
    host, port = broker.split(":", 1)
    port = int(port)
else:
    host, port = broker, 1883

topic = os.getenv("TOPIC", "bench/test")
qos   = int(os.getenv("QOS", 0))

def on_message(_cli, _ud, msg):
    try:
        ts_us = int(msg.payload.split(b"x", 1)[0])
        now_us = int(time.time() * 1_000_000)
        lat_ms = (now_us - ts_us) / 1000
        print(f"{ts_us},{lat_ms:.3f}", flush=True)
    except Exception as e:
        # ignore malformed payloads
        pass

client = mqtt.Client()
client.on_message = on_message
client.connect(host, port, keepalive=60)
client.subscribe(topic, qos=qos)
client.loop_forever()

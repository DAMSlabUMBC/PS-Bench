#!/usr/bin/env python3
"""
Latency measuring subscriber.
Prints  "seq,timestampUs,latencyMs"  to stdout; node_agent reads it.

Env vars:
  BROKER, TOPIC, QOS  (same defaults as pub.py)
"""
import os, time, struct
import paho.mqtt.client as mqtt

# ── configuration ──────────────────────────────────────────────────
broker = os.getenv("BROKER", "localhost")
if ":" in broker:
    host, port = broker.split(":", 1)
    port = int(port)
else:
    host, port = broker, 1883

topic = os.getenv("TOPIC", "bench/test")
qos   = int(os.getenv("QOS", 0))

# ── message callback ───────────────────────────────────────────────
def on_message(_cli, _ud, msg):
    try:
        # payload = 4‑byte seq (big‑endian) + 8‑byte ts + pad
        seq, ts_us = struct.unpack("!IQ", msg.payload[:12])
        now_us = int(time.time() * 1_000_000)
        lat_ms = (now_us - ts_us) / 1000
        print(f"{seq},{lat_ms:.3f}", flush=True) 
    except Exception:
        # silently ignore malformed frames
        pass

# ── MQTT setup ─────────────────────────────────────────────────────
client = mqtt.Client()
client.on_message = on_message
client.connect(host, port, keepalive=60)
client.subscribe(topic, qos=qos)
client.loop_forever()

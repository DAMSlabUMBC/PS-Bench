#!/usr/bin/env python3
"""
Simple MQTT publisher for the benchmark.

Env vars (all optional):
  BROKER = localhost[:PORT]
  TOPIC  = bench/test
  RATE   = 500           # messages per second
  SIZE   = 100           # payload bytes (minimum)
  QOS    = 0|1|2
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
rate  = float(os.getenv("RATE", 500))
size  = int(os.getenv("SIZE", 100))
qos   = int(os.getenv("QOS", 0))

# ── MQTT setup ─────────────────────────────────────────────────────
client = mqtt.Client()
client.connect(host, port, keepalive=60)
client.loop_start()

# ── publisher loop ─────────────────────────────────────────────────
sleep_us = 1_000_000 / rate
seq      = 0                    
pad      = b"x" * max(0, size - 12)   

while True:
    ts_us   = int(time.time() * 1_000_000)
    payload = struct.pack("!I", seq) + struct.pack("!Q", ts_us) + pad
    client.publish(topic, payload, qos=qos)
    seq = (seq + 1) & 0xFFFFFFFF
    time.sleep(sleep_us / 1_000_000)

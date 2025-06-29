#!/usr/bin/env python3
"""
Simple MQTT publisher for the benchmark.
Env vars (all optional):
  BROKER = localhost[:PORT]
  TOPIC  = bench/test
  RATE   = 500           # messages per second
  SIZE   = 100           # payload bytes
  QOS    = 0|1|2
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
rate  = float(os.getenv("RATE", 500))
size  = int(os.getenv("SIZE", 100))
qos   = int(os.getenv("QOS", 0))

client = mqtt.Client()
client.connect(host, port, keepalive=60)
client.loop_start()

sleep_us = 1_000_000 / rate
pad      = b"x" * max(0, size - 20)        # leave room for timestamp

while True:
    ts_us = int(time.time() * 1_000_000)
    payload = f"{ts_us}".encode() + pad
    client.publish(topic, payload, qos=qos)
    time.sleep(sleep_us / 1_000_000)

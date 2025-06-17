import os
import certifi
import json
from google.cloud import pubsub_v1
import websocket

os.environ["SSL_CERT_FILE"] = certifi.where()

project_id = "dataflow-poc-462411"
topic_id = "crypto-stream"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

def on_message(ws, message):
    print("Publishing:", message)
    publisher.publish(topic_path, message.encode("utf-8"))

def on_error(ws, error):
    print("WebSocket error:", error)

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed:", close_status_code, close_msg)

def on_open(ws):
    print("WebSocket connection opened.")

ws = websocket.WebSocketApp(
    "wss://stream.binance.com:9443/ws/btcusdt@trade",
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close,
)

ws.run_forever()

import json
from dotenv import load_dotenv
import os
from kafka import KafkaProducer
import websocket

load_dotenv()

rp_username = os.getenv('REDPANDA_USERNAME') 
rp_password = os.getenv('REDPANDA_PASSWORD')
rp_topic = os.getenv('REDPANDA_TOPIC')
rp_bootstrap = os.getenv('REDPANDA_BOOTSTRAP')

producer = KafkaProducer(
    bootstrap_servers=rp_bootstrap,
    security_protocol='SASL_SSL',
    sasl_mechanism='SCRAM-SHA-256',
    sasl_plain_username=rp_username,
    sasl_plain_password=rp_password,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def on_message(ws, message):
    dict_message = json.loads(message)
    print(f"Received message: {dict_message}")
    producer.send(rp_topic, value=dict_message)
    producer.flush()  # Ensure the message is sent immediately

ws_app = websocket.WebSocketApp(
    "wss://stream.binance.com:9443/ws/btcusdt@trade/ethusdt@trade",
    on_message=on_message
)
ws_app.run_forever()

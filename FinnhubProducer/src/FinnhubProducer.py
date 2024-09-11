import os
import time
from confluent_kafka import Producer
import websocket

from utils.common_function import load_schema, serialize_avro
from utils.logger import Logger

import json


class FinnhubProducer:
    def __init__(self, api_key, topic: str, symbols, producer: Producer, schema_path: str):
        self.api_key = api_key
        self.producer = producer
        self.topic = topic
        self.symbols = symbols
        self.avro_schema = load_schema(schema_path)
        self.producer = producer
        self.logger = Logger(self.__class__.__name__)
        websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={self.api_key}",
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close)

    def on_message(self, ws, message):
        serialized_message = serialize_avro(json.loads(message), self.avro_schema)
        self.producer.produce(self.topic, value=serialized_message, callback=self._producer_delivery_report)
        self.producer.poll(0)

    def on_error(self, ws, error):
        self.logger.error(error)

    def on_close(self, ws, close_status_code, close_msg):
        self.producer.flush()
        self.logger.info("### closed ###")


    def on_open(self, ws):
        for symbol in self.symbols:
            self.logger.info(f"Subscribing to {symbol}")
            ws.send(f'{{"type":"subscribe","symbol":"{symbol}"}}')

    def start(self):
        self.ws.on_open = self.on_open
        retry_attempts = 5
        for attempt in range(retry_attempts):
            try:
                self.ws.run_forever()
                break
            except Exception as e:
                self.logger.error(f"Connection failed: {e}. Retrying in 5 seconds...")
                time.sleep(5)
        else:
            self.logger.error("Max retry attempts reached. Exiting.")

    def stop(self):
        self.ws.close()

    def _producer_delivery_report(self, err, msg):
        if err is not None:
            self.logger.error(f"Message delivery failed: {err}")
        else:
            self.logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

if __name__ == "__main__":
    API_KEY = os.getenv("FINNHUB_API_KEY")
    topic = os.getenv("KAFKA_TOPIC")

    symbols = ["AAPL", "AMZN", "BINANCE:BTCUSDT", "IC MARKETS:1"]

    producer_config = {
        'bootstrap.servers': "localhost:9092"
    }
    kafka_producer = Producer(producer_config)
    producer = FinnhubProducer(API_KEY, "stock", symbols, kafka_producer, "schemas/trades.avsc")
    producer.start()




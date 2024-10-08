import argparse
import json
import os

import time

import websocket
from confluent_kafka import Producer

from utils.common_function import load_schema, serialize_avro
from utils.logger import Logger


class FinnhubProducer:
    def __init__(self, api_key, topic: str, symbols, producer: Producer, schema_path: str):
        self.api_key = api_key
        self.producer = producer
        self.topic = topic
        self.symbols = symbols
        self.cumulative_volume = {}
        for symbol in symbols:
            self.cumulative_volume[symbol] = 0
        self.avro_schema = load_schema(schema_path)
        self.producer = producer
        self.logger = Logger(self.__class__.__name__)
        websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={self.api_key}",
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close)

    def on_message(self, ws, message):
        json_message = json.loads(message)
        for trade in json_message["data"]:
            trade["cv"] = self.cumulative_volume[trade["s"]] + trade["v"]
            self.cumulative_volume[trade["s"]] = trade["cv"]
        message = json.dumps(json_message)
        serialized_message = serialize_avro(json.loads(message), self.avro_schema)
        self.producer.produce(self.topic, value=serialized_message, callback=self._producer_delivery_report)
        self.producer.poll(0)

    def on_error(self, ws, error):
        self.logger.error(error)

    def on_close(self, ws, close_status_code, close_msg):
        self.logger.info(f"Connection closed with status code {close_status_code} and message: {close_msg}")
        # self.logger.info("Reconnecting...")
        # self._reconnect(5)


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


    def _producer_delivery_report(self, err, msg):
        if err is not None:
            self.logger.error(f"Message delivery failed: {err}")
        else:
            self.logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def _shutdown_hook(self):
        self.logger.info("Shutting down...")
        try:
            self.producer.poll(1000)
            self.producer.flush()
            self.logger.info("Producer flushed successfully.")
            self.ws.close()
            self.logger.info("Websocket connection closed.")
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")
        self.logger.info("Shutdown complete.")

    def _reconnect(self, retry_attempts):
        self.logger.info("Reconnecting...")
        for attempt in range(retry_attempts):
            try:
                self.ws.run_forever()
                break
            except Exception as e:
                self.logger.error(f"Connection failed: {e}. Retrying in 5 seconds...")
                time.sleep(5)
        else:
            self.logger.error("Max retry attempts reached. Exiting.")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--api_key", type=str, required=True)
    parser.add_argument("--topic", type=str, required=True)
    parser.add_argument("--symbols", type=str, required=True)
    parser.add_argument("--bootstrap-servers", type=str, required=True)
    parser.add_argument("--schema-path", type=str, required=True)

    args = parser.parse_args()

    API_KEY = args.api_key
    topic = args.topic
    symbols = args.symbols.split(",")
    bootstrap_servers = args.bootstrap_servers
    schema_path = args.schema_path
    producer_config = {
        'bootstrap.servers': bootstrap_servers,
    }

    kafka_producer = Producer(producer_config)
    producer = FinnhubProducer(API_KEY, "stock", symbols, kafka_producer, schema_path)
    producer.start()





import json

import pandas as pd
from datetime import datetime as dt

from confluent_kafka import Consumer
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class KafkaConsumer:
    COLUMNS = ["timestamp", "close", "high", "low", "num_trades", "total_btc_volume", "total_usd_volume"]
    def __init__(self, bootstrap_severs: list[str], topic:str, lookback: int=100):
        self.bootstrap_severs = bootstrap_severs
        self.topic = topic
        self.lookback = lookback
        self.logger = logging.getLogger(self.__class__.__name__)
        self._connect()
        self.rows = pd.DataFrame([], columns=self.COLUMNS)


    def _connect(self):
        try:
            self.consumer = Consumer({
                'bootstrap.servers': ",".join(self.bootstrap_severs),
                'group.id': f"btc_prediction_{dt.now().strftime('%Y%m%d%H%M%S')}",
                'auto.offset.reset': 'earliest'
            })
            self.logger.info(f"Connected to Kafka with bootstrap servers: {self.bootstrap_severs}")
            self.consumer.subscribe([self.topic])
        except Exception as e:
            self.logger.error(f"Error connecting to Kafka: {e}")
            raise e

    def _process_message(self, message):
        mess = message.value().decode('utf-8')
        mess = json.loads(mess)
        row = pd.DataFrame([mess], columns=self.COLUMNS)
        self.rows = pd.concat([self.rows, row], axis=0)

    def get_messages(self):
        while True:
            message = self.consumer.poll(timeout=1.0)
            if message is None:
                continue
            if message.error():
                self.logger.error(f"Error: {message.error()}")
                continue
            self._process_message(message)
            if len(self.rows) < self.lookback:
                continue
            if len(self.rows) == self.lookback:
                return self.rows
            if len(self.rows) > self.lookback:
                self.rows = self.rows[1:]
                return self.rows


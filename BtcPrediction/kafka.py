import time
from datetime import datetime as dt

from confluent_kafka import Consumer
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class KafkaConsumer:
    def __init__(self, bootstrap_severs: list[str], topic:str, lookback: int=100):
        self.bootstrap_severs = bootstrap_severs
        self.topic = topic
        self.lookback = lookback
        self.logger = logging.getLogger(self.__class__.__name__)
        self._connect()
        self.messages = []


    def _connect(self):
        try:
            self.consumer = Consumer({
                'bootstrap.servers': ",".join(self.bootstrap_severs),
                'group.id': f"btc_prediction_{dt.now().strftime('%Y%m%d%H%M%S')}",
                'auto.offset.reset': 'latest'
            })
            self.logger.info(f"Connected to Kafka with bootstrap servers: {self.bootstrap_severs}")
            self.consumer.subscribe([self.topic])
        except Exception as e:
            self.logger.error(f"Error connecting to Kafka: {e}")
            raise e

    def _process_message(self, message):
        self.queue.append(message.value().decode('utf-8'))


    def get_messages(self):
        while True:
            message = self.consumer.poll(timeout=1.0)
            if message is None:
                continue
            if message.error():
                self.logger.error(f"Error: {message.error()}")
                continue
            self._process_message(message)
            if len(self.messages) < self.lookback:
                continue
            if len(self.messages) == self.lookback:
                return self.messages
            if len(self.messages) > self.lookback:
                self.messages = self.messages[1:]
                return self.messages


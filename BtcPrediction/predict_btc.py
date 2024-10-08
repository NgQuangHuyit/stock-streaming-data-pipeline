import time

import numpy as np
from keras._tf_keras.keras.models import load_model
import joblib
from pathlib import Path
from datetime import datetime as dt, timedelta

import pandas as pd

from kafka import KafkaConsumer
from cassandra_client import CassandraClient

class BTCPredictor:
    def __init__(self, model_path: str, x_scaler_path: str, y_scaler_path: str):
        self.modal_path = model_path
        self.x_scaler_path = x_scaler_path
        self.y_scaler_path = y_scaler_path
        self._load_model()
        self._load_scaler()


    def  _load_model(self):
        if Path.exists(Path(self.modal_path)):
            self.model = load_model(self.modal_path)
        else:
            raise FileNotFoundError(f"File {self.modal_path} not found")

    def _load_scaler(self):
        if Path.exists(Path(self.x_scaler_path)):
            self.x_scaler = joblib.load(self.x_scaler_path)
        else:
            raise FileNotFoundError(f"File {self.x_scaler_path} not found")
        if Path.exists(Path(self.y_scaler_path)):
            self.y_scaler = joblib.load(self.y_scaler_path)

    def predict(self, input_df: pd.DataFrame):
        if input_df.shape[0] < 100:
            raise ValueError("latest_rows must have at least 100 rows")
        scaled_df = self.x_scaler.transform(input_df)
        current_batch = scaled_df[-100:].reshape(1, 100, 6)
        predicted = self.model.predict(current_batch)
        predicted = self.y_scaler.inverse_transform(predicted)
        return predicted

    def scale_input(self, input_df: pd.DataFrame):
        return self.x_scaler.transform(input_df)

    def scale_output(self, output_df: pd.DataFrame):
        return self.y_scaler.inverse_transform(output_df)

    def predict_next_n_intervals(self, input_df: pd.DataFrame, n: int):
        scaled_df = self.x_scaler.transform(input_df)
        current_batch = scaled_df[-100:].reshape(1, 100, 6)
        predictions = []
        for i in range(n):
            predicted = self.model.predict(current_batch)
            predictions.append(predicted)
            current_batch = current_batch[1:]
            current_batch = np.append(current_batch, predicted)
        predictions = self.y_scaler.inverse_transform(predictions)
        return predictions

cql_update_previous ="""
UPDATE stock_market.btc_predict
SET curr_price = ? 
WHERE symbol = ? AND timestamp = ?"""

cql_insert_new = """
INSERT INTO stock_market.btc_predict (symbol, timestamp, predict_price)
VALUES (?, ?, ?)"""

if __name__ == "__main__":
    btc_predictor = BTCPredictor('btc_lstm.keras', 'scaler_x.pkl', 'scaler_y.pkl')

    kafka_consumer = KafkaConsumer(['localhost:9092'], 'btc_features', 100)
    db_client = CassandraClient(['localhost'], 'cassandra', 'password123')
    with db_client.get_session('stock_market') as session:
        update_stmt = session.prepare(cql_update_previous)
        insert_stmt = session.prepare(cql_insert_new)
        while True:
            messages = kafka_consumer.get_messages()
            last_ts = dt.strptime(messages['timestamp'].iloc[-1], "%Y-%m-%dT%H:%M:%S.%fZ")
            next_ts = last_ts + timedelta(seconds=10)
            print(f"Last timestamp: {last_ts}. Next timestamp: {next_ts}")
            messages.drop(columns=['timestamp'], inplace=True)
            predicted = btc_predictor.predict(messages)
            curr_price = messages['close'].iloc[-1]
            print(f"predicted price at {next_ts}: ", predicted)
            session.execute(insert_stmt, ('BTC', next_ts, predicted))
            session.execute(update_stmt, (curr_price, 'BTC', last_ts))
            time.sleep(1)



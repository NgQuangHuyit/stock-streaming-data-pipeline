import numpy as np
from keras._tf_keras.keras.models import load_model
import joblib
from pathlib import Path

import pandas as pd
from datetime import datetime as dt
from cassandra_client import CassandraClient


class BTCPredictor:
    def __init__(self, model_path: str, x_scaler_path: str, y_scaler_path: str):
        self.modal_path = model_path
        self.x_scaler_path = x_scaler_path
        self.y_scaler_path = y_scaler_path
        self._load_model()
        self._load_model()


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
        scaled_df = self.x_scaler.transform(latest_rows)
        current_batch = scaled_df[-100:].reshape(1, 100, 6)
        predicted = self.model.predict(current_batch)
        predicted = self.y_scaler.inverse_transform(predicted)
        return predicted

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


if __name__ == "__main__":
    client = CassandraClient(['localhost'], 'cassandra', 'password123')
    btc_predictor = BTCPredictor('btc_lstm.keras', 'scaler_x.pkl', 'scaler_y.pkl')

    with client.get_session('stock_market') as session:
        try:
            latest_rows = session.execute("SELECT close, high, low, num_trades, total_btc_volume, total_usd_volume FROM btc_aggregate LIMIT 100")
            # predicted = model.predict(latest_rows)
            df = pd.DataFrame(latest_rows.all())
            # print(df.shape)
            # scaled_df = x_scaler.transform(df)
            # current_batch = scaled_df[-100:].reshape(1, 100, 6)
            # predicted = model.predict(current_batch)
            # predicted = y_scaler.inverse_transform(predicted)
            # print(predicted)

        except Exception as e:
            print(f"Error: {e}")



import numpy as np
from cassandra.cluster import Session
from keras._tf_keras.keras.models import load_model
import joblib
from pathlib import Path

import pandas as pd
from datetime import datetime as dt
from cassandra_client import CassandraClient

def preprocess_data(data):

    return data

def load_scaler(path: str):
    if Path.exists(Path(path)):
        return joblib.load(path)
    else:
        raise FileNotFoundError(f"File {path} not found")



if __name__ == "__main__":
    client = CassandraClient(['localhost'], 'cassandra', 'password123')
    model = load_model('btc_lstm.keras')
    x_scaler = load_scaler('scaler_x.pkl')
    y_scaler = load_scaler('scaler_y.pkl')
    with client.get_session('stock_market') as session:
        try:
            latest_rows = session.execute("SELECT close, high, low, num_trades, total_btc_volume, total_usd_volume FROM btc_aggregate LIMIT 100")
            # predicted = model.predict(latest_rows)
            df = pd.DataFrame(latest_rows.all())
            print(df.shape)
            scaled_df = x_scaler.transform(df)
            current_batch = scaled_df[-100:].reshape(1, 100, 6)
            predicted = model.predict(current_batch)
            predicted = y_scaler.inverse_transform(predicted)
            print(predicted)
            session.excute("INSERT into")
        except Exception as e:
            print(f"Error: {e}")



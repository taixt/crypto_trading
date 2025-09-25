import csv
import os
from datetime import datetime, timezone

class DataLogger:
    def __init__(self, folder="data"):
        self.folder = folder
        os.makedirs(folder, exist_ok=True)
        self.files = {}

    def _get_file(self, data_type):
        if data_type not in self.files:
            path = os.path.join(self.folder, f"{data_type}.csv")
            exists = os.path.isfile(path)
            f = open(path, mode="a", newline="")
            writer = csv.writer(f)
            if not exists:
                # Write headers based on data type
                if data_type == "trades":
                    writer.writerow(["timestamp_utc","exchange","symbol","price","amount"])
                elif data_type == "orderbook":
                    writer.writerow(["timestamp_utc","exchange","symbol","bid","bid_size","ask","ask_size"])
                elif data_type == "ohlcv":
                    writer.writerow(["timestamp_utc","exchange","symbol","open","high","low","close","volume"])
                elif data_type == "tickers":
                    writer.writerow(["timestamp_utc","exchange","symbol","price","high_24h","low_24h","volume_24h"])
            self.files[data_type] = (f, writer)
        return self.files[data_type]

    def log(self, data_type, row):
        _, writer = self._get_file(data_type)
        writer.writerow(row)

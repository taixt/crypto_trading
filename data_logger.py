import os
import csv
import pandas as pd

class DataLogger:
    """
    Logs market data (trades, order books, tickers) to CSV files.
    CSV filename pattern: <category>_<exchange>_<symbol>.csv
    """
    def __init__(self, data_dir="data"):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)

    def _get_file_path(self, category, exchange, symbol):
        return os.path.join(self.data_dir, f"{category}_{exchange}_{symbol}.csv")

    def log(self, category, exchange, symbol, data, headers=None):
        """
        Logs a single row of data to CSV.
        :param category: trades/orderbook/tickers
        :param exchange: binance/bitfinex
        :param symbol: trading pair like btcusdt or tBTCUSD
        :param data: list of values
        :param headers: list of column names if file is new
        """
        file_path = self._get_file_path(category, exchange, symbol)
        file_exists = os.path.isfile(file_path)

        if not file_exists and headers:
            with open(file_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(headers)

        with open(file_path, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(data)

    def read_csv(self, category, exchange, symbol):
        file_path = self._get_file_path(category, exchange, symbol)
        if os.path.exists(file_path):
            return pd.read_csv(file_path, parse_dates=["timestamp"])
        else:
            return pd.DataFrame()

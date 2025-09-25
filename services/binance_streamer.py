import json
import websocket
import threading
import pandas as pd
from collections import deque
from datetime import datetime, timedelta, timezone

class BinanceStreamer:
    def __init__(self, symbol="btcusdt", window_seconds=120, ma_period=20, logger=None):
        self.symbol = symbol.lower()
        self.ws_url = f"wss://stream.binance.com:9443/ws/{self.symbol}@trade"
        self.timestamps = deque()
        self.prices = deque()
        self.volumes = deque()
        self.ma_period = ma_period
        self.window_seconds = window_seconds
        self.logger = logger
        self.running = False

    def on_message(self, ws, message):
        data = json.loads(message)
        price = float(data["p"])
        amount = float(data["q"])
        ts = pd.to_datetime(data["T"], unit="ms", utc=True)

        self.timestamps.append(ts)
        self.prices.append(price)
        self.volumes.append(amount)

        # Rolling window removal
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=self.window_seconds)
        while self.timestamps and self.timestamps[0].to_pydatetime() < cutoff:
            self.timestamps.popleft()
            self.prices.popleft()
            self.volumes.popleft()

        # Log to CSV
        if self.logger:
            self.logger.log("trades", [ts.isoformat(), "Binance", self.symbol.upper(), price, amount])

    def start_websocket(self):
        ws = websocket.WebSocketApp(
            self.ws_url,
            on_message=self.on_message,
            on_open=lambda ws: print(f"Connected to Binance {self.symbol.upper()} WebSocket"),
            on_error=lambda ws, err: print(f"Binance WS error: {err}"),
            on_close=lambda ws, code, msg: print("Binance WS closed")
        )
        threading.Thread(target=ws.run_forever, daemon=True).start()

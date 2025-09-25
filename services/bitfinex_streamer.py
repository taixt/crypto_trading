import json
import websocket
import threading
import pandas as pd
from collections import deque
from datetime import datetime, timedelta, timezone

class BitfinexStreamer:
    def __init__(self, symbol="tBTCUSD", window_seconds=120, logger=None):
        self.symbol = symbol
        self.ws_url = "wss://api-pub.bitfinex.com/ws/2"
        self.timestamps = deque()
        self.prices = deque()
        self.volumes = deque()
        self.window_seconds = window_seconds
        self.logger = logger
        self.running = False

    def on_message(self, ws, message):
        msg = json.loads(message)
        if isinstance(msg, dict):  # ignore events like 'subscribed'
            return
        if len(msg) < 2:
            return

        data = msg[1]
        if isinstance(data, list) and len(data) == 4:
            trade_id, ts_ms, amount, price = data
            ts = pd.to_datetime(ts_ms, unit="ms", utc=True)
            self.timestamps.append(ts)
            self.prices.append(price)
            self.volumes.append(amount)

            cutoff = datetime.now(timezone.utc) - timedelta(seconds=self.window_seconds)
            while self.timestamps and self.timestamps[0].to_pydatetime() < cutoff:
                self.timestamps.popleft()
                self.prices.popleft()
                self.volumes.popleft()

            if self.logger:
                self.logger.log("trades", [ts.isoformat(), "Bitfinex", self.symbol, price, amount])

    def on_open(self, ws):
        print(f"Connected to Bitfinex {self.symbol} WebSocket")
        ws.send(json.dumps({"event": "subscribe", "channel": "trades", "symbol": self.symbol}))

    def start_websocket(self):
        ws = websocket.WebSocketApp(
            self.ws_url,
            on_message=self.on_message,
            on_open=self.on_open,
            on_error=lambda ws, err: print(f"Bitfinex WS error: {err}"),
            on_close=lambda ws, code, msg: print("Bitfinex WS closed")
        )
        threading.Thread(target=ws.run_forever, daemon=True).start()

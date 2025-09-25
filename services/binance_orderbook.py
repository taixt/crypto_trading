import json
import websocket
import threading
from collections import deque
from datetime import datetime, timezone

class BinanceOrderBookStreamer:
    def __init__(self, symbol="btcusdt", top_n=5, logger=None):
        self.symbol = symbol.lower()
        self.top_n = top_n
        self.ws_url = f"wss://stream.binance.com:9443/ws/{self.symbol}@depth20@100ms"
        self.logger = logger
        self.bids = deque(maxlen=top_n)
        self.asks = deque(maxlen=top_n)

    def on_message(self, ws, message):
        data = json.loads(message)
        self.bids = sorted([[float(p), float(q)] for p, q in data["b"]], key=lambda x: -x[0])[:self.top_n]
        self.asks = sorted([[float(p), float(q)] for p, q in data["a"]], key=lambda x: x[0])[:self.top_n]

        timestamp = datetime.now(timezone.utc)
        if self.logger:
            for i in range(self.top_n):
                bid, bid_size = self.bids[i] if i < len(self.bids) else (0, 0)
                ask, ask_size = self.asks[i] if i < len(self.asks) else (0, 0)
                self.logger.log("orderbook", [timestamp.isoformat(), "Binance", self.symbol.upper(), bid, bid_size, ask, ask_size])

    def on_open(self, ws):
        print(f"Connected to Binance Order Book for {self.symbol.upper()}")

    def start(self):
        ws = websocket.WebSocketApp(self.ws_url, on_message=self.on_message, on_open=self.on_open)
        threading.Thread(target=ws.run_forever, daemon=True).start()

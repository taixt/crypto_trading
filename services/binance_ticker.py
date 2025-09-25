import json
import websocket
import threading
from datetime import datetime, timezone

class BinanceTickerStreamer:
    """
    Streams live Binance ticker data for a symbol and logs it.
    """
    def __init__(self, symbol="btcusdt", logger=None):
        self.symbol = symbol.lower()
        self.ws_url = f"wss://stream.binance.com:9443/ws/{self.symbol}@ticker"
        self.logger = logger
        self.price = 0
        self.high_24h = 0
        self.low_24h = 0
        self.volume_24h = 0

    def on_message(self, ws, message):
        data = json.loads(message)
        timestamp = datetime.now(timezone.utc)

        self.price = float(data.get("c", 0))
        self.high_24h = float(data.get("h", 0))
        self.low_24h = float(data.get("l", 0))
        self.volume_24h = float(data.get("v", 0))

        # Log ticker data
        if self.logger:
            self.logger.log(
                "tickers",
                [timestamp.isoformat(), "Binance", self.symbol.upper(),
                 self.price, self.high_24h, self.low_24h, self.volume_24h]
            )

    def on_open(self, ws):
        print(f"Connected to Binance ticker for {self.symbol.upper()}")

    def start(self):
        ws = websocket.WebSocketApp(
            self.ws_url,
            on_message=self.on_message,
            on_open=self.on_open,
            on_error=lambda ws, err: print(f"Binance ticker error: {err}"),
            on_close=lambda ws, code, msg: print("Binance ticker closed")
        )
        threading.Thread(target=ws.run_forever, daemon=True).start()

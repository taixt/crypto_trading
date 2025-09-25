import threading
import websocket
import json
import pandas as pd

class BinanceTickerStreamer:
    def __init__(self, symbol, exchange, logger=None):
        self.symbol = symbol.lower()
        self.exchange = exchange
        self.logger = logger
        self.ws = None
        self.base_url = f"wss://stream.binance.com:9443/ws/{self.symbol}@ticker"

    def _on_message(self, ws, message):
        data = json.loads(message)
        timestamp = pd.to_datetime(data['E'], unit='ms')
        price = float(data['c'])
        high = float(data['h'])
        low = float(data['l'])
        volume = float(data['v'])

        if self.logger:
            self.logger.log(
                category="tickers",
                exchange=self.exchange,
                symbol=self.symbol,
                data=[timestamp, price, high, low, volume],
                headers=["timestamp", "price", "high", "low", "volume"]
            )

    def _on_error(self, ws, error):
        print(f"Binance Ticker WebSocket Error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        print("Binance Ticker WebSocket closed")

    def _run(self):
        self.ws = websocket.WebSocketApp(
            self.base_url,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )
        self.ws.run_forever()

    def start(self):
        threading.Thread(target=self._run, daemon=True).start()


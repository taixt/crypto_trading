import threading
import websocket
import json
import pandas as pd

class BinanceOrderBookStreamer:
    def __init__(self, symbol, exchange, top_n=5, logger=None):
        self.symbol = symbol.lower()
        self.exchange = exchange
        self.top_n = top_n
        self.logger = logger
        self.ws = None
        self.base_url = f"wss://stream.binance.com:9443/ws/{self.symbol}@depth"

    def _on_message(self, ws, message):
        data = json.loads(message)
        timestamp = pd.to_datetime(data['E'], unit='ms')
        bids = data.get("b", [])[:self.top_n]
        asks = data.get("a", [])[:self.top_n]

        row = [timestamp]
        for i in range(self.top_n):
            row.append(float(bids[i][0]) if i < len(bids) else 0.0)
            row.append(float(bids[i][1]) if i < len(bids) else 0.0)
            row.append(float(asks[i][0]) if i < len(asks) else 0.0)
            row.append(float(asks[i][1]) if i < len(asks) else 0.0)

        headers = ["timestamp"]
        for i in range(1, self.top_n + 1):
            headers += [f"bid{i}", f"bid{i}_qty", f"ask{i}", f"ask{i}_qty"]

        if self.logger:
            self.logger.log(
                category="orderbook",
                exchange=self.exchange,
                symbol=self.symbol,
                data=row,
                headers=headers
            )

    def _on_error(self, ws, error):
        print(f"Binance OrderBook WebSocket Error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        print("Binance OrderBook WebSocket closed")

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



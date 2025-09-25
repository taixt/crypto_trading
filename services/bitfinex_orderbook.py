import threading
import websocket
import json
import pandas as pd

class BitfinexOrderBookStreamer:
    def __init__(self, symbol, exchange, top_n=5, logger=None):
        self.symbol = symbol
        self.exchange = exchange
        self.top_n = top_n
        self.logger = logger
        self.ws = None
        self.channel_id = None

    def _on_open(self, ws):
        ws.send(json.dumps({
            "event": "subscribe",
            "channel": "book",
            "symbol": self.symbol,
            "prec": "P0",
            "freq": "F0",
            "len": self.top_n
        }))

    def _on_message(self, ws, message):
        data = json.loads(message)

        # Handle subscription confirmation
        if isinstance(data, dict):
            if data.get("event") == "subscribed" and data.get("channel") == "book":
                self.channel_id = data["chanId"]
            return
        
        print(data)

        if isinstance(data, list) and len(data) > 1:
            if data[0] != self.channel_id or data[1] == "hb":
                return
            rows = data[1]  # snapshot or update
            timestamp = pd.Timestamp.now()
            bids = [r for r in rows if r[2] > 0][:self.top_n]
            asks = [r for r in rows if r[2] < 0][:self.top_n]
            row = [timestamp]
            for i in range(self.top_n):
                row.append(float(bids[i][0]) if i < len(bids) else 0.0)
                row.append(abs(float(bids[i][2])) if i < len(bids) else 0.0)
                row.append(float(asks[i][0]) if i < len(asks) else 0.0)
                row.append(abs(float(asks[i][2])) if i < len(asks) else 0.0)
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
        print(f"Bitfinex OrderBook WebSocket Error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        print("Bitfinex OrderBook WebSocket closed")

    def _run(self):
        self.ws = websocket.WebSocketApp(
            "wss://api-pub.bitfinex.com/ws/2",
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )
        self.ws.run_forever()

    def start(self):
        threading.Thread(target=self._run, daemon=True).start()




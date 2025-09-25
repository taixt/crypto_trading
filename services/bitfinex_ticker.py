import threading
import websocket
import json
import pandas as pd

class BitfinexTickerStreamer:
    def __init__(self, symbol, exchange, logger=None):
        self.symbol = symbol
        self.exchange = exchange
        self.logger = logger
        self.ws = None
        self.channel_id = None

    def _on_open(self, ws):
        ws.send(json.dumps({"event":"subscribe","channel":"ticker","symbol":self.symbol}))

    def _on_message(self, ws, message):
        data = json.loads(message)

        # Handle subscription confirmation
        if isinstance(data, dict):
            if data.get("event") == "subscribed" and data.get("channel") == "ticker":
                self.channel_id = data["chanId"]
            return

        if isinstance(data, list) and len(data) > 1:
            if data[0] != self.channel_id or data[1] == 'hb':
                return
            last_price = data[1][6]
            high = data[1][8]
            low = data[1][9]
            volume = data[1][7]
            timestamp = pd.Timestamp.now()
            if self.logger:
                self.logger.log(
                    category="tickers",
                    exchange=self.exchange,
                    symbol=self.symbol,
                    data=[timestamp, last_price, high, low, volume],
                    headers=["timestamp","price","high","low","volume"]
                )

    def _on_error(self, ws, error):
        print(f"Bitfinex Ticker WebSocket Error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        print("Bitfinex Ticker WebSocket closed")

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


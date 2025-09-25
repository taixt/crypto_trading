import threading
import websocket
import json
import pandas as pd

class BitfinexStreamer:
    def __init__(self, symbol, exchange, logger):
        self.symbol = symbol
        self.exchange = exchange
        self.logger = logger
        self.ws = None
        self.base_url = "wss://api-pub.bitfinex.com/ws/2"
        self.channel_id = None

    def _on_open(self, ws):
        ws.send(json.dumps({
            "event": "subscribe",
            "channel": "trades",
            "symbol": self.symbol
        }))

    def _on_message(self, ws, message):
        data = json.loads(message)

        # Handle subscription confirmation
        if isinstance(data, dict):
            if data.get("event") == "subscribed" and data.get("channel") == "trades":
                self.channel_id = data["chanId"]
            return

        # Ignore heartbeat messages
        if isinstance(data, list) and len(data) > 1:
            if data[0] != self.channel_id or data[1] == "hb":
                return
            payload = data
            if isinstance(payload[1], list):  # snapshot
                trades = payload[1]
            elif isinstance(payload[1],str) and payload[1] == 'te':  # single update
                trades = [payload[2]]
            else:
                return
            for trade in trades:
                trade_id, mts, amount, price = trade
                timestamp = pd.to_datetime(mts, unit='ms')
                if self.logger:
                    self.logger.log(
                        category="trades",
                        exchange=self.exchange,
                        symbol=self.symbol,
                        data=[timestamp, price, amount],
                        headers=["timestamp","price","volume"]
                    )

    def _on_error(self, ws, error):
        print(f"Bitfinex Trade WebSocket Error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        print("Bitfinex Trade WebSocket closed")

    def _run(self):
        self.ws = websocket.WebSocketApp(
            self.base_url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )
        self.ws.run_forever()

    def start_websocket(self):
        threading.Thread(target=self._run, daemon=True).start()


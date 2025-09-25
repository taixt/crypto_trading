import json
import websocket
import threading
from datetime import datetime, timezone

class BitfinexTickerStreamer:
    """
    Streams live Bitfinex ticker data for a symbol and logs it.
    """
    def __init__(self, symbol="tBTCUSD", logger=None):
        self.symbol = symbol
        self.ws_url = "wss://api-pub.bitfinex.com/ws/2"
        self.logger = logger
        self.channel_id = None
        self.last_price = 0
        self.high_24h = 0
        self.low_24h = 0
        self.volume_24h = 0

    def on_message(self, ws, message):
        msg = json.loads(message)

        # Handle subscription confirmation
        if isinstance(msg, dict):
            if msg.get("event") == "subscribed" and msg.get("channel") == "ticker":
                self.channel_id = msg["chanId"]
                print(f"Subscribed to Bitfinex ticker for {self.symbol}, channel ID: {self.channel_id}")
            return

        # Ignore unrelated channels
        if msg[0] != self.channel_id:
            return

        # msg[1] contains ticker data
        data = msg[1]
        if len(data) >= 10:
            bid, bid_size, ask, ask_size, daily_change, daily_change_perc, last_price, volume, high, low = data
            timestamp = datetime.now(timezone.utc)
            self.last_price = last_price
            self.high_24h = high
            self.low_24h = low
            self.volume_24h = volume

            if self.logger:
                self.logger.log(
                    "tickers",
                    [timestamp.isoformat(), "Bitfinex", self.symbol,
                     self.last_price, self.high_24h, self.low_24h, self.volume_24h]
                )

    def on_open(self, ws):
        print(f"Connected to Bitfinex ticker for {self.symbol}")
        sub_msg = {
            "event": "subscribe",
            "channel": "ticker",
            "symbol": self.symbol
        }
        ws.send(json.dumps(sub_msg))

    def start(self):
        ws = websocket.WebSocketApp(
            self.ws_url,
            on_message=self.on_message,
            on_open=self.on_open,
            on_error=lambda ws, err: print(f"Bitfinex ticker error: {err}"),
            on_close=lambda ws, code, msg: print("Bitfinex ticker closed")
        )
        threading.Thread(target=ws.run_forever, daemon=True).start()

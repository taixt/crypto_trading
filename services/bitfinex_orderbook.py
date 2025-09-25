import json
import websocket
import threading
from datetime import datetime, timezone

class BitfinexOrderBookStreamer:
    """
    Streams Bitfinex order book data (top N levels) and logs it.
    """
    def __init__(self, symbol="tBTCUSD", top_n=5, logger=None):
        self.symbol = symbol
        self.top_n = top_n
        self.ws_url = "wss://api-pub.bitfinex.com/ws/2"
        self.logger = logger
        self.channel_id = None

        self.bids = []  # list of [price, size]
        self.asks = []

    def on_message(self, ws, message):
        msg = json.loads(message)

        # Subscribe confirmation
        if isinstance(msg, dict):
            if msg.get("event") == "subscribed" and msg.get("channel") == "book":
                self.channel_id = msg["chanId"]
                print(f"Subscribed to Bitfinex {self.symbol} order book, channel ID: {self.channel_id}")
            return

        # Ignore messages from other channels
        if msg[0] != self.channel_id:
            return

        data = msg[1]

        # Snapshot
        if isinstance(data[0], list):
            bids = []
            asks = []
            for entry in data:
                price, count, amount = entry
                if amount > 0:
                    bids.append([price, amount])
                elif amount < 0:
                    asks.append([price, abs(amount)])
            self.bids = sorted(bids, key=lambda x: -x[0])[:self.top_n]
            self.asks = sorted(asks, key=lambda x: x[0])[:self.top_n]
        # Update
        else:
            price, count, amount = data
            if amount > 0:
                # Update bids
                self._update_side(self.bids, price, amount)
            elif amount < 0:
                # Update asks
                self._update_side(self.asks, price, abs(amount))

        # Log top N levels
        if self.logger:
            timestamp = datetime.now(timezone.utc)
            for i in range(self.top_n):
                bid, bid_size = self.bids[i] if i < len(self.bids) else (0, 0)
                ask, ask_size = self.asks[i] if i < len(self.asks) else (0, 0)
                self.logger.log("orderbook", [timestamp.isoformat(), "Bitfinex", self.symbol, bid, bid_size, ask, ask_size])

    def _update_side(self, side, price, size):
        """
        Update bids or asks list with new order book data.
        """
        for i, (p, _) in enumerate(side):
            if p == price:
                if size == 0:
                    side.pop(i)
                else:
                    side[i][1] = size
                return
        if size != 0:
            side.append([price, size])
        side.sort(key=lambda x: -x[0] if side == self.bids else x[0])
        del side[self.top_n:]

    def on_open(self, ws):
        print(f"Connected to Bitfinex {self.symbol} order book")
        # Subscribe to book channel, precision P0 (price level)
        sub_msg = {
            "event": "subscribe",
            "channel": "book",
            "symbol": self.symbol,
            "prec": "P0",  # Price precision
            "freq": "F0",  # Raw updates
            "len": self.top_n
        }
        ws.send(json.dumps(sub_msg))

    def start(self):
        ws = websocket.WebSocketApp(
            self.ws_url,
            on_message=self.on_message,
            on_open=self.on_open,
            on_error=lambda ws, err: print(f"Bitfinex OB error: {err}"),
            on_close=lambda ws, code, msg: print("Bitfinex OB closed")
        )
        threading.Thread(target=ws.run_forever, daemon=True).start()

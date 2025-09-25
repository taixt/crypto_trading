import threading
import websocket
import json
import pandas as pd
from collections import OrderedDict

class BitfinexOrderBookStreamer:
    def __init__(self, symbol, exchange, top_n=5, logger=None):
        self.symbol = symbol
        self.exchange = exchange
        self.top_n = top_n
        self.logger = logger
        self.ws = None
        self.channel_id = None
        self.bids = OrderedDict() 
        self.asks = OrderedDict()
        self.last_snapshot = None

    def _on_open(self, ws):
        ws.send(json.dumps({
            "event": "subscribe",
            "channel": "book",
            "symbol": self.symbol,
            "prec": "P0",
            "freq": "F0",
            "len": '25'
        }))

    def _on_message(self, ws, message):
        data = json.loads(message)

        if isinstance(data, dict):
            if data.get("event") == "subscribed":
                self.channel_id = data["chanId"]
            return

        if isinstance(data, list):
            chan_id = data[0]
            if chan_id != self.channel_id or data[1] == "hb":
                return

            payload = data[1]
            if isinstance(payload[0], list):  # snapshot
                for price, count, amount in payload:
                    self._process_update(price, count, amount)
            else:  # single update
                price, count, amount = payload
                self._process_update(price, count, amount)

            self._maybe_log_top_levels()

    def _process_update(self, price, count, amount):
        if count > 0:
            # Add or update price level
            if amount > 0:
                self.asks[price] = amount
            elif amount < 0:
                self.bids[price] = abs(amount)
        else:
            # Remove price level
            if amount == 1:
                self.asks.pop(price, None)
            elif amount == -1:
                self.bids.pop(price, None)

        # Keep bids sorted descending, asks sorted ascending
        self.bids = OrderedDict(sorted(self.bids.items(), key=lambda x: x[0], reverse=True))
        self.asks = OrderedDict(sorted(self.asks.items(), key=lambda x: x[0]))

    def _maybe_log_top_levels(self):
        # build current snapshot
        top_bids = list(self.bids.items())[:self.top_n]
        top_asks = list(self.asks.items())[:self.top_n]
        current_snapshot = tuple(top_bids + top_asks)

        # compare with last snapshot
        if current_snapshot == self.last_snapshot:
            return  # nothing changed -> skip logging

        self.last_snapshot = current_snapshot  # update last snapshot
        self._log_top_levels(top_bids, top_asks)

    def _log_top_levels(self, top_bids, top_asks):
        timestamp = pd.Timestamp.utcnow()
        row = [timestamp]

        for i in range(self.top_n):
            row.append(top_bids[i][0] if i < len(top_bids) else 0.0)
            row.append(top_bids[i][1] if i < len(top_bids) else 0.0)
            row.append(top_asks[i][0] if i < len(top_asks) else 0.0)
            row.append(top_asks[i][1] if i < len(top_asks) else 0.0)

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




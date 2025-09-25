import json
import pandas as pd
import websocket
import threading
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.animation import FuncAnimation
from collections import deque
import numpy as np
from datetime import datetime, timedelta, timezone

class PriceStreamer:
    def __init__(self, symbol="btcusdt", window_seconds=120, ma_period=20):
        """
        symbol: trading pair (e.g. 'btcusdt')
        window_seconds: how many seconds of data to display in the chart
        ma_period: number of trades for moving average
        """
        self.symbol = symbol.lower()
        self.ws_url = f"wss://stream.binance.com:9443/ws/{self.symbol}@trade"
        self.running = False

        self.timestamps = deque()
        self.prices = deque()
        self.ma_period = ma_period
        self.window_seconds = window_seconds

    def on_message(self, ws, message):
        data = json.loads(message)
        price = float(data["p"])
        # UTC aware timestamp
        ts = pd.to_datetime(data["T"], unit="ms", utc=True)
        self.timestamps.append(ts)
        self.prices.append(price)

        # Rolling time window: remove old trades
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=self.window_seconds)
        while len(self.timestamps) > 0 and self.timestamps[0].to_pydatetime() < cutoff:
            self.timestamps.popleft()
            self.prices.popleft()

        print(f"[{ts}] Price: {price}")

    def on_error(self, ws, error):
        print(f"WebSocket error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        print("WebSocket connection closed")

    def on_open(self, ws):
        print(f"Connected to Binance WebSocket stream for {self.symbol.upper()}")

    def start_websocket(self):
        ws = websocket.WebSocketApp(
            self.ws_url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open
        )
        ws.run_forever()

    def start_plotting(self):
        plt.style.use("ggplot")
        fig, ax = plt.subplots()

        ax.xaxis_date()
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S', tz=timezone.utc))

        price_line, = ax.plot([], [], lw=2, label="Price")
        ma_line, = ax.plot([], [], lw=2, label=f"MA{self.ma_period}", color="orange")

        def update(frame):
            if len(self.timestamps) > 0:
                x_data = mdates.date2num(list(self.timestamps))
                price_line.set_data(x_data, self.prices)

                if len(self.prices) >= self.ma_period:
                    ma_values = pd.Series(self.prices).rolling(self.ma_period).mean().to_numpy()
                    ma_line.set_data(x_data, ma_values)
                else:
                    ma_line.set_data([], [])

                ax.relim()
                ax.autoscale_view()

                # X-axis: last N seconds window
                ax.set_xlim(x_data[0], x_data[-1])

            return price_line, ma_line

        ani = FuncAnimation(fig, update, interval=1000, blit=False)
        plt.title(f"Live Price & MA{self.ma_period}: {self.symbol.upper()} (Last {self.window_seconds}s, UTC)")
        plt.xlabel("Time (UTC)")
        plt.ylabel("Price")
        plt.legend()
        plt.tight_layout()
        plt.show()

    def start(self):
        self.running = True
        ws_thread = threading.Thread(target=self.start_websocket, daemon=True)
        ws_thread.start()

        try:
            self.start_plotting()
        except KeyboardInterrupt:
            print("Stopping live chart...")
            self.running = False
